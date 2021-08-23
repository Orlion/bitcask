package bitcask

import (
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/Orlion/bitcask/internal"
	"github.com/Orlion/bitcask/internal/config"
	"github.com/Orlion/bitcask/internal/data"
	"github.com/Orlion/bitcask/internal/data/codec"
	"github.com/Orlion/bitcask/internal/index"
	"github.com/Orlion/bitcask/internal/metadata"
	"github.com/gofrs/flock"
	art "github.com/plar/go-adaptive-radix-tree"
)

const (
	ttlIndexFile = "ttl_index"
	lockfile     = "lock"
)

type Bitcask struct {
	mu         sync.RWMutex
	flock      *flock.Flock // 通过文件对工作目录上锁
	config     *config.Config
	options    []Option
	curr       data.Datafile
	path       string
	datafiles  map[int]data.Datafile
	metadata   *metadata.MetaData
	trie       art.Tree
	indexer    index.Indexer
	ttlIndex   art.Tree
	ttlIndexer index.Indexer
	isMerging  bool
}

func New() *Bitcask {
	b := new(Bitcask)
	return b
}

// 将kv写入到数据库
func (b *Bitcask) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	if b.config.MaxKeySize > 0 && uint32(len(key)) > b.config.MaxKeySize {
		return ErrLargeKey
	}

	if b.config.MaxValueSize > 0 && uint64(len(value)) > b.config.MaxValueSize {
		return ErrLargeValue
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	offset, n, err := b.put(key, value)
	if err != nil {
		return err
	}

	if b.config.Sync {
		if err := b.curr.Sync(); err != nil {
			return err
		}
	}

	b.metadata.IndexUpToDate = false

	if oldItem, found := b.trie.Search(key); found {
		// 如果是覆盖旧值，意味着旧值可以被回收，所以这里修改可回收空间大小
		b.metadata.ReclaimableSpace += oldItem.(internal.Item).Size
	}

	// 写入到内存中的trie索引
	item := internal.Item{FileId: b.curr.FileId(), Offset: offset, Size: n}
	b.trie.Insert(key, item)

	return nil
}

// 写入一对新的kv到磁盘，返回偏移量、占用空间
func (b *Bitcask) put(key, value []byte) (int64, int64, error) {
	// 写之前先根据当前datafile大小判断是否需要写到写文件中，如果需要则创建一个新文件出来，并设置curr为新文件
	if err := b.maybeRotate(); err != nil {
		return -1, 0, fmt.Errorf("error rotating active datafile: %w", err)
	}

	return b.curr.Write(internal.NewEntry(key, value, nil))
}

// 根据当前占用空间，切分新data文件出来
// 如果创建了新data文件则持久化当前索引到磁盘
func (b *Bitcask) maybeRotate() error {
	size := b.curr.Size()
	if size < int64(b.config.MaxDatafileSize) {
		// 不需要写新文件直接返回
		return nil
	}
	// 下面就是创建新文件的流程
	// 关闭当前文件
	err := b.curr.Close()
	if err != nil {
		return err
	}

	id := b.curr.FileId()

	// 将当前文件转为只读文件
	df, err := data.NewDatafile(b.path, id, true, b.config.MaxKeySize, b.config.MaxValueSize,
		b.config.FileFileModeBeforeUmask,
	)
	if err != nil {
		return err
	}

	b.datafiles[id] = df

	id++

	// 创建新可读可写data文件作为当前文件，之后都写入到该文件中
	curr, err := data.NewDatafile(b.path, id, false, b.config.MaxKeySize, b.config.MaxValueSize,
		b.config.FileFileModeBeforeUmask,
	)
	if err != nil {
		return err
	}

	// 替换当前文件
	b.curr = curr

	// 将当前索引保存到磁盘文件中
	// 在创建新文件时将索引持久化到磁盘
	err = b.saveIndexes()
	if err != nil {
		return err
	}

	return nil
}

// 将内存中的index和ttl_index保存到磁盘中
func (b *Bitcask) saveIndexes() error {
	tempIndex := "temp_index"
	if err := b.indexer.Save(b.trie, filepath.Join(b.path, tempIndex)); err != nil {
		return err
	}

	if err := os.Rename(filepath.Join(b.path, tempIndex), filepath.Join(b.path, "index")); err != nil {
		return err
	}

	if err := b.ttlIndexer.Save(b.ttlIndex, filepath.Join(b.path, tempIndex)); err != nil {
		return err
	}

	return os.Rename(filepath.Join(b.path, tempIndex), filepath.Join(b.path, ttlIndexFile))
}

func (b *Bitcask) Get(key []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	e, err := b.get(key)
	if err != nil {
		return nil, err
	}
	return e.Value, nil
}

func (b *Bitcask) get(key []byte) (internal.Entry, error) {
	// 先从内存的索引树中查找索引
	value, found := b.trie.Search(key)
	if !found {
		return internal.Entry{}, ErrKeyNotFound
	}

	if b.isExpired(key) {
		return internal.Entry{}, ErrKeyExpired
	}

	item := value.(internal.Item)
	var df data.Datafile
	if item.FileId == b.curr.FileId() {
		df = b.curr
	} else {
		df = b.datafiles[item.FileId]
	}
	// 根据索引到文件中获取数据
	e, err := df.ReadAt(item.Offset, item.Size)
	if err != nil {
		return internal.Entry{}, err
	}

	// 校验和检查
	checksum := crc32.ChecksumIEEE(e.Value)
	if checksum != e.Checksum {
		return internal.Entry{}, ErrChecksumFailed
	}

	return e, nil
}

func (b *Bitcask) isExpired(key []byte) bool {
	expiry, found := b.ttlIndex.Search(key)
	if !found {
		return false
	}
	return expiry.(time.Time).Before(time.Now().UTC())
}

func (b *Bitcask) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.delete(key)
}

func (b *Bitcask) delete(key []byte) error {
	_, _, err := b.put(key, []byte{})
	if err != nil {
		return err
	}
	if item, found := b.trie.Search(key); found {
		b.metadata.ReclaimableSpace += item.(internal.Item).Size + codec.MetaInfoSize + int64(len(key))
	}
	b.trie.Delete(key)
	b.ttlIndex.Delete(key)
	return nil
}

// 删除所有过期的key
func (b *Bitcask) RunGC() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.runGC()
}

func (b *Bitcask) runGC() error {
	keysToDelete := art.New()

	// 遍历所有带过期时间的key，收集
	// TODO: 为什么先收集完之后再删除，为什么不在遍历过程中直接删除？
	b.ttlIndex.ForEach(func(node art.Node) (cont bool) {
		if !b.isExpired(node.Key()) {
			return true
		}
		keysToDelete.Insert(node.Key(), true)
		return true
	})

	keysToDelete.ForEach(func(node art.Node) (cont bool) {
		b.delete(node.Key())
		return true
	})

	return nil
}

// 合并所有datafile，将旧key和已被删除的key删除
func (b *Bitcask) Merge() error {
	b.mu.Lock()
	if b.isMerging {
		b.mu.Unlock()
		return ErrMergeInProgress
	}

	b.isMerging = true
	b.mu.Unlock()
	defer func() {
		b.isMerging = false
	}()

	b.mu.RLock()
	// 关闭当前文件
	err := b.closeCurrentFile()
	if err != nil {
		b.mu.RUnlock()
		return err
	}
	filesToMerge := make([]int, 0, len(b.datafiles))
	for k := range b.datafiles {
		filesToMerge = append(filesToMerge, k)
	}
	err = b.openNewWritableFile()
	if err != nil {
		b.mu.RUnlock()
		return err
	}
	b.mu.RUnlock()

	sort.Ints(filesToMerge)
	// 创建合并用的临时文件夹
	temp, err := ioutil.TempDir(b.path, "merge")
	if err != nil {
		return err
	}
	// 合并结束后删除临时文件夹及所有内容
	defer os.RemoveAll(temp)

	// 在临时目录中创建一个merge用的db实例
	mdb, err := Open(temp, withConfig(b.config))

	// 遍历所有的键值对写到新db实例中
	err = b.Fold(func(key []byte) error {
		item, _ := b.trie.Search(key)
		if item.(internal.Item).FileId > filesToMerge[len(filesToMerge)-1] {
			// 如果key所在的文件id大于要merge的最后一个文件id则该key不需要参与merge(该文件中所有key不需要参与merge)
			return nil
		}

		e, err := b.get(key)
		if err != nil {
			return err
		}

		if e.Expiry != nil {
			// TODO: PutWithTTL
		} else {
			if err := mdb.Put(key, e.Value); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// 关闭临时实例
	if err = mdb.Close(); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// 关闭当前实例
	if err = b.close(); err != nil {
		return err
	}

	// 删除所有的datafile
	files, err := ioutil.ReadDir(b.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() || file.Name() == lockfile {
			continue
		}

		ids, err := internal.ParseIds([]string{file.Name()})
		if err != nil {
			return err
		}

		if len(ids) > 0 && ids[0] > filesToMerge[len(filesToMerge)-1] {
			continue
		}

		err = os.RemoveAll(path.Join(b.path, file.Name()))
		if err != nil {
			return err
		}
	}

	// 遍历临时工作目录下的所有文件通过rename挪到当前实例的工作目录中
	files, err = ioutil.ReadDir(mdb.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.Name() == lockfile {
			continue
		}

		err := os.Rename(path.Join(mdb.path, file.Name()), path.Join(b.path, file.Name()))
		if err != nil {
			return err
		}
	}

	b.metadata.ReclaimableSpace = 0

	// 重新打开实例
	return b.reopen()
}

func (b *Bitcask) Fold(f func(key []byte) error) (err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.trie.ForEach(func(node art.Node) bool {
		if err = f(node.Key()); err != nil {
			return false
		}

		return true
	})

	return
}

func (b *Bitcask) Close() error {
	b.mu.RLock()
	defer func() {
		b.mu.RUnlock()
		b.flock.Unlock()
	}()

	return b.close()
}

func (b *Bitcask) close() error {
	if err := b.saveIndexes(); err != nil {
		return err
	}

	b.metadata.IndexUpToDate = true
	if err := b.saveMetaData(); err != nil {
		return err
	}

	for _, df := range b.datafiles {
		if err := df.Close(); err != nil {
			return err
		}
	}

	return b.curr.Close()
}

func (b *Bitcask) saveMetaData() error {
	return b.metadata.Save(filepath.Join(b.path, "meta.json"), b.config.DirFileModeBeforeUmask)
}

func (b *Bitcask) closeCurrentFile() error {
	if err := b.curr.Close(); err != nil {
		return err
	}

	id := b.curr.FileId()
	df, err := data.NewDatafile(b.path, id, true, b.config.MaxKeySize, b.config.MaxValueSize, b.config.FileFileModeBeforeUmask)
	if err != nil {
		return err
	}

	b.datafiles[id] = df

	return nil
}

func (b *Bitcask) openNewWritableFile() error {
	id := b.curr.FileId() + 1
	curr, err := data.NewDatafile(b.path, id, false, b.config.MaxKeySize, b.config.MaxValueSize, b.config.FileFileModeBeforeUmask)
	if err != nil {
		return err
	}
	b.curr = curr
	return nil
}

func (b *Bitcask) Reopen() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.reopen()
}

func (b *Bitcask) reopen() error {
	// 从工作目录中加载出datafiles
	datafiles, lastId, err := loadDatafiles(b.path, b.config.MaxKeySize, b.config.MaxValueSize, b.config.FileFileModeBeforeUmask)
	if err != nil {
		return err
	}

	// 从工作目录出加载出索引
	t, ttlIndex, err := loadIndexes(b, datafiles, lastId)
	if err != nil {
		return err
	}

	// 创建当前可读写文件
	curr, err := data.NewDatafile(b.path, lastId, false, b.config.MaxKeySize, b.config.MaxValueSize, b.config.FileFileModeBeforeUmask)
	if err != nil {
		return nil
	}

	b.trie = t
	b.ttlIndex = ttlIndex
	b.curr = curr
	b.datafiles = datafiles

	return nil
}

func Open(path string, options ...Option) (*Bitcask, error) {
	var (
		cfg  *config.Config
		err  error
		meta *metadata.MetaData
	)

	// 加载配置文件
	configPath := filepath.Join(path, "config.json")
	if internal.Exists(configPath) {
		cfg, err = config.Load(path)
		if err != nil {
			return nil, err
		}
	} else {
		cfg = newDefaultConfig()
	}

	// 这里忽略掉版本升级的代码

	//
	for _, opt := range options {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	// 创建出工作目录
	if err := os.MkdirAll(path, cfg.DirFileModeBeforeUmask); err != nil {
		return nil, err
	}

	// 加载metadata
	meta, err = loadMetadata(path)
	if err != nil {
		return nil, err
	}

	bitcask := &Bitcask{
		flock:      flock.New(filepath.Join(path, lockfile)),
		config:     cfg,
		options:    options,
		path:       path,
		indexer:    index.NewIndexer(),
		ttlIndexer: index.NewTTLIndexer(),
		metadata:   meta,
	}

	ok, err := bitcask.flock.TryLock()
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, ErrDatabaseLocked
	}

	// 将配置保存到配置文件
	if err := cfg.Save(configPath); err != nil {
		return nil, err
	}

	if cfg.AutoRecovery {
		// 检查并恢复损坏的数据
		if err := data.CheckAndRecover(path, cfg); err != nil {
			return nil, fmt.Errorf("recovering database: %s", err)
		}
	}

	if err := bitcask.Reopen(); err != nil {
		return nil, err
	}

	return bitcask, nil
}

func loadMetadata(path string) (*metadata.MetaData, error) {
	if !internal.Exists(filepath.Join(path, "meta.json")) {
		meta := new(metadata.MetaData)
		return meta, nil
	}
	return metadata.Load(filepath.Join(path, "meta.json"))
}

// 加载指定目录中的datafile文件
func loadDatafiles(path string, maxKeySize uint32, maxValueSize uint64, fileModeBeforeUmask os.FileMode) (datafiles map[int]data.Datafile, lastId int, err error) {
	fns, err := internal.GetDatafiles(path)
	if err != nil {
		return nil, 0, err
	}

	ids, err := internal.ParseIds(fns)

	datafiles = make(map[int]data.Datafile, len(ids))

	for _, id := range ids {
		datafiles[id], err = data.NewDatafile(path, id, true, maxKeySize, maxValueSize, fileModeBeforeUmask)
		if err != nil {
			return
		}
	}

	if len(ids) > 0 {
		lastId = ids[len(ids)-1]
	}
	return
}

// 加载工作目录中的索引文件到内存中
// 如果没有索引文件则根据datafile重建
func loadIndexes(b *Bitcask, datafiles map[int]data.Datafile, lastId int) (art.Tree, art.Tree, error) {
	// 先尝试加载工作目录中的索引文件
	t, found, err := b.indexer.Load(filepath.Join(b.path, "index"), b.config.MaxKeySize)
	if err != nil {
		return nil, nil, err
	}

	// 尝试加载工作目录中的ttl_index
	ttlIndex, _, err := b.ttlIndexer.Load(filepath.Join(b.path, ttlIndexFile), b.config.MaxKeySize)
	if err != nil {
		return nil, nil, err
	}

	// 如果工作目录中有索引文件，并且在索引文件生成之后没有新数据写入则直接返回
	if found && b.metadata.IndexUpToDate {
		return t, ttlIndex, nil
	}

	if found {
		// 走到这里说明工作目录中有索引文件但是索引文件不是最新的
		// 因此需要从最后一个datafile中加载数据到索引中
		if err := loadIndexFromDatafile(t, ttlIndex, datafiles[lastId]); err != nil {
			return nil, nil, err
		}

		return t, ttlIndex, nil
	}

	// 走到这说明工作目录中没有索引文件
	// 按序从datafile中加载数据到索引中
	sortedDatafiles := getSortedDatafiles(datafiles)
	for _, df := range sortedDatafiles {
		if err := loadIndexFromDatafile(t, ttlIndex, df); err != nil {
			return nil, ttlIndex, err
		}
	}
	return t, ttlIndex, nil
}

// 将datafile中的项加载到索引中
func loadIndexFromDatafile(t art.Tree, ttlIndex art.Tree, df data.Datafile) error {
	var offset int64
	for {
		e, n, err := df.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
		}

		offset += n
		if len(e.Value) == 0 {
			t.Delete(e.Key)
			continue
		}

		item := internal.Item{FileId: df.FileId(), Offset: offset, Size: n}
		t.Insert(e.Key, item)
		if e.Expiry != nil {
			ttlIndex.Insert(e.Key, *e.Expiry)
		}
	}

	return nil
}

func getSortedDatafiles(datafiles map[int]data.Datafile) []data.Datafile {
	out := make([]data.Datafile, len(datafiles))
	idx := 0
	for _, df := range datafiles {
		out[idx] = df
		idx++
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].FileId() < out[j].FileId()
	})

	return out
}
