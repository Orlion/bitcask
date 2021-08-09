package bitcask

import (
	"fmt"
	"github.com/Orlion/bitcask/internal"
	"github.com/Orlion/bitcask/internal/config"
	"github.com/Orlion/bitcask/internal/data"
	"github.com/Orlion/bitcask/internal/data/codec"
	"github.com/Orlion/bitcask/internal/index"
	"github.com/Orlion/bitcask/internal/metadata"
	"github.com/gofrs/flock"
	art "github.com/plar/go-adaptive-radix-tree"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

const (
	ttlIndexFile = "ttl_index"
	lockfile     = "lock"
)

type Bitcask struct {
	mu         sync.RWMutex
	flock      *flock.Flock
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
	// TODO: 为何此时保存索引文件？
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
	err := b.openNewWritableFile()
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

	mdb, err := Open(temp, withConfig(b.config))
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

func Open(path string, options ...Option) (*Bitcask, error) {
	var (
		cfg  *config.Config
		err  error
		meta *metadata.MetaData
	)

	// 加载配置文件
	configPath := filepath.Join(path, "config.json")
	if internal.Exists(configPath) {
		cfg, err := config.Load(path)
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

	if err := os.MkdirAll(path, cfg.DirFileModeBeforeUmask); err != nil {
		return nil, err
	}

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

	}
}

func loadMetadata(path string) (*metadata.MetaData, error) {
	if !internal.Exists(filepath.Join(path, "meta.json")) {
		meta := new(metadata.MetaData)
		return meta, nil
	}
	return metadata.Load(filepath.Join(path, "meta.json"))
}
