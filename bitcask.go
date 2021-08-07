package bitcask

import (
	"fmt"
	"github.com/Orlion/bitcask/internal"
	"github.com/Orlion/bitcask/internal/data"
	"github.com/Orlion/bitcask/internal/index"
	"github.com/Orlion/bitcask/internal/metadata"
	art "github.com/plar/go-adaptive-radix-tree"
	"os"
	"path/filepath"
	"sync"
)

const (
	ttlIndexFile = "ttl_index"
)

type Bitcask struct {
	mu         sync.RWMutex
	config     *Config
	curr       data.Datafile
	path       string
	datafiles  map[int]data.Datafile
	metadata   *metadata.MetaData
	trie       art.Tree
	indexer    index.Indexer
	ttlIndex   art.Tree
	ttlIndexer index.Indexer
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
}

func (b *Bitcask) get(key []byte) (internal.Entry, error) {
	// 先从内存的索引树中查找索引
	value, found := b.trie.Search(key)
	if !found {
		return internal.Entry{},
	}
}
