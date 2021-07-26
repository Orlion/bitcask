package bitcask

import (
	"github.com/Orlion/bitcask/internal"
	"github.com/Orlion/bitcask/internal/data"
	"github.com/Orlion/bitcask/internal/metadata"
	art "github.com/plar/go-adaptive-radix-tree"
	"sync"
)

type Bitcask struct {
	mu        sync.RWMutex
	config    *Config
	curr      data.Datafile
	path      string
	datafiles map[int]data.Datafile
	metadata  *metadata.MetaData
	trie      art.Tree
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
	return 0, 0, nil
}

// 根据当前占用空间，切分新data文件出来
func (b *Bitcask) maybeRotate() error {
	size := b.curr.Size()
	if size < int64(b.config.MaxDatafileSize) {
		return nil
	}

	// 关闭当前文件
	err := b.curr.Close()
	if err != nil {
		return err
	}

	id := b.curr.FileId()

	// 创建只读data文件出来
	df, err := data.NewDatafile(b.path, id, true, b.config.MaxKeySize, b.config.MaxValueSize,
		b.config.FileFileModeBeforeUmask,
	)
	if err != nil {
		return err
	}

	b.datafiles[id] = df

	id++
	// 创建新可写data文件
	curr, err := data.NewDatafile(b.path, id, false, b.config.MaxKeySize, b.config.MaxValueSize,
		b.config.FileFileModeBeforeUmask,
	)
	if err != nil {
		return err
	}

	b.curr = curr

	err = b.saveIndexes()
	if err != nil {
		return err
	}

	return nil
}

// 将内存中的index和ttl_index保存到磁盘中
func (b *Bitcask) saveIndexes() error {
	return nil
}
