package index

import (
	"os"

	art "github.com/plar/go-adaptive-radix-tree"
)

type Indexer interface {
	Load(path string, maxKeySize uint32) (art.Tree, bool, error)
	Save(t art.Tree, path string) error
}

func NewIndexer() Indexer {
	return new(indexer)
}

type indexer struct{}

// 从磁盘文件中加载出索引树
func (i *indexer) Load(path string, maxKeySize uint32) (art.Tree, bool, error) {
	// TODO
	return nil, false, nil
}

// 将索引树保存到磁盘文件中
func (i *indexer) Save(t art.Tree, path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	// 将索引树持久化到文件中
	err = writeIndex(t, f)
	if err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return err
	}

	return f.Close()
}
