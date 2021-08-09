package index

import art "github.com/plar/go-adaptive-radix-tree"

type ttlIndexer struct{}

func NewTTLIndexer() Indexer {
	return ttlIndexer{}
}

func (i ttlIndexer) Save(t art.Tree, path string) error {
	return nil
}

func (i ttlIndexer) Load(path string, maxKeySize uint32) (art.Tree, bool, error) {
	return nil, false, nil
}
