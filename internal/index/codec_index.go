package index

import (
	"encoding/binary"
	"github.com/Orlion/bitcask/internal"
	art "github.com/plar/go-adaptive-radix-tree"
	"io"
)

const (
	int32Size  = 4
	int64Size  = 8
	fileIdSize = int32Size
	offsetSize = int64Size
	sizeSize   = int64Size
)

func writeBytes(b []byte, w io.Writer) error {
	s := make([]byte, int32Size)
	binary.BigEndian.PutUint32(s, uint32(len(b)))
	_, err := w.Write(s)
	if err != nil {
		return err
	}

	_, err = w.Write(b)
	if err != nil {
		return err
	}

	return nil
}

func writeItem(item internal.Item, w io.Writer) error {
	buf := make([]byte, fileIdSize+offsetSize+sizeSize)
	binary.BigEndian.PutUint32(buf[:fileIdSize], uint32(item.FileId))
	binary.BigEndian.PutUint64(buf[fileIdSize:fileIdSize+offsetSize], uint64(item.Offset))
	binary.BigEndian.PutUint64(buf[fileIdSize+offsetSize:], uint64(item.Size))
	_, err := w.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func writeIndex(t art.Tree, w io.Writer) (err error) {
	t.ForEach(func(node art.Node) bool {
		err = writeBytes(node.Key(), w)
		if err != nil {
			return false
		}

		item := node.Value().(internal.Item)
		err = writeItem(item, w)
		return err == nil
	})

	return
}
