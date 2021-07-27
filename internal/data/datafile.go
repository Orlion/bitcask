package data

import (
	"github.com/Orlion/bitcask/internal"
	"os"
)

type Datafile interface {
	FileId() int
	Name() string
	Close() error
	Sync() error // 刷盘
	Size() int64
	Read()
	Write(entry internal.Entry) (int64, int64, error)
}

func NewDatafile(path string, id int, readOnly bool, maskKeySize uint32, maxValueSize uint64, fileMode os.FileMode) (Datafile, error) {

	return nil, nil
}
