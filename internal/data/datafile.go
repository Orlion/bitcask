package data

import "os"

type Datafile interface {
	FileId() int
	Name() string
	Close() error
	Sync() error // 刷盘
	Size() int64
	Read()
}

func NewDatafile(path string, id int, readOnly bool, maskKeySize uint32, maxValueSize uint64, fileMode os.FileMode) (Datafile, error) {

	return nil, nil
}
