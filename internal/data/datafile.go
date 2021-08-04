package data

import (
	"fmt"
	"github.com/Orlion/bitcask/internal"
	"github.com/Orlion/bitcask/internal/data/codec"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/exp/mmap"
)

const (
	defaultDatafileFilename = "%09d.data"
)

type Datafile interface {
	FileId() int
	Name() string
	Close() error
	Sync() error // 刷盘
	Size() int64
	Read() (internal.Entry, int64, error)
	ReadAt(index, size int64) (internal.Entry, error)
	Write(entry internal.Entry) (int64, int64, error)
}

type datafile struct {
	sync.RWMutex
	id           int
	r            *os.File
	ra           *mmap.ReaderAt
	w            *os.File
	offset       int64
	dec          *codec.Decoder
	enc          *codec.Encoder
	maxKeySize   uint32
	maxValueSize uint64
}

func NewDatafile(path string, id int, readonly bool, maxKeySize uint32, maxValueSize uint64, fileMode os.FileMode) (Datafile, error) {
	var (
		r   *os.File
		w   *os.File
		ra  *mmap.ReaderAt
		err error
	)

	fn := filepath.Join(path, fmt.Sprintf(defaultDatafileFilename, id))

	if !readonly {
		w, err = os.OpenFile(fn, os.O_WRONLY|os.O_APPEND|os.O_CREATE, fileMode)
		if err != nil {
			return nil, err
		}
	}

	r, err = os.Open(fn)
	if err != nil {
		return nil, err
	}

	stat, err := r.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "error calling Stat()")
	}

	ra, err = mmap.Open(fn)
	if err != nil {
		return nil, err
	}

	offset := stat.Size()

	dec := codec.NewDecoder(r, maxKeySize, maxValueSize)
	enc := codec.NewEncoder(w)

	return &datafile{
		id: id,
		r:  r,
		ra: ra,
	}, nil
}

func (df *datafile) FileId() int {
	return df.id
}

func (df *datafile) Name() string {
	return df.r.Name()
}

func (df *datafile) Close() error {
	defer func() {
		df.ra.Close()
		df.r.Close()
	}()

	if df.w == nil {
		return nil
	}

	err := df.Sync()
	if err != nil {
		return err
	}

	return df.w.Close()
}

func (df *datafile) Sync() error {
	if df.w == nil {
		return nil
	}

	return df.w.Sync()
}

func (df *datafile) Size() int64 {
	df.RLock()
	defer df.RUnlock()
	return df.offset
}

//
func (df *datafile) Read() (e internal.Entry, n int64, err error) {
	df.Lock()
	defer df.Unlock()
	n, err = df.dec.Decode(&e)
	return
}

func (df *datafile) Write(entry internal.Entry) (int64, int64, error) {

}

func (df *datafile) ReadAt(index, size int64) (internal.Entry, error) {

}
