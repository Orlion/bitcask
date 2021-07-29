package codec

import "io"

type Decoder struct {
	r            io.Reader
	maxKeySize   uint32
	maxValueSize uint64
}

func NewDecoder(r io.Reader, maxKeySize uint32, maxValueSize uint64) *Decoder {
	return &Decoder{
		r:            r,
		maxKeySize:   maxKeySize,
		maxValueSize: maxValueSize,
	}
}
