package codec

import "io"

const (
	keySize      = 4
	valueSize    = 8
	checksumSize = 4
	ttlSize      = 8
)

type Encoder struct {
	w io.Writer
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w: w,
	}
}
