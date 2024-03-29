package codec

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/Orlion/bitcask/internal"

	"github.com/pkg/errors"
)

var (
	errCantDecodeOnNilEntry  = errors.New("can't decode on nil entry")
	errInvalidKeyOrValueSize = errors.New("key/value size is invalid")
	errTruncatedData         = errors.New("data is truncated")
)

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

func (d *Decoder) Decode(v *internal.Entry) (int64, error) {
	if v == nil {
		return 0, errCantDecodeOnNilEntry
	}

	prefixBuf := make([]byte, keySize+valueSize)
	if _, err := io.ReadFull(d.r, prefixBuf); err != nil {
		return 0, err
	}

	actualKeySize, actualValueSize, err := getKeyValueSizes(prefixBuf, d.maxKeySize, d.maxValueSize)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, uint64(actualKeySize)+actualValueSize+checksumSize+ttlSize)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return 0, err
	}

	decodeWithoutPrefix(buf, actualKeySize, v)
	return int64(keySize + valueSize + uint64(actualKeySize) + actualValueSize + checksumSize + ttlSize), nil
}

func decodeWithoutPrefix(buf []byte, valueOffset uint32, v *internal.Entry) {
	v.Key = buf[:valueOffset]
	v.Value = buf[valueOffset : len(buf)-checksumSize-ttlSize]
	v.Checksum = binary.BigEndian.Uint32(buf[len(buf)-checksumSize-ttlSize : len(buf)-ttlSize])
	v.Expiry = getKeyExpiry(buf)
}

func getKeyExpiry(buf []byte) *time.Time {
	expiry := binary.BigEndian.Uint64(buf[len(buf)-ttlSize:])
	if expiry == uint64(0) {
		return nil
	}
	t := time.Unix(int64(expiry), 0).UTC()
	return &t
}

func getKeyValueSizes(buf []byte, maxKeySize uint32, maxValueSize uint64) (uint32, uint64, error) {
	actualKeySize := binary.BigEndian.Uint32(buf[:keySize])
	actualValueSize := binary.BigEndian.Uint64(buf[keySize:])

	if (maxKeySize > 0 && actualKeySize > maxKeySize) || (maxValueSize > 0 && actualValueSize > maxValueSize) {
		return 0, 0, errInvalidKeyOrValueSize
	}

	return actualKeySize, actualValueSize, nil
}

func DecodeEntry(b []byte, e *internal.Entry, maxKeySize uint32, maxValueSize uint64) error {
	valueOffset, _, err := getKeyValueSizes(b, maxKeySize, maxValueSize)
	if err != nil {
		return errors.Wrap(err, "key/value size are invalid")
	}

	decodeWithoutPrefix(b[keySize+valueSize:], valueOffset, e)

	return nil
}

func IsCorruptedData(err error) bool {
	switch err {
	case errCantDecodeOnNilEntry, errInvalidKeyOrValueSize, errTruncatedData:
		return true
	default:
		return false
	}
}
