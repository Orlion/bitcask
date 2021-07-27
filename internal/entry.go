package internal

import (
	"hash/crc32"
	"time"
)

type Entry struct {
	Checksum uint32
	Key      []byte
	Offset   int64
	Value    []byte
	Expiry   *time.Time
}

func NewEntry(key, value []byte, expiry *time.Time) Entry {
	checksum := crc32.ChecksumIEEE(value)

	return Entry{
		Checksum: checksum,
		Key:      key,
		Value:    value,
		Expiry:   expiry,
	}
}
