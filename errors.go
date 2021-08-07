package bitcask

import "errors"

var (
	ErrEmptyKey    = errors.New("empty key")
	ErrLargeKey    = errors.New("large key")
	ErrLargeValue  = errors.New("large value")
	ErrKeyNotFound = errors.New("error: key not found")
)
