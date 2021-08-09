package bitcask

import "errors"

var (
	ErrEmptyKey        = errors.New("empty key")
	ErrLargeKey        = errors.New("large key")
	ErrLargeValue      = errors.New("large value")
	ErrKeyNotFound     = errors.New("error: key not found")
	ErrKeyExpired      = errors.New("error: key expired")
	ErrChecksumFailed  = errors.New("error: checksum failed")
	ErrMergeInProgress = errors.New("error: merge in progress")
	ErrDatabaseLocked  = errors.New("error: database locked")
)
