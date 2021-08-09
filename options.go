package bitcask

import (
	"github.com/Orlion/bitcask/internal/config"
	"os"
)

const (
	DefaultMaxDatafileSize         = 1 << 20 // 1MB
	DefaultDirFileModeBeforeUmask  = os.FileMode(0700)
	DefaultFileFileModeBeforeUmask = os.FileMode(0600)
	DefaultMaxKeySize              = uint32(64)
	DefaultMaxValueSize            = uint64(1 << 16) // 65K
	DefaultSync                    = false
	CurrentDBVersion               = uint32(1)
)

type Option func(*config.Config) error

func withConfig(src *config.Config) Option {
	return func(conf *config.Config) error {
		conf.MaxDatafileSize = src.MaxDatafileSize
		conf.MaxKeySize = src.MaxKeySize
		conf.MaxValueSize = src.MaxValueSize
		conf.Sync = src.Sync
		conf.DirFileModeBeforeUmask = src.DirFileModeBeforeUmask
		conf.FileFileModeBeforeUmask = src.FileFileModeBeforeUmask
		return nil
	}
}

func newDefaultConfig() *config.Config {
	return &config.Config{
		MaxDatafileSize:         DefaultMaxDatafileSize,
		MaxKeySize:              DefaultMaxKeySize,
		MaxValueSize:            DefaultMaxValueSize,
		Sync:                    DefaultSync,
		DirFileModeBeforeUmask:  DefaultDirFileModeBeforeUmask,
		FileFileModeBeforeUmask: DefaultFileFileModeBeforeUmask,
	}
}
