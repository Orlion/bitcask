package bitcask

import "github.com/Orlion/bitcask/internal/config"

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
