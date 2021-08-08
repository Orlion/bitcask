package config

import "os"

type Config struct {
	MaxDatafileSize         int    `json:"max_datafile_size"`
	MaxKeySize              uint32 `json:"max_key_size"`
	MaxValueSize            uint64 `json:"max_value_size"`
	Sync                    bool   `json:"sync"`
	AutoRecovery            bool   `json:"autorecovery"`
	DirFileModeBeforeUmask  os.FileMode
	FileFileModeBeforeUmask os.FileMode
}
