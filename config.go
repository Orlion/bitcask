package bitcask

import "os"

type Config struct {
	Sync                    bool
	AutoRecovery            bool
	MaxKeySize              uint32
	DBVersion               uint32
	DirFileModeBeforeUmask  os.FileMode
	FileFileModeBeforeUmask os.FileMode
	MaxDatafileSize         int
	MaxValueSize            uint64
}