package metadata

import (
	"github.com/Orlion/bitcask/internal"
	"os"
)

type MetaData struct {
	IndexUpToDate    bool  `json:"index_up_to_date"`
	ReclaimableSpace int64 `json:"reclaimable_space"` // 可回收的空间
}

func (m *MetaData) Save(path string, mode os.FileMode) error {
	return internal.SaveJson2File(m, path, mode)
}

func Load(path string) (*MetaData, error) {
	m := new(MetaData)
	err := internal.LoadFromFile(path, m)
	return m, err
}
