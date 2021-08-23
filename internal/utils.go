package internal

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func SaveJson2File(v interface{}, path string, mode os.FileMode) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, b, mode)
}
func LoadFromFile(path string, v interface{}) error {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(content, v)
}

func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// 获取path文件夹下所有data文件
func GetDatafiles(path string) ([]string, error) {
	fns, err := filepath.Glob(fmt.Sprintf("%s/*.data", path))
	if err != nil {
		return nil, err
	}
	sort.Strings(fns)
	return fns, nil
}

// 将文件中的id解析出来
func ParseIds(fns []string) ([]int, error) {
	var ids []int
	for _, fn := range fns {
		// fn: /xxx/xxx/123.data
		// after Base
		// fn: 123.data
		fn = filepath.Base(fn)
		// ext:.data
		ext := filepath.Ext(fn)
		if ext != ".data" {
			continue
		}

		id, err := strconv.ParseInt(strings.TrimSuffix(fn, ext), 10, 32)
		if err != nil {
			return nil, err
		}

		ids = append(ids, int(id))
	}

	sort.Ints(ids)
	return ids, nil
}
