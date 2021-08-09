package internal

import (
	"encoding/json"
	"io/ioutil"
	"os"
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
