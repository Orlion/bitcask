package data

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Orlion/bitcask/internal"
	"github.com/Orlion/bitcask/internal/config"
	"github.com/Orlion/bitcask/internal/data/codec"
)

func CheckAndRecover(path string, cfg *config.Config) error {
	dfs, err := internal.GetDatafiles(path)
	if err != nil {
		return err
	}

	if len(dfs) == 0 {
		return nil
	}

	f := dfs[len(dfs)-1]

}

// 恢复datafile
func recoverDatafile(path string, cfg *config.Config) (recovered bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return
	}

	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()

	dir, file := filepath.Split(path)
	// 创建一个用于恢复的临时文件
	rPath := filepath.Join(dir, fmt.Sprintf("%s.recovered", file))
	fr, err := os.OpenFile(rPath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return false, fmt.Errorf("creating the recovered datafle: %w", err)
	}

	defer func() {
		closeErr := fr.Close()
		if err == nil {
			err = closeErr
		}
	}()

	dec := codec.NewDecoder(f, cfg.MaxKeySize, cfg.MaxValueSize)
	enc := codec.NewEncoder(fr)
	e := internal.Entry{}

	// 标识是否损坏
	corrupted := false
	for !corrupted {
		// 从原文件中读出去迁移到恢复文件中
		_, err = dec.Decode(&e)
		if err == io.EOF {
			break
		}

		if codec.IsCorruptedData(err) {
			corrupted = true
			continue
		}

		if err != nil {
			return false, fmt.Errorf("unexpected error while reading datafile: %w", err)
		}

		if _, err := enc.Encode(e); err != nil {
			return false, fmt.Errorf("writing to recovered datafile: %w", err)
		}
	}

	if !corrupted {
		// 如果迁移过程中没有损坏数据则直接删除恢复文件即可
		if err := os.Remove(fr.Name()); err != nil {
			return false, fmt.Errorf("can't remove temporal recovered datafile: %w", fr.Name())
		}

		return false, nil
	}

	// 如果迁移过程中发现了损坏数据则用恢复文件替换掉原损坏文件
	if err := os.Rename(rPath, path); err != nil {
		return false, fmt.Errorf("removing corrupted file: %w", err)
	}

	return true, nil
}
