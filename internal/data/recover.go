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
	// 检查并回复最后一个datafile
	recovered, err := recoverDatafile(f, cfg)
	if err != nil {
		return err
	}

	if recovered {
		if err := os.Remove(filepath.Join(path, "index")); err != nil {
			return fmt.Errorf("error deleting the index on recovery: %s", err) // TODO: %s是不是应该写成%w
		}
	}

	return nil
}

// 恢复datafile
// 1. 创建一个用于恢复的临时文件
// 2. 从原文件中挨个导入到临时文件中
// 3. 如果遇到损坏的数据就跳过继续迁移，直到全部导完
// 4. 如果没有损坏的数据则删除临时文件，如果有则rename临时文件为原文件替换掉原文件
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
			// 读到文件尾结束
			break
		}

		// 检查是否遇到了损坏数据
		if codec.IsCorruptedData(err) {
			corrupted = true
			continue
		}

		if err != nil {
			return false, fmt.Errorf("unexpected error while reading datafile: %w", err)
		}

		// 写到用于恢复的临时文件中
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
