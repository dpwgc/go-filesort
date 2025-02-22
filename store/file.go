package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type File struct {
	filepath    string
	subFilepath string
}

func NewFile(filepath string) Store {
	if len(filepath) <= 0 {
		return nil
	}
	return &File{
		filepath:    filepath,
		subFilepath: fmt.Sprintf("%s%s/", filepath, NextID()),
	}
}

func (c *File) Write(rows []string) (string, error) {

	filename := c.nextFilename()

	if len(filename) <= 0 {
		return "", errors.New("filename is empty")
	}

	if len(rows) <= 0 {
		return "", errors.New("rows is empty")
	}

	// 自动创建目录
	err := c.autoCreateDir(c.filepath)
	if err != nil {
		return "", err
	}
	err = c.autoCreateDir(c.subFilepath)
	if err != nil {
		return "", err
	}

	return filename, os.WriteFile(filename, []byte(strings.Join(rows, "\n")), 0644)
}

func (c *File) Read(filename string) ([]string, error) {

	if len(filename) <= 0 {
		return nil, errors.New("filename is empty")
	}

	// 读取文件
	var bytes []byte
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return strings.Split(string(bytes), "\n"), err
}

func (c *File) Clear(filenames []string) error {
	/*
		for _, v := range filenames {
			// 只删除 filepath 里的文件
			if len(v) < 2 || v == c.filepath || !strings.HasPrefix(v, c.filepath) {
				continue
			}
			// fmt.Println("remove", v)
			_ = os.RemoveAll(v)
		}

	*/
	// 只删除 filepath 里的文件
	if len(c.subFilepath) < 2 || c.subFilepath == c.filepath || !strings.HasPrefix(c.subFilepath, c.filepath) {
		return errors.New("filepath error")
	}
	// fmt.Println(c.subFilepath)
	// 直接删除
	return os.RemoveAll(c.subFilepath)
}

// 临时存储路径生成
func (c *File) nextFilename() string {
	return fmt.Sprintf("%s%s.sort", c.subFilepath, NextID())
}

// 目录不存在时自动创建目录
func (c *File) autoCreateDir(path string) error {
	dir := filepath.Dir(path)
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return os.MkdirAll(dir, os.ModePerm)
	}
	if err != nil {
		return err
	}
	return nil
}
