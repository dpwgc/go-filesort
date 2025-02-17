package filesort

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type File struct {
	filepath  string
	filenames []string
}

func NewFile(filepath string) *File {
	return &File{
		filepath: filepath,
	}
}

func (c *File) Write(filename string, rows []string) error {

	if len(filename) <= 0 {
		return errors.New("filename is empty")
	}

	if len(c.filepath) <= 0 {
		return errors.New("filepath is empty")
	}

	if len(rows) <= 0 {
		return nil
	}

	c.filenames = append(c.filenames, filename)

	// 自动创建目录
	err := autoCreateDir(c.filepath)
	if err != nil {
		return err
	}

	return os.WriteFile(filename, []byte(strings.Join(rows, "\n")), 0644)
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

func (c *File) Close() {
	for _, v := range c.filenames {
		// fmt.Println("remove", v)
		_ = os.RemoveAll(v)
	}
}

// 目录不存在时自动创建目录
func autoCreateDir(path string) error {
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

var initTime int64
var id int64 = 0
var mutex sync.Mutex

func init() {
	initTime = time.Now().UnixMilli()
}

// 临时文件名生成
func nextFilename() string {
	mutex.Lock()
	defer mutex.Unlock()
	if id >= 9223372036854775806 {
		id = 0
	}
	id = id + 1
	return fmt.Sprintf("%v-%v.sort", initTime, id)
}
