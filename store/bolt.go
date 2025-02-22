package store

import (
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"os"
	"path/filepath"
	"strings"
)

type Bolt struct {
	filepath string
	filename string
	db       *bolt.DB
	table    []byte
}

func NewBolt(filepath string) Store {
	if len(filepath) <= 0 {
		return nil
	}
	return &Bolt{
		filepath: filepath,
		filename: "",
		db:       nil,
		table:    []byte("sort"),
	}
}

func (c *Bolt) Write(rows []string) (string, error) {

	key := c.nextKey()

	if len(key) <= 0 {
		return "", errors.New("key is empty")
	}

	if len(rows) <= 0 {
		return "", errors.New("rows is empty")
	}

	// 自动创建目录
	err := c.autoCreateDir()
	if err != nil {
		return "", err
	}

	// 自动创建库表
	if c.db == nil {
		// 在 filepath 下建一个新的临时数据库，名字随意，不重复就行
		c.filename = fmt.Sprintf("%s%s.sort.db", c.filepath, key)
		// 创建库
		c.db, err = bolt.Open(c.filename, 0600, nil)
		if err != nil {
			return "", err
		}
		// 创建表
		err = c.autoCreateTable()
		if err != nil {
			return "", err
		}
	}

	err = c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(c.table)
		if bucket != nil {
			err = bucket.Put([]byte(key), []byte(strings.Join(rows, "\n")))
			if err != nil {
				return err
			}
		}
		return nil
	})

	return key, err
}

func (c *Bolt) Read(key string) ([]string, error) {

	if len(key) <= 0 {
		return nil, errors.New("key is empty")
	}

	// 读取文件
	var bytes []byte

	err := c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(c.table)
		if bucket != nil {
			bytes = bucket.Get([]byte(key))
		}
		return nil
	})

	return strings.Split(string(bytes), "\n"), err
}

func (c *Bolt) Clear(keys []string) error {
	/*
		_ = c.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(c.table)
			if bucket != nil {
				for _, key := range keys {
					_ = bucket.Delete([]byte(key))
				}
			}
			return nil
		})
	*/
	// 只删除 filepath 里的文件
	if len(c.filename) < 2 || c.filename == c.filepath || !strings.HasPrefix(c.filename, c.filepath) {
		return errors.New("filepath error")
	}
	// fmt.Println(c.dbName)
	// 直接删除这个数据库
	return os.RemoveAll(c.filename)
}

// 临时存储路径生成
func (c *Bolt) nextKey() string {
	return NextID()
}

// 目录不存在时自动创建目录
func (c *Bolt) autoCreateDir() error {
	dir := filepath.Dir(c.filepath)
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return os.MkdirAll(dir, os.ModePerm)
	}
	if err != nil {
		return err
	}
	return nil
}

// 目录不存在时自动创建目录
func (c *Bolt) autoCreateTable() error {
	return c.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(c.table)
		return err
	})
}
