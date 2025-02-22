package store

import (
	"fmt"
	"sync"
	"time"
)

type Store interface {
	// Write 写入数据块，rows 最大长度取决于 blockSize 设置，返回存储路径和异常值
	Write(rows []string) (key string, err error)
	// Read 读取指定数据块，返回数据和异常值
	Read(key string) (rows []string, err error)
	// Clear 清除指定数据块
	Clear(keys []string) error
}

// NextID 唯一标识生成
func NextID() string {
	storeMutex.Lock()
	defer storeMutex.Unlock()
	if storeTimestamp <= 0 {
		storeTimestamp = time.Now().UnixMilli()
	}
	if storeId >= 9223372036854775806 {
		storeId = 0
	}
	storeId = storeId + 1
	return fmt.Sprintf("%v-%v", storeTimestamp, storeId)
}

var storeTimestamp int64 = 0
var storeId uint64 = 0
var storeMutex sync.Mutex
