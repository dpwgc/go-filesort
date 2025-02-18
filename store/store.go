package store

type Store interface {
	// Write 写入数据块，rows 最大长度取决于 blockSize 设置，返回存储路径和异常值
	Write(rows []string) (key string, err error)
	// Read 读取指定数据块，返回数据和异常值
	Read(key string) (rows []string, err error)
	// Clear 清除指定数据块
	Clear(keys []string)
}
