package main

import (
	"filesort"
	"fmt"
	"time"
)

type Log struct {
	Id int
}

func main() {

	// 新建排序，将在 ./temp 目录下新建临时文件， 每个文件至多存储5w行记录，按降序排序
	mySort := filesort.New(Log{}, "./temp", 50000, func(l Log, r Log) bool {
		return l.Id > r.Id
	})

	fmt.Println(time.Now(), "-- start")

	// 输入12w个ID，从1到120000
	i := 0
	_ = mySort.Input(func() (Log, bool) {
		if i >= 120000 {
			return Log{}, false
		}
		i++
		return Log{
			Id: i,
		}, true
	})

	// 倒序输出
	j := 0
	_ = mySort.Output(func(row Log) bool {
		// 只取前100条，等效于 MySQL 的 limit 100
		if j > 100 {
			return false
		}
		j++
		fmt.Println(row.Id)
		return true
	})

	fmt.Println(time.Now(), "-- end")
}
