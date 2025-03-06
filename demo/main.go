package main

import (
	"fmt"
	"github.com/dpwgc/go-filesort"
	"time"
)

type Log struct {
	Id int
}

func main() {

	id := 0

	// 使用 Bulk 函数新建一个排序流程
	bulk := filesort.Bulk(Log{})

	// 使用 Source、Target、OrderBy 函数设置输入输出及排序规则
	bulk.Source(func() ([]Log, error) {
		// 输入源：输入12w个ID，从1到120000
		if id >= 120000 {
			// 终止输入
			return nil, nil
		}
		id++
		return []Log{{
			Id: id,
		}}, nil
	}).Target(func(row Log) bool {
		// 输出目标：打印排序结果
		fmt.Println(row.Id)
		return true
	}).OrderBy(func(left Log, right Log) bool {
		// 排序规则：按ID大小倒序
		return left.Id > right.Id
	}).Limit(10) // 只输出结果中的前10个

	fmt.Println(time.Now(), "-- start")

	// 开始排序，在 ./temp 目录下存储临时数据，运行时会在该目录下新建一个临时的 boltdb 数据库，每个数据块对应一个 key，每个 key 至多存储1w行记录
	// 如果需要排序的数据总量没到1w，会直接在内存中排序，不走文件排序逻辑
	err := bulk.Run("./temp", 10000)
	if err != nil {
		panic(err)
	}

	fmt.Println(time.Now(), "-- end")
}
