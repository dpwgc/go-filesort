package main

import (
	"fmt"
	"github/dpwgc/go-filesort"
	"time"
)

type Log struct {
	Id int
}

func main() {

	// 使用 Bulk 函数新建批量排序程序
	// 使用 Source、Target、OrderBy 函数设置输入输出及排序规则
	id := 0
	mySort := filesort.Bulk(Log{}).Source(func() ([]Log, error) {
		// 输入源：输入12w个ID，从1到120000
		if id >= 120000 {
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

	// 开始排序，在 ./temp 目录下新建临时文件，每个临时文件至多存储1w行记录
	err := mySort.Run("./temp", 10000)
	if err != nil {
		panic(err)
	}

	fmt.Println(time.Now(), "-- end")
}
