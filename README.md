# FileSort
## 基于Go实现的文件排序，类似于MySQL的文件排序

***

### 使用方式

```
go get github/dpwgc/go-filesort
```

```go
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
```

***

### 函数说明

* Bulk：新建一个批量排序程序，传入一个泛型来确认排序对象类型。
* Source：设置一个输入源函数，程序运行时会一直尝试调用这个函数，以此来持续获取数据，直到这个函数第1个返回值为 nil，如果这个函数第2个 error 返回值不为 nil，则直接终止排序流程并报错。
* Target：设置一个输出目标函数，通过这个函数来按顺序逐个接收排序结果，如果想提前终止接收，可以令这个函数返回 false。
* OrderBy：设置排序规则，例如 return left.id > right.id 是倒序，return left.id < right.id 是升序。
* Limit：返回结果分页设置，与 MySQL 的 limit 功能相同。
* Run：运行这个批量排序程序。