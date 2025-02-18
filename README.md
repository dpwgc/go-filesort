# Go FileSort

## 基于Go实现的文件排序，类似于MySQL的排序逻辑

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

	// 开始排序，在 ./temp 目录下新建临时文件，每个临时文件至多存储1w行记录
	// 如果需要排序的数据总量没到1w，会直接在内存中排序，不走文件排序逻辑
	err := bulk.Run("./temp", 10000)
	if err != nil {
		panic(err)
	}

	fmt.Println(time.Now(), "-- end")
}
```

***

### 函数说明

* Bulk：新建一个排序流程，传入一个泛型来确认排序对象类型。
* Source：设置一个输入源函数，程序运行时会一直尝试调用这个函数，以此来持续获取数据，直到这个函数第1个返回值为 nil，如果这个函数第2个 error 返回值不为 nil，则直接终止排序流程并报错。
* Target：设置一个输出目标函数，通过这个函数来按顺序逐个接收排序结果，如果想提前终止接收，可以令这个函数返回 false。
* OrderBy：设置排序规则，例如 return left.id > right.id 是倒序，return left.id < right.id 是升序。
* Limit：返回结果分页设置，与 MySQL 的 limit 功能相同，limit 10 或者 limit 0,10。
* Run：指定一个临时文件目录和分块大小，运行这个排序流程，每个流程只能执行一次 Run。

*** 

### 自定义存储

#### 可以自行实现 store.Store 接口，自定义临时数据存储方式

```go
package store

type Store interface {
	// Write 写入数据块，rows 最大长度取决于 blockSize 设置，返回存储路径和异常值
	Write(rows []string) (key string, err error)
	// Read 读取指定数据块，返回数据和异常值
	Read(key string) (rows []string, err error)
	// Clear 清除指定数据块
	Clear(keys []string)
}
```