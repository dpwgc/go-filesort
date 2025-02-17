package filesort

import (
	"errors"
	"sort"
	"strings"
)

type Sort[T any] struct {
	file         *File
	maxBlockSize int
	blockNum     int
	compare      func(left T, right T) bool
	benchmark    T
	source       func() ([]T, error)
	target       func(row T) bool
	limit        bool
	cursor       int
	size         int
}

// Bulk 新建批量排序程序
func Bulk[T any](row T) *Sort[T] {
	return &Sort[T]{
		benchmark: row,
		limit:     false,
		cursor:    0,
		size:      0,
	}
}

// Source 输入源设置
// 用于流式输入原始数据
// 传入一个函数，程序会一直尝试调用这个函数，以此来持续获取数据，直到这个函数第1个返回值为 nil，如果这个函数第2个 error 返回值不为 nil，则直接终止排序流程并报错
func (c *Sort[T]) Source(source func() ([]T, error)) *Sort[T] {
	c.source = source
	return c
}

// Target 输出目标设置
// 用于流式输出排序结果
// 传入一个函数，通过这个函数来按顺序逐个接收排序结果，如果想提前终止接收，可以令这个函数返回 false
func (c *Sort[T]) Target(target func(row T) bool) *Sort[T] {
	c.target = target
	return c
}

// OrderBy 排序规则设置
// 例如 return left.id > right.id 是倒序，return left.id < right.id 是升序
func (c *Sort[T]) OrderBy(compare func(left T, right T) bool) *Sort[T] {
	c.compare = compare
	return c
}

// Limit 返回结果分页设置
// 与 MySQL 的 limit 功能相同
func (c *Sort[T]) Limit(n ...int) *Sort[T] {
	if len(n) == 1 && n[0] >= 0 {
		c.limit = true
		c.size = n[0]
	}
	if len(n) == 2 && n[0] >= 0 && n[1] >= 0 {
		c.limit = true
		c.cursor = n[0]
		c.size = n[1]
	}
	return c
}

// Run 运行批量排序程序
// filepath：临时文件存放文件夹
// maxBlockSize：每个分块的大小限制
func (c *Sort[T]) Run(filepath string, maxBlockSize int) error {
	if len(filepath) <= 0 {
		return errors.New("filepath is empty")
	}
	if maxBlockSize <= 0 {
		return errors.New("max block size is empty")
	}
	if c.source == nil {
		return errors.New("source is nil")
	}
	if c.target == nil {
		return errors.New("target is nil")
	}
	if !strings.HasSuffix(filepath, "/") {
		filepath = filepath + "/"
	}
	c.file = NewFile(filepath)
	defer c.file.Close()
	c.maxBlockSize = maxBlockSize
	err := c.input()
	if err != nil {
		return err
	}
	return c.output()
}

func (c *Sort[T]) input() error {
	first := true
	currentBlock := make([]T, 0)
	for {
		rows, err := c.source()
		if err != nil {
			return err
		}
		if len(rows) <= 0 {
			break
		}
		for _, row := range rows {
			if first {
				c.benchmark = row
				first = false
			}
			if len(currentBlock)+1 > c.maxBlockSize {
				// 对这个数据块进行排序
				sort.Sort(sortBase[T]{currentBlock, c.compare})
				blockString, err := c.toS(currentBlock)
				if err != nil {
					return err
				}
				err = c.file.Write(c.file.filepath+nextFilename(), blockString)
				if err != nil {
					return err
				}
				c.blockNum = c.blockNum + 1
				currentBlock = make([]T, 0)
			}
			currentBlock = append(currentBlock, row)
		}
	}
	// 处理最后一个块
	if len(currentBlock) > 0 {
		// 对这个数据块进行排序
		sort.Sort(sortBase[T]{currentBlock, c.compare})
		blockString, err := c.toS(currentBlock)
		if err != nil {
			return err
		}
		err = c.file.Write(c.file.filepath+nextFilename(), blockString)
		if err != nil {
			return err
		}
		c.blockNum = c.blockNum + 1
		clear(currentBlock)
	}
	return nil
}

// Target 输出目标
// 用于流式输出排序结果
// 传入一个函数，通过这个函数来按顺序接收排序结果，如果想提前终止接收，可以令这个函数返回 false
func (c *Sort[T]) output() error {

	cursor := 0
	size := 0

	// 用于记录每个块的当前位置
	pointers := make([]int, c.blockNum)
	// 用于标记哪些块已经处理完成
	complete := make([]bool, c.blockNum)

	// 比较所有块的数据，合并处理
	for !allComplete(complete) {
		var compareValue = c.benchmark // 初始化对比值
		compareIndex := -1
		for i, f := range c.file.filenames {
			bs, err := c.file.Read(f)
			if err != nil {
				return err
			}
			block, err := c.toT(bs)
			if err != nil {
				return err
			}
			if !complete[i] && pointers[i] < len(block) {
				current := block[pointers[i]]
				if c.compare(current, compareValue) || compareIndex == -1 {
					compareValue = current
					compareIndex = i
				}
			}
			clear(bs)
			clear(block)
		}
		if compareIndex != -1 {
			pointers[compareIndex]++
			if c.limit {
				// 如果还未到达指定游标
				if c.cursor > cursor {
					cursor++
					continue
				}
				// 超过返回长度限制
				if size >= c.size {
					break
				}
				size++
			}
			// 是否提前结束
			if !c.target(compareValue) {
				break
			}
		} else {
			// 全部处理完毕，打破循环
			break
		}
	}
	return nil
}

func (c *Sort[T]) toT(s []string) ([]T, error) {
	var l []T
	for _, v := range s {
		var t T
		err := FromBase64(v, &t)
		if err != nil {
			return nil, err
		}
		l = append(l, t)
	}
	return l, nil
}

func (c *Sort[T]) toS(t []T) ([]string, error) {
	var l []string
	for _, v := range t {
		bs, err := ToBase64(v)
		if err != nil {
			return nil, err
		}
		l = append(l, bs)
	}
	return l, nil
}

func allComplete(complete []bool) bool {
	for _, c := range complete {
		if !c {
			return false
		}
	}
	return true
}

type sortBase[T any] struct {
	rows    []T
	compare func(l, r T) bool
}

func (s sortBase[T]) Len() int           { return len(s.rows) }
func (s sortBase[T]) Less(i, j int) bool { return s.compare(s.rows[i], s.rows[j]) }
func (s sortBase[T]) Swap(i, j int)      { s.rows[i], s.rows[j] = s.rows[j], s.rows[i] }
