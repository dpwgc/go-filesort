package filesort

import (
	"errors"
	"github.com/dpwgc/go-filesort/store"
	"sort"
	"strings"
)

type Sort[T any] struct {
	store     store.Store
	tempKeys  []string
	blockSize int
	blockNum  int
	compare   func(left T, right T) bool
	benchmark T
	source    func() ([]T, error)
	target    func(row T) bool
	limit     bool
	cursor    int
	size      int
	memory    []T
	repeat    bool
}

// Bulk 新建一个排序流程
func Bulk[T any](row T) *Sort[T] {
	return &Sort[T]{
		blockSize: 0,
		blockNum:  0,
		benchmark: row,
		limit:     false,
		cursor:    0,
		size:      0,
		repeat:    false,
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

// Run 运行排序流程，排序过程中产生的数据将临时存储在 boltdb 中，方法结束后会自动删除数据
// filename：boltdb 文件名
// blockSize：每个分块的大小限制
func (c *Sort[T]) Run(filepath string, blockSize int) error {
	if len(filepath) <= 0 {
		return errors.New("filepath is empty")
	}
	if !strings.HasSuffix(filepath, "/") {
		filepath = filepath + "/"
	}
	return c.RunStore(store.NewBolt(filepath), blockSize)
}

func (c *Sort[T]) RunFile(filepath string, blockSize int) error {
	if len(filepath) <= 0 {
		return errors.New("filepath is empty")
	}
	if !strings.HasSuffix(filepath, "/") {
		filepath = filepath + "/"
	}
	return c.RunStore(store.NewFile(filepath), blockSize)
}

// RunStore 运行批量排序流程，这里可以传一个自定义的存储接口
func (c *Sort[T]) RunStore(store store.Store, blockSize int) error {
	if store == nil {
		return errors.New("store is nil")
	}
	if blockSize <= 0 {
		return errors.New("block size is empty")
	}
	if c.source == nil {
		return errors.New("source is nil")
	}
	if c.target == nil {
		return errors.New("target is nil")
	}
	if c.repeat {
		return errors.New("can not run repeatedly")
	}
	c.store = store
	c.blockSize = blockSize
	c.repeat = true
	err := c.input()
	defer c.store.Clear(c.tempKeys)
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
			if len(currentBlock)+1 > c.blockSize {
				// 对这个数据块进行排序
				sort.Sort(SortBase[T]{currentBlock, c.compare})
				blockString, err := c.toS(currentBlock)
				if err != nil {
					return err
				}
				temp, err := c.store.Write(blockString)
				if err != nil {
					return err
				}
				c.tempKeys = append(c.tempKeys, temp)
				c.blockNum = c.blockNum + 1
				currentBlock = make([]T, 0)
			}
			currentBlock = append(currentBlock, row)
		}
	}
	// 处理最后一个块
	if len(currentBlock) > 0 {
		// 对这个数据块进行排序
		sort.Sort(SortBase[T]{currentBlock, c.compare})
		// 如果输入的数据量太小，还没一个数据块大的话，直接在内存里排序就行，不用文件排序了
		if c.blockNum == 0 {
			c.memory = currentBlock
			return nil
		}
		blockString, err := c.toS(currentBlock)
		if err != nil {
			return err
		}
		temp, err := c.store.Write(blockString)
		if err != nil {
			return err
		}
		c.tempKeys = append(c.tempKeys, temp)
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

	// 数据量级小，直接在内存排序，输出结果
	if len(c.memory) > 0 {
		for _, m := range c.memory {
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
			if !c.target(m) {
				break
			}
		}
	}

	// 用于记录每个块的当前位置
	pointers := make([]int, c.blockNum)
	// 用于标记哪些块已经处理完成
	complete := make([]bool, c.blockNum)

	// 比较所有块的数据，合并处理
	for !c.allComplete(complete) {
		var compareValue = c.benchmark // 初始化对比值
		compareIndex := -1
		for i, f := range c.tempKeys {
			bs, err := c.store.Read(f)
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

func (c *Sort[T]) allComplete(complete []bool) bool {
	for _, c := range complete {
		if !c {
			return false
		}
	}
	return true
}
