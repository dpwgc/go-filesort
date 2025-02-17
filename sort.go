package filesort

import (
	"sort"
	"strings"
)

type Sort[T any] struct {
	file         *File
	maxBlockSize int
	blockNum     int
	compare      func(l T, r T) bool
	benchmark    T
}

// New 新建排序
// filepath：存放临时文件的文件夹
// maxBlockSize：每个文件的最大行数
// compare：排序规则，例如 return l.id > r.id 是倒序，return l.id < r.id 是升序
func New[T any](row T, filepath string, maxBlockSize int, compare func(l T, r T) bool) *Sort[T] {
	if len(filepath) <= 0 {
		filepath = "./temp/"
	}
	if maxBlockSize <= 0 {
		maxBlockSize = 5000
	}
	if !strings.HasSuffix(filepath, "/") {
		filepath = filepath + "/"
	}
	return &Sort[T]{
		file:         NewFile(filepath),
		maxBlockSize: maxBlockSize,
		compare:      compare,
	}
}

// Input 输入
// 用于流式导入原始数据
// 传入一个函数handle，程序会一直尝试调用这个handle函数，以此来持续获取数据，直到handle第二个返回值为false
func (c *Sort[T]) Input(handle func() (T, bool)) error {
	first := true
	currentBlock := make([]string, 0)
	for {
		row, ok := handle()
		if !ok {
			break
		}
		if first {
			c.benchmark = row
			first = false
		}
		if len(currentBlock)+1 > c.maxBlockSize {
			err := c.file.Write(c.file.filepath+nextFilename(), currentBlock)
			if err != nil {
				return err
			}
			c.blockNum = c.blockNum + 1
			currentBlock = make([]string, 0)
		}
		bs, err := ToBase64(row)
		if err != nil {
			return err
		}
		currentBlock = append(currentBlock, bs)
	}
	// 处理最后一个块
	if len(currentBlock) > 0 {
		err := c.file.Write(c.file.filepath+nextFilename(), currentBlock)
		if err != nil {
			return err
		}
		c.blockNum = c.blockNum + 1
	}
	return nil
}

// Output 输出
// 用于流式输出排序结果
// 传入一个函数handle，通过这个函数来按顺序接收排序结果，如果想提前终止接收，可以令这个函数返回false
func (c *Sort[T]) Output(handle func(row T) bool) error {

	defer c.file.Close()

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
			// 对这个数据块进行排序
			sort.Sort(sortBase[T]{block, c.compare})
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
			// 是否提前结束
			if !handle(compareValue) {
				return nil
			}
			pointers[compareIndex]++
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
