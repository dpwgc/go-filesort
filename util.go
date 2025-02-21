package filesort

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
)

func ToBase64(v any) (string, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(v)
	if err != nil {
		return "", err
	}
	s := base64.StdEncoding.EncodeToString(buf.Bytes())
	return s, nil
}

func FromBase64(s string, v any) error {
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	decoder := gob.NewDecoder(bytes.NewReader(data))
	return decoder.Decode(v)
}

type SortBase[T any] struct {
	rows    []T
	compare func(l, r T) bool
}

func (s SortBase[T]) Len() int           { return len(s.rows) }
func (s SortBase[T]) Less(i, j int) bool { return s.compare(s.rows[i], s.rows[j]) }
func (s SortBase[T]) Swap(i, j int)      { s.rows[i], s.rows[j] = s.rows[j], s.rows[i] }
