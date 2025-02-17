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
