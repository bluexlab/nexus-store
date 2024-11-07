package util

import (
	"bytes"
	"encoding/json"
	"io"
)

func NewObjectJsonReader(obj any) (io.Reader, error) {
	buffer := &bytes.Buffer{}
	jsonEncoder := json.NewEncoder(buffer)
	err := jsonEncoder.Encode(obj)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}
