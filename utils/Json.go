package utils

import "encoding/json"

func MustJson(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func MustJsonStr(v any) string {
	return string(MustJson(v))
}
