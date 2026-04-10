// Package codec encodes and decodes Forge wire messages (MessagePack or JSON).
package codec

import (
	"encoding/json"

	"github.com/vmihailenco/msgpack/v5"
)

// Format selects the on-the-wire encoding.
type Format int

const (
	FormatMsgpack Format = iota
	FormatJSON
)

// Marshal serializes v using the given format.
func Marshal(f Format, v any) ([]byte, error) {
	if f == FormatMsgpack {
		return msgpack.Marshal(v)
	}
	return json.Marshal(v)
}

// Unmarshal deserializes data into v using the given format.
func Unmarshal(f Format, data []byte, v any) error {
	if f == FormatMsgpack {
		return msgpack.Unmarshal(data, v)
	}
	return json.Unmarshal(data, v)
}
