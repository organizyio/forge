package codec

import (
	"encoding/json"
	"testing"
)

type wireReq struct {
	ID     string `msgpack:"id"     json:"id"`
	Method string `msgpack:"method" json:"method"`
	Params any    `msgpack:"params" json:"params,omitempty"`
}

func TestMarshalUnmarshal_JSON(t *testing.T) {
	t.Parallel()
	v := wireReq{ID: "r1", Method: "ping", Params: map[string]string{"k": "v"}}
	data, err := Marshal(FormatJSON, v)
	if err != nil {
		t.Fatal(err)
	}
	var out wireReq
	if err := Unmarshal(FormatJSON, data, &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != v.ID || out.Method != v.Method {
		t.Fatalf("got %+v", out)
	}
	// JSON unmarshals object params as map[string]any
	pm, ok := out.Params.(map[string]any)
	if !ok || pm["k"] != "v" {
		t.Fatalf("params: got %#v", out.Params)
	}
}

func TestMarshalUnmarshal_Msgpack(t *testing.T) {
	t.Parallel()
	v := wireReq{ID: "r2", Method: "job_status", Params: map[string]string{"job_id": "j1"}}
	data, err := Marshal(FormatMsgpack, v)
	if err != nil {
		t.Fatal(err)
	}
	var out wireReq
	if err := Unmarshal(FormatMsgpack, data, &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != v.ID || out.Method != v.Method {
		t.Fatalf("got %+v", out)
	}
	pm, ok := out.Params.(map[string]interface{})
	if !ok || pm["job_id"] != "j1" {
		t.Fatalf("params: got %#v", out.Params)
	}
}

func TestRoundtrip_WireResponseShape(t *testing.T) {
	t.Parallel()
	type wireResp struct {
		ID      string           `msgpack:"id"      json:"id"`
		OK      bool             `msgpack:"ok"      json:"ok"`
		Payload *json.RawMessage `msgpack:"payload" json:"payload,omitempty"`
	}
	raw := json.RawMessage(`{"state":"ok"}`)
	v := wireResp{ID: "req-1", OK: true, Payload: &raw}
	for _, f := range []Format{FormatJSON, FormatMsgpack} {
		f := f
		t.Run(formatName(f), func(t *testing.T) {
			t.Parallel()
			data, err := Marshal(f, v)
			if err != nil {
				t.Fatal(err)
			}
			var out wireResp
			if err := Unmarshal(f, data, &out); err != nil {
				t.Fatal(err)
			}
			if out.ID != v.ID || out.OK != v.OK {
				t.Fatalf("got %+v", out)
			}
			if out.Payload == nil || string(*out.Payload) != `{"state":"ok"}` {
				t.Fatalf("payload: %v", out.Payload)
			}
		})
	}
}

func formatName(f Format) string {
	if f == FormatMsgpack {
		return "msgpack"
	}
	return "json"
}
