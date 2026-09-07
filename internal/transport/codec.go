package transport

import (
	"encoding/json"
)

// Codec implements grpc.CallOption-compatible encode/decode via grpc.ForceServerCodec.
// It makes gRPC carry typed JSON frames so no protobuf codegen is required for
// phase 1; wire format stays gRPC over HTTP/2.
type Codec struct{}

func (Codec) Name() string { return "node-agent-json" }

func (Codec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (Codec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
