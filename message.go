// Package websocket provides WebSocket server and client implementations with authentication and message routing.
package websocket

import (
	"encoding/json"
	"time"
)

// ClientMessage 客户端发送的消息结构
type ClientMessage struct {
	EventType string          `json:"eventType"`
	Token     string          `json:"token,omitempty"`
	Nonce     string          `json:"nonce,omitempty"`
	Sign      string          `json:"sign,omitempty"`
	EventData json.RawMessage `json:"eventData"`
}

// ServerMessage 服务端响应/推送消息（统一格式）
type ServerMessage struct {
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data,omitempty"`
	EventType string      `json:"eventType"`
	Message   string      `json:"message"`
	Code      int         `json:"code,omitempty"`
	Success   bool        `json:"success"`
}
