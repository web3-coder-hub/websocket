package websocket

import (
	"encoding/json"
	"time"
)

// ClientMessage 客户端发送的消息结构
type ClientMessage struct {
	EventType string          `json:"eventType"`       // 事件类型
	EventData json.RawMessage `json:"eventData"`       // 事件数据（延迟解析）
	Token     string          `json:"token,omitempty"` // JWT Token（LOGIN 事件必填）
	Nonce     string          `json:"nonce,omitempty"` // 签名随机数
	Sign      string          `json:"sign,omitempty"`  // HMAC-SHA256 签名
}

// ServerMessage 服务端响应/推送消息（统一格式）
type ServerMessage struct {
	EventType string      `json:"eventType"`      // 事件类型
	Success   bool        `json:"success"`        // 是否成功
	Code      int         `json:"code,omitempty"` // 错误码
	Message   string      `json:"message"`        // 消息内容
	Data      interface{} `json:"data,omitempty"` // 数据（响应数据或推送数据）
	Timestamp time.Time   `json:"timestamp"`      // 时间戳
}
