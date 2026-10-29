package adapter

import (
	"context"
	"net/http"
	"time"
)

// Opcode WebSocket 消息类型
type Opcode int

const (
	Text   Opcode = 1
	Binary Opcode = 2
	Close  Opcode = 8
	Ping   Opcode = 9
	Pong   Opcode = 10
)

// ClientOption 客户端连接选项
type ClientOption func(*ClientOptions)

// ClientOptions 客户端连接配置
type ClientOptions struct {
	Compression bool
	// 可以继续添加其他通用选项
}

// WithCompression 启用压缩
func WithCompression() ClientOption {
	return func(o *ClientOptions) {
		o.Compression = true
	}
}

// ServerCallback 服务端回调接口
type ServerCallback interface {
	OnOpen(conn ServerConn)
	OnMessage(conn ServerConn, op Opcode, data []byte)
	OnClose(conn ServerConn, err error)
}

// ClientCallback 客户端回调接口
type ClientCallback interface {
	OnOpen(conn ClientConn)
	OnMessage(conn ClientConn, op Opcode, data []byte)
	OnClose(conn ClientConn, err error)
	OnError(conn ClientConn, err error)
}

// ServerConn 服务端连接接口
type ServerConn interface {
	// WriteTimeout 写入消息，带超时
	WriteTimeout(op Opcode, data []byte, timeout time.Duration) error
	// Close 关闭连接
	Close() error
	// StartReadLoop 开始读取循环
	StartReadLoop()
	// RemoteAddr 获取远程地址
	RemoteAddr() string
}

// ClientConn 客户端连接接口
type ClientConn interface {
	// Write 写入消息
	Write(op Opcode, data []byte) error
	// WriteTimeout 写入消息，带超时
	WriteTimeout(op Opcode, data []byte, timeout time.Duration) error
	// Close 关闭连接
	Close() error
	// ReadMessage 读取消息（阻塞）
	ReadMessage() (Opcode, []byte, error)
	// IsConnected 是否已连接
	IsConnected() bool
}

// SyncClientConn 支持同步请求的客户端连接
type SyncClientConn interface {
	ClientConn
	// Request 发送同步请求并等待响应
	Request(ctx context.Context, op Opcode, data []byte) ([]byte, error)
}

// ServerAdapter 服务端适配器接口
type ServerAdapter interface {
	// Upgrade 升级 HTTP 连接为 WebSocket
	Upgrade(w http.ResponseWriter, r *http.Request, callback ServerCallback) (ServerConn, error)
	// Name 适配器名称
	Name() string
}

// ClientAdapter 客户端适配器接口
type ClientAdapter interface {
	// Dial 建立 WebSocket 连接
	Dial(url string, headers http.Header, callback ClientCallback) (ClientConn, error)
	// DialWithOptions 建立 WebSocket 连接（带选项）
	DialWithOptions(url string, headers http.Header, callback ClientCallback, options ...ClientOption) (ClientConn, error)
	// Name 适配器名称
	Name() string
}

// UpgradeOptions 服务端升级选项
type UpgradeOptions struct {
	ReadBufferSize  int
	WriteBufferSize int
	Subprotocols    []string
}

// DialOptions 客户端连接选项
type DialOptions struct {
	ReadBufferSize   int
	WriteBufferSize  int
	HandshakeTimeout time.Duration
	Subprotocols     []string
	TLSConfig        interface{} // *tls.Config
}
