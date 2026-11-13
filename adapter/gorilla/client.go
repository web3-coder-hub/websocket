// Package gorilla provides WebSocket adapter implementation using gorilla/websocket library.
package gorilla

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/web3-coder-hub/websocket/adapter"
)

// ClientAdapter gorilla/websocket 客户端适配器
type ClientAdapter struct {
	dialer websocket.Dialer
}

// NewClientAdapter 创建 gorilla 客户端适配器
// 如果 dialer 为 nil，则使用默认配置（不启用压缩）
// 调用方可以传入自定义 dialer 来控制压缩等选项
func NewClientAdapter(dialer *websocket.Dialer) adapter.ClientAdapter {
	if dialer == nil {
		dialer = &websocket.Dialer{
			HandshakeTimeout: 45 * time.Second,
		}
	}
	return &ClientAdapter{dialer: *dialer}
}

func (a *ClientAdapter) Name() string {
	return "gorilla-client"
}

func (a *ClientAdapter) Dial(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
	return a.DialWithOptions(url, headers, callback)
}

func (a *ClientAdapter) DialWithOptions(url string, headers http.Header, callback adapter.ClientCallback, options ...adapter.ClientOption) (adapter.ClientConn, error) {
	// 解析适配器选项
	opts := &adapter.ClientOptions{}
	for _, opt := range options {
		opt(opts)
	}

	// 应用选项到 dialer
	dialer := a.dialer
	if opts.Compression {
		dialer.EnableCompression = true
	}

	conn, _, err := dialer.Dial(url, headers)
	if err != nil {
		return nil, err
	}

	clientConn := &ClientConn{
		conn:     conn,
		callback: callback,
		done:     make(chan struct{}),
	}
	clientConn.connected.Store(1)

	if callback != nil {
		callback.OnOpen(clientConn)
	}

	// 启动读取循环
	go clientConn.readLoop()

	return clientConn, nil
}

// ClientConn gorilla 客户端连接实现
type ClientConn struct {
	callback  adapter.ClientCallback
	conn      *websocket.Conn
	done      chan struct{}
	closeOnce sync.Once
	connected atomic.Int32
}

func (c *ClientConn) Write(op adapter.Opcode, data []byte) error {
	return c.conn.WriteMessage(int(op), data)
}

func (c *ClientConn) WriteTimeout(op adapter.Opcode, data []byte, timeout time.Duration) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	return c.conn.WriteMessage(int(op), data)
}

func (c *ClientConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.connected.Store(0)
		close(c.done)
		if c.conn != nil {
			err = c.conn.Close()
		}
	})
	return err
}

func (c *ClientConn) ReadMessage() (adapter.Opcode, []byte, error) {
	messageType, data, err := c.conn.ReadMessage()
	return adapter.Opcode(messageType), data, err
}

func (c *ClientConn) IsConnected() bool {
	return c.connected.Load() == 1
}

// readLoop 读取循环
func (c *ClientConn) readLoop() {
	defer func() {
		c.connected.Store(0)
		if c.callback != nil {
			c.callback.OnClose(c, nil)
		}
	}()

	for {
		select {
		case <-c.done:
			return
		default:
			messageType, data, err := c.conn.ReadMessage()
			if err != nil {
				if c.callback != nil {
					c.callback.OnError(c, err)
					c.callback.OnClose(c, err)
				}
				return
			}

			if c.callback != nil {
				c.callback.OnMessage(c, adapter.Opcode(messageType), data)
			}
		}
	}
}
