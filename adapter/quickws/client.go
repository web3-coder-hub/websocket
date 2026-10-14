package quickws

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antlabs/quickws"
	"github.com/web3-coder-hub/websocket/adapter"
)

// ClientAdapter quickws 客户端适配器
type ClientAdapter struct{}

// NewClientAdapter 创建 quickws 客户端适配器
func NewClientAdapter() adapter.ClientAdapter {
	return &ClientAdapter{}
}

func (a *ClientAdapter) Name() string {
	return "quickws-client"
}

func (a *ClientAdapter) Dial(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
	clientConn := &ClientConn{
		url:      url,
		headers:  headers,
		callback: callback,
		readChan: make(chan message, 10),
		done:     make(chan struct{}),
	}
	clientConn.connected.Store(1)

	wrapper := &clientCallbackWrapper{
		callback:   callback,
		clientConn: clientConn,
	}

	conn, err := quickws.Dial(url, quickws.WithClientCallback(wrapper))
	if err != nil {
		clientConn.connected.Store(0)
		return nil, err
	}

	clientConn.conn = conn

	// 启动读取循环
	go conn.StartReadLoop()

	return clientConn, nil
}

// ClientConn quickws 客户端连接实现
type ClientConn struct {
	conn      *quickws.Conn
	url       string
	headers   http.Header
	callback  adapter.ClientCallback
	connected atomic.Int32
	readChan  chan message
	done      chan struct{}
	closeOnce sync.Once
}

type message struct {
	op   adapter.Opcode
	data []byte
	err  error
}

func (c *ClientConn) Write(op adapter.Opcode, data []byte) error {
	// #nosec G115 - Opcode 转换是安全的，值域在 0-10 范围内
	return c.conn.WriteMessage(quickws.Opcode(op), data)
}

func (c *ClientConn) WriteTimeout(op adapter.Opcode, data []byte, timeout time.Duration) error {
	// quickws 的 WriteMessage 不支持超时，使用默认写入
	// #nosec G115 - Opcode 转换是安全的，值域在 0-10 范围内
	return c.conn.WriteMessage(quickws.Opcode(op), data)
}

func (c *ClientConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.connected.Store(0)
		if c.conn != nil {
			err = c.conn.Close()
		}
		close(c.done)
	})
	return err
}

func (c *ClientConn) ReadMessage() (adapter.Opcode, []byte, error) {
	select {
	case msg := <-c.readChan:
		return msg.op, msg.data, msg.err
	case <-c.done:
		return 0, nil, adapter.ErrConnClosed
	}
}

func (c *ClientConn) IsConnected() bool {
	return c.connected.Load() == 1
}

// clientCallbackWrapper quickws 客户端回调适配器
type clientCallbackWrapper struct {
	callback   adapter.ClientCallback
	clientConn *ClientConn
}

func (w *clientCallbackWrapper) OnOpen(conn *quickws.Conn) {
	if w.callback != nil {
		w.callback.OnOpen(w.clientConn)
	}
}

func (w *clientCallbackWrapper) OnMessage(conn *quickws.Conn, op quickws.Opcode, data []byte) {
	if w.clientConn != nil {
		// 将消息放入通道供 ReadMessage 读取
		select {
		case w.clientConn.readChan <- message{op: adapter.Opcode(op), data: data}:
		case <-w.clientConn.done:
		}
	}

	if w.callback != nil {
		w.callback.OnMessage(w.clientConn, adapter.Opcode(op), data)
	}
}

func (w *clientCallbackWrapper) OnClose(conn *quickws.Conn, err error) {
	if w.clientConn != nil {
		w.clientConn.connected.Store(0)
	}
	if w.callback != nil {
		w.callback.OnClose(w.clientConn, err)
	}
}
