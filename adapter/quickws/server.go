package quickws

import (
	"net/http"
	"time"

	"github.com/antlabs/quickws"
	"github.com/web3-coder-hub/websocket/adapter"
)

// ServerAdapter quickws 服务端适配器
type ServerAdapter struct{}

// NewServerAdapter 创建 quickws 服务端适配器
func NewServerAdapter() adapter.ServerAdapter {
	return &ServerAdapter{}
}

func (a *ServerAdapter) Name() string {
	return "quickws"
}

func (a *ServerAdapter) Upgrade(w http.ResponseWriter, r *http.Request, callback adapter.ServerCallback) (adapter.ServerConn, error) {
	wrapper := &serverCallbackWrapper{callback: callback}
	conn, err := quickws.Upgrade(w, r, quickws.WithServerCallback(wrapper))
	if err != nil {
		return nil, err
	}

	serverConn := &ServerConn{
		conn:     conn,
		callback: callback,
	}
	wrapper.serverConn = serverConn

	return serverConn, nil
}

// ServerConn quickws 服务端连接实现
type ServerConn struct {
	conn     *quickws.Conn
	callback adapter.ServerCallback
}

func (c *ServerConn) WriteTimeout(op adapter.Opcode, data []byte, timeout time.Duration) error {
	// #nosec G115 - Opcode 转换是安全的，值域在 0-10 范围内
	return c.conn.WriteTimeout(quickws.Opcode(op), data, timeout)
}

func (c *ServerConn) Close() error {
	return c.conn.Close()
}

func (c *ServerConn) StartReadLoop() {
	c.conn.StartReadLoop()
}

func (c *ServerConn) RemoteAddr() string {
	// quickws 不直接暴露 RemoteAddr，返回空字符串
	// 如果需要，可以在 Upgrade 时通过 http.Request 获取
	return ""
}

// serverCallbackWrapper quickws 回调适配器
type serverCallbackWrapper struct {
	callback   adapter.ServerCallback
	serverConn *ServerConn
}

func (w *serverCallbackWrapper) OnOpen(conn *quickws.Conn) {
	if w.callback != nil && w.serverConn != nil {
		w.callback.OnOpen(w.serverConn)
	}
}

func (w *serverCallbackWrapper) OnMessage(conn *quickws.Conn, op quickws.Opcode, data []byte) {
	if w.callback != nil && w.serverConn != nil {
		w.callback.OnMessage(w.serverConn, adapter.Opcode(op), data)
	}
}

func (w *serverCallbackWrapper) OnClose(conn *quickws.Conn, err error) {
	if w.callback != nil && w.serverConn != nil {
		w.callback.OnClose(w.serverConn, err)
	}
}
