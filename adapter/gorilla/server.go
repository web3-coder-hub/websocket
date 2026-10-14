package gorilla

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/web3-coder-hub/websocket/adapter"
)

// ServerAdapter gorilla/websocket 服务端适配器
type ServerAdapter struct {
	upgrader websocket.Upgrader
}

// NewServerAdapter 创建 gorilla 服务端适配器
func NewServerAdapter(upgrader *websocket.Upgrader) adapter.ServerAdapter {
	if upgrader == nil {
		upgrader = &websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		}
	}
	return &ServerAdapter{upgrader: *upgrader}
}

func (a *ServerAdapter) Name() string {
	return "gorilla"
}

func (a *ServerAdapter) Upgrade(w http.ResponseWriter, r *http.Request, callback adapter.ServerCallback) (adapter.ServerConn, error) {
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}

	serverConn := &ServerConn{
		conn:     conn,
		callback: callback,
	}

	if callback != nil {
		callback.OnOpen(serverConn)
	}

	return serverConn, nil
}

// ServerConn gorilla 服务端连接实现
type ServerConn struct {
	conn     *websocket.Conn
	callback adapter.ServerCallback
}

func (c *ServerConn) WriteTimeout(op adapter.Opcode, data []byte, timeout time.Duration) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(timeout))
	return c.conn.WriteMessage(int(op), data)
}

func (c *ServerConn) Close() error {
	return c.conn.Close()
}

func (c *ServerConn) StartReadLoop() {
	defer func() {
		if c.callback != nil {
			c.callback.OnClose(c, nil)
		}
		_ = c.conn.Close()
	}()

	for {
		messageType, data, err := c.conn.ReadMessage()
		if err != nil {
			if c.callback != nil {
				c.callback.OnClose(c, err)
			}
			return
		}

		if c.callback != nil {
			c.callback.OnMessage(c, adapter.Opcode(messageType), data)
		}
	}
}

func (c *ServerConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}
