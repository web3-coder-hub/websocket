package client

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/web3-coder-hub/logger"
	"github.com/web3-coder-hub/websocket/adapter"
)

// Config 客户端配置
type Config struct {
	URL                  string            `json:"url"`
	Headers              map[string]string `json:"headers"`
	ReconnectEnabled     bool              `json:"reconnect_enabled"`
	ReconnectInterval    time.Duration     `json:"reconnect_interval"`
	MaxReconnectAttempts int               `json:"max_reconnect_attempts"`
	HeartbeatInterval    time.Duration     `json:"heartbeat_interval"`
	HeartbeatTimeout     time.Duration     `json:"heartbeat_timeout"`
	WriteTimeout         time.Duration     `json:"write_timeout"`
	HandshakeTimeout     time.Duration     `json:"handshake_timeout"`
	EnableAutoHeartbeat  bool              `json:"enable_auto_heartbeat"`
}

// MessageHandler 消息处理器
type MessageHandler func(msg *Message) error

// EventHandler 事件处理器
type EventHandler func()

// Client WebSocket 客户端
type Client struct {
	config  Config
	adapter adapter.ClientAdapter
	conn    adapter.ClientConn
	logger  logger.Logger

	// 状态
	connected atomic.Bool
	closed    atomic.Bool

	// ID
	id        string
	sessionID string

	// 消息处理
	handlers   map[string]MessageHandler
	handlersMu sync.RWMutex

	// 事件处理
	onConnect    EventHandler
	onDisconnect EventHandler
	onError      func(error)

	// 重连
	reconnecting     atomic.Bool
	reconnectAttempt int

	// 心跳
	lastPongTime   atomic.Int64
	heartbeatTimer *time.Timer
	heartbeatStop  chan struct{}

	// 控制
	done      chan struct{}
	closeOnce sync.Once
}

// Message 接收到的消息
type Message struct {
	EventType string          `json:"eventType"`
	EventData json.RawMessage `json:"eventData,omitempty"`
	Channel   string          `json:"channel,omitempty"`
	Timestamp time.Time       `json:"timestamp,omitempty"`
	Success   bool            `json:"success,omitempty"`
	Message   string          `json:"message,omitempty"`
	Code      int             `json:"code,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
}

// NewClient 创建客户端
func NewClient(cfg Config, adapter adapter.ClientAdapter, l logger.Logger) *Client {
	if cfg.ReconnectInterval == 0 {
		cfg.ReconnectInterval = 5 * time.Second
	}
	if cfg.MaxReconnectAttempts == 0 {
		cfg.MaxReconnectAttempts = 10
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 10 * time.Second
	}

	return &Client{
		config:        cfg,
		adapter:       adapter,
		logger:        l,
		id:            uuid.New().String(),
		handlers:      make(map[string]MessageHandler),
		done:          make(chan struct{}),
		heartbeatStop: make(chan struct{}),
	}
}

// Connect 连接到服务器
func (c *Client) Connect() error {
	if c.connected.Load() {
		return nil
	}

	headers := make(http.Header)
	for k, v := range c.config.Headers {
		headers.Set(k, v)
	}

	callback := &callbackImpl{client: c}
	conn, err := c.adapter.Dial(c.config.URL, headers, callback)
	if err != nil {
		c.logger.Error("failed to connect",
			logger.Field{Key: "url", Value: c.config.URL},
			logger.Field{Key: "error", Value: err.Error()},
		)
		return err
	}

	c.conn = conn
	c.connected.Store(true)
	c.reconnectAttempt = 0

	c.logger.Info("connected to server",
		logger.Field{Key: "url", Value: c.config.URL},
		logger.Field{Key: "clientId", Value: c.id},
	)

	// 启动心跳
	if c.config.EnableAutoHeartbeat {
		go c.heartbeatLoop()
	}

	if c.onConnect != nil {
		c.onConnect()
	}

	return nil
}

// Disconnect 断开连接
func (c *Client) Disconnect() error {
	return c.Close()
}

// Close 关闭客户端
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.connected.Store(false)
		close(c.done)

		// 停止心跳
		if c.heartbeatTimer != nil {
			c.heartbeatTimer.Stop()
		}
		select {
		case c.heartbeatStop <- struct{}{}:
		default:
		}

		if c.conn != nil {
			err = c.conn.Close()
		}

		c.logger.Info("client closed", logger.Field{Key: "clientId", Value: c.id})
	})
	return err
}

// Send 发送消息
func (c *Client) Send(eventType string, eventData interface{}) error {
	if !c.connected.Load() {
		return adapter.ErrNotConnected
	}

	data, err := json.Marshal(eventData)
	if err != nil {
		return err
	}

	msg := map[string]interface{}{
		"eventType": eventType,
		"eventData": json.RawMessage(data),
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return c.conn.WriteTimeout(adapter.Text, payload, c.config.WriteTimeout)
}

// SendRaw 发送原始数据
func (c *Client) SendRaw(data []byte) error {
	if !c.connected.Load() {
		return adapter.ErrNotConnected
	}
	return c.conn.WriteTimeout(adapter.Text, data, c.config.WriteTimeout)
}

// RegisterHandler 注册消息处理器
func (c *Client) RegisterHandler(eventType string, handler MessageHandler) {
	c.handlersMu.Lock()
	c.handlers[eventType] = handler
	c.handlersMu.Unlock()
}

// OnConnect 设置连接成功回调
func (c *Client) OnConnect(handler EventHandler) {
	c.onConnect = handler
}

// OnDisconnect 设置断开连接回调
func (c *Client) OnDisconnect(handler EventHandler) {
	c.onDisconnect = handler
}

// OnError 设置错误回调
func (c *Client) OnError(handler func(error)) {
	c.onError = handler
}

// IsConnected 是否已连接
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

// GetID 获取客户端 ID
func (c *Client) GetID() string {
	return c.id
}

// GetSessionID 获取会话 ID
func (c *Client) GetSessionID() string {
	return c.sessionID
}

// SetSessionID 设置会话 ID
func (c *Client) SetSessionID(sessionID string) {
	c.sessionID = sessionID
}

// heartbeatLoop 心跳循环
func (c *Client) heartbeatLoop() {
	c.heartbeatTimer = time.NewTimer(c.config.HeartbeatInterval)
	defer c.heartbeatTimer.Stop()

	for {
		select {
		case <-c.heartbeatTimer.C:
			if c.connected.Load() {
				c.sendPing()
				c.checkPongTimeout()
			}
			c.heartbeatTimer.Reset(c.config.HeartbeatInterval)
		case <-c.heartbeatStop:
			return
		case <-c.done:
			return
		}
	}
}

// sendPing 发送 ping
func (c *Client) sendPing() {
	msg := map[string]interface{}{
		"eventType": "ping",
	}
	data, _ := json.Marshal(msg)
	_ = c.conn.WriteTimeout(adapter.Text, data, c.config.WriteTimeout)
}

// checkPongTimeout 检查 pong 超时
func (c *Client) checkPongTimeout() {
	lastPong := time.Unix(0, c.lastPongTime.Load())
	if !lastPong.IsZero() && time.Since(lastPong) > c.config.HeartbeatTimeout {
		c.logger.Warn("heartbeat timeout, reconnecting")
		c.reconnect()
	}
}

// reconnect 重连
func (c *Client) reconnect() {
	if !c.config.ReconnectEnabled || c.closed.Load() {
		return
	}

	if !c.reconnecting.CompareAndSwap(false, true) {
		return
	}
	defer c.reconnecting.Store(false)

	c.connected.Store(false)
	if c.conn != nil {
		_ = c.conn.Close()
	}

	for c.reconnectAttempt < c.config.MaxReconnectAttempts {
		c.reconnectAttempt++

		c.logger.Info("attempting to reconnect",
			logger.Field{Key: "attempt", Value: c.reconnectAttempt},
			logger.Field{Key: "maxAttempts", Value: c.config.MaxReconnectAttempts},
		)

		time.Sleep(c.config.ReconnectInterval)

		if err := c.Connect(); err == nil {
			c.logger.Info("reconnected successfully")
			return
		}
	}

	c.logger.Error("max reconnect attempts reached, giving up")
}

// handleMessage 处理接收到的消息
func (c *Client) handleMessage(data []byte) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		c.logger.Error("failed to unmarshal message", logger.Field{Key: "error", Value: err.Error()})
		return
	}

	// 处理 pong
	if msg.EventType == "pong" {
		c.lastPongTime.Store(time.Now().UnixNano())
		return
	}

	// 查找并执行处理器
	c.handlersMu.RLock()
	handler, ok := c.handlers[msg.EventType]
	c.handlersMu.RUnlock()

	if ok {
		if err := handler(&msg); err != nil {
			c.logger.Error("message handler error",
				logger.Field{Key: "eventType", Value: msg.EventType},
				logger.Field{Key: "error", Value: err.Error()},
			)
		}
	}
}

// callbackImpl 客户端回调实现
type callbackImpl struct {
	client *Client
}

func (cb *callbackImpl) OnOpen(conn adapter.ClientConn) {
	cb.client.logger.Info("connection opened")
}

func (cb *callbackImpl) OnMessage(conn adapter.ClientConn, op adapter.Opcode, data []byte) {
	if op == adapter.Text {
		cb.client.handleMessage(data)
	}
}

func (cb *callbackImpl) OnClose(conn adapter.ClientConn, err error) {
	cb.client.connected.Store(false)
	cb.client.logger.Info("connection closed", logger.Field{Key: "error", Value: err})

	if cb.client.onDisconnect != nil {
		cb.client.onDisconnect()
	}

	// 尝试重连
	if cb.client.config.ReconnectEnabled && !cb.client.closed.Load() {
		go cb.client.reconnect()
	}
}

func (cb *callbackImpl) OnError(conn adapter.ClientConn, err error) {
	cb.client.logger.Error("connection error", logger.Field{Key: "error", Value: err.Error()})
	if cb.client.onError != nil {
		cb.client.onError(err)
	}
}
