package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/web3-coder-hub/logger"
	"github.com/web3-coder-hub/websocket/adapter"
	"github.com/web3-coder-hub/websocket/constant"
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

	// 同步请求配置
	RequestIDField  string `json:"request_id_field"`  // 请求ID字段名，默认 "requestId"
	ResponseIDField string `json:"response_id_field"` // 响应ID字段名，默认 "requestId"
}

// MessageHandler 消息处理器 - 接收解析后的消息数据（map格式）
type MessageHandler func(data map[string]any) error

// handlerEntry 处理器条目
type handlerEntry struct {
	handler        MessageHandler
	eventTypeField string // 该处理器对应的事件类型字段名
}

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
	handlers   map[string]*handlerEntry // 事件处理器映射
	handlersMu sync.RWMutex

	// 事件处理
	onConnect    func()
	onDisconnect func()
	onError      func(error)

	// 重连
	reconnecting     atomic.Bool
	reconnectAttempt int

	// 控制
	done      chan struct{}
	closeOnce sync.Once

	// 同步请求支持
	requestManager *RequestManager
}

// NewClient 创建客户端
func NewClient(cfg Config, adapter adapter.ClientAdapter, l logger.Logger) *Client {
	if cfg.ReconnectInterval == 0 {
		cfg.ReconnectInterval = 5 * time.Second
	}
	if cfg.MaxReconnectAttempts == 0 {
		cfg.MaxReconnectAttempts = 10
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	if cfg.RequestIDField == "" {
		cfg.RequestIDField = "requestId"
	}
	if cfg.ResponseIDField == "" {
		cfg.ResponseIDField = "requestId"
	}

	return &Client{
		config:         cfg,
		adapter:        adapter,
		logger:         l,
		id:             uuid.New().String(),
		handlers:       make(map[string]*handlerEntry),
		done:           make(chan struct{}),
		requestManager: NewRequestManager(),
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

		// 清理所有pending请求
		c.requestManager.ClearAll(errors.New(constant.GetErrorMessage(constant.ErrCodeConnClose)))

		if c.conn != nil {
			err = c.conn.Close()
		}

		c.logger.Info("client closed", logger.Field{Key: "clientId", Value: c.id})
	})
	return err
}

// Send 发送数据（支持map[string]any自动序列化，或[]byte直接发送）
func (c *Client) Send(data any) error {
	if !c.connected.Load() {
		return errors.New(constant.GetErrorMessage(constant.ErrCodeConnNotConnected))
	}

	var payload []byte
	var err error

	switch v := data.(type) {
	case []byte:
		payload = v
	case map[string]any:
		payload, err = json.Marshal(v)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported data type: %T, expected []byte or map[string]any", data)
	}

	return c.conn.WriteTimeout(adapter.Text, payload, c.config.WriteTimeout)
}

// Request 发送同步请求并等待响应
// data 参数是业务请求数据（map格式），方法会自动注入请求ID字段
func (c *Client) Request(ctx context.Context, data map[string]any) (map[string]any, error) {
	if !c.connected.Load() {
		return nil, errors.New(constant.GetErrorMessage(constant.ErrCodeConnNotConnected))
	}

	// 1. 生成请求ID
	requestID := c.requestManager.GenerateRequestID()

	// 2. 注册pending请求
	pr, err := c.requestManager.RegisterRequest(requestID)
	if err != nil {
		return nil, err
	}

	// 确保清理：无论成功或失败，都要从pending列表中移除
	defer func() {
		if err != nil {
			c.requestManager.CancelRequest(requestID)
		}
	}()

	// 3. 创建map副本并注入请求ID（避免修改原始map导致数据竞争）
	requestData := make(map[string]any, len(data)+1)
	for k, v := range data {
		requestData[k] = v
	}
	requestData[c.config.RequestIDField] = requestID

	// 4. 序列化
	payload, err := json.Marshal(requestData)
	if err != nil {
		return nil, err
	}

	// 5. 发送消息
	err = c.conn.WriteTimeout(adapter.Text, payload, c.config.WriteTimeout)
	if err != nil {
		return nil, err
	}

	// 6. 等待响应
	respData, err := c.requestManager.WaitForResponse(ctx, pr)
	if err != nil {
		return nil, err
	}

	// 7. 反序列化响应为 map
	var respMap map[string]any
	if err := json.Unmarshal(respData, &respMap); err != nil {
		return nil, err
	}

	return respMap, nil
}

// RegisterHandler 注册事件处理器
// eventType: 事件类型值（如 "trade", "kline"）
// eventTypeField: 事件类型字段名（如 "e", "event", "type"）
// handler: 事件处理函数
func (c *Client) RegisterHandler(eventType string, eventTypeField string, handler MessageHandler) {
	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()

	c.handlers[eventType] = &handlerEntry{
		handler:        handler,
		eventTypeField: eventTypeField,
	}
}

// OnConnect 设置连接成功回调
func (c *Client) OnConnect(handler func()) {
	c.onConnect = handler
}

// OnDisconnect 设置断开连接回调
func (c *Client) OnDisconnect(handler func()) {
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
	// 1. 尝试解析为JSON
	var msgData any
	err := json.Unmarshal(data, &msgData)

	// 2. 解析失败，作为原始文本处理
	if err != nil {
		c.handleNonMapMessage(string(data))
		return
	}

	// 3. 解析成功，判断是否为map
	msgMap, isMap := msgData.(map[string]any)
	if !isMap {
		// 非map的JSON（字符串、数组等）
		c.handleNonMapMessage(msgData)
		return
	}

	// 4. 检查是否是同步请求的响应
	if responseID, ok := msgMap[c.config.ResponseIDField].(string); ok && responseID != "" {
		if c.requestManager.NotifyResponse(responseID, data) {
			return
		}
	}

	// 5. 遍历处理器，查找匹配的事件类型
	c.handlersMu.RLock()
	handlers := c.handlers
	c.handlersMu.RUnlock()

	for eventType, entry := range handlers {
		// 支持嵌套字段访问（如 "event.account" 或 "channel.type"）
		if fieldValue := getNestedField(msgMap, entry.eventTypeField); fieldValue == eventType {
			if err := entry.handler(msgMap); err != nil {
				c.logger.Error("event handler error",
					logger.Field{Key: "eventType", Value: eventType},
					logger.Field{Key: "error", Value: err.Error()},
				)
			}
			return
		}
	}

	// 6. 没有匹配的处理器
	c.logger.Debug("no handler matched for message")
}

// getNestedField 从map中获取嵌套字段的值（支持 "a.b.c" 格式）
// 返回字符串类型的值，如果字段不存在或类型不匹配则返回空字符串
func getNestedField(m map[string]any, fieldPath string) string {
	if fieldPath == "" {
		return ""
	}

	// 分割字段路径
	parts := strings.Split(fieldPath, ".")
	var current any = m

	// 逐层访问
	for _, part := range parts {
		currentMap, ok := current.(map[string]any)
		if !ok {
			return ""
		}

		current, ok = currentMap[part]
		if !ok {
			return ""
		}
	}

	// 最终值必须是字符串类型
	if str, ok := current.(string); ok {
		return str
	}

	return ""
}

// handleNonMapMessage 处理非map类型的消息（原始文本或JSON非对象类型）
func (c *Client) handleNonMapMessage(rawData any) {
	wrappedMsg := map[string]any{
		"_raw": rawData,
	}

	c.handlersMu.RLock()
	handlers := c.handlers
	c.handlersMu.RUnlock()

	for _, entry := range handlers {
		if err := entry.handler(wrappedMsg); err != nil {
			c.logger.Error("event handler error for non-map message",
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

	// 通知所有pending请求连接已关闭
	cb.client.requestManager.ClearAll(errors.New(constant.GetErrorMessage(constant.ErrCodeConnClose)))

	if err != nil {
		cb.client.logger.Info("connection closed", logger.Field{Key: "error", Value: err.Error()})
	} else {
		cb.client.logger.Info("connection closed")
	}

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
