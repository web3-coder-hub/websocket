package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/web3-coder-hub/websocket/auth"
	"github.com/web3-coder-hub/websocket/constant"

	"github.com/antlabs/quickws"
	"github.com/google/uuid"

	"github.com/web3-coder-hub/logger"
)

// MessageHandler 消息处理函数类型
type MessageHandler func(client *Client, msg *ClientMessage) error

// DisconnectHandler 断开连接事件处理函数类型
type DisconnectHandler func(client *Client)

// Config WebSocket配置
type Config struct {
	Port                  int    `yaml:"port"`
	Path                  string `yaml:"path"`
	SessionTimeoutSeconds int    `yaml:"session_timeout_seconds"`
	HeartbeatIntervalSecs int    `yaml:"heartbeat_interval_seconds"`
	MustLogin             bool   `yaml:"must_login"`
	EnableSecurity        bool   `yaml:"enable_security"`
	SecuritySecret        string `yaml:"security_secret"`
}

// Server WebSocket 服务器
type Server struct {
	config       Config
	hub          *Hub
	logger       logger.Logger
	handlers     map[string]MessageHandler
	mu           sync.RWMutex
	onDisconnect DisconnectHandler
	httpServer   *http.Server
	auth         *auth.Authenticator
}

// NewServer 创建 WebSocket 服务器
func NewServer(cfg Config, hub *Hub, l logger.Logger) *Server {
	return &Server{
		config:   cfg,
		hub:      hub,
		logger:   l,
		handlers: make(map[string]MessageHandler),
	}
}

// SetAuthenticator 设置连接层认证器，设置后所有 WebSocket 升级请求都需要通过认证
func (s *Server) SetAuthenticator(a *auth.Authenticator) {
	s.auth = a
}

// SetDisconnectHandler 设置断开连接回调
func (s *Server) SetDisconnectHandler(handler DisconnectHandler) {
	s.onDisconnect = handler
}

// RegisterHandler 注册消息处理器
func (s *Server) RegisterHandler(eventType string, handler MessageHandler) {
	s.mu.Lock()
	s.handlers[eventType] = handler
	s.mu.Unlock()
}

// getHandler 获取消息处理器
func (s *Server) getHandler(eventType string) (MessageHandler, bool) {
	s.mu.RLock()
	handler, ok := s.handlers[eventType]
	s.mu.RUnlock()
	return handler, ok
}

// Start 启动服务器
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc(s.config.Path, s.handleWebSocket)

	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.config.Port),
		Handler: mux,
	}

	s.logger.Info("websocket server starting",
		logger.Field{Key: "port", Value: s.config.Port},
		logger.Field{Key: "path", Value: s.config.Path},
	)

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("websocket server error",
				logger.Field{Key: "error", Value: err.Error()},
			)
		}
	}()

	return nil
}

// Stop 停止服务器
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("stopping websocket server")
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// handleWebSocket 处理 WebSocket 连接请求
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	authResult, err := s.auth.Authenticate(r)
	if err != nil {
		s.logger.Warn("websocket auth failed",
			logger.Field{Key: "remoteAddr", Value: r.RemoteAddr},
			logger.Field{Key: "error", Value: err.Error()},
		)
		auth.SendAuthError(w, err)
		return
	}

	connID := uuid.New().String()

	callback := &wsCallback{
		server: s,
		connID: connID,
	}

	conn, err := quickws.Upgrade(w, r,
		quickws.WithServerCallback(callback),
	)
	if err != nil {
		s.logger.Error("websocket upgrade failed",
			logger.Field{Key: "error", Value: err.Error()},
		)
		http.Error(w, "websocket upgrade failed", http.StatusBadRequest)
		return
	}

	heartbeatInterval := time.Duration(s.config.HeartbeatIntervalSecs) * time.Second
	wsPath := path.Base(s.config.Path)
	client := NewClient(connID, conn, s.hub, s.hub.config, s.logger, heartbeatInterval, wsPath)
	client.RemoteAddr = r.RemoteAddr

	client.SessionId = authResult.UserSessionId
	client.ClientId = authResult.ClientId
	client.ClientType = authResult.ClientType
	client.RemoteAddr = authResult.ClientIp
	client.UserId = authResult.UserId
	client.AccountId = authResult.AccountId
	client.IsBmsLogin = authResult.IsBmsLogin
	client.AuthExpiry = authResult.TokenExpiry
	callback.client = client

	s.hub.Register(client)
	go client.WritePump()
	go client.HeartbeatPump()

	// 自动登录：握手阶段已获取 accountId 时，自动触发 login 流程调用订阅中心
	if authResult.AccountId != "" {
		if handler, ok := s.getHandler(constant.EventLogin); ok {
			loginData, _ := json.Marshal(&LoginEventData{
				UserId:        authResult.UserId,
				AccountId:     authResult.AccountId,
				AccountIds:    authResult.AccountIds,
				UserSessionId: authResult.UserSessionId,
				ClientId:      authResult.ClientId,
			})
			_ = handler(client, &ClientMessage{
				EventType: constant.EventLogin,
				EventData: loginData,
			})
		}
	}

	client.SendSuccess("", "Welcome to WebSocket server", nil)

	s.logger.Info("websocket client connected",
		logger.Field{Key: "connId", Value: connID},
		logger.Field{Key: "clientId", Value: authResult.ClientId},
		logger.Field{Key: "sessionId", Value: authResult.UserSessionId},
		logger.Field{Key: "clientIp", Value: authResult.ClientIp},
	)

	conn.StartReadLoop()
}

// wsCallback quickws 回调实现
type wsCallback struct {
	server *Server
	connID string
	client *Client
}

func (c *wsCallback) OnOpen(_ *quickws.Conn) {}

func (c *wsCallback) OnMessage(_ *quickws.Conn, _ quickws.Opcode, data []byte) {
	if c.client == nil {
		return
	}

	c.client.UpdateLastPing()

	var msg ClientMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		c.client.SendError("", ErrCodeParamInvalid, "Invalid message format")
		return
	}

	// 处理心跳：收到客户端 ping 后重置服务端心跳计时器
	if msg.EventType == constant.EventPing || msg.EventType == constant.EventPong {
		c.client.ResetHeartbeat()
		c.client.SendSuccess(constant.EventPong, constant.EventPong, nil)
		return
	}

	// 消息签名校验（非心跳事件，开启 security 时）
	if c.server.config.EnableSecurity {
		if !c.server.verifyMessageSign(c.client, &msg) {
			c.client.SendError(msg.EventType, ErrCodeSignInvalid, GetErrorMessage(ErrCodeSignInvalid))
			return
		}
	}

	// LOGIN 事件：JWT Token 二次校验 + accountId 覆盖
	if msg.EventType == constant.EventLogin {
		if err := c.server.validateLoginToken(c.client, &msg); err != nil {
			return
		}
	}

	// 检查登录状态
	if c.server.config.MustLogin && !c.client.IsLoggedIn() && msg.EventType != constant.EventLogin {
		c.client.SendError(msg.EventType, ErrCodeNotLogin, GetErrorMessage(ErrCodeNotLogin))
		return
	}

	// 认证过期检查：已登录但 Token 已过期，拒绝非 login 事件，提示客户端重新 login
	if msg.EventType != constant.EventLogin && !c.client.AuthExpiry.IsZero() && time.Now().After(c.client.AuthExpiry) {
		c.client.SendError(msg.EventType, ErrCodeAuthFailed, "Authentication expired, please re-login")
		return
	}

	// 执行处理器
	handler, ok := c.server.getHandler(msg.EventType)
	if !ok {
		c.client.SendError(msg.EventType, ErrCodeParamInvalid, "Unknown event type")
		return
	}

	if err := handler(c.client, &msg); err != nil {
		c.server.logger.Error("message handler error",
			logger.Field{Key: "eventType", Value: msg.EventType},
			logger.Field{Key: "error", Value: err.Error()},
		)
	}
}

func (c *wsCallback) OnClose(_ *quickws.Conn, _ error) {
	if c.client == nil {
		return
	}

	if c.server.onDisconnect != nil {
		c.server.onDisconnect(c.client)
	}

	c.client.Close()

	c.server.logger.Info("websocket client disconnected",
		logger.Field{Key: "connId", Value: c.connID},
		logger.Field{Key: "userId", Value: c.client.UserId},
	)
}

// validateLoginToken 在 LOGIN 事件中校验认证状态。
// 如果握手/上次 login 的认证缓存仍有效（JWT 未过期），直接跳过 Token 校验；
// 如果已过期或从未认证，要求客户端携带新 Token 并刷新缓存。
func (s *Server) validateLoginToken(client *Client, msg *ClientMessage) error {
	if s.auth == nil {
		return nil
	}

	// 认证缓存仍有效 → 跳过 Token 校验
	if client.IsAuthValid() {
		return nil
	}

	// 过期或未认证 → 要求新 Token
	if msg.Token == "" {
		errMsg := "token is required"
		if !client.AuthExpiry.IsZero() {
			errMsg = "Authentication expired, please provide a new token"
		}
		client.SendError(constant.EventLogin, ErrCodeAuthFailed, errMsg)
		return fmt.Errorf("token required: auth expired or not set")
	}

	result, err := s.auth.ValidateToken(msg.Token)
	if err != nil {
		s.logger.Warn("login token validation failed",
			logger.Field{Key: "sessionId", Value: client.SessionId},
			logger.Field{Key: "error", Value: err.Error()},
		)
		client.SendError(constant.EventLogin, ErrCodeAuthFailed, "token validation failed")
		return err
	}

	// 刷新 Client 上的认证缓存
	if result.UserId != "" {
		client.UserId = result.UserId
	}
	if result.AccountId != "" {
		client.AccountId = result.AccountId
	}
	client.AuthExpiry = result.TokenExpiry

	return nil
}

// verifyMessageSign 校验消息的 HMAC-SHA256 签名
func (s *Server) verifyMessageSign(client *Client, msg *ClientMessage) bool {
	eventDataStr := ""
	if msg.EventData != nil {
		eventDataStr = string(msg.EventData)
	}
	return auth.VerifyHmacSign(
		client.ClientType, client.ClientId,
		msg.Nonce, eventDataStr,
		s.config.SecuritySecret, msg.Sign,
	)
}
