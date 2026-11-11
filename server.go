package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/web3-coder-hub/cache"
	"github.com/web3-coder-hub/logger"
	"github.com/web3-coder-hub/websocket/adapter"
	"github.com/web3-coder-hub/websocket/auth"
	"github.com/web3-coder-hub/websocket/constant"
)

// MessageHandler 消息处理函数类型
type MessageHandler func(client *Client, msg *ClientMessage) error

// DisconnectHandler 断开连接事件处理函数类型
type DisconnectHandler func(client *Client)

type Config struct {
	Port                   int                      `yaml:"port"`
	Path                   string                   `yaml:"path"`
	SessionTimeoutSeconds  int                      `yaml:"session_timeout_seconds"`
	HeartbeatIntervalSecs  int                      `yaml:"heartbeat_interval_seconds"`
	AuthType               string                   `yaml:"auth_type"`                    // 认证类型：空=不认证，token=Token认证，sign=签名认证
	PendingAuthTimeoutSecs int                      `yaml:"pending_auth_timeout_seconds"` // 待认证状态超时时间（秒），默认 10 秒
	EnableSecurity         bool                     `yaml:"enable_security"`
	SecuritySecret         string                   `yaml:"security_secret"`
	CredentialParseFunc    auth.CredentialParseFunc `yaml:"-"` // 凭证解析函数
	Cache                  cache.Cache              `yaml:"-"` // 缓存实例（用于凭证缓存）
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
	adapter      adapter.ServerAdapter
}

// NewServer 创建 WebSocket 服务器
func NewServer(cfg Config, hub *Hub, adapter adapter.ServerAdapter, l logger.Logger) *Server {
	if l == nil {
		l = logger.Default()
	}

	// 根据配置创建认证器
	var authenticator *auth.Authenticator
	if cfg.AuthType == constant.AuthTypeToken {
		// Token 认证需要提供解析函数
		if cfg.CredentialParseFunc != nil {
			strategy := auth.NewTokenAuthStrategy(auth.TokenAuthConfig{
				Cache:     cfg.Cache,
				Logger:    l,
				ParseFunc: cfg.CredentialParseFunc,
			})
			authenticator = auth.NewAuthenticator(cfg.AuthType, strategy, cfg.PendingAuthTimeoutSecs, l)
		} else {
			// 如果没有提供解析函数，创建一个无策略的认证器
			authenticator = auth.NewAuthenticator(cfg.AuthType, nil, cfg.PendingAuthTimeoutSecs, l)
		}
	} else if cfg.AuthType != constant.AuthTypeNone && cfg.AuthType != "" {
		// 其他认证类型（如签名认证），创建无策略的认证器
		authenticator = auth.NewAuthenticator(cfg.AuthType, nil, cfg.PendingAuthTimeoutSecs, l)
	}

	return &Server{
		config:   cfg,
		hub:      hub,
		logger:   l,
		handlers: make(map[string]MessageHandler),
		auth:     authenticator,
		adapter:  adapter,
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
		Addr:              fmt.Sprintf(":%d", s.config.Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second, // 防止 Slowloris 攻击
	}

	if s.logger != nil {
		s.logger.Info("websocket server starting",
			logger.Field{Key: "port", Value: s.config.Port},
			logger.Field{Key: "path", Value: s.config.Path},
		)
	}

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			if s.logger != nil {
				s.logger.Error("websocket server error",
					logger.Field{Key: "error", Value: err.Error()},
				)
			}
		}
	}()

	return nil
}

// Stop 停止服务器
func (s *Server) Stop(ctx context.Context) error {
	if s.logger != nil {
		s.logger.Info("stopping websocket server")
	}
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// handleWebSocket 处理 WebSocket 连接请求
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	connID := uuid.New().String()

	callback := &wsCallback{
		server: s,
		connID: connID,
	}

	conn, err := s.adapter.Upgrade(w, r, callback)
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
	callback.client = client

	// 认证处理
	if err := s.authenticateClient(r, client); err != nil {
		s.logger.Warn("websocket auth failed",
			logger.Field{Key: "remoteAddr", Value: r.RemoteAddr},
			logger.Field{Key: "error", Value: err.Error()},
		)
		auth.SendAuthError(w, err)
		return
	}

	s.hub.Register(client)
	go client.WritePump()
	go client.HeartbeatPump()

	client.SendSuccess("", "Welcome to WebSocket server", nil)

	s.logger.Info("websocket client connected",
		logger.Field{Key: "connId", Value: connID},
		logger.Field{Key: "clientId", Value: client.ClientId},
		logger.Field{Key: "sessionId", Value: client.SessionId},
		logger.Field{Key: "clientIp", Value: client.ClientIp},
	)

	conn.StartReadLoop()
}

// wsCallback 适配器回调实现
type wsCallback struct {
	server *Server
	connID string
	client *Client
}

func (c *wsCallback) OnOpen(_ adapter.ServerConn) {}

func (c *wsCallback) OnMessage(_ adapter.ServerConn, op adapter.Opcode, data []byte) {
	if c.client == nil {
		return
	}

	c.client.UpdateLastPing()

	var msg ClientMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		c.client.SendError("", constant.ErrCodeParamInvalid, "Invalid message format")
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
			c.client.SendError(msg.EventType, constant.ErrCodeSignInvalid, constant.GetErrorMessage(constant.ErrCodeSignInvalid))
			return
		}
	}

	// 待认证超时检查：如果客户端处于待认证状态且已超时，拒绝所有非 LOGIN 事件
	if c.client.IsPendingAuth() && msg.EventType != constant.EventLogin {
		if c.client.IsAuthExpired() {
			c.client.SendError(msg.EventType, constant.ErrCodeAuthFailed, "Authentication timeout, connection will be closed")
			c.client.Close()
			return
		}
		// 待认证期间只允许 LOGIN 事件
		c.client.SendError(msg.EventType, constant.ErrCodeNotLogin, "Please login first")
		return
	}

	// LOGIN 事件：凭证校验
	if msg.EventType == constant.EventLogin {
		if err := c.server.validateCredential(c.client, &msg); err != nil {
			return
		}
	}

	// 检查登录状态（需要认证时）
	requiresAuth := c.server.config.AuthType == constant.AuthTypeToken || c.server.config.AuthType == constant.AuthTypeSign
	if requiresAuth && !c.client.IsLoggedIn() && msg.EventType != constant.EventLogin {
		c.client.SendError(msg.EventType, constant.ErrCodeNotLogin, constant.GetErrorMessage(constant.ErrCodeNotLogin))
		return
	}

	// 认证过期检查：已认证但状态为过期，拒绝非 login 事件
	if msg.EventType != constant.EventLogin && c.client.IsAuthExpired() {
		c.client.SendError(msg.EventType, constant.ErrCodeAuthFailed, "Authentication expired, please re-login")
		return
	}

	// 执行处理器
	handler, ok := c.server.getHandler(msg.EventType)
	if !ok {
		c.client.SendError(msg.EventType, constant.ErrCodeParamInvalid, "Unknown event type")
		return
	}

	if err := handler(c.client, &msg); err != nil {
		c.server.logger.Error("message handler error",
			logger.Field{Key: "eventType", Value: msg.EventType},
			logger.Field{Key: "error", Value: err.Error()},
		)
	}
}

func (c *wsCallback) OnClose(_ adapter.ServerConn, _ error) {
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

// authenticateClient 在 WebSocket 升级后处理客户端认证
func (s *Server) authenticateClient(r *http.Request, client *Client) error {
	// 未配置认证器：生成匿名会话标识
	if s.auth == nil {
		client.SetSessionId(auth.GenerateSessionId())
		return nil
	}

	clientId := auth.GenerateClientId(r)
	clientIp := auth.ExtractRealIP(r)

	// 获取认证策略
	strategy := s.auth.GetStrategy()

	// 设置基本信息
	client.SetClientId(clientId)
	client.SetClientIp(clientIp)

	// 判断是否需要认证
	requiresAuth := s.auth.GetAuthType() == constant.AuthTypeToken || s.auth.GetAuthType() == constant.AuthTypeSign

	// 无需认证：允许匿名连接
	if !requiresAuth {
		client.SetClientType(constant.DefaultClientType)
		client.SetSessionId(auth.GenerateSessionIdFromClientId(clientId))
		client.SetState(int32(constant.StateConnected))
		return nil
	}

	// 需要认证：委托给策略处理
	if strategy != nil {
		authenticated, err := strategy.Authenticate(r, client)
		if err != nil {
			return err
		}
		if authenticated {
			return nil
		}
	}

	// 策略未认证：设置为待认证状态，允许后续通过 LOGIN 事件认证
	client.SetClientType(constant.DefaultClientType)
	client.SetSessionId(auth.GenerateSessionIdFromClientId(clientId))
	client.SetAuthDeadline(time.Now().Add(time.Duration(s.auth.GetPendingAuthTimeout()) * time.Second))
	client.SetState(int32(constant.StatePendingAuth))
	return nil
}

// validateCredential 在 LOGIN 事件中校验凭证并更新客户端认证状态。
// 处理三种情况：
// 1. 待认证状态（StatePendingAuth）：强制要求提供有效凭证并完成认证
// 2. 认证已过期：要求提供新凭证刷新认证
// 3. 认证有效：跳过凭证校验
// 凭证类型取决于认证策略（token/sign 等），从凭证中解析的信息会更新客户端状态
func (s *Server) validateCredential(client *Client, msg *ClientMessage) error {
	if s.auth == nil {
		return nil
	}

	strategy := s.auth.GetStrategy()
	if strategy == nil {
		return nil
	}

	// 待认证状态：必须提供凭证完成认证
	if client.IsPendingAuth() {
		if msg.Token == "" {
			client.SendError(constant.EventLogin, constant.ErrCodeAuthFailed, "Credential is required for authentication")
			return fmt.Errorf("credential required for pending auth")
		}

		if err := strategy.Verify(msg.Token, client); err != nil {
			s.logger.Warn("pending auth credential verification failed",
				logger.Field{Key: "sessionId", Value: client.SessionId},
				logger.Field{Key: "error", Value: err.Error()},
			)
			client.SendError(constant.EventLogin, constant.ErrCodeAuthFailed, "Credential verification failed")
			return err
		}

		s.logger.Info("pending auth completed",
			logger.Field{Key: "sessionId", Value: client.SessionId},
			logger.Field{Key: "userId", Value: client.UserId},
			logger.Field{Key: "clientType", Value: client.ClientType},
		)
		return nil
	}

	// 认证缓存仍有效 → 跳过凭证校验
	if client.IsAuthValid() {
		return nil
	}

	// 过期或未认证 → 要求新凭证
	if msg.Token == "" {
		errMsg := "credential is required"
		if client.IsAuthExpired() {
			errMsg = "Authentication expired, please provide a new credential"
		}
		client.SendError(constant.EventLogin, constant.ErrCodeAuthFailed, errMsg)
		return fmt.Errorf("credential required: auth expired or not set")
	}

	if err := strategy.Verify(msg.Token, client); err != nil {
		s.logger.Warn("login credential verification failed",
			logger.Field{Key: "sessionId", Value: client.SessionId},
			logger.Field{Key: "error", Value: err.Error()},
		)
		client.SendError(constant.EventLogin, constant.ErrCodeAuthFailed, "credential verification failed")
		return err
	}

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
