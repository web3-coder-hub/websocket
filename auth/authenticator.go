package auth

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/web3-coder-hub/cache"
	"github.com/web3-coder-hub/logger"
	"github.com/web3-coder-hub/websocket/constant"
)

// AuthStrategy 认证策略接口
type AuthStrategy interface {
	// Authenticate 执行认证逻辑，返回是否成功认证
	Authenticate(r *http.Request, client AuthClient) (authenticated bool, err error)
	// Verify 验证凭证并更新客户端认证信息（用于 LOGIN 事件）
	Verify(credential string, client AuthClient) error
}

// Authenticator WebSocket 连接层认证器
type Authenticator struct {
	strategy               AuthStrategy
	authType               string // 认证类型：空=不认证，token=Token认证，sign=签名认证
	pendingAuthTimeoutSecs int    // 待认证状态的超时时间（秒）
	logger                 logger.Logger
}

// AuthClient 定义可被认证的对象接口
// 实现此接口的类型可以被 Authenticator 进行认证和授权操作
type AuthClient interface {
	// 设置认证信息
	SetSessionId(sessionId string)
	SetUserId(userId string)
	SetAccountId(accountId string)
	SetClientId(clientId string)
	SetClientIp(clientIp string)
	SetClientType(clientType string)
	SetAccountIds(accountIds string)
	SetAuthDeadline(deadline time.Time)

	// 状态管理
	SetState(state int32)
	GetState() int32

	// 获取认证信息
	GetClientId() string
	GetClientType() string
	GetClientIp() string
}

// NewAuthenticator 创建认证器实例
func NewAuthenticator(c cache.Cache, authType string, pendingAuthTimeoutSecs int, l logger.Logger) *Authenticator {
	if pendingAuthTimeoutSecs <= 0 {
		pendingAuthTimeoutSecs = 10 // 默认 10 秒
	}

	if l == nil {
		l = logger.Default()
	}

	var withLogger logger.Logger
	if l != nil {
		withLogger = l.With(logger.Field{Key: "component", Value: "authenticator"})
	}

	// 根据认证类型创建对应的策略
	var strategy AuthStrategy
	switch authType {
	case constant.AuthTypeToken:
		strategy = NewTokenAuthStrategy(c, l)
	case constant.AuthTypeSign:
		// TODO: 实现签名认证策略
		strategy = nil
	default:
		strategy = nil // 无需认证
	}

	return &Authenticator{
		strategy:               strategy,
		authType:               authType,
		pendingAuthTimeoutSecs: pendingAuthTimeoutSecs,
		logger:                 withLogger,
	}
}

// AuthError 认证错误，包含 HTTP 状态码和业务错误码
type AuthError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *AuthError) Error() string {
	return e.Message
}

// GetStrategy 获取认证策略
func (a *Authenticator) GetStrategy() AuthStrategy {
	if a == nil {
		return nil
	}
	return a.strategy
}

// GetAuthType 获取认证类型
func (a *Authenticator) GetAuthType() string {
	if a == nil {
		return ""
	}
	return a.authType
}

// GetPendingAuthTimeout 获取待认证超时时间（秒）
func (a *Authenticator) GetPendingAuthTimeout() int {
	if a == nil {
		return 0
	}
	return a.pendingAuthTimeoutSecs
}

// SendAuthError 向客户端写入认证失败的 HTTP JSON 响应
func SendAuthError(w http.ResponseWriter, err error) {
	authErr, ok := err.(*AuthError)
	if !ok {
		authErr = &AuthError{StatusCode: http.StatusInternalServerError, Code: "HANDSHAKE_ERROR", Message: "Internal server error"}
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(authErr.StatusCode)

	body := map[string]interface{}{
		"success":   false,
		"code":      authErr.Code,
		"message":   authErr.Message,
		"timestamp": time.Now().UnixMilli(),
	}
	_ = json.NewEncoder(w).Encode(body)
}
