package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/web3-coder-hub/cache"
	"github.com/web3-coder-hub/logger"
	"github.com/web3-coder-hub/websocket/constant"
)

const (
	tokenHeader       = "Access-Token"
	bearerPrefix      = "Bearer "
	userDataCacheName = "LOGIN_USER"
	defaultClientType = "APP"
)

// JWTPayload JWT 标准 Payload 结构（泛型版本）
// T: content 字段的类型，可以是任意结构
type JWTPayload[T any] struct {
	Content T     `json:"content"` // 自定义业务数据
	Exp     int64 `json:"exp"`     // 过期时间（Unix 秒时间戳，0 表示无过期）
}

// TokenAuthStrategy Token 认证策略（泛型版本）
// JWTUserT: JWT content 中的登录用户信息类型
// CacheUserT: 缓存中存储的用户数据类型
type TokenAuthStrategy[JWTUserT, CacheUserT any] struct {
	cache  cache.Cache
	logger logger.Logger
	// parseTokenFunc 解析 token 并返回认证信息的函数
	// 返回: userId, accountId, clientType, expiry, error
	parseTokenFunc func(token string, cache cache.Cache, logger logger.Logger) (string, string, string, time.Time, error)
}

// CredentialParseFunc token 解析函数类型
type CredentialParseFunc func(token string, cache cache.Cache, logger logger.Logger) (userId, accountId, clientType string, expiry time.Time, err error)

// NewTokenAuthStrategy 创建 Token 认证策略实例
// parseTokenFunc: 自定义的 token 解析函数，如果为 nil 则必须在使用前设置
func NewTokenAuthStrategy(c cache.Cache, l logger.Logger, parseTokenFunc CredentialParseFunc) *TokenAuthStrategy[any, any] {
	if l == nil {
		l = logger.Default()
	}
	var withLogger logger.Logger
	if l != nil {
		withLogger = l.With(logger.Field{Key: "component", Value: "token_auth"})
	}
	return &TokenAuthStrategy[any, any]{
		cache:          c,
		logger:         withLogger,
		parseTokenFunc: parseTokenFunc,
	}
}

// Authenticate 执行 Token 认证逻辑
func (s *TokenAuthStrategy[JWTUserT, CacheUserT]) Authenticate(r *http.Request, client AuthClient) (authenticated bool, err error) {
	token := extractToken(r)
	if token == "" {
		return false, nil // 无 token，返回未认证状态
	}

	if s.parseTokenFunc == nil {
		return false, errors.New("token parse function not set")
	}

	clientId := client.GetClientId()
	clientIp := client.GetClientIp()

	// 解析 Token
	userId, accountId, clientType, _, err := s.parseTokenFunc(token, s.cache, s.logger)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("token validation failed",
				logger.Field{Key: "error", Value: err.Error()},
				logger.Field{Key: "remoteAddr", Value: clientIp},
			)
		}
		return false, errors.New(constant.GetErrorMessage(constant.ErrCodeAuthFailed))
	}

	// 如果 token 中未提供 clientType，使用默认值
	if clientType == "" {
		clientType = defaultClientType
	}

	// 设置认证信息
	client.SetUserId(userId)
	client.SetAccountId(accountId)
	client.SetClientType(clientType)
	client.SetSessionId(sha256Hex(clientType + "-" + clientId))
	client.SetState(int32(constant.StateAuthenticated))

	if s.logger != nil {
		s.logger.Info("websocket auth success",
			logger.Field{Key: "userId", Value: userId},
			logger.Field{Key: "sessionId", Value: sha256Hex(clientType + "-" + clientId)},
			logger.Field{Key: "clientIp", Value: clientIp},
		)
	}

	return true, nil
}

// Verify 校验 JWT Token 并更新客户端认证信息
func (s *TokenAuthStrategy[JWTUserT, CacheUserT]) Verify(credential string, client AuthClient) error {
	if credential == "" {
		return fmt.Errorf("token is required")
	}

	if s.parseTokenFunc == nil {
		return errors.New("token parse function not set")
	}

	userId, accountId, clientType, _, err := s.parseTokenFunc(credential, s.cache, s.logger)
	if err != nil {
		return err
	}

	// 更新客户端认证信息
	if userId != "" {
		client.SetUserId(userId)
	}
	if accountId != "" {
		client.SetAccountId(accountId)
	}
	if clientType != "" {
		client.SetClientType(clientType)
		// 根据新的 ClientType 重新生成 SessionId
		clientId := client.GetClientId()
		client.SetSessionId(sha256Hex(clientType + "-" + clientId))
	}

	// 设置为已认证状态
	client.SetState(int32(constant.StateAuthenticated))

	return nil
}

// extractToken 从请求头中提取 Access-Token，支持 Bearer 前缀
func extractToken(r *http.Request) string {
	h := r.Header.Get(tokenHeader)
	if h == "" {
		return ""
	}
	if strings.HasPrefix(h, bearerPrefix) {
		return h[len(bearerPrefix):]
	}
	return h
}

// ParseJWT 解码 JWT payload（不校验签名）- 泛型版本
// T: JWT content 字段的类型
// 返回: content 数据、exp 时间戳（秒，0 表示无过期）、错误
func ParseJWT[T any](token string) (*T, int64, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, 0, fmt.Errorf("invalid JWT format: expected 3 parts, got %d", len(parts))
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode JWT payload: %w", err)
	}

	var claims JWTPayload[T]
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, 0, fmt.Errorf("failed to parse JWT claims: %w", err)
	}

	return &claims.Content, claims.Exp, nil
}

// ParseJWTWithStringContent 解码 JWT payload（content 为 JSON 字符串）- 泛型版本
// T: 需要从 content 字符串解析的类型
// 返回: 解析后的 content 数据、exp 时间戳（秒，0 表示无过期）、错误
func ParseJWTWithStringContent[T any](token string) (*T, int64, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, 0, fmt.Errorf("invalid JWT format: expected 3 parts, got %d", len(parts))
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode JWT payload: %w", err)
	}

	var claims JWTPayload[string]
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, 0, fmt.Errorf("failed to parse JWT claims: %w", err)
	}

	if claims.Content == "" {
		return nil, 0, fmt.Errorf("JWT content claim is empty")
	}

	var content T
	if err := json.Unmarshal([]byte(claims.Content), &content); err != nil {
		return nil, 0, fmt.Errorf("failed to parse JWT content: %w", err)
	}

	return &content, claims.Exp, nil
}

// ParseUserDataFromCache 从缓存中解析用户数据 - 泛型版本
// T: 缓存中存储的用户数据类型
func ParseUserDataFromCache[T any](ctx context.Context, c cache.Cache, cacheKey string) (*T, error) {
	if c == nil {
		return nil, fmt.Errorf("cache is nil")
	}

	fullKey := userDataCacheName + "::" + cacheKey
	raw, exists, err := c.Get(ctx, fullKey)
	if err != nil {
		return nil, fmt.Errorf("cache lookup failed: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("user data not found in cache")
	}

	var data T
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal user data: %w", err)
	}

	return &data, nil
}
