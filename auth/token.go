package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

var (
	errTokenInvalid = &AuthError{StatusCode: http.StatusUnauthorized, Code: "TOKEN_INVALID", Message: "Invalid or expired access token"}
)

// jwtPayload JWT 标准 Payload 结构
type jwtPayload struct {
	Content string `json:"content"` // 自定义业务数据（JSON 字符串）
	Exp     int64  `json:"exp"`     // 过期时间（Unix 秒时间戳，0 表示无过期）
}

// loginUser JWT content 字段中的用户信息
type loginUser struct {
	DataCacheKey string `json:"dataCacheKey"` // Redis 缓存键（用于查询完整用户数据）
	ClientType   string `json:"clientType"`   // 客户端类型（APP/WEB/BMS 等）
}

// userData Redis 中存储的用户数据
type userData struct {
	UserId interface{} `json:"userId"` // 用户 ID（可能是 string 或 number）
	ActId  interface{} `json:"actId"`  // 账户 ID（可能是 string 或 number）
}

// TokenAuthStrategy Token 认证策略
type TokenAuthStrategy struct {
	cache  cache.Cache
	logger logger.Logger
}

// NewTokenAuthStrategy 创建 Token 认证策略实例
func NewTokenAuthStrategy(c cache.Cache, l logger.Logger) *TokenAuthStrategy {
	if l == nil {
		l = logger.Default()
	}
	var withLogger logger.Logger
	if l != nil {
		withLogger = l.With(logger.Field{Key: "component", Value: "token_auth"})
	}
	return &TokenAuthStrategy{
		cache:  c,
		logger: withLogger,
	}
}

// Authenticate 执行 Token 认证逻辑
func (s *TokenAuthStrategy) Authenticate(r *http.Request, client AuthClient) (authenticated bool, err error) {
	token := extractToken(r)
	if token == "" {
		return false, nil // 无 token，返回未认证状态
	}

	clientId := client.GetClientId()
	clientIp := client.GetClientIp()

	// 解析 Token
	userId, accountId, clientType, _, err := s.parseToken(token)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("token validation failed",
				logger.Field{Key: "error", Value: err.Error()},
				logger.Field{Key: "remoteAddr", Value: clientIp},
			)
		}
		return false, errTokenInvalid
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
func (s *TokenAuthStrategy) Verify(credential string, client AuthClient) error {
	if credential == "" {
		return fmt.Errorf("token is required")
	}

	userId, accountId, clientType, _, err := s.parseToken(credential)
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

// parseToken 解析 JWT 并通过 Redis 查询用户数据
func (s *TokenAuthStrategy) parseToken(token string) (userId, accountId, clientType string, expiry time.Time, err error) {
	lu, exp, err := parseJWT(token)
	if err != nil {
		return "", "", "", time.Time{}, err
	}

	if exp > 0 {
		expiry = time.Unix(exp, 0)
	}

	clientType = lu.ClientType

	if s.cache == nil || lu.DataCacheKey == "" {
		return "", "", clientType, expiry, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cacheKey := userDataCacheName + "::" + lu.DataCacheKey
	raw, exists, cacheErr := s.cache.Get(ctx, cacheKey)
	if cacheErr != nil {
		return "", "", "", time.Time{}, fmt.Errorf("cache lookup failed: %w", cacheErr)
	}
	if !exists {
		return "", "", "", time.Time{}, fmt.Errorf("user data not found in cache, token may be expired")
	}

	var ud userData
	if err := json.Unmarshal(raw, &ud); err != nil {
		s.logger.Warn("failed to unmarshal UserData from cache",
			logger.Field{Key: "cacheKey", Value: cacheKey},
			logger.Field{Key: "error", Value: err.Error()},
		)
		return "", "", clientType, expiry, nil
	}

	userId = toString(ud.UserId)
	accountId = toString(ud.ActId)
	return userId, accountId, clientType, expiry, nil
}

// parseJWT 解码 JWT payload（不校验签名），返回 loginUser 和 exp 时间戳（秒，0 表示无过期）
func parseJWT(token string) (*loginUser, int64, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, 0, fmt.Errorf("invalid JWT format: expected 3 parts, got %d", len(parts))
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode JWT payload: %w", err)
	}

	var claims jwtPayload
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, 0, fmt.Errorf("failed to parse JWT claims: %w", err)
	}

	if claims.Content == "" {
		return nil, 0, fmt.Errorf("JWT content claim is empty")
	}

	var lu loginUser
	if err := json.Unmarshal([]byte(claims.Content), &lu); err != nil {
		return nil, 0, fmt.Errorf("failed to parse JWT content: %w", err)
	}

	return &lu, claims.Exp, nil
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
