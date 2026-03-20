package auth

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/web3-coder-hub/cache"
	"github.com/web3-coder-hub/logger"
)

const (
	tokenHeader      = "Access-Token"
	clientIDHeader   = "Client-ID"
	clientTypeHeader = "Client-Type"
	accountIdsHeader = "Account-Ids"
	bearerPrefix     = "Bearer "

	defaultClientType = "APP"

	// cache 缓存名称
	userDataCacheName = "User_Data"
)

// Authenticator WebSocket 连接层认证器
// 负责在 WebSocket 握手阶段验证 JWT Token，通过缓存读取用户数据，
type Authenticator struct {
	cache     cache.Cache
	logger    logger.Logger
	mustLogin bool
}

// NewAuthenticator 创建认证器实例
func NewAuthenticator(c cache.Cache, mustLogin bool, l logger.Logger) *Authenticator {
	return &Authenticator{
		cache:     c,
		mustLogin: mustLogin,
		logger:    l.With(logger.Field{Key: "component", Value: "authenticator"}),
	}
}

// AuthResult 认证成功后返回的完整用户数据
type AuthResult struct {
	UserSessionId string    // MD5(clientType + "-" + clientId)
	UserId        string    // 用户ID，来自 Redis UserData
	AccountId     string    // 账户ID，来自 Redis UserData
	ClientId      string    // 客户端标识
	ClientIp      string    // 客户端真实 IP
	ClientType    string    // 客户端类型（APP/WEB/BMS 等）
	IsBmsLogin    bool      // 是否 BMS 后台登录
	AccountIds    string    // 多账户 ID 列表
	TokenExpiry   time.Time // JWT 过期时间，零值表示无 Token 或 Token 中无 exp
}

// jwtPayload JWT payload 结构，上游认证系统将用户信息序列化为 content 字段
type jwtPayload struct {
	Content string `json:"content"`
	Exp     int64  `json:"exp,omitempty"`
	Iat     int64  `json:"iat,omitempty"`
}

// loginUser 从 JWT content 字段反序列化的用户身份信息
type loginUser struct {
	Name         string `json:"name"`
	DataCacheKey string `json:"dataCacheKey"`
	AuthCacheKey string `json:"authCacheKey"`
}

// userData Redis 缓存中的用户数据
type userData struct {
	UserId     interface{} `json:"userId"`
	ActId      interface{} `json:"actId"`
	IsBmsLogin interface{} `json:"isBmsLogin"`
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

var (
	errTokenMissing = &AuthError{StatusCode: http.StatusUnauthorized, Code: "AUTH_TOKEN_MISSING", Message: "Access-Token is required"}
	errTokenInvalid = &AuthError{StatusCode: http.StatusUnauthorized, Code: "AUTH_TOKEN_INVALID", Message: "Access-Token is invalid or expired"}
)

// Authenticate 在 WebSocket 升级前验证 HTTP 请求。
// 支持 nil receiver：当未配置 Authenticator 时，从请求中提取 clientId 等信息生成匿名身份标识，
func (a *Authenticator) Authenticate(r *http.Request) (*AuthResult, error) {
	clientType := extractParam(r, clientTypeHeader, defaultClientType)
	clientId := extractClientId(r)
	clientIp := extractRealIP(r)
	accountIds := extractParam(r, accountIdsHeader, "")

	token := ""
	if a != nil {
		token = extractToken(r)
	}

	// nil receiver / mustLogin=false 且无 Token：允许匿名连接
	if token == "" {
		if a != nil && a.mustLogin {
			return nil, errTokenMissing
		}
		return &AuthResult{
			UserSessionId: md5Hex(clientType + "-" + clientId),
			ClientId:      clientId,
			ClientIp:      clientIp,
			ClientType:    clientType,
			AccountIds:    accountIds,
		}, nil
	}

	userId, accountId, isBmsLogin, expiry, err := a.parseToken(token)
	if err != nil {
		a.logger.Warn("token validation failed",
			logger.Field{Key: "error", Value: err.Error()},
			logger.Field{Key: "remoteAddr", Value: clientIp},
		)
		return nil, errTokenInvalid
	}

	result := &AuthResult{
		UserSessionId: md5Hex(clientType + "-" + clientId),
		UserId:        userId,
		AccountId:     accountId,
		ClientId:      clientId,
		ClientIp:      clientIp,
		ClientType:    clientType,
		IsBmsLogin:    isBmsLogin || strings.EqualFold(clientType, "BMS"),
		AccountIds:    accountIds,
		TokenExpiry:   expiry,
	}

	a.logger.Info("websocket auth success",
		logger.Field{Key: "userId", Value: result.UserId},
		logger.Field{Key: "sessionId", Value: result.UserSessionId},
		logger.Field{Key: "clientIp", Value: clientIp},
	)

	return result, nil
}

// ValidateToken 校验 JWT Token 并通过 Redis 获取用户数据。
// 用于 LOGIN 事件中的二次身份校验，与握手阶段共享同一套 JWT + Redis 逻辑。
func (a *Authenticator) ValidateToken(token string) (*AuthResult, error) {
	if a == nil {
		return nil, fmt.Errorf("authenticator not configured")
	}
	if token == "" {
		return nil, fmt.Errorf("token is required")
	}

	userId, accountId, isBmsLogin, expiry, err := a.parseToken(token)
	if err != nil {
		return nil, err
	}

	return &AuthResult{
		UserId:      userId,
		AccountId:   accountId,
		IsBmsLogin:  isBmsLogin,
		TokenExpiry: expiry,
	}, nil
}

// parseToken 解析 JWT 并通过 Redis 查询用户数据。
// 返回 userId/accountId/isBmsLogin 以及 JWT 过期时间。
func (a *Authenticator) parseToken(token string) (userId, accountId string, isBmsLogin bool, expiry time.Time, err error) {
	lu, exp, err := parseJWT(token)
	if err != nil {
		return "", "", false, time.Time{}, err
	}

	if exp > 0 {
		expiry = time.Unix(exp, 0)
	}

	if a.cache == nil || lu.DataCacheKey == "" {
		return "", "", false, expiry, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cacheKey := userDataCacheName + "::" + lu.DataCacheKey
	raw, exists, cacheErr := a.cache.Get(ctx, cacheKey)
	if cacheErr != nil {
		return "", "", false, time.Time{}, fmt.Errorf("cache lookup failed: %w", cacheErr)
	}
	if !exists {
		return "", "", false, time.Time{}, fmt.Errorf("user data not found in cache, token may be expired")
	}

	var ud userData
	if err := json.Unmarshal(raw, &ud); err != nil {
		a.logger.Warn("failed to unmarshal UserData from cache",
			logger.Field{Key: "cacheKey", Value: cacheKey},
			logger.Field{Key: "error", Value: err.Error()},
		)
		return "", "", false, expiry, nil
	}

	userId = toString(ud.UserId)
	accountId = toString(ud.ActId)
	if bms, ok := ud.IsBmsLogin.(bool); ok && bms {
		isBmsLogin = true
	}
	return userId, accountId, isBmsLogin, expiry, nil
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

//  参数提取

// extractToken 从请求头或 URL 查询参数中提取 Access-Token，请求头优先，支持 Bearer 前缀
func extractToken(r *http.Request) string {
	if h := r.Header.Get(tokenHeader); h != "" {
		if strings.HasPrefix(h, bearerPrefix) {
			return h[len(bearerPrefix):]
		}
		return h
	}
	return r.URL.Query().Get(tokenHeader)
}

// extractClientId 从请求头或查询参数中提取 Client-ID，
// 都未提供时通过 MD5(User-Agent + "|" + RemoteIP) 自动生成
func extractClientId(r *http.Request) string {
	if h := r.Header.Get(clientIDHeader); h != "" {
		return h
	}
	if q := r.URL.Query().Get(clientIDHeader); q != "" {
		return q
	}

	fingerprint := r.UserAgent() + "|" + extractRealIP(r)
	return md5Hex(fingerprint)
}

// extractParam 从请求头或查询参数中提取指定参数，都为空时返回 defaultVal
func extractParam(r *http.Request, name, defaultVal string) string {
	if h := r.Header.Get(name); h != "" {
		return h
	}
	if q := r.URL.Query().Get(name); q != "" {
		return q
	}
	return defaultVal
}

// extractRealIP 提取客户端真实 IP，支持反向代理头
// 优先级：X-Forwarded-For > X-Real-IP > Proxy-Client-IP > WL-Proxy-Client-IP > RemoteAddr
func extractRealIP(r *http.Request) string {
	for _, header := range []string{"X-Forwarded-For", "X-Real-IP", "Proxy-Client-IP", "WL-Proxy-Client-IP"} {
		if val := r.Header.Get(header); val != "" {
			// X-Forwarded-For 可能包含多个 IP（逗号分隔），取第一个
			if idx := strings.IndexByte(val, ','); idx > 0 {
				val = val[:idx]
			}
			ip := strings.TrimSpace(val)
			if ip != "" && ip != "unknown" {
				return ip
			}
		}
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

//  JWT 解析

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

//  工具函数

// md5Hex 计算 MD5 并返回 32 位小写十六进制字符串
func md5Hex(s string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(s)))
}

// toString 将 interface{} 安全转为字符串（兼容 JSON 中数字/字符串类型）
func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case json.Number:
		return val.String()
	default:
		return fmt.Sprintf("%v", val)
	}
}
