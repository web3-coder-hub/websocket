package auth

import (
	"crypto/sha256"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/web3-coder-hub/websocket/constant"
)

// GenerateClientID 通过 SHA256(User-Agent + "|" + RemoteIP)[:32] 自动生成客户端标识
func GenerateClientID(r *http.Request) string {
	fingerprint := r.UserAgent() + "|" + ExtractRealIP(r)
	hash := sha256.Sum256([]byte(fingerprint))
	return fmt.Sprintf("%x", hash)[:32]
}

// GenerateSessionID 生成随机会话标识（用于未配置认证器时的匿名会话）
func GenerateSessionID() string {
	timestamp := time.Now().UnixNano()
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d", timestamp)))
	sessionID := fmt.Sprintf("%d-%x", timestamp, hash[:4])
	if len(sessionID) > 32 {
		return sessionID[:32]
	}
	return sessionID
}

// GenerateSessionIDFromClientID 根据 clientID 生成会话标识
func GenerateSessionIDFromClientID(clientID string) string {
	hash := sha256.Sum256([]byte(constant.DefaultClientType + "-" + clientID))
	return fmt.Sprintf("%x", hash)[:32]
}

// ExtractHeaderOnly 仅从请求头中提取指定参数，不支持查询参数
// 用于敏感信息（如 Account-Ids）以避免在 URL 中暴露
func ExtractHeaderOnly(r *http.Request, name, defaultVal string) string {
	if h := r.Header.Get(name); h != "" {
		return h
	}
	return defaultVal
}

// ExtractRealIP 提取客户端真实 IP，支持反向代理头
// 优先级：X-Forwarded-For > X-Real-IP > Proxy-Client-IP > WL-Proxy-Client-IP > RemoteAddr
func ExtractRealIP(r *http.Request) string {
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

// sha256Hex 计算 SHA256 并返回前 32 位小写十六进制字符串（用于会话 ID 生成）
// #nosec G401 - SHA256 用于非加密场景的会话 ID 生成，不需要更强的加密算法
func sha256Hex(s string) string {
	hash := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", hash)[:32]
}
