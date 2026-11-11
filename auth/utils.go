package auth

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/web3-coder-hub/websocket/constant"
)

// GenerateClientId 通过 SHA256(User-Agent + "|" + RemoteIP)[:32] 自动生成客户端标识
func GenerateClientId(r *http.Request) string {
	fingerprint := r.UserAgent() + "|" + ExtractRealIP(r)
	hash := sha256.Sum256([]byte(fingerprint))
	return fmt.Sprintf("%x", hash)[:32]
}

// GenerateSessionId 生成随机会话标识（用于未配置认证器时的匿名会话）
func GenerateSessionId() string {
	timestamp := time.Now().UnixNano()
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d", timestamp)))
	return fmt.Sprintf("%d-%x", timestamp, hash[:4])[:32]
}

// GenerateSessionIdFromClientId 根据 clientId 生成会话标识
func GenerateSessionIdFromClientId(clientId string) string {
	hash := sha256.Sum256([]byte(constant.DefaultClientType + "-" + clientId))
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
