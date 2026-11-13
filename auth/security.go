package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// VerifyHmacSign 验证 HMAC-SHA256 签名
// 签名算法：HMAC-SHA256(ClientType=xxx&ClientId=xxx&Nonce=xxx&Param=sortedEventData, secret)
func VerifyHmacSign(clientType, clientID, nonce, param, secret, sign string) bool {
	if sign == "" {
		return false
	}
	expected := GenerateHmacSign(clientType, clientID, nonce, param, secret)
	return hmac.Equal([]byte(expected), []byte(sign))
}

// GenerateHmacSign 生成 HMAC-SHA256 签名
func GenerateHmacSign(clientType, clientID, nonce, param, secret string) string {
	sorted := SortAndSerializeParam(param)
	data := buildHmacSignData(clientType, clientID, nonce, sorted)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}

func buildHmacSignData(clientType, clientID, nonce, param string) string {
	var sb strings.Builder
	sb.WriteString("ClientType=")
	sb.WriteString(clientType)
	sb.WriteString("&ClientId=")
	sb.WriteString(clientID)
	sb.WriteString("&Nonce=")
	sb.WriteString(nonce)
	sb.WriteString("&Param=")
	sb.WriteString(param)
	return sb.String()
}

// SortAndSerializeParam 按 key 排序后重新序列化 JSON
func SortAndSerializeParam(param string) string {
	if param == "" {
		return ""
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(param), &m); err != nil {
		return param
	}
	return sortAndSerialize(m)
}

func sortAndSerialize(m map[string]interface{}) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v := m[k]
		switch val := v.(type) {
		case map[string]interface{}:
			parts = append(parts, fmt.Sprintf(`%q:%s`, k, sortAndSerialize(val)))
		default:
			b, err := json.Marshal(val)
			if err != nil {
				// Fallback to empty for non-marshalable values
				b = []byte("null")
			}
			parts = append(parts, fmt.Sprintf(`%q:%s`, k, string(b)))
		}
	}
	return "{" + strings.Join(parts, ",") + "}"
}
