package constant

const (
	// 客户端错误 4xxx
	ErrCodeParamInvalid = 4001 // 参数校验失败
	ErrCodeNotLogin     = 4002 // 未登录
	ErrCodePermission   = 4003 // 权限不足
	ErrCodeSignInvalid  = 4004 // 签名校验失败
	ErrCodeAuthFailed   = 4401 // 认证失败

	// 服务端错误 5xxx
	ErrCodeInternal         = 5001 // 服务内部错误
	ErrCodeConnClose        = 5005 // 连接已关闭
	ErrCodeConnNotConnected = 5006 // 连接未建立
	ErrCodeConnReadTimeout  = 5007 // 连接读取超时
	ErrCodeConnWriteTimeout = 5008 // 连接写入超时
)

// 错误消息映射
var ErrorMessages = map[int]string{
	ErrCodeParamInvalid:     "Parameter validation failed",
	ErrCodeNotLogin:         "Not logged in",
	ErrCodePermission:       "Permission denied",
	ErrCodeSignInvalid:      "Signature verification failed",
	ErrCodeAuthFailed:       "Authentication failed",
	ErrCodeInternal:         "Internal server error",
	ErrCodeConnClose:        "Connection closed",
	ErrCodeConnNotConnected: "Connection not established",
	ErrCodeConnReadTimeout:  "Connection read timeout",
	ErrCodeConnWriteTimeout: "Connection write timeout",
}

// GetErrorMessage 获取错误消息
func GetErrorMessage(code int) string {
	if msg, ok := ErrorMessages[code]; ok {
		return msg
	}
	return "Unknown error"
}
