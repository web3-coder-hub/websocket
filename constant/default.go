// Package constant defines constants for WebSocket events, states, and error codes.
package constant

const (
	// EventLogin 登录事件
	EventLogin = "login"
	// EventSubscribe 订阅事件
	EventSubscribe = "subscribe"
	// EventUnsubscribe 取消订阅事件
	EventUnsubscribe = "unsubscribe"
	// EventPong 心跳响应事件
	EventPong = "pong"
	// EventPing 心跳请求事件
	EventPing = "ping"
)

// ClientState 客户端状态
type ClientState int32

const (
	StateConnected     ClientState = iota // 已连接（匿名，无认证）
	StatePendingAuth                      // 待认证（mustLogin=true 但握手时无 token）
	StateAuthenticated                    // 已认证（token 有效）
	StateAuthExpired                      // 认证已过期（token 过期）
	StateLoggedIn                         // 已登录（应用层登录状态）
	StateClosed                           // 已关闭
)

const (
	// 认证类型
	AuthTypeNone  = ""      // 不需要认证
	AuthTypeToken = "token" // Token 认证
	AuthTypeSign  = "sign"  // 签名认证
)

const (
	// DefaultClientType 默认客户端类型
	DefaultClientType = "APP"
)
