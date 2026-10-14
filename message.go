package websocket

import (
	"encoding/json"
	"time"
)

// ClientMessage 客户端发送的消息结构
type ClientMessage struct {
	EventType string          `json:"eventType"`       // 事件类型
	EventData json.RawMessage `json:"eventData"`       // 事件数据（延迟解析）
	Token     string          `json:"token,omitempty"` // JWT Token（LOGIN 事件必填）
	Nonce     string          `json:"nonce,omitempty"` // 签名随机数
	Sign      string          `json:"sign,omitempty"`  // HMAC-SHA256 签名
}

// Response 服务端通用响应消息
type Response struct {
	Timestamp time.Time   `json:"timestamp"`      // 时间戳
	Message   string      `json:"message"`        // 消息内容
	Success   bool        `json:"success"`        // 是否成功
	Data      interface{} `json:"data,omitempty"` // 数据
	Code      int         `json:"code,omitempty"` // 错误码
	EventType string      `json:"eventType"`      // 事件类型
}

// PushMessage 推送消息结构
// 同时作为 Kafka ws_public_receive_topic 的入站消息格式
// 和推送给客户端的出站消息格式。
// 入站时 Timestamp 为零值（omitempty 不序列化），出站时由 NewPushMessage 设置。
type PushMessage struct {
	Channel       string      `json:"channel"`                 // 频道名称（quote/kline/depth 等）
	Product       string      `json:"product,omitempty"`       // 产品代码
	UserSessionId string      `json:"userSessionId,omitempty"` // 目标会话ID，多个逗号分隔
	Timestamp     time.Time   `json:"timestamp,omitempty"`     // 推送时间戳（出站时设置）
	Data          interface{} `json:"data"`                    // 推送数据
}

// HeartbeatData 心跳消息数据
type HeartbeatData struct {
	Type      string `json:"type"`
	Path      string `json:"path"`
	Timestamp int64  `json:"timestamp"`
}

// NewPushMessage 创建推送消息（设置当前时间戳）
func NewPushMessage(channel, product string, data interface{}) *PushMessage {
	return &PushMessage{
		Channel:   channel,
		Product:   product,
		Timestamp: time.Now(),
		Data:      data,
	}
}

// LoginEventData 登录事件数据
type LoginEventData struct {
	UserId        string `json:"userId"`                  // 用户ID
	AccountId     string `json:"accountId"`               // 账户ID（当前登录的账户）
	AccountIds    string `json:"accountIds,omitempty"`    // 登录过的账户ID列表
	UserSessionId string `json:"userSessionId"`           // 用户SessionId
	ClientId      string `json:"clientId"`                // 客户端ID
	OperateUserId string `json:"operateUserId,omitempty"` // BMS操作人ID
}

// SubscribeEventData 订阅事件数据
type SubscribeEventData struct {
	SubList   []SubItem `json:"subList"`             // 订阅列表
	PageIndex string    `json:"pageIndex,omitempty"` // 页面索引
}

// SubItem 订阅项
type SubItem struct {
	Channel    string `json:"channel"`              // 频道
	Symbol     string `json:"symbol"`               // 产品代码
	TimeWindow string `json:"timeWindow,omitempty"` // 时间窗口（K线用）
	OrderId    string `json:"orderId,omitempty"`    // 订单ID（订单浮动盈亏用）
}

// MarketOrderEventData 市价单事件数据
type MarketOrderEventData struct {
	Action       string `json:"action"`                 // 操作类型: open/close
	SymbolCode   string `json:"symbolCode,omitempty"`   // 合约代码
	Side         int    `json:"side,omitempty"`         // 交易方向 1:买 2:卖
	Leverage     int    `json:"leverage,omitempty"`     // 杠杆倍数
	OpenOrderAmt string `json:"openOrderAmt,omitempty"` // 开仓金额
	OrderQty     string `json:"orderQty,omitempty"`     // 下单数量
	QuotePrice   string `json:"quotePrice,omitempty"`   // 报价价格（可选）
	OrderId      string `json:"orderId,omitempty"`      // 订单ID（平仓用）
	OrdLimitId   string `json:"ordLimitId,omitempty"`   // 关联的挂单ID（平仓时可选）
	Language     string `json:"language,omitempty"`     // 语言代码
	TradeToken   string `json:"tradeToken,omitempty"`   // BMS 交易令牌
}

// LimitOrderEventData 挂单事件数据
type LimitOrderEventData struct {
	SymbolCode      string `json:"symbolCode"`             // 合约代码
	Side            int    `json:"side"`                   // 交易方向
	Leverage        int    `json:"leverage"`               // 杠杆倍数
	LimitQuotePrice string `json:"limitQuotePrice"`        // 挂单价格
	OpenOrderAmt    string `json:"openOrderAmt,omitempty"` // 开仓金额
	OrderQty        string `json:"orderQty,omitempty"`     // 下单数量
	Language        string `json:"language,omitempty"`     // 语言代码
}

// LimitOrderCancelEventData 取消挂单事件数据
type LimitOrderCancelEventData struct {
	OrdLimitId string `json:"ordLimitId"`           // 挂单ID
	Language   string `json:"language,omitempty"`   // 语言代码
	TradeToken string `json:"tradeToken,omitempty"` // BMS 交易令牌
}

const (
	// 客户端错误 4xxx
	ErrCodeParamInvalid = 4001 // 参数校验失败
	ErrCodeNotLogin     = 4002 // 未登录
	ErrCodePermission   = 4003 // 权限不足
	ErrCodeSignInvalid  = 4004 // 签名校验失败
	ErrCodeAuthFailed   = 4401 // 认证失败

	// 服务端错误 5xxx
	ErrCodeInternal  = 5001 // 服务内部错误
	ErrCodeMQ        = 5002 // 消息队列错误
	ErrCodeDatabase  = 5003 // 数据库错误
	ErrCodeSubCenter = 5004 // 订阅中心调用失败
)

// 错误消息映射
var ErrorMessages = map[int]string{
	ErrCodeParamInvalid: "Parameter validation failed",
	ErrCodeNotLogin:     "Not logged in",
	ErrCodePermission:   "Permission denied",
	ErrCodeSignInvalid:  "Signature verification failed",
	ErrCodeAuthFailed:   "Authentication failed",
	ErrCodeInternal:     "Internal server error",
	ErrCodeMQ:           "Message queue error",
	ErrCodeDatabase:     "Database error",
	ErrCodeSubCenter:    "Subscription center error",
}

// GetErrorMessage 获取错误消息
func GetErrorMessage(code int) string {
	if msg, ok := ErrorMessages[code]; ok {
		return msg
	}
	return "Unknown error"
}
