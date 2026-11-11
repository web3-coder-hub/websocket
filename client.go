package websocket

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/web3-coder-hub/logger"
	"github.com/web3-coder-hub/websocket/adapter"
	"github.com/web3-coder-hub/websocket/auth"
	"github.com/web3-coder-hub/websocket/constant"
)

// Client 表示一个 WebSocket 客户端连接
type Client struct {
	// 连接标识
	ID        string // 连接唯一ID
	SessionId string // 会话ID（用于消息推送定位）

	// 用户信息（登录后设置）
	UserId     string   // 用户ID
	AccountId  string   // 当前账户ID
	AccountIds []string // 所有账户ID列表
	ClientId   string   // 客户端ID
	ClientType string   // 客户端类型（APP/WEB/BMS 等）
	ClientIp   string   // 客户端真实 IP
	RemoteAddr string   // 客户端远程地址（IP:Port）

	// WebSocket 连接
	conn adapter.ServerConn

	// Hub 引用
	hub *Hub

	// 消息发送通道
	send chan []byte

	// 关闭信号：用于 select 中安全退出
	done chan struct{}

	// 状态
	state     atomic.Int32
	closeOnce sync.Once

	// 配置
	writeWait time.Duration

	// 日志
	logger logger.Logger

	// 心跳
	heartbeatInterval time.Duration
	heartbeatReset    chan struct{}
	wsPath            string

	// 认证信息
	AuthDeadline time.Time // 认证截止时间（用于待认证状态的超时控制）
	accountIds   string    // 多账户 ID 列表（逗号分隔）

	// 连接时间
	ConnectedAt time.Time
	lastPingAt  atomic.Int64 // Unix 纳秒时间戳
}

// 确保 Client 实现了 auth.AuthClient 接口
var _ auth.AuthClient = (*Client)(nil)

// 实现 auth.AuthClient 接口

func (c *Client) SetSessionId(sessionId string)      { c.SessionId = sessionId }
func (c *Client) SetUserId(userId string)            { c.UserId = userId }
func (c *Client) SetAccountId(accountId string)      { c.AccountId = accountId }
func (c *Client) SetClientId(clientId string)        { c.ClientId = clientId }
func (c *Client) SetClientIp(clientIp string)        { c.ClientIp = clientIp }
func (c *Client) SetClientType(clientType string)    { c.ClientType = clientType }
func (c *Client) SetAccountIds(accountIds string)    { c.accountIds = accountIds }
func (c *Client) SetAuthDeadline(deadline time.Time) { c.AuthDeadline = deadline }
func (c *Client) SetState(state int32)               { c.state.Store(state) }
func (c *Client) GetState() int32                    { return c.state.Load() }
func (c *Client) GetClientId() string                { return c.ClientId }
func (c *Client) GetClientType() string              { return c.ClientType }
func (c *Client) GetClientIp() string                { return c.ClientIp }

// GetAccountIdsString 获取账户 ID 列表字符串
func (c *Client) GetAccountIdsString() string {
	return c.accountIds
}

// 状态检查辅助方法

// IsPendingAuth 检查是否处于待认证状态
func (c *Client) IsPendingAuth() bool {
	return constant.ClientState(c.state.Load()) == constant.StatePendingAuth
}

// IsAuthenticated 检查是否已认证
func (c *Client) IsAuthenticated() bool {
	return constant.ClientState(c.state.Load()) == constant.StateAuthenticated
}

// IsAuthExpired 检查认证是否已过期
func (c *Client) IsAuthExpired() bool {
	state := constant.ClientState(c.state.Load())
	if state == constant.StatePendingAuth {
		return time.Now().After(c.AuthDeadline)
	}
	return state == constant.StateAuthExpired
}

// IsAuthValid 检查认证是否有效（已认证且未过期）
func (c *Client) IsAuthValid() bool {
	return constant.ClientState(c.state.Load()) == constant.StateAuthenticated
}

// NewClient 创建新的客户端实例
func NewClient(id string, conn adapter.ServerConn, hub *Hub, cfg HubConfig, l logger.Logger, heartbeatInterval time.Duration, wsPath string) *Client {
	c := &Client{
		ID:                id,
		conn:              conn,
		hub:               hub,
		send:              make(chan []byte, cfg.SendBufferSize),
		done:              make(chan struct{}),
		ConnectedAt:       time.Now(),
		writeWait:         cfg.WriteWait,
		heartbeatInterval: heartbeatInterval,
		heartbeatReset:    make(chan struct{}, 1),
		wsPath:            wsPath,
		logger:            l,
	}
	c.state.Store(int32(constant.StateConnected))
	return c
}

// IsLoggedIn 检查是否已登录
func (c *Client) IsLoggedIn() bool {
	return constant.ClientState(c.state.Load()) == constant.StateLoggedIn
}

// IsClosed 检查连接是否已关闭
func (c *Client) IsClosed() bool {
	return constant.ClientState(c.state.Load()) == constant.StateClosed
}

// SetLoginInfo 设置登录信息
func (c *Client) SetLoginInfo(userId, accountId string, accountIds []string, clientId, sessionId string, isBmsLogin bool) {
	c.UserId = userId
	c.AccountId = accountId
	c.AccountIds = accountIds
	c.ClientId = clientId
	c.SessionId = sessionId
	c.state.Store(int32(constant.StateLoggedIn))
}

// Send 发送消息到客户端（非阻塞）
func (c *Client) Send(data []byte) (sent bool) {
	defer func() {
		if recover() != nil {
			sent = false
		}
	}()
	select {
	case c.send <- data:
		return true
	case <-c.done:
		return false
	default:
		return false
	}
}

// SendWithTimeout 发送消息，带超时
func (c *Client) SendWithTimeout(data []byte, timeout time.Duration) (sent bool) {
	defer func() {
		if recover() != nil {
			sent = false
		}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case c.send <- data:
		return true
	case <-c.done:
		return false
	case <-timer.C:
		return false
	}
}

// SendBlocking 阻塞发送消息
func (c *Client) SendBlocking(data []byte) (sent bool) {
	defer func() {
		if recover() != nil {
			sent = false
		}
	}()
	select {
	case c.send <- data:
		return true
	case <-c.done:
		return false
	}
}

// SendJSON 发送 JSON 消息
func (c *Client) SendJSON(v interface{}) bool {
	data, err := json.Marshal(v)
	if err != nil {
		return false
	}
	return c.Send(data)
}

// SendSuccess 发送成功响应
func (c *Client) SendSuccess(eventType, message string, data interface{}) bool {
	return c.SendJSON(&ServerMessage{
		Timestamp: time.Now(),
		Message:   message,
		Success:   true,
		Data:      data,
		Code:      0,
		EventType: eventType,
	})
}

// SendError 发送错误响应
func (c *Client) SendError(eventType string, code int, message string) bool {
	return c.SendJSON(&ServerMessage{
		Timestamp: time.Now(),
		Message:   message,
		Success:   false,
		Data:      nil,
		Code:      code,
		EventType: eventType,
	})
}

// Close 关闭客户端连接
//
// 关闭顺序：done → conn → send → hub.Unregister
//   - 先关闭 done：通知 HeartbeatPump 退出，Send 系列方法立即返回 false
//   - 再关闭 conn：使 WritePump 中正在进行的 WriteTimeout 立即失败并返回
//   - 最后关闭 send：WritePump 已退出，安全关闭通道
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.state.Store(int32(constant.StateClosed))
		close(c.done)
		if c.conn != nil {
			_ = c.conn.Close()
		}
		close(c.send)
		if c.hub != nil {
			c.hub.Unregister(c)
		}
	})
}

// ResetHeartbeat 重置心跳计时器（收到客户端 ping 时调用）
func (c *Client) ResetHeartbeat() {
	select {
	case c.heartbeatReset <- struct{}{}:
	default:
	}
}

// HeartbeatPump 在客户端沉默超过心跳间隔时发送心跳 ping。
// 收到客户端主动 ping 会重置计时器，避免重复发送。
// 注意：心跳发送失败（如 send channel 拥塞）不应终止心跳泵，
// 连接的生命周期由 done channel 控制（WritePump 检测到底层连接断开后触发 Close）。
func (c *Client) HeartbeatPump() {
	if c.heartbeatInterval <= 0 {
		return
	}
	timer := time.NewTimer(c.heartbeatInterval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			now := time.Now()
			msg := &ServerMessage{
				Timestamp: now,
				Success:   true,
				EventType: constant.EventPing,
				Data: map[string]interface{}{
					"type":      constant.EventPing,
					"path":      c.wsPath,
					"timestamp": now.UnixMilli(),
				},
			}
			c.SendJSON(msg)
			c.UpdateLastPing()
			timer.Reset(c.heartbeatInterval)
		case <-c.heartbeatReset:
			c.UpdateLastPing()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(c.heartbeatInterval)
		case <-c.done:
			return
		}
	}
}

// WritePump 消息写入泵
func (c *Client) WritePump() {
	defer c.Close()
	for message := range c.send {
		if err := c.conn.WriteTimeout(adapter.Text, message, c.writeWait); err != nil {
			return
		}
	}
}

// UpdateLastPing 更新最后心跳时间
func (c *Client) UpdateLastPing() {
	c.lastPingAt.Store(time.Now().UnixNano())
}

// GetLastPingAt 获取最后心跳时间
func (c *Client) GetLastPingAt() time.Time {
	ns := c.lastPingAt.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}
