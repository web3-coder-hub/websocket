package websocket

import (
	"strings"
	"sync"
	"time"

	"github.com/web3-coder-hub/logger"
)

// 分片数量，必须为2的幂次方
const (
	shardCount = 32
	shardMask  = shardCount - 1 // 0x1F，用于位运算取模
)

// HubConfig defines the configuration for WebSocket Hub.
type HubConfig struct {
	WriteWait      time.Duration `yaml:"write_wait"`
	PongWait       time.Duration `yaml:"pong_wait"`
	PingPeriod     time.Duration `yaml:"ping_period"`
	MaxMessageSize int64         `yaml:"max_message_size"`
	SendBufferSize int           `yaml:"send_buffer_size"`
}

// Hub 管理所有客户端连接
type Hub struct {
	logger         logger.Logger   // 日志
	clients        *ShardedClients // 所有客户端连接，key 是连接ID
	sessions       *ShardedClients // sessionId 到 client 的映射，用于快速查找
	userClients    *ShardedClients // userId 到 clients 的映射（一个用户可能有多个连接）
	accountClients *ShardedClients // accountId 到 clients 的映射
	register       chan *Client    // 注册通道
	unregister     chan *Client    // 注销通道
	done           chan struct{}   // 关闭信号
	config         HubConfig       // 配置
	doneOnce       sync.Once       // 确保只关闭一次
}

// NewHub 创建新的 Hub 实例
func NewHub(cfg HubConfig, l logger.Logger) *Hub {
	return &Hub{
		clients:        NewShardedClients(),
		sessions:       NewShardedClients(),
		userClients:    NewShardedClients(),
		accountClients: NewShardedClients(),
		register:       make(chan *Client, 100),
		unregister:     make(chan *Client, 100),
		config:         cfg,
		logger:         l,
		done:           make(chan struct{}),
	}
}

// ClientShard 客户端分片
type ClientShard struct {
	clients map[string]*Client
	mu      sync.RWMutex
}

// ShardedClients 分片客户端映射
type ShardedClients struct {
	shards [shardCount]*ClientShard
}

// NewShardedClients 创建分片客户端映射
func NewShardedClients() *ShardedClients {
	sc := &ShardedClients{}
	for i := 0; i < shardCount; i++ {
		sc.shards[i] = &ClientShard{
			clients: make(map[string]*Client),
		}
	}
	return sc
}

// fnv32a 内联 FNV-1a 哈希
func fnv32a(key string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return h
}

// getShard 根据key获取分片
func (sc *ShardedClients) getShard(key string) *ClientShard {
	return sc.shards[fnv32a(key)&shardMask]
}

// Store 存储客户端
func (sc *ShardedClients) Store(key string, client *Client) {
	shard := sc.getShard(key)
	shard.mu.Lock()
	shard.clients[key] = client
	shard.mu.Unlock()
}

// Load 获取客户端
func (sc *ShardedClients) Load(key string) (*Client, bool) {
	shard := sc.getShard(key)
	shard.mu.RLock()
	client, ok := shard.clients[key]
	shard.mu.RUnlock()
	return client, ok
}

// Delete 删除客户端
func (sc *ShardedClients) Delete(key string) {
	shard := sc.getShard(key)
	shard.mu.Lock()
	delete(shard.clients, key)
	shard.mu.Unlock()
}

// LoadAndDelete 获取并删除客户端
func (sc *ShardedClients) LoadAndDelete(key string) (*Client, bool) {
	shard := sc.getShard(key)
	shard.mu.Lock()
	client, ok := shard.clients[key]
	if ok {
		delete(shard.clients, key)
	}
	shard.mu.Unlock()
	return client, ok
}

// Range 遍历所有客户端
func (sc *ShardedClients) Range(fn func(key string, client *Client) bool) {
	for i := 0; i < shardCount; i++ {
		shard := sc.shards[i]
		shard.mu.RLock()
		for k, v := range shard.clients {
			if !fn(k, v) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

// Count 获取客户端数量
func (sc *ShardedClients) Count() int {
	count := 0
	for i := 0; i < shardCount; i++ {
		shard := sc.shards[i]
		shard.mu.RLock()
		count += len(shard.clients)
		shard.mu.RUnlock()
	}
	return count
}

// RangeByPrefix 遍历指定前缀的客户端
func (sc *ShardedClients) RangeByPrefix(prefix string, fn func(key string, client *Client) bool) {
	for i := 0; i < shardCount; i++ {
		shard := sc.shards[i]
		shard.mu.RLock()
		for k, v := range shard.clients {
			if strings.HasPrefix(k, prefix) {
				if !fn(k, v) {
					shard.mu.RUnlock()
					return
				}
			}
		}
		shard.mu.RUnlock()
	}
}

// Run 运行 Hub（在独立的 goroutine 中）
func (h *Hub) Run() {
	if h.logger != nil {
		h.logger.Info("websocket hub started")
	}
	for {
		select {
		case client := <-h.register:
			h.addClient(client)
		case client := <-h.unregister:
			h.removeClient(client)
		case <-h.done:
			h.closeAllClients()
			return
		}
	}
}

// Stop 停止 Hub
func (h *Hub) Stop() {
	h.doneOnce.Do(func() {
		close(h.done)
	})
}

// Register 注册客户端
func (h *Hub) Register(client *Client) {
	select {
	case h.register <- client:
	case <-h.done:
	}
}

// Unregister 注销客户端
func (h *Hub) Unregister(client *Client) {
	select {
	case h.unregister <- client:
	case <-h.done:
	}
}

// addClient 添加客户端到管理
func (h *Hub) addClient(client *Client) {
	h.clients.Store(client.ID, client)

	if client.SessionID != "" {
		h.sessions.Store(client.SessionID, client)
	}

	if h.logger != nil {
		h.logger.Info("client registered",
			logger.Field{Key: "clientId", Value: client.ID},
		)
	}
}

// removeClient 从管理中移除客户端
func (h *Hub) removeClient(client *Client) {
	if _, ok := h.clients.LoadAndDelete(client.ID); !ok {
		return
	}

	// 从session映射移除
	if client.SessionID != "" {
		h.sessions.Delete(client.SessionID)
	}

	// 从userId映射移除
	if client.UserID != "" {
		h.removeFromUserClients(client.UserID, client)
	}

	// 从accountId映射移除
	if client.AccountID != "" {
		h.removeFromAccountClients(client.AccountID, client)
	}

	// 从所有accountIds映射移除
	for _, accountID := range client.AccountIds {
		h.removeFromAccountClients(accountID, client)
	}

	if h.logger != nil {
		h.logger.Info("client unregistered",
			logger.Field{Key: "clientId", Value: client.ID},
			logger.Field{Key: "userID", Value: client.UserID},
		)
	}
}

// RegisterLogin 注册登录信息
func (h *Hub) RegisterLogin(client *Client, userID, accountID string, accountIds []string, clientID, sessionID string, isBmsLogin bool) {
	client.SetLoginInfo(userID, accountID, accountIds, clientID, sessionID, isBmsLogin)

	// 添加到session映射
	h.sessions.Store(sessionID, client)

	// 添加到userId映射
	h.addToUserClients(userID, client)

	// 添加到accountId映射
	if accountID != "" {
		h.addToAccountClients(accountID, client)
	}

	// 添加到所有accountIds映射
	for _, accID := range accountIds {
		h.addToAccountClients(accID, client)
	}

	if h.logger != nil {
		h.logger.Info("client logged in",
			logger.Field{Key: "clientId", Value: client.ID},
			logger.Field{Key: "userID", Value: userID},
			logger.Field{Key: "accountID", Value: accountID},
			logger.Field{Key: "sessionID", Value: sessionID},
		)
	}
}

// addToUserClients 添加客户端到用户映射
func (h *Hub) addToUserClients(userID string, client *Client) {
	h.userClients.Store(userID+":"+client.ID, client)
}

// removeFromUserClients 从用户映射中移除客户端
func (h *Hub) removeFromUserClients(userID string, client *Client) {
	h.userClients.Delete(userID + ":" + client.ID)
}

// addToAccountClients 添加客户端到账户映射
func (h *Hub) addToAccountClients(accountID string, client *Client) {
	h.accountClients.Store(accountID+":"+client.ID, client)
}

// removeFromAccountClients 从账户映射中移除客户端
func (h *Hub) removeFromAccountClients(accountID string, client *Client) {
	h.accountClients.Delete(accountID + ":" + client.ID)
}

// GetClient 根据连接ID获取客户端
func (h *Hub) GetClient(id string) (*Client, bool) {
	return h.clients.Load(id)
}

// GetClientBySession 根据SessionId获取客户端
func (h *Hub) GetClientBySession(sessionID string) (*Client, bool) {
	return h.sessions.Load(sessionID)
}

// GetClientsByUserID 根据UserId获取所有客户端
func (h *Hub) GetClientsByUserID(userID string) []*Client {
	var clients []*Client
	prefix := userID + ":"
	h.userClients.RangeByPrefix(prefix, func(_ string, client *Client) bool {
		if !client.IsClosed() {
			clients = append(clients, client)
		}
		return true
	})
	return clients
}

// GetClientsByAccountID 根据AccountId获取所有客户端
func (h *Hub) GetClientsByAccountID(accountID string) []*Client {
	var clients []*Client
	prefix := accountID + ":"
	h.accountClients.RangeByPrefix(prefix, func(_ string, client *Client) bool {
		if !client.IsClosed() {
			clients = append(clients, client)
		}
		return true
	})
	return clients
}

// Broadcast 广播消息到所有客户端
func (h *Hub) Broadcast(data []byte) {
	h.clients.Range(func(_ string, client *Client) bool {
		if !client.IsClosed() {
			client.Send(data)
		}
		return true
	})
}

// BroadcastToLoggedIn 广播消息到所有已登录的客户端
func (h *Hub) BroadcastToLoggedIn(data []byte) {
	h.clients.Range(func(_ string, client *Client) bool {
		if client.IsLoggedIn() && !client.IsClosed() {
			client.Send(data)
		}
		return true
	})
}

// SendToUser 发送消息给指定用户的所有连接
func (h *Hub) SendToUser(userID string, data []byte) int {
	count := 0
	for _, client := range h.GetClientsByUserID(userID) {
		if client.Send(data) {
			count++
		}
	}
	return count
}

// SendToUserWithTimeout 发送消息给指定用户，带超时
func (h *Hub) SendToUserWithTimeout(userID string, data []byte, timeout time.Duration) int {
	count := 0
	for _, client := range h.GetClientsByUserID(userID) {
		if client.SendWithTimeout(data, timeout) {
			count++
		}
	}
	return count
}

// SendToAccount 发送消息给指定账户的所有连接
func (h *Hub) SendToAccount(accountID string, data []byte) int {
	count := 0
	for _, client := range h.GetClientsByAccountID(accountID) {
		if client.Send(data) {
			count++
		}
	}
	return count
}

// SendToSession 发送消息给指定会话
func (h *Hub) SendToSession(sessionID string, data []byte) bool {
	if client, ok := h.GetClientBySession(sessionID); ok {
		return client.Send(data)
	}
	return false
}

// SendToSessionWithTimeout 发送消息给指定会话，带超时
func (h *Hub) SendToSessionWithTimeout(sessionID string, data []byte, timeout time.Duration) bool {
	if client, ok := h.GetClientBySession(sessionID); ok {
		return client.SendWithTimeout(data, timeout)
	}
	return false
}

// SendToSessionBlocking 阻塞发送消息给指定会话
func (h *Hub) SendToSessionBlocking(sessionID string, data []byte) bool {
	if client, ok := h.GetClientBySession(sessionID); ok {
		return client.SendBlocking(data)
	}
	return false
}

// closeAllClients 关闭所有客户端
func (h *Hub) closeAllClients() {
	h.clients.Range(func(_ string, client *Client) bool {
		client.Close()
		return true
	})
}

// GetTotalConnections 获取当前连接数
func (h *Hub) GetTotalConnections() int {
	return h.clients.Count()
}

// ForEachClient 遍历所有客户端
func (h *Hub) ForEachClient(fn func(*Client) bool) {
	h.clients.Range(func(_ string, client *Client) bool {
		return fn(client)
	})
}

// ForEachLoggedInClient 遍历所有已登录的客户端
func (h *Hub) ForEachLoggedInClient(fn func(*Client) bool) {
	h.clients.Range(func(_ string, client *Client) bool {
		if client.IsLoggedIn() {
			return fn(client)
		}
		return true
	})
}

// CleanupInactive 清理不活跃的连接
func (h *Hub) CleanupInactive(timeout time.Duration) int {
	count := 0
	now := time.Now()
	h.clients.Range(func(_ string, client *Client) bool {
		lastPing := client.GetLastPingAt()
		if lastPing.IsZero() {
			if now.Sub(client.ConnectedAt) > timeout {
				client.Close()
				count++
			}
		} else {
			if now.Sub(lastPing) > timeout {
				client.Close()
				count++
			}
		}
		return true
	})
	return count
}
