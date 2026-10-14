# WebSocket 服务器

[English](./README.md) | 中文文档

一个高性能、生产就绪的 Go 语言 WebSocket 服务器实现，专为实时消息传输设计，具有身份认证、消息路由和可扩展客户端管理等高级特性。

## 功能特性

### 核心能力
- **高性能架构**：使用 FNV-1a 哈希的分片客户端管理，最小化锁竞争
- **灵活的身份认证**：基于 JWT 的认证，集成 Redis 缓存，支持匿名连接
- **消息安全**：HMAC-SHA256 签名验证确保消息完整性
- **多租户支持**：按用户 ID、账户 ID 和会话 ID 跟踪客户端
- **心跳管理**：自动 ping/pong 机制，可配置心跳间隔
- **优雅关闭**：清理连接关闭和资源释放

### 高级特性
- **分片客户端存储**：32 分片并发映射减少高负载场景下的锁竞争
- **灵活的消息路由**：向特定用户、账户、会话发送消息或广播给所有人
- **身份认证缓存**：握手阶段 JWT 验证后缓存认证信息，避免重复 Redis 查询
- **自动登录**：握手阶段获取到认证数据时自动触发登录流程
- **BMS 支持**：后台管理系统登录，支持操作员追踪
- **超时控制**：消息发送操作可配置超时时间

## 架构设计

### 核心组件

#### Hub（中心管理器）
维护所有连接的中心管理器：
- 按连接 ID 索引的客户端连接
- 会话映射，支持快速基于会话的查找
- 用户和账户映射，支持一对多关系
- 注册/注销通道，实现线程安全的客户端生命周期管理

#### Client（客户端）
表示单个 WebSocket 连接：
- 唯一的连接 ID 和会话 ID
- 用户身份信息（userId、accountId、accountIds）
- 客户端元数据（clientType、clientId、remoteAddr）
- 可配置缓冲区大小的消息发送通道
- 状态管理（已连接、已登录、已关闭）
- 认证缓存，避免冗余的 token 验证

#### Server（服务器）
处理 WebSocket 升级的 HTTP 服务器：
- 可插拔的消息处理器，支持不同事件类型
- 通过 Redis 缓存进行 JWT token 验证
- HMAC 签名验证，确保消息安全
- 断开连接回调，用于清理操作

#### Authenticator（身份认证器）
连接级身份认证，支持：
- JWT token 解析和验证
- 基于 Redis 的用户数据查询
- 从代理头提取真实 IP
- 匿名连接的客户端指纹识别
- Token 过期追踪和刷新提示

## 安装

```bash
go get github.com/web3-coder-hub/websocket
```

## 快速开始

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/web3-coder-hub/websocket"
    "github.com/web3-coder-hub/cache"
    "github.com/web3-coder-hub/logger"
)

func main() {
    // 初始化日志
    l := logger.NewLogger()
    
    // 初始化缓存（Redis）
    c := cache.NewCache(/* cache config */)
    
    // 配置 Hub
    hubConfig := websocket.HubConfig{
        WriteWait:      10 * time.Second,
        PongWait:       60 * time.Second,
        PingPeriod:     54 * time.Second,
        MaxMessageSize: 512 * 1024,
        SendBufferSize: 256,
    }
    
    // 创建并启动 Hub
    hub := websocket.NewHub(hubConfig, l)
    go hub.Run()
    
    // 配置服务器
    serverConfig := websocket.Config{
        Port:                  8080,
        Path:                  "/ws",
        SessionTimeoutSeconds: 3600,
        HeartbeatIntervalSecs: 30,
        MustLogin:             false, // 允许匿名连接
        EnableSecurity:        true,  // 启用 HMAC 签名验证
        SecuritySecret:        "your-secret-key",
    }
    
    // 创建服务器
    server := websocket.NewServer(serverConfig, hub, c, l)
    
    // 注册消息处理器
    server.RegisterHandler("login", handleLogin)
    server.RegisterHandler("subscribe", handleSubscribe)
    server.RegisterHandler("message", handleMessage)
    
    // 设置断开连接处理器
    server.SetDisconnectHandler(func(client *websocket.Client) {
        log.Printf("客户端断开连接: %s", client.ID)
    })
    
    // 启动服务器
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }
    
    // 等待关闭信号
    // ... 关闭逻辑 ...
    
    // 优雅关闭
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    server.Stop(ctx)
    hub.Stop()
}

func handleLogin(client *websocket.Client, msg *websocket.ClientMessage) error {
    var data websocket.LoginEventData
    if err := json.Unmarshal(msg.EventData, &data); err != nil {
        client.SendError("login", websocket.ErrCodeParamInvalid, "登录数据无效")
        return err
    }
    
    // 注册登录信息
    client.hub.RegisterLogin(
        client,
        data.UserId,
        data.AccountId,
        []string{data.AccountId},
        data.ClientId,
        data.UserSessionId,
        false,
    )
    
    client.SendSuccess("login", "登录成功", nil)
    return nil
}

func handleSubscribe(client *websocket.Client, msg *websocket.ClientMessage) error {
    var data websocket.SubscribeEventData
    if err := json.Unmarshal(msg.EventData, &data); err != nil {
        client.SendError("subscribe", websocket.ErrCodeParamInvalid, "订阅数据无效")
        return err
    }
    
    // 处理订阅逻辑
    // ...
    
    client.SendSuccess("subscribe", "订阅成功", nil)
    return nil
}

func handleMessage(client *websocket.Client, msg *websocket.ClientMessage) error {
    // 处理自定义消息
    return nil
}
```

## 消息格式

### 客户端到服务器

```json
{
  "eventType": "login",
  "eventData": {
    "userId": "user123",
    "accountId": "acc456"
  },
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "nonce": "random-string",
  "sign": "hmac-sha256-signature"
}
```

### 服务器到客户端（响应）

```json
{
  "timestamp": "2026-10-14T10:30:00Z",
  "eventType": "login",
  "success": true,
  "message": "登录成功",
  "code": 0,
  "data": {}
}
```

### 服务器到客户端（推送）

```json
{
  "channel": "quote",
  "product": "BTCUSDT",
  "timestamp": "2026-10-14T10:30:00Z",
  "data": {
    "price": "50000.00",
    "volume": "123.45"
  }
}
```

## 身份认证

### 连接认证（握手阶段）

服务器支持多种认证模式：

1. **JWT Token 认证**：通过 `Access-Token` 请求头或查询参数提供 token
2. **匿名连接**：当 `MustLogin = false` 时允许
3. **自动生成客户端 ID**：未提供时基于 User-Agent + IP 的 MD5 自动生成

```bash
# 使用 token 连接
ws://localhost:8080/ws?Access-Token=eyJhbGc...&Client-ID=client123

# 使用请求头（推荐）
Access-Token: Bearer eyJhbGc...
Client-ID: client123
Client-Type: APP
Account-Ids: acc1,acc2,acc3
```

### 消息认证（HMAC 签名）

当 `EnableSecurity = true` 时，所有非心跳消息都需要 HMAC-SHA256 签名：

**签名算法**：
```
sign = HMAC-SHA256(
  "ClientType=APP&ClientId=xxx&Nonce=xxx&Param=sortedJSON",
  secret
)
```

`eventData` 会在签名前按 key 排序，以确保一致性。

### Token 过期处理

- JWT 过期时间在握手阶段缓存到 `Client.AuthExpiry`
- 过期的连接在处理非 login 事件时会收到 `ErrCodeAuthFailed` 错误
- 客户端必须发送新的 `login` 事件并携带新 token 来刷新认证

## 消息路由

### 发送到特定目标

```go
// 发送给用户（所有连接）
hub.SendToUser("user123", messageData)

// 发送给账户（所有连接）
hub.SendToAccount("acc456", messageData)

// 发送给会话（单个连接）
hub.SendToSession("session-id", messageData)

// 带超时发送
hub.SendToUserWithTimeout("user123", messageData, 5*time.Second)

// 阻塞发送
hub.SendToSessionBlocking("session-id", messageData)
```

### 广播消息

```go
// 广播给所有已连接的客户端
hub.Broadcast(messageData)

// 仅广播给已登录的客户端
hub.BroadcastToLoggedIn(messageData)
```

### 查询客户端

```go
// 根据连接 ID 获取客户端
client, ok := hub.GetClient("conn-id")

// 根据会话 ID 获取客户端
client, ok := hub.GetClientBySession("session-id")

// 获取用户的所有客户端
clients := hub.GetClientsByUserId("user123")

// 获取账户的所有客户端
clients := hub.GetClientsByAccountId("acc456")

// 获取总连接数
count := hub.GetTotalConnections()
```

## 客户端管理

### 分片架构

Hub 使用 32 分片并发映射来减少锁竞争：

- **FNV-1a 哈希**：快速、均匀地将 key 分布到各个分片
- **每个分片独立锁**：每个分片有自己的互斥锁
- **基于前缀的扫描**：高效地进行用户/账户范围查询

### 客户端状态

- **StateConnected**：WebSocket 升级后的初始状态
- **StateLoggedIn**：成功登录事件后的状态
- **StateClosed**：连接已关闭，等待清理

### 清理操作

```go
// 清理不活跃的连接
removedCount := hub.CleanupInactive(5 * time.Minute)

// 遍历所有客户端
hub.ForEachClient(func(client *Client) bool {
    // 处理客户端
    return true // 继续遍历
})

// 仅遍历已登录的客户端
hub.ForEachLoggedInClient(func(client *Client) bool {
    // 处理已登录的客户端
    return true
})
```

## 心跳机制

服务器实现双向心跳：

1. **服务器到客户端 Ping**：按配置的间隔自动发送
2. **客户端到服务器 Ping**：重置服务器心跳计时器，避免冗余 ping
3. **心跳频道**：使用 `heartbeat` 频道，事件类型为 `ping`/`pong`

```json
{
  "channel": "heartbeat",
  "timestamp": "2026-10-14T10:30:00Z",
  "data": {
    "type": "ping",
    "path": "ws",
    "timestamp": 1697280600000
  }
}
```

## 错误码

### 客户端错误（4xxx）
- `4001` - 参数校验失败
- `4002` - 未登录
- `4003` - 权限不足
- `4004` - 签名校验失败
- `4401` - 认证失败

### 服务端错误（5xxx）
- `5001` - 服务内部错误
- `5002` - 消息队列错误
- `5003` - 数据库错误
- `5004` - 订阅中心错误

## 配置说明

### Hub 配置

```go
type HubConfig struct {
    WriteWait      time.Duration // 写入超时（例如 10s）
    PongWait       time.Duration // Pong 等待超时（例如 60s）
    PingPeriod     time.Duration // Ping 间隔（例如 54s）
    MaxMessageSize int64         // 最大消息大小（字节）
    SendBufferSize int           // 发送通道缓冲区大小
}
```

### 服务器配置

```go
type Config struct {
    Port                  int    // 服务器端口
    Path                  string // WebSocket 端点路径
    SessionTimeoutSeconds int    // 会话超时时间
    HeartbeatIntervalSecs int    // 心跳间隔
    MustLogin             bool   // 接收消息前是否必须登录
    EnableSecurity        bool   // 是否启用 HMAC 签名验证
    SecuritySecret        string // HMAC 密钥
}
```

## 依赖项

- [antlabs/quickws](https://github.com/antlabs/quickws) - 高性能 WebSocket 库
- [google/uuid](https://github.com/google/uuid) - UUID 生成
- [web3-coder-hub/cache](https://github.com/web3-coder-hub/cache) - 缓存抽象层（Redis/Memory/BadgerDB）
- [web3-coder-hub/logger](https://github.com/web3-coder-hub/logger) - 结构化日志

## 性能特性

- **分片机制**：32 个分片最小化高并发场景下的锁竞争
- **非阻塞发送**：默认发送操作通过通道缓冲实现非阻塞
- **连接池化**：高效的每连接一个 goroutine 模型
- **内存效率**：频繁读取的状态使用原子操作

## 最佳实践

1. **缓冲区大小**：根据消息频率设置 `SendBufferSize`，避免消息丢失
2. **心跳调优**：保持 `PingPeriod < PongWait`，防止误判断开连接
3. **Token 刷新**：在 token 过期前提示客户端重新登录
4. **优雅关闭**：始终调用 hub 和 server 的 `Stop()` 方法，确保连接干净关闭
5. **错误处理**：注册断开连接处理器，清理外部资源

## 许可证

本项目是 web3-coder-hub 生态系统的一部分。

## 版本历史

- `v1.0.5` - 最新稳定版本
- `v1.0.4` - 增强的认证和安全性
- `v1.0.3` - 分片客户端管理
- `v1.0.2` - 初始多租户支持
- `v1.0.1` - 首个标记版本
