# WebSocket 库

[English](./README.md) | 中文文档

一个高性能、生产就绪的 Go 语言 WebSocket 库，提供服务端和客户端实现。具有可插拔的适配器架构，支持多种 WebSocket 库、身份认证、消息路由、自动重连和可扩展的连接管理等特性。

## 功能特性

### 核心能力

#### 服务端特性
- **高性能架构**：使用 FNV-1a 哈希的分片客户端管理，最小化锁竞争
- **灵活的身份认证**：基于策略模式的认证，支持 token、签名和自定义方法，使用可插拔的缓存
- **延迟认证**：浏览器友好的认证方式，允许在连接建立后通过 LOGIN 事件认证
- **消息安全**：HMAC-SHA256 签名验证确保消息完整性
- **多租户支持**：按用户 ID、账户 ID 和会话 ID 跟踪客户端
- **心跳管理**：自动 ping/pong 机制，可配置心跳间隔
- **优雅关闭**：清理连接关闭和资源释放

#### 客户端特性
- **自动重连**：可配置重试间隔和最大尝试次数
- **心跳检测**：自动 ping/pong 健康监控
- **事件驱动**：OnConnect、OnDisconnect、OnError 回调
- **消息路由**：基于事件的消息处理器注册
- **会话管理**：自动会话 ID 跟踪
- **线程安全**：支持并发使用

### 高级特性
- **可插拔适配器**：通过统一适配器接口支持多种 WebSocket 库（QuickWS、Gorilla 等）
- **策略模式认证**：可插拔的认证策略（token、签名、自定义），提供良好的扩展性
- **分片客户端存储**：32 分片并发映射减少高负载场景下的锁竞争
- **灵活的消息路由**：向特定用户、账户、会话发送消息或广播给所有人
- **身份认证缓存**：凭证验证后缓存认证信息，避免重复查询
- **延迟认证**：支持无法在握手时设置自定义头的浏览器和客户端
- **超时控制**：消息发送操作可配置超时时间

## 架构设计

### 适配器架构

```
┌─────────────────────────────────────────────────────────────┐
│              应用层 (服务端/客户端)                           │
├─────────────────────────────────────────────────────────────┤
│                     适配器接口层                              │
├─────────────────────────────────────────────────────────────┤
│  QuickWS 适配器  │  Gorilla 适配器  │  自定义适配器        │
└─────────────────────────────────────────────────────────────┘
```

库使用可插拔的适配器模式，可以：
- 在不改变应用代码的情况下切换 WebSocket 库
- 同时支持多个库
- 为特殊需求添加自定义适配器

### 服务端组件

#### Hub（中心管理器）
维护所有连接的中心管理器：
- 按连接 ID 索引的客户端连接
- 会话映射，支持快速基于会话的查找
- 用户和账户映射，支持一对多关系
- 注册/注销通道，实现线程安全的客户端生命周期管理

#### Server Connection（服务端客户端）
表示服务端的单个 WebSocket 连接：
- 唯一的连接 ID 和会话 ID
- 用户身份信息（userID、accountID、accountIds）
- 客户端元数据（clientType、clientID、remoteAddr）
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
- 策略模式实现可插拔的认证方法
- 基于 JWT 的 Token 认证，通过缓存查询用户数据
- 签名认证（计划中）
- 从代理头提取真实 IP
- 匿名连接的客户端指纹识别
- 浏览器客户端的延迟认证
- 可配置的待认证超时时间

### 客户端组件

#### WebSocket 客户端
功能完善的 WebSocket 客户端：
- 支持指数退避的自动重连
- 心跳监控和超时检测
- 事件驱动的消息处理
- 连接状态管理
- 线程安全的消息发送

#### 适配器
- **QuickWS 适配器**：高性能默认适配器
- **Gorilla 适配器**：支持流行的社区库
- **自定义适配器**：易于为任何 WebSocket 库实现

## 安装

```bash
go get github.com/web3-coder-hub/websocket
```

## 快速开始

### 服务端示例

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/web3-coder-hub/websocket"
    "github.com/web3-coder-hub/websocket/adapter/quickws"
    "github.com/web3-coder-hub/cache"
    "github.com/web3-coder-hub/logger"
)

func main() {
    // 初始化日志
    l, _ := logger.New("websocket-server", nil)
    
    // 初始化缓存
    c, _ := cache.NewBigCache(100, 10*time.Minute)
    
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
    
    // 创建适配器
    adapter := quickws.NewServerAdapter()
    
    // 配置服务器
    serverConfig := websocket.Config{
        Port:                   8080,
        Path:                   "/ws",
        SessionTimeoutSeconds:  3600,
        HeartbeatIntervalSecs:  30,
        AuthType:               "token",              // 认证类型：""、"token"、"sign"
        PendingAuthTimeoutSecs: 10,                   // 待认证超时时间
        EnableSecurity:         true,
        SecuritySecret:         "your-secret-key",
        CredentialParseFunc:    parseMyToken,         // 自定义 token 解析器（token 认证必需）
        Cache:                  c,                    // 缓存实例，用于身份认证
    }
    
    // 创建服务器
    server := websocket.NewServer(serverConfig, hub, adapter, l)
    
    // 注册消息处理器
    server.RegisterHandler("login", handleLogin)
    server.RegisterHandler("subscribe", handleSubscribe)
    
    // 设置断开连接处理器
    server.SetDisconnectHandler(func(client *websocket.Client) {
        log.Printf("客户端断开连接: %s", client.ID)
    })
    
    // 启动服务器
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }
    
    // 优雅关闭
    // ...
}

func handleLogin(client *websocket.Client, msg *websocket.ClientMessage) error {
    // 处理登录
    client.SendSuccess("login", "登录成功", nil)
    return nil
}

func handleSubscribe(client *websocket.Client, msg *websocket.ClientMessage) error {
    // 处理订阅
    client.SendSuccess("subscribe", "订阅成功", nil)
    return nil
}
```

### 客户端示例

```go
package main

import (
    "log"
    "time"
    
    "github.com/web3-coder-hub/logger"
    "github.com/web3-coder-hub/websocket/adapter/quickws"
    "github.com/web3-coder-hub/websocket/client"
)

func main() {
    // 创建日志
    l, _ := logger.New("websocket-client", nil)
    
    // 配置客户端
    cfg := client.Config{
        URL: "ws://localhost:8080/ws",
        Headers: map[string]string{
            "Access-Token": "your-jwt-token",
            "Client-ID":    "client-001",
            "Client-Type":  "APP",
        },
        ReconnectEnabled:     true,
        ReconnectInterval:    5 * time.Second,
        MaxReconnectAttempts: 10,
        HeartbeatInterval:    30 * time.Second,
        HeartbeatTimeout:     10 * time.Second,
        EnableAutoHeartbeat:  true,
    }
    
    // 创建适配器（QuickWS）
    adapter := quickws.NewClientAdapter()
    
    // 创建客户端
    c := client.NewClient(cfg, adapter, l)
    
    // 注册事件处理器
    c.OnConnect(func() {
        log.Println("已连接到服务器")
        
        // 发送登录消息
        loginData := map[string]interface{}{
            "userId":        "user123",
            "accountId":     "acc456",
            "userSessionId": "session-001",
        }
        c.Send("login", loginData)
    })
    
    c.OnDisconnect(func() {
        log.Println("已断开连接")
    })
    
    c.OnError(func(err error) {
        log.Printf("错误: %v", err)
    })
    
    // 注册消息处理器
    c.RegisterHandler("login", func(msg *client.Message) error {
        log.Printf("登录响应: %s", msg.Message)
        
        // 订阅数据
        subscribeData := map[string]interface{}{
            "subList": []map[string]string{
                {"channel": "quote", "symbol": "BTCUSDT"},
            },
        }
        return c.Send("subscribe", subscribeData)
    })
    
    c.RegisterHandler("subscribe", func(msg *client.Message) error {
        log.Printf("订阅响应: %s", msg.Message)
        return nil
    })
    
    // 连接到服务器
    if err := c.Connect(); err != nil {
        log.Fatal(err)
    }
    
    // 保持运行
    select {}
}
```

完整的工作示例请参阅 [examples/](./examples/)。

## 适配器支持

### 切换不同的适配器

库通过适配器支持多种 WebSocket 实现：

**使用 QuickWS（默认）：**
```go
import "github.com/web3-coder-hub/websocket/adapter/quickws"

adapter := quickws.NewClientAdapter()
client := client.NewClient(config, adapter, logger)
```

**使用 Gorilla WebSocket：**
```go
import (
    "github.com/gorilla/websocket"
    gorillaAdapter "github.com/web3-coder-hub/websocket/adapter/gorilla"
)

dialer := &websocket.Dialer{
    HandshakeTimeout: 45 * time.Second,
}
adapter := gorillaAdapter.NewClientAdapter(dialer)
client := client.NewClient(config, adapter, logger)
```

### 创建自定义适配器

实现 `ClientAdapter` 接口即可支持任何 WebSocket 库：

```go
type ClientAdapter interface {
    Dial(url string, headers http.Header, callback ClientCallback) (ClientConn, error)
    Name() string
}

type ClientConn interface {
    Write(op Opcode, data []byte) error
    WriteTimeout(op Opcode, data []byte, timeout time.Duration) error
    Close() error
    ReadMessage() (Opcode, []byte, error)
    IsConnected() bool
}
```

详细的适配器实现指南请参阅 [ARCHITECTURE.md](./ARCHITECTURE.md)。

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

### 认证类型

服务器通过 `AuthType` 配置支持三种认证模式：

1. **无需认证 (`AuthType: ""`)**
   - 允许匿名连接
   - 自动生成会话 ID 用于追踪
   - 适用于公开的 WebSocket 服务

2. **Token 认证 (`AuthType: "token"`)**
   - 基于 JWT 的认证，使用可插拔的缓存
   - 支持握手认证和延迟认证两种方式
   - 通过 HTTP 头或 LOGIN 事件验证 token
   - 应用层决定缓存实现（Redis、内存等）
   - 完全泛型 - 无预定义数据结构

3. **签名认证 (`AuthType: "sign"`)** _（计划中）_
   - 基于 HMAC 的请求签名
   - 防重放攻击
   - 服务端到服务端认证

### 连接认证

**基于请求头的认证：**
```bash
# 使用自定义请求头连接
ws://localhost:8080/ws

请求头：
Access-Token: Bearer eyJhbGc...
Client-ID: client123
Client-Type: APP
Account-Ids: acc1,acc2,acc3
```

### 延迟认证（浏览器支持）

浏览器原生 WebSocket API 无法设置自定义请求头。库支持通过 LOGIN 事件进行延迟认证：

**流程：**
1. 客户端无凭证连接 → 服务器设置 `StatePendingAuth` 状态
2. 客户端在超时时间内发送 LOGIN 事件并携带凭证
3. 服务器验证通过后更新客户端状态为 `StateAuthenticated`

**JavaScript 示例：**
```javascript
const ws = new WebSocket('ws://localhost:8080/ws');

ws.onopen = () => {
  // 发送 LOGIN 事件并携带凭证
  ws.send(JSON.stringify({
    eventType: 'login',
    token: 'eyJhbGciOiJIUzI1NiIs...',
    eventData: {}
  }));
};
```

**配置：**
```go
serverConfig := websocket.Config{
    AuthType:               "token",
    PendingAuthTimeoutSecs: 10,  // 客户端必须在 10 秒内完成认证
}
```

### Token 过期和刷新

- 可以通过发送新的 LOGIN 事件刷新凭证
- 认证过期后，受保护的操作会触发 `ErrCodeAuthFailed` 错误
- 成功重新认证后，客户端状态自动更新

### 泛型 JWT 解析和自定义认证

库提供了泛型辅助函数用于实现自定义认证逻辑。所有 JWT 结构由应用层定义 - 库不对数据格式做任何假设。

#### 定义数据结构

```go
// 定义你自己的 JWT content 结构
type MyLoginUser struct {
    UserID     string `json:"userId"`
    CacheKey   string `json:"cacheKey"`
    ClientType string `json:"clientType"`
    TenantID   string `json:"tenantId"`  // 任何你需要的自定义字段
}

// 定义缓存的用户数据结构
type MyUserData struct {
    UserID    string `json:"userId"`
    AccountID string `json:"accountId"`
    Roles     []string `json:"roles"`
}
```

#### 实现自定义 Token 解析器

```go
import (
    "context"
    "fmt"
    "time"
    
    "github.com/web3-coder-hub/cache"
    "github.com/web3-coder-hub/logger"
    "github.com/web3-coder-hub/websocket/auth"
)

// 实现 CredentialParseFunc 自定义逻辑
func parseMyToken(token string, c cache.Cache, l logger.Logger) (
    userID, accountID, clientType string, 
    expiry time.Time, 
    err error,
) {
    // 1. 使用泛型 ParseJWT 函数解析 JWT
    loginUser, exp, err := auth.ParseJWT[MyLoginUser](token)
    if err != nil {
        return "", "", "", time.Time{}, fmt.Errorf("解析 JWT 失败: %w", err)
    }

    // 2. 设置过期时间
    if exp > 0 {
        expiry = time.Unix(exp, 0)
    }

    // 3. 可选：从缓存获取用户数据
    if c != nil && loginUser.CacheKey != "" {
        ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
        defer cancel()

        // 应用层定义完整的缓存键格式
        // 库不会添加任何前缀
        cacheKey := fmt.Sprintf("tenant:%s:user:%s", loginUser.TenantID, loginUser.CacheKey)
        
        userData, err := auth.ParseCache[MyUserData](ctx, c, cacheKey)
        if err == nil {
            return userData.UserID, userData.AccountID, loginUser.ClientType, expiry, nil
        }

        // 缓存未命中不是致命错误，继续使用 JWT 数据
        if l != nil {
            l.Warn("从缓存获取用户数据失败", 
                logger.Field{Key: "error", Value: err.Error()})
        }
    }

    // 4. 返回 JWT 中的数据
    return loginUser.UserID, "", loginUser.ClientType, expiry, nil
}
```

#### 使用自定义解析器配置服务器

**基础配置（使用默认设置）：**

```go
cfg := websocket.Config{
    Port:                   8080,
    Path:                   "/ws",
    AuthType:               constant.AuthTypeToken,
    CredentialParseFunc:    parseMyToken,      // 你的自定义解析器
    Cache:                  myCache,
}

server := websocket.NewServer(cfg, hub, adapter, logger.Default())
```

**高级配置（自定义请求头和默认值）：**

```go
// 创建自定义 token 认证策略
authConfig := auth.TokenAuthConfig{
    Cache:             myCache,
    Logger:            logger.Default(),
    ParseFunc:         parseMyToken,
    TokenHeader:       "X-Auth-Token",     // 自定义请求头名称（默认："Access-Token"）
    BearerPrefix:      "Token ",           // 自定义前缀（默认："Bearer "）
    AccountIdsHeader:  "X-Account-Ids",    // 自定义 Account IDs 请求头（默认："Account-Ids"）
    DefaultClientType: "WEB",              // 自定义默认值（默认："APP"）
}

strategy := auth.NewTokenAuthStrategy(authConfig)
authenticator := auth.NewAuthenticator(constant.AuthTypeToken, strategy, 30, logger.Default())

// 创建服务器并设置自定义认证器
cfg := websocket.Config{
    Port: 8080,
    Path: "/ws",
}

server := websocket.NewServer(cfg, hub, adapter, logger.Default())
server.SetAuthenticator(authenticator)
```

#### 可用的泛型辅助函数

**`ParseJWT[T any](token string) (*T, int64, error)`**
- 解析 `content` 字段直接包含数据结构的 JWT
- 返回：解析后的 content、过期时间戳（Unix 秒）、错误
- 示例：`{"content": {"userId": "123"}, "exp": 1234567890}`

**`ParseCache[T any](ctx context.Context, c cache.Cache, cacheKey string) (*T, error)`**
- 从缓存解析用户数据，完全控制缓存键格式
- 应用层定义完整的缓存键（无自动前缀/分隔符）
- 返回：解析后的数据、错误

#### 关键设计原则

1. **无预定义结构**：库不定义任何默认的用户、登录或 JWT payload 结构
2. **完全灵活**：定义任何字段、类型或嵌套结构
3. **完整缓存控制**：应用层定义完整的缓存键格式
4. **可配置常量**：Token 请求头、Bearer 前缀、Account IDs 请求头和客户端类型默认值均可自定义
5. **类型安全**：Go 泛型确保编译时类型检查

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

- **StateConnected**：WebSocket 连接已建立（物理连接）
- **StatePendingAuth**：等待认证（当 AuthType 需要认证但未提供凭证时）
- **StateAuthenticated**：凭证验证成功
- **StateLoggedIn**：应用层登录完成
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
    Port                   int                      // 服务器端口
    Path                   string                   // WebSocket 端点路径
    SessionTimeoutSeconds  int                      // 会话超时时间
    HeartbeatIntervalSecs  int                      // 心跳间隔
    AuthType               string                   // 认证类型：""、"token"、"sign"
    PendingAuthTimeoutSecs int                      // 待认证超时时间（秒）
    EnableSecurity         bool                     // 是否启用 HMAC 签名验证
    SecuritySecret         string                   // HMAC 密钥
    CredentialParseFunc    auth.CredentialParseFunc // 自定义 token 解析器（token 认证必需）
    Cache                  cache.Cache              // 缓存实例，用于身份认证
}
```

## 依赖项

- [antlabs/quickws](https://github.com/antlabs/quickws) - 高性能 WebSocket 库（默认适配器）
- [gorilla/websocket](https://github.com/gorilla/websocket) - 可选的 WebSocket 库
- [google/uuid](https://github.com/google/uuid) - UUID 生成
- [web3-coder-hub/cache](https://github.com/web3-coder-hub/cache) - 缓存抽象层（Redis/Memory/BadgerDB）
- [web3-coder-hub/logger](https://github.com/web3-coder-hub/logger) - 结构化日志

## 性能特性

### 服务端
- **分片机制**：32 个分片最小化高并发场景下的锁竞争
- **非阻塞发送**：默认发送操作通过通道缓冲实现非阻塞
- **连接池化**：高效的每连接一个 goroutine 模型
- **内存效率**：频繁读取的状态使用原子操作

### 客户端
- **自动重连**：可配置的退避和重试策略
- **心跳监控**：主动的连接健康检查
- **事件驱动**：基于回调的架构，开销最小
- **线程安全**：并发安全的消息发送

## 最佳实践

### 服务端
1. **缓冲区大小**：根据消息频率设置 `SendBufferSize`，避免消息丢失
2. **心跳调优**：保持 `PingPeriod < PongWait`，防止误判断开连接
3. **Token 刷新**：在 token 过期前提示客户端重新登录
4. **优雅关闭**：始终调用 hub 和 server 的 `Stop()` 方法，确保连接干净关闭
5. **错误处理**：注册断开连接处理器，清理外部资源

### 客户端
1. **重连策略**：根据使用场景配置 `MaxReconnectAttempts` 和 `ReconnectInterval`
2. **心跳设置**：设置 `HeartbeatTimeout > HeartbeatInterval` 避免误超时
3. **消息处理器**：保持处理器轻量级；使用 goroutine 处理重任务
4. **资源清理**：使用完毕后始终调用 `Close()`
5. **错误恢复**：实现 `OnError` 和 `OnDisconnect` 处理器进行优雅降级

## 许可证

本项目是 web3-coder-hub 生态系统的一部分。

