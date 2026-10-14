# WebSocket Library

[中文文档](./README_CN.md) | English

A high-performance, production-ready WebSocket library in Go with both server and client implementations. Features pluggable adapter architecture supporting multiple WebSocket libraries, authentication, message routing, auto-reconnection, and scalable connection management.

## Features

### Core Capabilities

#### Server Features
- **High-Performance Architecture**: Sharded client management using FNV-1a hashing for minimal lock contention
- **Flexible Authentication**: JWT-based authentication with Redis cache integration and optional anonymous connections
- **Message Security**: HMAC-SHA256 signature verification for message integrity
- **Multi-Tenancy Support**: Track clients by user ID, account ID, and session ID
- **Heartbeat Management**: Automatic ping/pong with configurable intervals
- **Graceful Shutdown**: Clean connection closure and resource cleanup

#### Client Features
- **Auto Reconnection**: Configurable retry intervals and maximum attempts
- **Heartbeat Detection**: Automatic ping/pong health monitoring
- **Event-Driven**: OnConnect, OnDisconnect, OnError callbacks
- **Message Routing**: Event-based message handler registration
- **Session Management**: Automatic session ID tracking
- **Thread-Safe**: Safe for concurrent use

### Advanced Features
- **Pluggable Adapters**: Support multiple WebSocket libraries (QuickWS, Gorilla, etc.) through unified adapter interface
- **Sharded Client Storage**: 32-shard concurrent map reduces lock contention in high-load scenarios
- **Flexible Message Routing**: Send messages to specific users, accounts, sessions, or broadcast to all
- **Authentication Caching**: Cache authentication info after JWT validation during handshake to avoid redundant Redis queries
- **Auto-Login**: Automatic login flow triggered when authentication data is available during handshake
- **BMS Support**: Backend management system login with operator tracking
- **Timeout Controls**: Configurable timeouts for message sending operations

## Architecture

### Adapter Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Application Layer (Server/Client)              │
├─────────────────────────────────────────────────────────────┤
│                   Adapter Interface Layer                    │
├─────────────────────────────────────────────────────────────┤
│  QuickWS Adapter  │  Gorilla Adapter  │  Custom Adapters   │
└─────────────────────────────────────────────────────────────┘
```

The library uses a pluggable adapter pattern that allows you to:
- Switch WebSocket libraries without changing application code
- Support multiple libraries simultaneously
- Add custom adapters for specialized requirements

### Server Components

#### Hub
Central connection manager that maintains:
- Client connections indexed by connection ID
- Session mappings for fast session-based lookups
- User and account mappings supporting one-to-many relationships
- Registration/unregistration channels for thread-safe client lifecycle management

#### Server Connection (Server-side Client)
Represents individual WebSocket connections on the server:
- Unique connection ID and session ID
- User identity (userId, accountId, accountIds)
- Client metadata (clientType, clientId, remoteAddr)
- Message send channel with configurable buffer size
- State management (Connected, LoggedIn, Closed)
- Authentication cache to avoid redundant token validation

#### Server
HTTP server handling WebSocket upgrades with:
- Pluggable message handlers for different event types
- JWT token validation via Redis cache
- HMAC signature verification for secure messaging
- Disconnect callbacks for cleanup operations

#### Authenticator
Connection-level authentication supporting:
- JWT token parsing and validation
- Redis-based user data lookup
- Real IP extraction from proxy headers
- Client fingerprinting for anonymous connections
- Token expiry tracking and refresh prompting

### Client Components

#### WebSocket Client
Full-featured WebSocket client with:
- Automatic reconnection with exponential backoff support
- Heartbeat monitoring and timeout detection
- Event-driven message handling
- Connection state management
- Thread-safe message sending

#### Adapters
- **QuickWS Adapter**: High-performance default adapter
- **Gorilla Adapter**: Popular community library support
- **Custom Adapters**: Easy to implement for any WebSocket library

## Installation

```bash
go get github.com/web3-coder-hub/websocket
```

## Quick Start

### Server Example

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
    // Initialize logger
    l, _ := logger.New("websocket-server", nil)
    
    // Initialize cache
    c, _ := cache.NewBigCache(100, 10*time.Minute)
    
    // Configure hub
    hubConfig := websocket.HubConfig{
        WriteWait:      10 * time.Second,
        PongWait:       60 * time.Second,
        PingPeriod:     54 * time.Second,
        MaxMessageSize: 512 * 1024,
        SendBufferSize: 256,
    }
    
    // Create hub and start
    hub := websocket.NewHub(hubConfig, l)
    go hub.Run()
    
    // Configure server
    serverConfig := websocket.Config{
        Port:                  8080,
        Path:                  "/ws",
        SessionTimeoutSeconds: 3600,
        HeartbeatIntervalSecs: 30,
        MustLogin:             false,
        EnableSecurity:        true,
        SecuritySecret:        "your-secret-key",
    }
    
    // Create server
    server := websocket.NewServer(serverConfig, hub, c, l)
    
    // Register message handlers
    server.RegisterHandler("login", handleLogin)
    server.RegisterHandler("subscribe", handleSubscribe)
    
    // Set disconnect handler
    server.SetDisconnectHandler(func(client *websocket.Client) {
        log.Printf("Client disconnected: %s", client.ID)
    })
    
    // Start server
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }
    
    // Graceful shutdown
    // ...
}

func handleLogin(client *websocket.Client, msg *websocket.ClientMessage) error {
    // Handle login
    client.SendSuccess("login", "Login successful", nil)
    return nil
}

func handleSubscribe(client *websocket.Client, msg *websocket.ClientMessage) error {
    // Handle subscription
    client.SendSuccess("subscribe", "Subscribed successfully", nil)
    return nil
}
```

### Client Example

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
    // Create logger
    l, _ := logger.New("websocket-client", nil)
    
    // Configure client
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
    
    // Create adapter (QuickWS)
    adapter := quickws.NewClientAdapter()
    
    // Create client
    c := client.NewClient(cfg, adapter, l)
    
    // Register event handlers
    c.OnConnect(func() {
        log.Println("Connected to server")
        
        // Send login message
        loginData := map[string]interface{}{
            "userId":        "user123",
            "accountId":     "acc456",
            "userSessionId": "session-001",
        }
        c.Send("login", loginData)
    })
    
    c.OnDisconnect(func() {
        log.Println("Disconnected from server")
    })
    
    c.OnError(func(err error) {
        log.Printf("Error: %v", err)
    })
    
    // Register message handlers
    c.RegisterHandler("login", func(msg *client.Message) error {
        log.Printf("Login response: %s", msg.Message)
        
        // Subscribe to data
        subscribeData := map[string]interface{}{
            "subList": []map[string]string{
                {"channel": "quote", "symbol": "BTCUSDT"},
            },
        }
        return c.Send("subscribe", subscribeData)
    })
    
    c.RegisterHandler("subscribe", func(msg *client.Message) error {
        log.Printf("Subscribe response: %s", msg.Message)
        return nil
    })
    
    // Connect to server
    if err := c.Connect(); err != nil {
        log.Fatal(err)
    }
    
    // Keep running
    select {}
}
```

See [examples/](./examples/) for complete working examples.

## Adapter Support

### Switching Between Adapters

The library supports multiple WebSocket implementations through adapters:

**Using QuickWS (Default):**
```go
import "github.com/web3-coder-hub/websocket/adapter/quickws"

adapter := quickws.NewClientAdapter()
client := client.NewClient(config, adapter, logger)
```

**Using Gorilla WebSocket:**
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

### Creating Custom Adapters

Implement the `ClientAdapter` interface to support any WebSocket library:

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

See [ARCHITECTURE.md](./ARCHITECTURE.md) for detailed adapter implementation guide.

## Message Format

### Client to Server

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

### Server to Client (Response)

```json
{
  "timestamp": "2026-10-14T10:30:00Z",
  "eventType": "login",
  "success": true,
  "message": "Login successful",
  "code": 0,
  "data": {}
}
```

### Server to Client (Push)

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

## Authentication

### Connection Authentication (Handshake)

The server supports multiple authentication modes:

1. **JWT Token Authentication**: Token provided via `Access-Token` header or query parameter
2. **Anonymous Connections**: Allowed when `MustLogin = false`
3. **Auto-generated Client ID**: Based on User-Agent + IP when not provided

```bash
# Connect with token
ws://localhost:8080/ws?Access-Token=eyJhbGc...&Client-ID=client123

# Headers (preferred)
Access-Token: Bearer eyJhbGc...
Client-ID: client123
Client-Type: APP
Account-Ids: acc1,acc2,acc3
```

### Message Authentication (HMAC Signature)

When `EnableSecurity = true`, all non-heartbeat messages require HMAC-SHA256 signature:

**Signing Algorithm**:
```
sign = HMAC-SHA256(
  "ClientType=APP&ClientId=xxx&Nonce=xxx&Param=sortedJSON",
  secret
)
```

The `eventData` is sorted by key before signing to ensure consistency.

### Token Expiry Handling

- JWT expiry time is cached in `Client.AuthExpiry` during handshake
- Expired connections receive `ErrCodeAuthFailed` for non-login events
- Clients must send new `login` event with fresh token to refresh authentication

## Message Routing

### Send to Specific Targets

```go
// Send to user (all connections)
hub.SendToUser("user123", messageData)

// Send to account (all connections)
hub.SendToAccount("acc456", messageData)

// Send to session (single connection)
hub.SendToSession("session-id", messageData)

// Send with timeout
hub.SendToUserWithTimeout("user123", messageData, 5*time.Second)

// Blocking send
hub.SendToSessionBlocking("session-id", messageData)
```

### Broadcast Messages

```go
// Broadcast to all connected clients
hub.Broadcast(messageData)

// Broadcast to logged-in clients only
hub.BroadcastToLoggedIn(messageData)
```

### Query Clients

```go
// Get client by connection ID
client, ok := hub.GetClient("conn-id")

// Get client by session ID
client, ok := hub.GetClientBySession("session-id")

// Get all clients for a user
clients := hub.GetClientsByUserId("user123")

// Get all clients for an account
clients := hub.GetClientsByAccountId("acc456")

// Get total connection count
count := hub.GetTotalConnections()
```

## Client Management

### Sharded Architecture

The hub uses a 32-shard concurrent map to reduce lock contention:

- **FNV-1a Hashing**: Fast, uniform distribution of keys across shards
- **Per-Shard Locking**: Each shard has its own mutex
- **Prefix-based Scanning**: Efficient range queries for user/account lookups

### Client States

- **StateConnected**: Initial state after WebSocket upgrade
- **StateLoggedIn**: After successful login event
- **StateClosed**: Connection closed, pending cleanup

### Cleanup Operations

```go
// Clean up inactive connections
removedCount := hub.CleanupInactive(5 * time.Minute)

// Iterate all clients
hub.ForEachClient(func(client *Client) bool {
    // Process client
    return true // continue iteration
})

// Iterate logged-in clients only
hub.ForEachLoggedInClient(func(client *Client) bool {
    // Process logged-in client
    return true
})
```

## Heartbeat Mechanism

The server implements bidirectional heartbeat:

1. **Server-to-Client Ping**: Sent automatically at configured intervals
2. **Client-to-Server Ping**: Resets server heartbeat timer to avoid redundant pings
3. **Heartbeat Channel**: `heartbeat` with `ping`/`pong` event types

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

## Error Codes

### Client Errors (4xxx)
- `4001` - Parameter validation failed
- `4002` - Not logged in
- `4003` - Permission denied
- `4004` - Signature verification failed
- `4401` - Authentication failed

### Server Errors (5xxx)
- `5001` - Internal server error
- `5002` - Message queue error
- `5003` - Database error
- `5004` - Subscription center error

## Configuration

### Hub Configuration

```go
type HubConfig struct {
    WriteWait      time.Duration // Write timeout (e.g., 10s)
    PongWait       time.Duration // Pong wait timeout (e.g., 60s)
    PingPeriod     time.Duration // Ping interval (e.g., 54s)
    MaxMessageSize int64         // Max message size in bytes
    SendBufferSize int           // Send channel buffer size
}
```

### Server Configuration

```go
type Config struct {
    Port                  int    // Server port
    Path                  string // WebSocket endpoint path
    SessionTimeoutSeconds int    // Session timeout
    HeartbeatIntervalSecs int    // Heartbeat interval
    MustLogin             bool   // Require login before accepting messages
    EnableSecurity        bool   // Enable HMAC signature verification
    SecuritySecret        string // HMAC secret key
}
```

## Dependencies

- [antlabs/quickws](https://github.com/antlabs/quickws) - High-performance WebSocket library (default adapter)
- [gorilla/websocket](https://github.com/gorilla/websocket) - Alternative WebSocket library (optional)
- [google/uuid](https://github.com/google/uuid) - UUID generation
- [web3-coder-hub/cache](https://github.com/web3-coder-hub/cache) - Cache abstraction (Redis/Memory/BadgerDB)
- [web3-coder-hub/logger](https://github.com/web3-coder-hub/logger) - Structured logging

## Performance Characteristics

### Server
- **Sharding**: 32 shards minimize lock contention in high-concurrency scenarios
- **Non-blocking Sends**: Default send operations are non-blocking with channel buffering
- **Connection Pooling**: Efficient goroutine-per-connection model
- **Memory Efficiency**: Atomic operations for frequently-read state

### Client
- **Automatic Reconnection**: Configurable backoff and retry strategies
- **Heartbeat Monitoring**: Proactive connection health checks
- **Event-Driven**: Minimal overhead with callback-based architecture
- **Thread-Safe**: Concurrent-safe message sending

## Best Practices

### Server
1. **Buffer Sizing**: Set `SendBufferSize` based on message frequency to avoid dropped messages
2. **Heartbeat Tuning**: Keep `PingPeriod < PongWait` to prevent false disconnections
3. **Token Refresh**: Prompt clients to re-login before token expiry
4. **Graceful Shutdown**: Always call `Stop()` on hub and server to close connections cleanly
5. **Error Handling**: Register disconnect handlers to clean up external resources

### Client
1. **Reconnection Strategy**: Configure `MaxReconnectAttempts` and `ReconnectInterval` based on your use case
2. **Heartbeat Settings**: Set `HeartbeatTimeout > HeartbeatInterval` to avoid false timeouts
3. **Message Handlers**: Keep handlers lightweight; use goroutines for heavy processing
4. **Resource Cleanup**: Always call `Close()` when done
5. **Error Recovery**: Implement `OnError` and `OnDisconnect` handlers for graceful degradation

## License

This project is part of the web3-coder-hub ecosystem.

## Version History

- `v1.0.5` - Latest stable release
- `v1.0.4` - Enhanced authentication and security
- `v1.0.3` - Sharded client management
- `v1.0.2` - Initial multi-tenancy support
- `v1.0.1` - First tagged release
