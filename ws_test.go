package websocket

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/web3-coder-hub/cache"
	"github.com/web3-coder-hub/logger"
	"github.com/web3-coder-hub/websocket/adapter/quickws"
	"github.com/web3-coder-hub/websocket/constant"
)

// TestServerCreation tests server initialization
func TestServerCreation(t *testing.T) {
	cfg := Config{
		Port:                   8080,
		Path:                   "/ws",
		SessionTimeoutSeconds:  300,
		HeartbeatIntervalSecs:  30,
		AuthType:               constant.AuthTypeToken,
		PendingAuthTimeoutSecs: 10,
	}

	hub := NewHub(HubConfig{SendBufferSize: 256}, logger.Default())
	testCache, err := cache.NewBigCache(10, 5*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer testCache.Close()

	serverAdapter := quickws.NewServerAdapter()

	cfg.Cache = testCache
	server := NewServer(cfg, hub, serverAdapter, logger.Default())

	if server == nil {
		t.Fatal("NewServer returned nil")
	}

	if server.config.Port != cfg.Port {
		t.Errorf("Expected port %d, got %d", cfg.Port, server.config.Port)
	}

	if server.config.Path != cfg.Path {
		t.Errorf("Expected path %s, got %s", cfg.Path, server.config.Path)
	}

	if server.hub != hub {
		t.Error("Hub not set correctly")
	}

	if server.adapter == nil {
		t.Error("Adapter not set")
	}
}

// TestRegisterHandler tests message handler registration
func TestRegisterHandler(t *testing.T) {
	cfg := Config{
		Port: 8080,
		Path: "/ws",
	}

	hub := NewHub(HubConfig{SendBufferSize: 256}, logger.Default())
	testCache, _ := cache.NewBigCache(10, 5*time.Minute)
	defer testCache.Close()

	cfg.Cache = testCache
	server := NewServer(cfg, hub, quickws.NewServerAdapter(), logger.Default())

	handlerCalled := false
	testHandler := func(client *Client, msg *ClientMessage) error {
		handlerCalled = true
		return nil
	}

	server.RegisterHandler("test_event", testHandler)

	handler, ok := server.getHandler("test_event")
	if !ok {
		t.Fatal("Handler not registered")
	}

	if handler == nil {
		t.Fatal("Retrieved handler is nil")
	}

	// Test handler execution
	handler(nil, &ClientMessage{EventType: "test_event"})
	if !handlerCalled {
		t.Error("Handler was not called")
	}
}

// TestDisconnectHandler tests disconnect callback
func TestDisconnectHandler(t *testing.T) {
	cfg := Config{
		Port: 8080,
		Path: "/ws",
	}

	hub := NewHub(HubConfig{SendBufferSize: 256}, logger.Default())
	testCache, _ := cache.NewBigCache(10, 5*time.Minute)
	defer testCache.Close()

	cfg.Cache = testCache
	server := NewServer(cfg, hub, quickws.NewServerAdapter(), logger.Default())

	disconnectCalled := false
	var disconnectedClient *Client

	server.SetDisconnectHandler(func(client *Client) {
		disconnectCalled = true
		disconnectedClient = client
	})

	// Create a test client (minimal, just for handler testing)
	client := &Client{ID: "test-id"}

	// Trigger disconnect
	if server.onDisconnect != nil {
		server.onDisconnect(client)
	}

	if !disconnectCalled {
		t.Error("Disconnect handler was not called")
	}

	if disconnectedClient != client {
		t.Error("Disconnect handler received wrong client")
	}
}

// TestServerStartStop tests server lifecycle
func TestServerStartStop(t *testing.T) {
	cfg := Config{
		Port: 0, // Use random port
		Path: "/ws",
	}

	hub := NewHub(HubConfig{SendBufferSize: 256}, logger.Default())
	testCache, _ := cache.NewBigCache(10, 5*time.Minute)
	defer testCache.Close()

	cfg.Cache = testCache
	server := NewServer(cfg, hub, quickws.NewServerAdapter(), logger.Default())

	err := server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Stop(ctx)
	if err != nil {
		t.Errorf("Failed to stop server: %v", err)
	}
}

// TestClientStateTransitions tests client state management
func TestClientStateTransitions(t *testing.T) {
	client := &Client{}
	client.state.Store(int32(constant.StateConnected))

	// Initial state should be Connected
	if constant.ClientState(client.GetState()) != constant.StateConnected {
		t.Error("Initial state should be Connected")
	}

	// Test state transitions
	client.SetState(int32(constant.StatePendingAuth))
	if !client.IsPendingAuth() {
		t.Error("State should be PendingAuth")
	}

	client.SetState(int32(constant.StateAuthenticated))
	if !client.IsAuthenticated() {
		t.Error("State should be Authenticated")
	}

	client.SetState(int32(constant.StateLoggedIn))
	if !client.IsLoggedIn() {
		t.Error("State should be LoggedIn")
	}

	client.SetState(int32(constant.StateClosed))
	if !client.IsClosed() {
		t.Error("State should be Closed")
	}
}

// TestHubOperations tests hub client management
func TestHubOperations(t *testing.T) {
	hub := NewHub(HubConfig{SendBufferSize: 256}, logger.Default())

	// Start hub
	go hub.Run()
	defer hub.Stop()

	// Create test client
	client := &Client{
		ID:        "test-id",
		SessionId: "test-session",
		send:      make(chan []byte, 10),
		done:      make(chan struct{}),
	}
	client.state.Store(int32(constant.StateConnected))

	// Test Register
	hub.Register(client)
	time.Sleep(50 * time.Millisecond) // Wait for async registration

	// Test GetClient
	retrieved, ok := hub.GetClient("test-id")
	if !ok {
		t.Error("Failed to retrieve client by ID")
	}
	if retrieved != client {
		t.Error("Retrieved client doesn't match")
	}

	// Test GetClientBySession
	retrieved, ok = hub.GetClientBySession("test-session")
	if !ok {
		t.Error("Failed to retrieve client by SessionId")
	}
	if retrieved != client {
		t.Error("Retrieved client doesn't match")
	}

	// Test Unregister
	hub.Unregister(client)
	time.Sleep(50 * time.Millisecond) // Wait for async unregistration

	_, ok = hub.GetClient("test-id")
	if ok {
		t.Error("Client should be removed after unregister")
	}
}

// TestClientSendMethods tests client message sending
func TestClientSendMethods(t *testing.T) {
	client := &Client{
		send: make(chan []byte, 10),
		done: make(chan struct{}),
	}
	client.state.Store(int32(constant.StateConnected))

	t.Run("Send", func(t *testing.T) {
		testData := []byte("test message")
		sent := client.Send(testData)
		if !sent {
			t.Error("Send failed")
		}

		select {
		case msg := <-client.send:
			if string(msg) != string(testData) {
				t.Errorf("Expected %s, got %s", testData, msg)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Timeout waiting for message")
		}
	})

	t.Run("SendJSON", func(t *testing.T) {
		testData := map[string]string{"key": "value"}
		sent := client.SendJSON(testData)
		if !sent {
			t.Error("SendJSON failed")
		}

		select {
		case msg := <-client.send:
			var decoded map[string]string
			if err := json.Unmarshal(msg, &decoded); err != nil {
				t.Errorf("Failed to unmarshal: %v", err)
			}
			if decoded["key"] != "value" {
				t.Errorf("Expected value, got %s", decoded["key"])
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Timeout waiting for message")
		}
	})

	t.Run("SendSuccess", func(t *testing.T) {
		sent := client.SendSuccess("test_event", "Success", map[string]string{"data": "test"})
		if !sent {
			t.Error("SendSuccess failed")
		}

		select {
		case msg := <-client.send:
			var resp ServerMessage
			if err := json.Unmarshal(msg, &resp); err != nil {
				t.Errorf("Failed to unmarshal: %v", err)
			}
			if !resp.Success {
				t.Error("Response should be successful")
			}
			if resp.Message != "Success" {
				t.Errorf("Expected 'Success', got %s", resp.Message)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Timeout waiting for message")
		}
	})

	t.Run("SendError", func(t *testing.T) {
		sent := client.SendError("test_event", 400, "Error message")
		if !sent {
			t.Error("SendError failed")
		}

		select {
		case msg := <-client.send:
			var resp ServerMessage
			if err := json.Unmarshal(msg, &resp); err != nil {
				t.Errorf("Failed to unmarshal: %v", err)
			}
			if resp.Success {
				t.Error("Response should not be successful")
			}
			if resp.Code != 400 {
				t.Errorf("Expected code 400, got %d", resp.Code)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Timeout waiting for message")
		}
	})
}

// TestClientAuthMethods tests client authentication methods
func TestClientAuthMethods(t *testing.T) {
	hub := NewHub(HubConfig{SendBufferSize: 256}, logger.Default())
	go hub.Run()
	defer hub.Stop()

	client := &Client{
		ID:        "test-id",
		SessionId: "test-session",
		send:      make(chan []byte, 10),
		done:      make(chan struct{}),
	}
	client.state.Store(int32(constant.StateConnected))

	t.Run("SetAndGetMethods", func(t *testing.T) {
		client.SetSessionId("session-123")
		if client.SessionId != "session-123" {
			t.Errorf("Expected session-123, got %s", client.SessionId)
		}

		client.SetUserId("user-456")
		if client.UserId != "user-456" {
			t.Errorf("Expected user-456, got %s", client.UserId)
		}

		client.SetAccountId("account-789")
		if client.AccountId != "account-789" {
			t.Errorf("Expected account-789, got %s", client.AccountId)
		}

		client.SetClientId("client-abc")
		if client.GetClientId() != "client-abc" {
			t.Errorf("Expected client-abc, got %s", client.GetClientId())
		}

		client.SetClientType("APP")
		if client.GetClientType() != "APP" {
			t.Errorf("Expected APP, got %s", client.GetClientType())
		}

		client.SetClientIp("192.168.1.100")
		if client.GetClientIp() != "192.168.1.100" {
			t.Errorf("Expected 192.168.1.100, got %s", client.GetClientIp())
		}

		client.SetAccountIds("acc1,acc2,acc3")
		if client.GetAccountIdsString() != "acc1,acc2,acc3" {
			t.Errorf("Expected acc1,acc2,acc3, got %s", client.GetAccountIdsString())
		}
	})

	t.Run("AuthDeadline", func(t *testing.T) {
		deadline := time.Now().Add(10 * time.Second)
		client.SetAuthDeadline(deadline)
		if !client.AuthDeadline.Equal(deadline) {
			t.Error("AuthDeadline not set correctly")
		}

		// Set PendingAuth state to test deadline expiration logic
		client.SetState(int32(constant.StatePendingAuth))
		if client.IsAuthExpired() {
			t.Error("Auth should not be expired with future deadline")
		}

		// Set expired deadline
		client.SetAuthDeadline(time.Now().Add(-1 * time.Second))
		if !client.IsAuthExpired() {
			t.Error("Auth should be expired with past deadline in PendingAuth state")
		}

		// Test StateAuthExpired
		client.SetState(int32(constant.StateAuthExpired))
		if !client.IsAuthExpired() {
			t.Error("Auth should be expired with StateAuthExpired")
		}
	})

	t.Run("IsAuthValid", func(t *testing.T) {
		client.SetState(int32(constant.StateAuthenticated))
		if !client.IsAuthValid() {
			t.Error("Auth should be valid for authenticated state")
		}

		client.SetState(int32(constant.StateLoggedIn))
		if client.IsAuthValid() {
			t.Error("Auth should not be valid for logged in state (only StateAuthenticated counts as valid)")
		}

		client.SetState(int32(constant.StateConnected))
		if client.IsAuthValid() {
			t.Error("Auth should not be valid for connected state")
		}
	})

	t.Run("SetLoginInfo", func(t *testing.T) {
		client.SetLoginInfo("user-999", "account-888", []string{"acc1", "acc2"}, "client-777", "session-666", false)

		if client.UserId != "user-999" {
			t.Errorf("Expected user-999, got %s", client.UserId)
		}
		if client.AccountId != "account-888" {
			t.Errorf("Expected account-888, got %s", client.AccountId)
		}
		if client.ClientId != "client-777" {
			t.Errorf("Expected client-777, got %s", client.ClientId)
		}
		if client.SessionId != "session-666" {
			t.Errorf("Expected session-666, got %s", client.SessionId)
		}
		if !client.IsLoggedIn() {
			t.Error("Client should be in logged in state")
		}
	})
}

// TestClientSendWithTimeout tests SendWithTimeout method
func TestClientSendWithTimeout(t *testing.T) {
	client := &Client{
		send: make(chan []byte, 1),
		done: make(chan struct{}),
	}
	client.state.Store(int32(constant.StateConnected))

	t.Run("Success", func(t *testing.T) {
		testData := []byte("test message")
		sent := client.SendWithTimeout(testData, 100*time.Millisecond)
		if !sent {
			t.Error("SendWithTimeout should succeed")
		}

		select {
		case msg := <-client.send:
			if string(msg) != string(testData) {
				t.Errorf("Expected %s, got %s", testData, msg)
			}
		case <-time.After(200 * time.Millisecond):
			t.Error("Timeout waiting for message")
		}
	})

	t.Run("Timeout", func(t *testing.T) {
		// Fill the channel
		client.send <- []byte("blocking message")

		testData := []byte("test message")
		sent := client.SendWithTimeout(testData, 10*time.Millisecond)
		if sent {
			t.Error("SendWithTimeout should timeout")
		}

		// Clear the channel
		<-client.send
	})
}

// TestClientSendBlocking tests SendBlocking method
func TestClientSendBlocking(t *testing.T) {
	client := &Client{
		send: make(chan []byte, 1),
		done: make(chan struct{}),
	}
	client.state.Store(int32(constant.StateConnected))

	testData := []byte("test message")

	// Start a goroutine to consume from the channel
	go func() {
		<-client.send
	}()

	sent := client.SendBlocking(testData)
	if !sent {
		t.Error("SendBlocking should succeed")
	}
}

// TestClientHeartbeat tests heartbeat methods
func TestClientHeartbeat(t *testing.T) {
	client := &Client{
		heartbeatReset: make(chan struct{}, 1),
	}

	t.Run("ResetHeartbeat", func(t *testing.T) {
		client.ResetHeartbeat()

		select {
		case <-client.heartbeatReset:
			// Success
		case <-time.After(100 * time.Millisecond):
			t.Error("Heartbeat reset signal not sent")
		}
	})

	t.Run("UpdateAndGetLastPing", func(t *testing.T) {
		client.UpdateLastPing()
		lastPing := client.GetLastPingAt()
		if lastPing.IsZero() {
			t.Error("LastPingAt should be set")
		}

		// Verify it's recent
		now := time.Now()
		if now.Sub(lastPing) > time.Second {
			t.Error("LastPingAt should be recent")
		}
	})
}

// TestClientClose tests Close method
func TestClientClose(t *testing.T) {
	client := &Client{
		send: make(chan []byte, 10),
		done: make(chan struct{}),
	}
	client.state.Store(int32(constant.StateConnected))

	client.Close()

	if !client.IsClosed() {
		t.Error("Client should be closed")
	}

	// Verify done channel is closed
	select {
	case <-client.done:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Error("Done channel should be closed")
	}

	// Test idempotency - calling Close again should not panic
	client.Close()
}

// BenchmarkClientSend benchmarks client message sending
func BenchmarkClientSend(b *testing.B) {
	client := &Client{
		send: make(chan []byte, 10000),
		done: make(chan struct{}),
	}
	client.state.Store(int32(constant.StateConnected))

	testData := []byte(`{"event":"test","data":"benchmark"}`)

	// Drain channel in background
	go func() {
		for range client.send {
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Send(testData)
	}
}

// BenchmarkClientSendJSON benchmarks JSON message sending
func BenchmarkClientSendJSON(b *testing.B) {
	client := &Client{
		send: make(chan []byte, 10000),
		done: make(chan struct{}),
	}
	client.state.Store(int32(constant.StateConnected))

	testData := map[string]interface{}{
		"event": "test",
		"data":  "benchmark",
	}

	// Drain channel in background
	go func() {
		for range client.send {
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.SendJSON(testData)
	}
}

// BenchmarkHubBroadcast benchmarks hub broadcast performance
func BenchmarkHubBroadcast(b *testing.B) {
	hub := NewHub(HubConfig{SendBufferSize: 256}, logger.Default())

	// Register multiple clients
	clientCount := 100
	clients := make([]*Client, clientCount)
	for i := 0; i < clientCount; i++ {
		client := &Client{
			ID:   "bench-id",
			send: make(chan []byte, 256),
			done: make(chan struct{}),
		}
		client.state.Store(int32(constant.StateConnected))

		// Drain channel in background
		go func(c *Client) {
			for range c.send {
			}
		}(client)

		clients[i] = client
		hub.Register(client)
	}

	time.Sleep(100 * time.Millisecond)

	testMsg := &ServerMessage{
		Timestamp: time.Now(),
		Success:   true,
		EventType: "test_event",
		Data:      map[string]string{"key": "value"},
	}

	testData, _ := json.Marshal(testMsg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hub.Broadcast(testData)
	}

	// Cleanup
	for _, client := range clients {
		close(client.send)
	}
}

// BenchmarkJSONMarshal benchmarks JSON marshaling
func BenchmarkJSONMarshal(b *testing.B) {
	msg := &ServerMessage{
		Timestamp: time.Now(),
		Message:   "test message",
		Success:   true,
		Data:      map[string]string{"key": "value"},
		Code:      0,
		EventType: "test_event",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(msg)
	}
}
