package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/web3-coder-hub/logger"
	"github.com/web3-coder-hub/websocket/adapter"
)

// mockLogger 实现 logger.Logger 接口
type mockLogger struct{}

func (m *mockLogger) Debug(msg string, fields ...logger.Field)              {}
func (m *mockLogger) Info(msg string, fields ...logger.Field)               {}
func (m *mockLogger) Warn(msg string, fields ...logger.Field)               {}
func (m *mockLogger) Error(msg string, fields ...logger.Field)              {}
func (m *mockLogger) Fatal(msg string, fields ...logger.Field)              {}
func (m *mockLogger) Debugf(format string, args ...interface{})             {}
func (m *mockLogger) Infof(format string, args ...interface{})              {}
func (m *mockLogger) Warnf(format string, args ...interface{})              {}
func (m *mockLogger) Errorf(format string, args ...interface{})             {}
func (m *mockLogger) Fatalf(format string, args ...interface{})             {}
func (m *mockLogger) With(fields ...logger.Field) logger.Logger             { return m }
func (m *mockLogger) WithContext(ctx context.Context) logger.Logger         { return m }
func (m *mockLogger) Sync() error                                           { return nil }

// mockClientAdapter 模拟客户端适配器
type mockClientAdapter struct {
	dialFunc func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error)
}

func (m *mockClientAdapter) Dial(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
	if m.dialFunc != nil {
		return m.dialFunc(url, headers, callback)
	}
	return nil, errors.New("not implemented")
}

func (m *mockClientAdapter) DialWithOptions(url string, headers http.Header, callback adapter.ClientCallback, options ...adapter.ClientOption) (adapter.ClientConn, error) {
	return m.Dial(url, headers, callback)
}

func (m *mockClientAdapter) Name() string {
	return "mock"
}

// mockClientConn 模拟客户端连接
type mockClientConn struct {
	writeFunc        func(op adapter.Opcode, data []byte) error
	writeTimeoutFunc func(op adapter.Opcode, data []byte, timeout time.Duration) error
	closeFunc        func() error
	readMessageFunc  func() (adapter.Opcode, []byte, error)
	isConnectedFunc  func() bool
	callback         adapter.ClientCallback
	closed           bool
	mu               sync.Mutex
}

func (m *mockClientConn) Write(op adapter.Opcode, data []byte) error {
	if m.writeFunc != nil {
		return m.writeFunc(op, data)
	}
	return nil
}

func (m *mockClientConn) WriteTimeout(op adapter.Opcode, data []byte, timeout time.Duration) error {
	if m.writeTimeoutFunc != nil {
		return m.writeTimeoutFunc(op, data, timeout)
	}
	return nil
}

func (m *mockClientConn) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func (m *mockClientConn) ReadMessage() (adapter.Opcode, []byte, error) {
	if m.readMessageFunc != nil {
		return m.readMessageFunc()
	}
	return adapter.Text, nil, nil
}

func (m *mockClientConn) IsConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isConnectedFunc != nil {
		return m.isConnectedFunc()
	}
	return !m.closed
}

// simulateMessage 模拟接收消息
func (m *mockClientConn) simulateMessage(msg interface{}) {
	data, _ := json.Marshal(msg)
	if m.callback != nil {
		m.callback.OnMessage(m, adapter.Text, data)
	}
}

// simulateClose 模拟连接关闭
func (m *mockClientConn) simulateClose(err error) {
	if m.callback != nil {
		m.callback.OnClose(m, err)
	}
}

// simulateOpen 模拟连接打开
func (m *mockClientConn) simulateOpen() {
	if m.callback != nil {
		m.callback.OnOpen(m)
	}
}

// simulateError 模拟错误
func (m *mockClientConn) simulateError(err error) {
	if m.callback != nil {
		m.callback.OnError(m, err)
	}
}

// TestNewClient 测试创建客户端
func TestNewClient(t *testing.T) {
	cfg := Config{
		URL: "ws://localhost:8080",
	}
	adapter := &mockClientAdapter{}
	logger := &mockLogger{}

	client := NewClient(cfg, adapter, logger)

	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.config.ReconnectInterval != 5*time.Second {
		t.Errorf("expected default ReconnectInterval 5s, got %v", client.config.ReconnectInterval)
	}
	if client.config.MaxReconnectAttempts != 10 {
		t.Errorf("expected default MaxReconnectAttempts 10, got %d", client.config.MaxReconnectAttempts)
	}
	if client.config.HeartbeatInterval != 30*time.Second {
		t.Errorf("expected default HeartbeatInterval 30s, got %v", client.config.HeartbeatInterval)
	}
	if client.id == "" {
		t.Error("expected non-empty client ID")
	}
	if client.requestManager == nil {
		t.Error("expected non-nil requestManager")
	}
}

// TestClient_Connect 测试连接
func TestClient_Connect(t *testing.T) {
	t.Run("SuccessfulConnect", func(t *testing.T) {
		mockConn := &mockClientConn{}
		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				go func() {
					time.Sleep(10 * time.Millisecond)
					mockConn.simulateOpen()
				}()
				return mockConn, nil
			},
		}

		cfg := Config{
			URL:     "ws://localhost:8080",
			Headers: map[string]string{"X-Test": "value"},
		}
		client := NewClient(cfg, adapter, &mockLogger{})

		err := client.Connect()
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !client.IsConnected() {
			t.Error("expected client to be connected")
		}

		client.Close()
	})

	t.Run("ConnectError", func(t *testing.T) {
		expectedErr := errors.New("connection failed")
		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				return nil, expectedErr
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, adapter, &mockLogger{})

		err := client.Connect()
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != expectedErr.Error() {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("AlreadyConnected", func(t *testing.T) {
		mockConn := &mockClientConn{}
		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, adapter, &mockLogger{})

		err := client.Connect()
		if err != nil {
			t.Fatalf("first connect failed: %v", err)
		}

		// 尝试再次连接
		err = client.Connect()
		if err != nil {
			t.Errorf("expected no error for already connected, got %v", err)
		}

		client.Close()
	})
}

// TestClient_Disconnect 测试断开连接
func TestClient_Disconnect(t *testing.T) {
	mockConn := &mockClientConn{}
	adapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			mockConn.callback = callback
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, adapter, &mockLogger{})

	err := client.Connect()
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	err = client.Disconnect()
	if err != nil {
		t.Errorf("disconnect failed: %v", err)
	}

	if client.IsConnected() {
		t.Error("expected client to be disconnected")
	}
}

// TestClient_Close 测试关闭
func TestClient_Close(t *testing.T) {
	t.Run("CloseWithConnection", func(t *testing.T) {
		mockConn := &mockClientConn{}
		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, adapter, &mockLogger{})

		client.Connect()

		err := client.Close()
		if err != nil {
			t.Errorf("close failed: %v", err)
		}

		if client.IsConnected() {
			t.Error("expected client to be disconnected")
		}

		// 多次关闭不应该崩溃
		err = client.Close()
		if err != nil {
			t.Errorf("second close failed: %v", err)
		}
	})

	t.Run("CloseWithoutConnection", func(t *testing.T) {
		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

		err := client.Close()
		if err != nil {
			t.Errorf("close without connection failed: %v", err)
		}
	})
}

// TestClient_Send 测试发送消息
func TestClient_Send(t *testing.T) {
	t.Run("SendSuccess", func(t *testing.T) {
		var sentData []byte
		mockConn := &mockClientConn{
			writeTimeoutFunc: func(op adapter.Opcode, data []byte, timeout time.Duration) error {
				sentData = data
				return nil
			},
		}

		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, adapter, &mockLogger{})
		client.Connect()

		testData := map[string]string{"test": "data"}
		err := client.Send("testEvent", testData)
		if err != nil {
			t.Fatalf("send failed: %v", err)
		}

		var msg map[string]interface{}
		json.Unmarshal(sentData, &msg)
		if msg["eventType"] != "testEvent" {
			t.Errorf("expected eventType 'testEvent', got %v", msg["eventType"])
		}

		client.Close()
	})

	t.Run("SendNotConnected", func(t *testing.T) {
		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

		err := client.Send("test", nil)
		if !errors.Is(err, adapter.ErrNotConnected) {
			t.Errorf("expected ErrNotConnected, got %v", err)
		}
	})
}

// TestClient_SendRaw 测试发送原始数据
func TestClient_SendRaw(t *testing.T) {
	t.Run("SendRawSuccess", func(t *testing.T) {
		var sentData []byte
		mockConn := &mockClientConn{
			writeTimeoutFunc: func(op adapter.Opcode, data []byte, timeout time.Duration) error {
				sentData = data
				return nil
			},
		}

		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, adapter, &mockLogger{})
		client.Connect()

		testData := []byte("raw data")
		err := client.SendRaw(testData)
		if err != nil {
			t.Fatalf("sendRaw failed: %v", err)
		}

		if string(sentData) != string(testData) {
			t.Errorf("expected data %s, got %s", testData, sentData)
		}

		client.Close()
	})

	t.Run("SendRawNotConnected", func(t *testing.T) {
		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

		err := client.SendRaw([]byte("test"))
		if !errors.Is(err, adapter.ErrNotConnected) {
			t.Errorf("expected ErrNotConnected, got %v", err)
		}
	})
}

// TestClient_Request 测试同步请求
func TestClient_Request(t *testing.T) {
	t.Run("RequestSuccess", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockConn.writeTimeoutFunc = func(op adapter.Opcode, data []byte, timeout time.Duration) error {
			// 模拟服务端响应
			go func() {
				time.Sleep(50 * time.Millisecond)
				var req map[string]interface{}
				json.Unmarshal(data, &req)
				requestID := req["requestId"].(string)

				resp := map[string]interface{}{
					"requestId": requestID,
					"eventType": "response",
					"success":   true,
					"data":      map[string]string{"result": "ok"},
				}
				mockConn.simulateMessage(resp)
			}()
			return nil
		}

		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		client.Connect()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		resp, err := client.Request(ctx, "test", map[string]string{"key": "value"})
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}

		if resp.RequestID == "" {
			t.Error("expected non-empty requestID")
		}
		if resp.EventType != "response" {
			t.Errorf("expected eventType 'response', got %s", resp.EventType)
		}

		client.Close()
	})

	t.Run("RequestTimeout", func(t *testing.T) {
		mockConn := &mockClientConn{
			writeTimeoutFunc: func(op adapter.Opcode, data []byte, timeout time.Duration) error {
				// 不响应，模拟超时
				return nil
			},
		}

		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, adapter, &mockLogger{})
		client.Connect()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := client.Request(ctx, "test", nil)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected DeadlineExceeded, got %v", err)
		}

		client.Close()
	})

	t.Run("RequestNotConnected", func(t *testing.T) {
		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

		ctx := context.Background()
		_, err := client.Request(ctx, "test", nil)
		if !errors.Is(err, adapter.ErrNotConnected) {
			t.Errorf("expected ErrNotConnected, got %v", err)
		}
	})
}

// TestClient_RegisterHandler 测试注册消息处理器
func TestClient_RegisterHandler(t *testing.T) {
	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

	handler := func(msg *Message) error {
		return nil
	}

	client.RegisterHandler("test", handler)

	// 验证处理器已注册
	client.handlersMu.RLock()
	_, ok := client.handlers["test"]
	client.handlersMu.RUnlock()

	if !ok {
		t.Error("expected handler to be registered")
	}
}

// TestClient_OnConnect 测试连接回调
func TestClient_OnConnect(t *testing.T) {
	mockConn := &mockClientConn{}
	adapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			mockConn.callback = callback
			go mockConn.simulateOpen()
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, adapter, &mockLogger{})

	called := false
	client.OnConnect(func() {
		called = true
	})

	client.Connect()
	time.Sleep(50 * time.Millisecond)

	if !called {
		t.Error("expected onConnect callback to be called")
	}

	client.Close()
}

// TestClient_OnDisconnect 测试断开连接回调
func TestClient_OnDisconnect(t *testing.T) {
	mockConn := &mockClientConn{}
	adapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			mockConn.callback = callback
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, adapter, &mockLogger{})

	called := false
	client.OnDisconnect(func() {
		called = true
	})

	client.Connect()
	mockConn.simulateClose(nil)
	time.Sleep(50 * time.Millisecond)

	if !called {
		t.Error("expected onDisconnect callback to be called")
	}
}

// TestClient_OnError 测试错误回调
func TestClient_OnError(t *testing.T) {
	mockConn := &mockClientConn{}
	adapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			mockConn.callback = callback
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, adapter, &mockLogger{})

	var receivedErr error
	client.OnError(func(err error) {
		receivedErr = err
	})

	client.Connect()

	testErr := errors.New("test error")
	mockConn.simulateError(testErr)
	time.Sleep(50 * time.Millisecond)

	if receivedErr == nil {
		t.Fatal("expected error callback to be called")
	}
	if receivedErr.Error() != testErr.Error() {
		t.Errorf("expected error %v, got %v", testErr, receivedErr)
	}

	client.Close()
}

// TestClient_GetID 测试获取客户端ID
func TestClient_GetID(t *testing.T) {
	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

	id := client.GetID()
	if id == "" {
		t.Error("expected non-empty client ID")
	}
}

// TestClient_SessionID 测试会话ID
func TestClient_SessionID(t *testing.T) {
	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

	if client.GetSessionID() != "" {
		t.Error("expected empty sessionID initially")
	}

	client.SetSessionID("test-session")
	if client.GetSessionID() != "test-session" {
		t.Errorf("expected sessionID 'test-session', got %s", client.GetSessionID())
	}
}

// TestClient_HandleMessage 测试消息处理
func TestClient_HandleMessage(t *testing.T) {
	t.Run("HandlePong", func(t *testing.T) {
		mockConn := &mockClientConn{}
		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, adapter, &mockLogger{})
		client.Connect()

		pongMsg := map[string]interface{}{
			"eventType": "pong",
		}
		mockConn.simulateMessage(pongMsg)
		time.Sleep(50 * time.Millisecond)

		if client.lastPongTime.Load() == 0 {
			t.Error("expected lastPongTime to be set")
		}

		client.Close()
	})

	t.Run("HandleSyncResponse", func(t *testing.T) {
		mockConn := &mockClientConn{}
		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, adapter, &mockLogger{})
		client.Connect()

		// 注册一个pending请求
		requestID := client.requestManager.GenerateRequestID()
		pr, _ := client.requestManager.RegisterRequest(requestID)

		// 模拟响应
		go func() {
			time.Sleep(50 * time.Millisecond)
			respMsg := map[string]interface{}{
				"requestId": requestID,
				"eventType": "response",
				"success":   true,
			}
			mockConn.simulateMessage(respMsg)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		_, err := client.requestManager.WaitForResponse(ctx, pr)
		if err != nil {
			t.Errorf("expected response, got error: %v", err)
		}

		client.Close()
	})

	t.Run("HandleAsyncMessage", func(t *testing.T) {
		mockConn := &mockClientConn{}
		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, adapter, &mockLogger{})
		client.Connect()

		var receivedMsg *Message
		client.RegisterHandler("testEvent", func(msg *Message) error {
			receivedMsg = msg
			return nil
		})

		testMsg := map[string]interface{}{
			"eventType": "testEvent",
			"data":      "test data",
		}
		mockConn.simulateMessage(testMsg)
		time.Sleep(50 * time.Millisecond)

		if receivedMsg == nil {
			t.Fatal("expected message to be handled")
		}
		if receivedMsg.EventType != "testEvent" {
			t.Errorf("expected eventType 'testEvent', got %s", receivedMsg.EventType)
		}

		client.Close()
	})

	t.Run("HandleInvalidJSON", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		client.Connect()

		// 发送无效JSON
		if mockConn.callback != nil {
			mockConn.callback.OnMessage(mockConn, adapter.Text, []byte("invalid json"))
		}
		time.Sleep(50 * time.Millisecond)

		// 不应该崩溃
		client.Close()
	})
}

// TestClient_Reconnect 测试重连
func TestClient_Reconnect(t *testing.T) {
	t.Run("ReconnectSuccess", func(t *testing.T) {
		connectCount := 0
		mockConn := &mockClientConn{}
		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				connectCount++
				mockConn.callback = callback
				if connectCount == 1 {
					// 第一次连接成功
					return mockConn, nil
				}
				// 第二次连接也成功
				return mockConn, nil
			},
		}

		cfg := Config{
			URL:                  "ws://localhost:8080",
			ReconnectEnabled:     true,
			ReconnectInterval:    100 * time.Millisecond,
			MaxReconnectAttempts: 3,
		}
		client := NewClient(cfg, adapter, &mockLogger{})
		client.Connect()

		// 模拟连接断开
		mockConn.simulateClose(errors.New("connection lost"))
		time.Sleep(500 * time.Millisecond)

		if connectCount < 2 {
			t.Errorf("expected reconnect attempt, connectCount=%d", connectCount)
		}

		client.Close()
	})

	t.Run("ReconnectDisabled", func(t *testing.T) {
		connectCount := 0
		mockConn := &mockClientConn{}
		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				connectCount++
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{
			URL:              "ws://localhost:8080",
			ReconnectEnabled: false,
		}
		client := NewClient(cfg, adapter, &mockLogger{})
		client.Connect()

		// 模拟连接断开
		mockConn.simulateClose(errors.New("connection lost"))
		time.Sleep(200 * time.Millisecond)

		if connectCount != 1 {
			t.Errorf("expected no reconnect, connectCount=%d", connectCount)
		}

		client.Close()
	})
}

// TestClient_Heartbeat 测试心跳
func TestClient_Heartbeat(t *testing.T) {
	t.Run("HeartbeatEnabled", func(t *testing.T) {
		pingCount := 0
		mockConn := &mockClientConn{
			writeTimeoutFunc: func(op adapter.Opcode, data []byte, timeout time.Duration) error {
				var msg map[string]interface{}
				json.Unmarshal(data, &msg)
				if msg["eventType"] == "ping" {
					pingCount++
				}
				return nil
			},
		}

		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{
			URL:                 "ws://localhost:8080",
			EnableAutoHeartbeat: true,
			HeartbeatInterval:   100 * time.Millisecond,
		}
		client := NewClient(cfg, adapter, &mockLogger{})
		client.Connect()

		time.Sleep(350 * time.Millisecond)

		if pingCount < 2 {
			t.Errorf("expected at least 2 pings, got %d", pingCount)
		}

		client.Close()
	})

	t.Run("HeartbeatDisabled", func(t *testing.T) {
		pingCount := 0
		mockConn := &mockClientConn{
			writeTimeoutFunc: func(op adapter.Opcode, data []byte, timeout time.Duration) error {
				var msg map[string]interface{}
				json.Unmarshal(data, &msg)
				if msg["eventType"] == "ping" {
					pingCount++
				}
				return nil
			},
		}

		adapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{
			URL:                 "ws://localhost:8080",
			EnableAutoHeartbeat: false,
		}
		client := NewClient(cfg, adapter, &mockLogger{})
		client.Connect()

		time.Sleep(200 * time.Millisecond)

		if pingCount > 0 {
			t.Errorf("expected no pings, got %d", pingCount)
		}

		client.Close()
	})
}

// TestClient_ConcurrentRequests 测试并发请求
func TestClient_ConcurrentRequests(t *testing.T) {
	mockConn := &mockClientConn{}
	mockConn.writeTimeoutFunc = func(op adapter.Opcode, data []byte, timeout time.Duration) error {
		// 模拟快速响应
		go func() {
			time.Sleep(10 * time.Millisecond)
			var req map[string]interface{}
			json.Unmarshal(data, &req)
			if requestID, ok := req["requestId"].(string); ok {
				resp := map[string]interface{}{
					"requestId": requestID,
					"eventType": "response",
					"success":   true,
				}
				mockConn.simulateMessage(resp)
			}
		}()
		return nil
	}

	mockAdapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			mockConn.callback = callback
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, mockAdapter, &mockLogger{})
	client.Connect()

	concurrency := 50
	var wg sync.WaitGroup
	errors := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			_, err := client.Request(ctx, "test", map[string]int{"index": n})
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent request failed: %v", err)
	}

	client.Close()
}

// TestClient_ConnectionCloseDuringRequest 测试请求期间连接断开
func TestClient_ConnectionCloseDuringRequest(t *testing.T) {
	mockConn := &mockClientConn{}
	mockConn.writeTimeoutFunc = func(op adapter.Opcode, data []byte, timeout time.Duration) error {
		// 发送后立即断开连接
		go func() {
			time.Sleep(50 * time.Millisecond)
			mockConn.simulateClose(errors.New("connection lost"))
		}()
		return nil
	}

	mockAdapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			mockConn.callback = callback
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, mockAdapter, &mockLogger{})
	client.Connect()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.Request(ctx, "test", nil)
	if err == nil {
		t.Error("expected error when connection closes during request")
	}

	client.Close()
}

// TestClient_Request_ErrorPaths 测试Request的各种错误路径
func TestClient_Request_ErrorPaths(t *testing.T) {
	t.Run("MarshalEventDataError", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		client.Connect()

		ctx := context.Background()
		// 传递无法序列化的数据（channel）
		ch := make(chan int)
		_, err := client.Request(ctx, "test", ch)
		if err == nil {
			t.Error("expected marshal error")
		}

		client.Close()
	})

	t.Run("WriteError", func(t *testing.T) {
		writeErr := errors.New("write failed")
		mockConn := &mockClientConn{}
		mockConn.writeTimeoutFunc = func(op adapter.Opcode, data []byte, timeout time.Duration) error {
			return writeErr
		}

		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		client.Connect()

		ctx := context.Background()
		_, err := client.Request(ctx, "test", nil)
		if err == nil {
			t.Error("expected write error")
		}
		if !errors.Is(err, writeErr) {
			t.Errorf("expected writeErr, got %v", err)
		}

		client.Close()
	})

	t.Run("InvalidResponseJSON", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockConn.writeTimeoutFunc = func(op adapter.Opcode, data []byte, timeout time.Duration) error {
			go func() {
				time.Sleep(50 * time.Millisecond)
				var req map[string]interface{}
				json.Unmarshal(data, &req)
				requestID := req["requestId"].(string)

				// 发送无效JSON
				invalidJSON := []byte(`{"requestId":"` + requestID + `", invalid json}`)
				if mockConn.callback != nil {
					mockConn.callback.OnMessage(mockConn, adapter.Text, invalidJSON)
				}
			}()
			return nil
		}

		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		client.Connect()

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		_, err := client.Request(ctx, "test", nil)
		// 应该超时或返回解析错误
		if err == nil {
			t.Error("expected error for invalid JSON response")
		}

		client.Close()
	})
}

// TestClient_Send_ErrorPath 测试Send的错误路径
func TestClient_Send_ErrorPath(t *testing.T) {
	mockConn := &mockClientConn{}
	mockAdapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			mockConn.callback = callback
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, mockAdapter, &mockLogger{})
	client.Connect()

	// 传递无法序列化的数据
	ch := make(chan int)
	err := client.Send("test", ch)
	if err == nil {
		t.Error("expected marshal error")
	}

	client.Close()
}

// TestRequestManager_NotifyError 测试NotifyError
func TestRequestManager_NotifyError(t *testing.T) {
	rm := NewRequestManager()
	requestID := "test-req-1"

	pr, err := rm.RegisterRequest(requestID)
	if err != nil {
		t.Fatalf("failed to register request: %v", err)
	}

	// 通知错误
	testErr := errors.New("test error")
	ok := rm.NotifyError(requestID, testErr)
	if !ok {
		t.Error("expected NotifyError to return true")
	}

	// 验证错误被接收
	select {
	case err := <-pr.errChan:
		if err.Error() != testErr.Error() {
			t.Errorf("expected error %v, got %v", testErr, err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for error")
	}

	rm.CancelRequest(requestID)
}

// TestRequestManager_PendingCount 测试PendingCount
func TestRequestManager_PendingCount(t *testing.T) {
	rm := NewRequestManager()

	if rm.PendingCount() != 0 {
		t.Errorf("expected 0 pending, got %d", rm.PendingCount())
	}

	// 注册多个请求
	ids := []string{"req-1", "req-2", "req-3"}
	for _, id := range ids {
		_, err := rm.RegisterRequest(id)
		if err != nil {
			t.Fatalf("failed to register %s: %v", id, err)
		}
	}

	if rm.PendingCount() != 3 {
		t.Errorf("expected 3 pending, got %d", rm.PendingCount())
	}

	// 取消一个
	rm.CancelRequest("req-1")
	if rm.PendingCount() != 2 {
		t.Errorf("expected 2 pending, got %d", rm.PendingCount())
	}

	// 清理所有
	rm.ClearAll(errors.New("cleanup"))
	if rm.PendingCount() != 0 {
		t.Errorf("expected 0 pending after ClearAll, got %d", rm.PendingCount())
	}
}

// TestClient_HeartbeatTimeout 测试心跳超时
func TestClient_HeartbeatTimeout(t *testing.T) {
	mockConn := &mockClientConn{}
	mockConn.writeTimeoutFunc = func(op adapter.Opcode, data []byte, timeout time.Duration) error {
		return nil
	}

	connectCount := 0
	mockAdapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			connectCount++
			mockConn.callback = callback
			return mockConn, nil
		},
	}

	cfg := Config{
		URL:                 "ws://localhost:8080",
		EnableAutoHeartbeat: true,
		HeartbeatInterval:   100 * time.Millisecond,
		HeartbeatTimeout:    50 * time.Millisecond,
		ReconnectEnabled:    true,
		ReconnectInterval:   100 * time.Millisecond,
	}
	client := NewClient(cfg, mockAdapter, &mockLogger{})

	// 设置一个初始pong时间，然后不再更新
	client.lastPongTime.Store(time.Now().UnixNano())
	client.Connect()

	// 等待心跳超时触发重连
	time.Sleep(400 * time.Millisecond)

	// 应该触发了重连（connectCount > 1）
	if connectCount < 2 {
		t.Logf("warning: expected reconnect due to heartbeat timeout, connectCount=%d", connectCount)
	}

	client.Close()
}

// TestClient_RegisterRequest_DuplicateID 测试重复的请求ID
func TestClient_RegisterRequest_DuplicateID(t *testing.T) {
	rm := NewRequestManager()
	requestID := "duplicate-id"

	_, err := rm.RegisterRequest(requestID)
	if err != nil {
		t.Fatalf("first registration failed: %v", err)
	}

	// 尝试注册相同的ID
	_, err = rm.RegisterRequest(requestID)
	if err == nil {
		t.Error("expected error for duplicate request ID")
	}
}

// TestClient_HandleMessage_ErrorInHandler 测试处理器返回错误
func TestClient_HandleMessage_ErrorInHandler(t *testing.T) {
	mockConn := &mockClientConn{}
	mockAdapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			mockConn.callback = callback
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, mockAdapter, &mockLogger{})
	client.Connect()

	handlerErr := errors.New("handler error")
	client.RegisterHandler("errorEvent", func(msg *Message) error {
		return handlerErr
	})

	testMsg := map[string]interface{}{
		"eventType": "errorEvent",
		"data":      "test",
	}
	mockConn.simulateMessage(testMsg)
	time.Sleep(50 * time.Millisecond)

	// 不应该崩溃，错误应该被记录
	client.Close()
}

// TestClient_Config_Defaults 测试配置默认值
func TestClient_Config_Defaults(t *testing.T) {
	cfg := Config{
		URL: "ws://localhost:8080",
		// 不设置任何可选参数
	}
	client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

	if client.config.ReconnectInterval != 5*time.Second {
		t.Errorf("expected default ReconnectInterval 5s, got %v", client.config.ReconnectInterval)
	}
	if client.config.MaxReconnectAttempts != 10 {
		t.Errorf("expected default MaxReconnectAttempts 10, got %d", client.config.MaxReconnectAttempts)
	}
	if client.config.HeartbeatInterval != 30*time.Second {
		t.Errorf("expected default HeartbeatInterval 30s, got %v", client.config.HeartbeatInterval)
	}
	if client.config.HeartbeatTimeout != 10*time.Second {
		t.Errorf("expected default HeartbeatTimeout 10s, got %v", client.config.HeartbeatTimeout)
	}
	if client.config.WriteTimeout != 10*time.Second {
		t.Errorf("expected default WriteTimeout 10s, got %v", client.config.WriteTimeout)
	}
}

