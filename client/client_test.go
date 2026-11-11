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
	"github.com/web3-coder-hub/websocket/constant"
)

// mockLogger 实现 logger.Logger 接口
type mockLogger struct{}

func (m *mockLogger) Debug(msg string, fields ...logger.Field)      {}
func (m *mockLogger) Info(msg string, fields ...logger.Field)       {}
func (m *mockLogger) Warn(msg string, fields ...logger.Field)       {}
func (m *mockLogger) Error(msg string, fields ...logger.Field)      {}
func (m *mockLogger) Fatal(msg string, fields ...logger.Field)      {}
func (m *mockLogger) Debugf(format string, args ...interface{})     {}
func (m *mockLogger) Infof(format string, args ...interface{})      {}
func (m *mockLogger) Warnf(format string, args ...interface{})      {}
func (m *mockLogger) Errorf(format string, args ...interface{})     {}
func (m *mockLogger) Fatalf(format string, args ...interface{})     {}
func (m *mockLogger) With(fields ...logger.Field) logger.Logger     { return m }
func (m *mockLogger) WithContext(ctx context.Context) logger.Logger { return m }
func (m *mockLogger) Sync() error                                   { return nil }

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

// TestNewClient 测试客户端创建
func TestNewClient(t *testing.T) {
	cfg := Config{
		URL: "ws://localhost:8080",
	}
	mockAdapter := &mockClientAdapter{}
	logger := &mockLogger{}

	client := NewClient(cfg, mockAdapter, logger)

	if client == nil {
		t.Fatal("Expected client to be created")
	}

	if client.config.URL != cfg.URL {
		t.Errorf("Expected URL %s, got %s", cfg.URL, client.config.URL)
	}

	// Check defaults
	if client.config.ReconnectInterval != 5*time.Second {
		t.Errorf("Expected ReconnectInterval 5s, got %v", client.config.ReconnectInterval)
	}

	if client.config.RequestIDField != "requestId" {
		t.Errorf("Expected RequestIDField 'requestId', got %s", client.config.RequestIDField)
	}

	if client.config.ResponseIDField != "requestId" {
		t.Errorf("Expected ResponseIDField 'requestId', got %s", client.config.ResponseIDField)
	}
}

// TestClient_Connect 测试连接
func TestClient_Connect(t *testing.T) {
	t.Run("SuccessfulConnect", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				go callback.OnOpen(mockConn)
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})

		err := client.Connect()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if !client.IsConnected() {
			t.Error("Expected client to be connected")
		}
	})

	t.Run("ConnectError", func(t *testing.T) {
		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				return nil, errors.New("connection failed")
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})

		err := client.Connect()
		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if client.IsConnected() {
			t.Error("Expected client to not be connected")
		}
	})

	t.Run("AlreadyConnected", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})

		_ = client.Connect()
		err := client.Connect()
		if err != nil {
			t.Fatalf("Expected no error on second connect, got %v", err)
		}
	})
}

// TestClient_Disconnect 测试断开连接
func TestClient_Disconnect(t *testing.T) {
	mockConn := &mockClientConn{}
	mockAdapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, mockAdapter, &mockLogger{})

	_ = client.Connect()
	err := client.Disconnect()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

// TestClient_Close 测试关闭客户端
func TestClient_Close(t *testing.T) {
	t.Run("CloseWithConnection", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})

		_ = client.Connect()
		err := client.Close()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if client.IsConnected() {
			t.Error("Expected client to be disconnected")
		}
	})

	t.Run("CloseWithoutConnection", func(t *testing.T) {
		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

		err := client.Close()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
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

		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		_ = client.Connect()

		testData := map[string]any{"test": "data"}
		err := client.Send(testData)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		expectedJSON := `{"test":"data"}`
		if string(sentData) != expectedJSON {
			t.Errorf("Expected data %s, got %s", expectedJSON, sentData)
		}
	})

	t.Run("SendRawNotConnected", func(t *testing.T) {
		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

		err := client.Send(map[string]any{"test": "data"})
		if err == nil || err.Error() != constant.GetErrorMessage(constant.ErrCodeConnNotConnected) {
			t.Errorf("Expected ErrNotConnected, got %v", err)
		}
	})

	t.Run("SendBytes", func(t *testing.T) {
		var sentData []byte
		mockConn := &mockClientConn{
			writeTimeoutFunc: func(op adapter.Opcode, data []byte, timeout time.Duration) error {
				sentData = data
				return nil
			},
		}

		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		_ = client.Connect()

		testData := []byte(`{"raw":"bytes"}`)
		err := client.Send(testData)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if string(sentData) != string(testData) {
			t.Errorf("Expected data %s, got %s", testData, sentData)
		}
	})

	t.Run("SendUnsupportedType", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		_ = client.Connect()

		err := client.Send("invalid string type")
		if err == nil {
			t.Error("Expected error for unsupported type, got nil")
		}
	})
}

// TestClient_Request 测试同步请求
func TestClient_Request(t *testing.T) {
	t.Run("RequestSuccess", func(t *testing.T) {
		var sentData []byte
		var mu sync.Mutex
		mockConn := &mockClientConn{
			writeTimeoutFunc: func(op adapter.Opcode, data []byte, timeout time.Duration) error {
				mu.Lock()
				sentData = data
				mu.Unlock()
				return nil
			},
		}

		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		_ = client.Connect()

		// Start goroutine to simulate response
		go func() {
			time.Sleep(50 * time.Millisecond)

			// Extract requestId from sent data
			mu.Lock()
			data := sentData
			mu.Unlock()

			var req map[string]interface{}
			json.Unmarshal(data, &req)
			requestID := req["requestId"].(string)

			// Simulate response
			response := map[string]interface{}{
				"requestId": requestID,
				"status":    200,
				"result":    "success",
			}
			respData, _ := json.Marshal(response)
			mockConn.callback.OnMessage(mockConn, adapter.Text, respData)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		requestData := map[string]interface{}{
			"action": "test",
		}
		resp, err := client.Request(ctx, requestData)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if resp["result"] != "success" {
			t.Errorf("Expected result 'success', got %v", resp["result"])
		}
	})

	t.Run("RequestTimeout", func(t *testing.T) {
		mockConn := &mockClientConn{
			writeTimeoutFunc: func(op adapter.Opcode, data []byte, timeout time.Duration) error {
				return nil
			},
		}

		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		_ = client.Connect()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := client.Request(ctx, map[string]interface{}{"action": "test"})
		if err != context.DeadlineExceeded {
			t.Errorf("Expected DeadlineExceeded, got %v", err)
		}
	})

	t.Run("RequestNotConnected", func(t *testing.T) {
		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

		ctx := context.Background()
		_, err := client.Request(ctx, map[string]interface{}{"action": "test"})
		if err == nil || err.Error() != constant.GetErrorMessage(constant.ErrCodeConnNotConnected) {
			t.Errorf("Expected ErrNotConnected, got %v", err)
		}
	})
}

// TestClient_OnMessage 测试消息处理
func TestClient_MessageHandling(t *testing.T) {
	t.Run("WithEventTypeField", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		_ = client.Connect()

		// Register handler with event type field
		var receivedData map[string]any
		client.RegisterHandler("notification", "event", func(data map[string]any) error {
			receivedData = data
			return nil
		})

		testData := []byte(`{"event":"notification","message":"hello"}`)
		mockConn.callback.OnMessage(mockConn, adapter.Text, testData)

		time.Sleep(50 * time.Millisecond)

		if receivedData == nil {
			t.Fatal("Expected to receive data")
		}
		if receivedData["event"] != "notification" {
			t.Errorf("Expected event 'notification', got %v", receivedData["event"])
		}
		if receivedData["message"] != "hello" {
			t.Errorf("Expected message 'hello', got %v", receivedData["message"])
		}
	})

	t.Run("MultipleHandlers", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		_ = client.Connect()

		var tradeData, klineData map[string]any
		client.RegisterHandler("trade", "event", func(data map[string]any) error {
			tradeData = data
			return nil
		})
		client.RegisterHandler("kline", "event", func(data map[string]any) error {
			klineData = data
			return nil
		})

		// Send trade event
		tradeMsg := []byte(`{"event":"trade","symbol":"BTCUSDT"}`)
		mockConn.callback.OnMessage(mockConn, adapter.Text, tradeMsg)
		time.Sleep(50 * time.Millisecond)

		// Send kline event
		klineMsg := []byte(`{"event":"kline","symbol":"ETHUSDT"}`)
		mockConn.callback.OnMessage(mockConn, adapter.Text, klineMsg)
		time.Sleep(50 * time.Millisecond)

		if tradeData == nil || tradeData["symbol"] != "BTCUSDT" {
			t.Errorf("Expected trade symbol BTCUSDT, got %v", tradeData)
		}
		if klineData == nil || klineData["symbol"] != "ETHUSDT" {
			t.Errorf("Expected kline symbol ETHUSDT, got %v", klineData)
		}
	})

	t.Run("NestedFieldAccess", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		_ = client.Connect()

		var receivedData map[string]any
		// 注册处理器，使用嵌套字段 "channel.type"
		client.RegisterHandler("account", "channel.type", func(data map[string]any) error {
			receivedData = data
			return nil
		})

		// 发送包含嵌套字段的消息
		testData := []byte(`{"channel":{"type":"account","id":"123"},"balance":1000}`)
		mockConn.callback.OnMessage(mockConn, adapter.Text, testData)

		time.Sleep(50 * time.Millisecond)

		if receivedData == nil {
			t.Fatal("Expected handler to be called")
		}

		channel, ok := receivedData["channel"].(map[string]any)
		if !ok {
			t.Fatal("Expected channel to be a map")
		}

		if channel["type"] != "account" {
			t.Errorf("Expected channel type 'account', got %v", channel["type"])
		}
		if receivedData["balance"] != float64(1000) {
			t.Errorf("Expected balance 1000, got %v", receivedData["balance"])
		}
	})
}

// TestClient_OnConnect 测试连接回调
func TestClient_OnConnect(t *testing.T) {
	mockConn := &mockClientConn{}
	connectCalled := false

	mockAdapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, mockAdapter, &mockLogger{})

	client.OnConnect(func() {
		connectCalled = true
	})

	_ = client.Connect()

	time.Sleep(50 * time.Millisecond)

	if !connectCalled {
		t.Error("Expected OnConnect to be called")
	}
}

// TestClient_OnDisconnect 测试断开连接回调
func TestClient_OnDisconnect(t *testing.T) {
	mockConn := &mockClientConn{}
	disconnectCalled := false

	mockAdapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			mockConn.callback = callback
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, mockAdapter, &mockLogger{})

	client.OnDisconnect(func() {
		disconnectCalled = true
	})

	_ = client.Connect()
	mockConn.callback.OnClose(mockConn, nil)

	time.Sleep(50 * time.Millisecond)

	if !disconnectCalled {
		t.Error("Expected OnDisconnect to be called")
	}
}

// TestClient_OnError 测试错误回调
func TestClient_OnError(t *testing.T) {
	mockConn := &mockClientConn{}
	var receivedError error

	mockAdapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			mockConn.callback = callback
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, mockAdapter, &mockLogger{})

	client.OnError(func(err error) {
		receivedError = err
	})

	_ = client.Connect()

	testError := errors.New("test error")
	mockConn.callback.OnError(mockConn, testError)

	time.Sleep(50 * time.Millisecond)

	if receivedError != testError {
		t.Errorf("Expected error %v, got %v", testError, receivedError)
	}
}

// TestClient_GetID 测试获取客户端ID
func TestClient_GetID(t *testing.T) {
	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

	id := client.GetID()
	if id == "" {
		t.Error("Expected non-empty client ID")
	}
}

// TestClient_SessionID 测试会话ID
func TestClient_SessionID(t *testing.T) {
	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

	testSessionID := "test-session-123"
	client.SetSessionID(testSessionID)

	sessionID := client.GetSessionID()
	if sessionID != testSessionID {
		t.Errorf("Expected session ID %s, got %s", testSessionID, sessionID)
	}
}

// TestClient_HandleMessage 测试消息处理
func TestClient_HandleMessage(t *testing.T) {
	t.Run("HandleSyncResponse", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		_ = client.Connect()

		// 注册一个pending请求
		requestID := "test-request-123"
		pr, _ := client.requestManager.RegisterRequest(requestID)

		// 模拟收到响应
		response := map[string]interface{}{
			"requestId": requestID,
			"result":    "success",
		}
		respData, _ := json.Marshal(response)
		mockConn.callback.OnMessage(mockConn, adapter.Text, respData)

		// 等待响应
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		data, err := client.requestManager.WaitForResponse(ctx, pr)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if string(data) != string(respData) {
			t.Errorf("Expected data %s, got %s", respData, data)
		}
	})

	t.Run("HandleAsyncMessage", func(t *testing.T) {
		mockConn := &mockClientConn{}
		mockAdapter := &mockClientAdapter{
			dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
				mockConn.callback = callback
				return mockConn, nil
			},
		}

		cfg := Config{URL: "ws://localhost:8080"}
		client := NewClient(cfg, mockAdapter, &mockLogger{})
		_ = client.Connect()

		var receivedData map[string]any
		client.RegisterHandler("notification", "event", func(data map[string]any) error {
			receivedData = data
			return nil
		})

		testData := []byte(`{"event":"notification","message":"hello"}`)
		mockConn.callback.OnMessage(mockConn, adapter.Text, testData)

		time.Sleep(50 * time.Millisecond)

		if receivedData == nil || receivedData["message"] != "hello" {
			t.Errorf("Expected message 'hello', got %v", receivedData)
		}
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
		_ = client.Connect()

		// Invalid JSON - unmarshal will fail, message will be ignored
		testData := []byte(`{invalid json}`)
		mockConn.callback.OnMessage(mockConn, adapter.Text, testData)

		time.Sleep(50 * time.Millisecond)

		// No error should occur, message is simply ignored
	})
}

// TestClient_ConcurrentRequests 测试并发请求
func TestClient_ConcurrentRequests(t *testing.T) {
	mockConn := &mockClientConn{}
	mockConn.writeTimeoutFunc = func(op adapter.Opcode, data []byte, timeout time.Duration) error {
		// Simulate async response
		go func() {
			time.Sleep(10 * time.Millisecond)
			var req map[string]interface{}
			json.Unmarshal(data, &req)
			requestID := req["requestId"].(string)

			response := map[string]interface{}{
				"requestId": requestID,
				"result":    "success",
			}
			respData, _ := json.Marshal(response)
			mockConn.callback.OnMessage(mockConn, adapter.Text, respData)
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
	_ = client.Connect()

	// Send 10 concurrent requests
	var wg sync.WaitGroup
	for n := 0; n < 10; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			requestData := map[string]interface{}{
				"index": n,
			}
			_, err := client.Request(ctx, requestData)
			if err != nil {
				t.Errorf("Request %d failed: %v", n, err)
			}
		}(n)
	}

	wg.Wait()
}

// TestClient_ConnectionCloseDuringRequest 测试请求期间连接关闭
func TestClient_ConnectionCloseDuringRequest(t *testing.T) {
	mockConn := &mockClientConn{
		writeTimeoutFunc: func(op adapter.Opcode, data []byte, timeout time.Duration) error {
			return nil
		},
	}

	mockAdapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			mockConn.callback = callback
			return mockConn, nil
		},
	}

	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, mockAdapter, &mockLogger{})
	_ = client.Connect()

	// Start request
	go func() {
		time.Sleep(50 * time.Millisecond)
		mockConn.callback.OnClose(mockConn, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := client.Request(ctx, map[string]interface{}{"action": "test"})
	if err == nil || err.Error() != constant.GetErrorMessage(constant.ErrCodeConnClose) {
		t.Errorf("Expected ErrConnClosed, got %v", err)
	}
}

// TestClient_Config_Defaults 测试配置默认值
func TestClient_Config_Defaults(t *testing.T) {
	cfg := Config{URL: "ws://localhost:8080"}
	client := NewClient(cfg, &mockClientAdapter{}, &mockLogger{})

	if client.config.ReconnectInterval != 5*time.Second {
		t.Errorf("Expected ReconnectInterval 5s, got %v", client.config.ReconnectInterval)
	}

	if client.config.MaxReconnectAttempts != 10 {
		t.Errorf("Expected MaxReconnectAttempts 10, got %d", client.config.MaxReconnectAttempts)
	}

	if client.config.WriteTimeout != 10*time.Second {
		t.Errorf("Expected WriteTimeout 10s, got %v", client.config.WriteTimeout)
	}

	if client.config.RequestIDField != "requestId" {
		t.Errorf("Expected RequestIDField 'requestId', got %s", client.config.RequestIDField)
	}

	if client.config.ResponseIDField != "requestId" {
		t.Errorf("Expected ResponseIDField 'requestId', got %s", client.config.ResponseIDField)
	}
}

// TestClient_CustomIDFields 测试自定义ID字段
func TestClient_CustomIDFields(t *testing.T) {
	var sentData []byte
	mockConn := &mockClientConn{}
	mockConn.writeTimeoutFunc = func(op adapter.Opcode, data []byte, timeout time.Duration) error {
		sentData = data
		// Simulate response
		go func() {
			time.Sleep(10 * time.Millisecond)
			var req map[string]interface{}
			json.Unmarshal(data, &req)
			customID := req["id"].(string)

			response := map[string]interface{}{
				"requestId": customID, // Use different field in response
				"result":    "success",
			}
			respData, _ := json.Marshal(response)
			mockConn.callback.OnMessage(mockConn, adapter.Text, respData)
		}()
		return nil
	}

	mockAdapter := &mockClientAdapter{
		dialFunc: func(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
			mockConn.callback = callback
			return mockConn, nil
		},
	}

	cfg := Config{
		URL:             "ws://localhost:8080",
		RequestIDField:  "id",        // Custom request field
		ResponseIDField: "requestId", // Custom response field
	}
	client := NewClient(cfg, mockAdapter, &mockLogger{})
	_ = client.Connect()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	requestData := map[string]interface{}{
		"action": "test",
	}
	resp, err := client.Request(ctx, requestData)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify request has custom "id" field
	var req map[string]interface{}
	json.Unmarshal(sentData, &req)
	if _, ok := req["id"]; !ok {
		t.Error("Expected 'id' field in request")
	}

	// Verify response
	if resp["result"] != "success" {
		t.Errorf("Expected result 'success', got %v", resp["result"])
	}
}
