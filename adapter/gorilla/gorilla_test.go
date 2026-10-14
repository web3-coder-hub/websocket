package gorilla

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/web3-coder-hub/websocket/adapter"
)

// Mock callback for testing
type mockServerCallback struct {
	onOpenCalled    bool
	onMessageCalled bool
	onCloseCalled   bool
	messages        [][]byte
	mu              sync.Mutex
}

func (m *mockServerCallback) OnOpen(conn adapter.ServerConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onOpenCalled = true
}

func (m *mockServerCallback) OnMessage(conn adapter.ServerConn, op adapter.Opcode, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onMessageCalled = true
	m.messages = append(m.messages, data)
}

func (m *mockServerCallback) OnClose(conn adapter.ServerConn, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onCloseCalled = true
}

func (m *mockServerCallback) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onOpenCalled = false
	m.onMessageCalled = false
	m.onCloseCalled = false
	m.messages = nil
}

type mockClientCallback struct {
	onOpenCalled    bool
	onMessageCalled bool
	onCloseCalled   bool
	onErrorCalled   bool
	messages        [][]byte
	errors          []error
	mu              sync.Mutex
}

func (m *mockClientCallback) OnOpen(conn adapter.ClientConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onOpenCalled = true
}

func (m *mockClientCallback) OnMessage(conn adapter.ClientConn, op adapter.Opcode, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onMessageCalled = true
	m.messages = append(m.messages, data)
}

func (m *mockClientCallback) OnClose(conn adapter.ClientConn, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onCloseCalled = true
}

func (m *mockClientCallback) OnError(conn adapter.ClientConn, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onErrorCalled = true
	m.errors = append(m.errors, err)
}

func (m *mockClientCallback) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onOpenCalled = false
	m.onMessageCalled = false
	m.onCloseCalled = false
	m.onErrorCalled = false
	m.messages = nil
	m.errors = nil
}

// TestServerAdapter tests the server adapter
func TestServerAdapter(t *testing.T) {
	sa := NewServerAdapter(nil)

	if sa == nil {
		t.Fatal("NewServerAdapter returned nil")
	}

	if name := sa.Name(); name != "gorilla" {
		t.Errorf("Expected adapter name 'gorilla', got '%s'", name)
	}
}

// TestServerAdapterWithCustomUpgrader tests creating adapter with custom upgrader
func TestServerAdapterWithCustomUpgrader(t *testing.T) {
	upgrader := &websocket.Upgrader{
		ReadBufferSize:  2048,
		WriteBufferSize: 2048,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	sa := NewServerAdapter(upgrader)

	if sa == nil {
		t.Fatal("NewServerAdapter returned nil")
	}

	serverAdapter := sa.(*ServerAdapter)
	if serverAdapter.upgrader.ReadBufferSize != 2048 {
		t.Errorf("Expected ReadBufferSize 2048, got %d", serverAdapter.upgrader.ReadBufferSize)
	}
}

// TestServerAdapterUpgrade tests the upgrade process
func TestServerAdapterUpgrade(t *testing.T) {
	sa := NewServerAdapter(nil)
	callback := &mockServerCallback{}

	// Create a test server
	var serverConn adapter.ServerConn
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := sa.Upgrade(w, r, callback)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		serverConn = conn

		// Start read loop in goroutine
		go conn.StartReadLoop()
	}))
	defer server.Close()

	// Convert http:// to ws://
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// Connect with a WebSocket client
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer clientConn.Close()

	// Wait for connection to be established
	time.Sleep(100 * time.Millisecond)

	// Check if OnOpen was called
	callback.mu.Lock()
	openCalled := callback.onOpenCalled
	callback.mu.Unlock()

	if !openCalled {
		t.Error("Expected OnOpen to be called")
	}

	// Send a message from client
	testMessage := []byte("test message from client")
	err = clientConn.WriteMessage(websocket.TextMessage, testMessage)
	if err != nil {
		t.Fatalf("Failed to write message: %v", err)
	}

	// Wait for message to be received
	time.Sleep(200 * time.Millisecond)

	// Check if OnMessage was called
	callback.mu.Lock()
	messageCalled := callback.onMessageCalled
	messageCount := len(callback.messages)
	var receivedMessage []byte
	if messageCount > 0 {
		receivedMessage = callback.messages[0]
	}
	callback.mu.Unlock()

	if !messageCalled {
		t.Error("Expected OnMessage to be called")
	}

	if messageCount != 1 {
		t.Errorf("Expected 1 message, got %d", messageCount)
	}

	if string(receivedMessage) != string(testMessage) {
		t.Errorf("Expected message '%s', got '%s'", testMessage, receivedMessage)
	}

	// Test server writing to client
	if serverConn != nil {
		responseMessage := []byte("response from server")
		err = serverConn.WriteTimeout(adapter.Text, responseMessage, time.Second)
		if err != nil {
			t.Errorf("Failed to write from server: %v", err)
		}

		// Read message on client side
		_, msg, err := clientConn.ReadMessage()
		if err != nil {
			t.Errorf("Failed to read on client: %v", err)
		}

		if string(msg) != string(responseMessage) {
			t.Errorf("Expected response '%s', got '%s'", responseMessage, msg)
		}
	}

	// Close client connection
	clientConn.Close()

	// Wait for OnClose to be called
	time.Sleep(200 * time.Millisecond)

	callback.mu.Lock()
	closeCalled := callback.onCloseCalled
	callback.mu.Unlock()

	if !closeCalled {
		t.Error("Expected OnClose to be called")
	}
}

// TestServerConn tests the server connection
func TestServerConn(t *testing.T) {
	t.Run("RemoteAddr", func(t *testing.T) {
		// Create a mock connection through actual WebSocket connection
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upgrader := websocket.Upgrader{
				CheckOrigin: func(r *http.Request) bool { return true },
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()

			serverConn := &ServerConn{conn: conn}
			addr := serverConn.RemoteAddr()

			if addr == "" {
				t.Error("Expected non-empty RemoteAddr")
			}

			// Send result back
			conn.WriteMessage(websocket.TextMessage, []byte(addr))
		}))
		defer server.Close()

		wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
		clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("Failed to dial: %v", err)
		}
		defer clientConn.Close()

		// Read the address
		_, msg, err := clientConn.ReadMessage()
		if err != nil {
			t.Fatalf("Failed to read message: %v", err)
		}

		if len(msg) == 0 {
			t.Error("Expected non-empty address")
		}
	})
}

// TestClientAdapter tests the client adapter
func TestClientAdapter(t *testing.T) {
	ca := NewClientAdapter(nil)

	if ca == nil {
		t.Fatal("NewClientAdapter returned nil")
	}

	if name := ca.Name(); name != "gorilla-client" {
		t.Errorf("Expected adapter name 'gorilla-client', got '%s'", name)
	}
}

// TestClientAdapterWithCustomDialer tests creating adapter with custom dialer
func TestClientAdapterWithCustomDialer(t *testing.T) {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 30 * time.Second,
		ReadBufferSize:   2048,
		WriteBufferSize:  2048,
	}

	ca := NewClientAdapter(dialer)

	if ca == nil {
		t.Fatal("NewClientAdapter returned nil")
	}

	clientAdapter := ca.(*ClientAdapter)
	if clientAdapter.dialer.HandshakeTimeout != 30*time.Second {
		t.Errorf("Expected HandshakeTimeout 30s, got %v", clientAdapter.dialer.HandshakeTimeout)
	}
}

// TestClientAdapterDial tests the dial process
func TestClientAdapterDial(t *testing.T) {
	// Create a test WebSocket server
	callback := &mockServerCallback{}
	serverAdapter := NewServerAdapter(nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := serverAdapter.Upgrade(w, r, callback)
		if err != nil {
			return
		}
		go conn.StartReadLoop()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	t.Run("SuccessfulDial", func(t *testing.T) {
		clientAdapter := NewClientAdapter(nil)
		clientCallback := &mockClientCallback{}

		conn, err := clientAdapter.Dial(wsURL, nil, clientCallback)
		if err != nil {
			t.Fatalf("Failed to dial: %v", err)
		}
		defer conn.Close()

		// Wait for connection
		time.Sleep(100 * time.Millisecond)

		if !conn.IsConnected() {
			t.Error("Expected connection to be connected")
		}

		clientCallback.mu.Lock()
		openCalled := clientCallback.onOpenCalled
		clientCallback.mu.Unlock()

		if !openCalled {
			t.Error("Expected OnOpen to be called")
		}
	})

	t.Run("InvalidURL", func(t *testing.T) {
		clientAdapter := NewClientAdapter(nil)
		clientCallback := &mockClientCallback{}

		_, err := clientAdapter.Dial("invalid://url", nil, clientCallback)
		if err == nil {
			t.Error("Expected error for invalid URL")
		}
	})

	t.Run("NonExistentServer", func(t *testing.T) {
		clientAdapter := NewClientAdapter(nil)
		clientCallback := &mockClientCallback{}

		_, err := clientAdapter.Dial("ws://localhost:99999", nil, clientCallback)
		if err == nil {
			t.Error("Expected error for non-existent server")
		}
	})
}

// TestClientConn tests the client connection methods
func TestClientConn(t *testing.T) {
	// Create test server
	callback := &mockServerCallback{}
	serverAdapter := NewServerAdapter(nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := serverAdapter.Upgrade(w, r, callback)
		if err != nil {
			return
		}
		go conn.StartReadLoop()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	t.Run("WriteAndRead", func(t *testing.T) {
		clientAdapter := NewClientAdapter(nil)
		clientCallback := &mockClientCallback{}

		conn, err := clientAdapter.Dial(wsURL, nil, clientCallback)
		if err != nil {
			t.Fatalf("Failed to dial: %v", err)
		}
		defer conn.Close()

		time.Sleep(100 * time.Millisecond)

		// Write a message
		testMessage := []byte("test message")
		err = conn.Write(adapter.Text, testMessage)
		if err != nil {
			t.Errorf("Failed to write: %v", err)
		}

		// Wait for message to be received
		time.Sleep(200 * time.Millisecond)

		callback.mu.Lock()
		messageCount := len(callback.messages)
		callback.mu.Unlock()

		if messageCount != 1 {
			t.Errorf("Expected 1 message on server, got %d", messageCount)
		}
	})

	t.Run("WriteTimeout", func(t *testing.T) {
		clientAdapter := NewClientAdapter(nil)
		clientCallback := &mockClientCallback{}

		conn, err := clientAdapter.Dial(wsURL, nil, clientCallback)
		if err != nil {
			t.Fatalf("Failed to dial: %v", err)
		}
		defer conn.Close()

		time.Sleep(100 * time.Millisecond)

		// Write with timeout
		testMessage := []byte("test with timeout")
		err = conn.WriteTimeout(adapter.Text, testMessage, time.Second)
		if err != nil {
			t.Errorf("WriteTimeout failed: %v", err)
		}
	})

	t.Run("IsConnected", func(t *testing.T) {
		clientAdapter := NewClientAdapter(nil)
		clientCallback := &mockClientCallback{}

		conn, err := clientAdapter.Dial(wsURL, nil, clientCallback)
		if err != nil {
			t.Fatalf("Failed to dial: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		if !conn.IsConnected() {
			t.Error("Expected connection to be connected")
		}

		conn.Close()
		time.Sleep(50 * time.Millisecond)

		if conn.IsConnected() {
			t.Error("Expected connection to be disconnected")
		}
	})

	t.Run("Close", func(t *testing.T) {
		clientAdapter := NewClientAdapter(nil)
		clientCallback := &mockClientCallback{}

		conn, err := clientAdapter.Dial(wsURL, nil, clientCallback)
		if err != nil {
			t.Fatalf("Failed to dial: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		err = conn.Close()
		if err != nil {
			t.Errorf("Close returned error: %v", err)
		}

		// Test double close
		err = conn.Close()
		if err != nil {
			t.Errorf("Second close returned error: %v", err)
		}
	})
}

// TestClientReadLoop tests the read loop
func TestClientReadLoop(t *testing.T) {
	callback := &mockServerCallback{}
	serverAdapter := NewServerAdapter(nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := serverAdapter.Upgrade(w, r, callback)
		if err != nil {
			return
		}
		go conn.StartReadLoop()

		// Send messages to client
		time.Sleep(100 * time.Millisecond)
		conn.WriteTimeout(adapter.Text, []byte("message 1"), time.Second)
		time.Sleep(50 * time.Millisecond)
		conn.WriteTimeout(adapter.Text, []byte("message 2"), time.Second)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	clientAdapter := NewClientAdapter(nil)
	clientCallback := &mockClientCallback{}

	conn, err := clientAdapter.Dial(wsURL, nil, clientCallback)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	// Wait for messages
	time.Sleep(500 * time.Millisecond)

	clientCallback.mu.Lock()
	messageCount := len(clientCallback.messages)
	clientCallback.mu.Unlock()

	if messageCount < 2 {
		t.Errorf("Expected at least 2 messages, got %d", messageCount)
	}
}

// TestOpcodeConversion tests opcode conversion
func TestOpcodeConversion(t *testing.T) {
	testCases := []struct {
		name     string
		op       adapter.Opcode
		expected int
	}{
		{"Text", adapter.Text, websocket.TextMessage},
		{"Binary", adapter.Binary, websocket.BinaryMessage},
		{"Close", adapter.Close, websocket.CloseMessage},
		{"Ping", adapter.Ping, websocket.PingMessage},
		{"Pong", adapter.Pong, websocket.PongMessage},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			converted := int(tc.op)
			if converted != tc.expected {
				t.Errorf("Expected opcode %d, got %d", tc.expected, converted)
			}
		})
	}
}

// BenchmarkServerUpgrade benchmarks the server upgrade
func BenchmarkServerUpgrade(b *testing.B) {
	adapter := NewServerAdapter(nil)
	callback := &mockServerCallback{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := adapter.Upgrade(w, r, callback)
		if err != nil {
			return
		}
		conn.Close()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			b.Fatal(err)
		}
		conn.Close()
	}
}

// BenchmarkClientWrite benchmarks client write
func BenchmarkClientWrite(b *testing.B) {
	callback := &mockServerCallback{}
	serverAdapter := NewServerAdapter(nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := serverAdapter.Upgrade(w, r, callback)
		if err != nil {
			return
		}
		go conn.StartReadLoop()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	clientAdapter := NewClientAdapter(nil)
	clientCallback := &mockClientCallback{}

	conn, err := clientAdapter.Dial(wsURL, nil, clientCallback)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	testMessage := []byte("benchmark test message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Write(adapter.Text, testMessage)
	}
}
