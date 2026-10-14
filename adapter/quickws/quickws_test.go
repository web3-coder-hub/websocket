package quickws

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/antlabs/quickws"
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
	sa := NewServerAdapter()

	if sa == nil {
		t.Fatal("NewServerAdapter returned nil")
	}

	if name := sa.Name(); name != "quickws" {
		t.Errorf("Expected adapter name 'quickws', got '%s'", name)
	}
}

// TestServerAdapterUpgrade tests the upgrade process
func TestServerAdapterUpgrade(t *testing.T) {
	sa := NewServerAdapter()
	callback := &mockServerCallback{}

	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := sa.Upgrade(w, r, callback)
		if err != nil {
			// This is expected for non-websocket requests
			return
		}

		if conn == nil {
			t.Error("Upgrade returned nil connection")
			return
		}

		// Start read loop
		go conn.StartReadLoop()

		// Wait a bit for the connection to be established
		time.Sleep(100 * time.Millisecond)

		// Write a test message
		err = conn.WriteTimeout(adapter.Text, []byte("test message"), time.Second)
		if err != nil {
			t.Logf("WriteTimeout error: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
		conn.Close()
	}))
	defer server.Close()

	// Test upgrade with invalid request (non-websocket)
	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	resp.Body.Close()

	// The upgrade should fail for non-websocket requests
	if resp.StatusCode == http.StatusOK {
		t.Error("Expected upgrade to fail for non-websocket request")
	}
}

// TestServerConn tests the server connection
func TestServerConn(t *testing.T) {
	// Create a mock quickws connection
	// Note: We can't easily test this without a real connection
	// so we'll test the wrapper methods
	t.Run("RemoteAddr", func(t *testing.T) {
		conn := &ServerConn{}
		addr := conn.RemoteAddr()
		// quickws doesn't expose RemoteAddr, so we expect empty string
		if addr != "" {
			t.Errorf("Expected empty RemoteAddr, got '%s'", addr)
		}
	})
}

// TestClientAdapter tests the client adapter
func TestClientAdapter(t *testing.T) {
	ca := NewClientAdapter()

	if ca == nil {
		t.Fatal("NewClientAdapter returned nil")
	}

	if name := ca.Name(); name != "quickws-client" {
		t.Errorf("Expected adapter name 'quickws-client', got '%s'", name)
	}
}

// TestClientAdapterDial tests the dial process
func TestClientAdapterDial(t *testing.T) {
	ca := NewClientAdapter()
	callback := &mockClientCallback{}

	// Test dial with invalid URL
	t.Run("InvalidURL", func(t *testing.T) {
		_, err := ca.Dial("invalid://url", nil, callback)
		if err == nil {
			t.Error("Expected error for invalid URL")
		}
	})

	// Test dial with non-existent server
	t.Run("NonExistentServer", func(t *testing.T) {
		_, err := ca.Dial("ws://localhost:99999", nil, callback)
		if err == nil {
			t.Error("Expected error for non-existent server")
		}
	})
}

// TestClientConn tests the client connection
func TestClientConn(t *testing.T) {
	t.Run("IsConnected", func(t *testing.T) {
		conn := &ClientConn{}
		conn.connected.Store(1)

		if !conn.IsConnected() {
			t.Error("Expected IsConnected to return true")
		}

		conn.connected.Store(0)
		if conn.IsConnected() {
			t.Error("Expected IsConnected to return false")
		}
	})

	t.Run("Close", func(t *testing.T) {
		conn := &ClientConn{
			done: make(chan struct{}),
		}
		conn.connected.Store(1)

		err := conn.Close()
		if err != nil {
			t.Errorf("Close returned error: %v", err)
		}

		if conn.IsConnected() {
			t.Error("Expected connection to be closed")
		}

		// Test double close (should not panic)
		err = conn.Close()
		if err != nil {
			t.Errorf("Second close returned error: %v", err)
		}
	})

	t.Run("Write", func(t *testing.T) {
		// Skip testing Write with nil conn to avoid panic
		// This method is tested via integration tests
		t.Skip("Write method tested via integration tests")
	})

	t.Run("WriteTimeout", func(t *testing.T) {
		// Skip testing WriteTimeout with nil conn to avoid panic
		// This method is tested via integration tests
		t.Skip("WriteTimeout method tested via integration tests")
	})
}

// TestServerCallbackWrapper tests the callback wrapper
func TestServerCallbackWrapper(t *testing.T) {
	callback := &mockServerCallback{}
	wrapper := &serverCallbackWrapper{
		callback: callback,
	}

	// Create a mock ServerConn
	serverConn := &ServerConn{}
	wrapper.serverConn = serverConn

	t.Run("OnOpen", func(t *testing.T) {
		callback.reset()
		wrapper.OnOpen(nil)

		if !callback.onOpenCalled {
			t.Error("Expected OnOpen to be called")
		}
	})

	t.Run("OnMessage", func(t *testing.T) {
		callback.reset()
		testData := []byte("test message")
		wrapper.OnMessage(nil, quickws.Text, testData)

		if !callback.onMessageCalled {
			t.Error("Expected OnMessage to be called")
		}

		if len(callback.messages) != 1 {
			t.Errorf("Expected 1 message, got %d", len(callback.messages))
		} else if string(callback.messages[0]) != string(testData) {
			t.Errorf("Expected message '%s', got '%s'", testData, callback.messages[0])
		}
	})

	t.Run("OnClose", func(t *testing.T) {
		callback.reset()
		wrapper.OnClose(nil, nil)

		if !callback.onCloseCalled {
			t.Error("Expected OnClose to be called")
		}
	})
}

// TestClientCallbackWrapper tests the client callback wrapper
func TestClientCallbackWrapper(t *testing.T) {
	callback := &mockClientCallback{}
	clientConn := &ClientConn{
		readChan: make(chan message, 10),
		done:     make(chan struct{}),
	}
	clientConn.connected.Store(1)

	wrapper := &clientCallbackWrapper{
		callback:   callback,
		clientConn: clientConn,
	}

	t.Run("OnOpen", func(t *testing.T) {
		callback.reset()
		wrapper.OnOpen(nil)

		if !callback.onOpenCalled {
			t.Error("Expected OnOpen to be called")
		}
	})

	t.Run("OnMessage", func(t *testing.T) {
		callback.reset()
		testData := []byte("test message")
		wrapper.OnMessage(nil, quickws.Text, testData)

		// Wait for message to be processed
		time.Sleep(50 * time.Millisecond)

		if !callback.onMessageCalled {
			t.Error("Expected OnMessage to be called")
		}

		if len(callback.messages) != 1 {
			t.Errorf("Expected 1 message, got %d", len(callback.messages))
		}

		// Check if message was added to readChan
		select {
		case msg := <-clientConn.readChan:
			if string(msg.data) != string(testData) {
				t.Errorf("Expected message '%s' in channel, got '%s'", testData, msg.data)
			}
		case <-time.After(time.Second):
			t.Error("Timeout waiting for message in channel")
		}
	})

	t.Run("OnClose", func(t *testing.T) {
		callback.reset()
		wrapper.OnClose(nil, nil)

		if !callback.onCloseCalled {
			t.Error("Expected OnClose to be called")
		}

		if clientConn.IsConnected() {
			t.Error("Expected connection to be marked as disconnected")
		}
	})
}

// TestClientConnReadMessage tests the ReadMessage method
func TestClientConnReadMessage(t *testing.T) {
	t.Run("SuccessfulRead", func(t *testing.T) {
		conn := &ClientConn{
			readChan: make(chan message, 10),
			done:     make(chan struct{}),
		}
		conn.connected.Store(1)

		testData := []byte("test message")
		conn.readChan <- message{
			op:   adapter.Text,
			data: testData,
		}

		op, data, err := conn.ReadMessage()
		if err != nil {
			t.Errorf("ReadMessage returned error: %v", err)
		}

		if op != adapter.Text {
			t.Errorf("Expected opcode %d, got %d", adapter.Text, op)
		}

		if string(data) != string(testData) {
			t.Errorf("Expected data '%s', got '%s'", testData, data)
		}
	})

	t.Run("ReadAfterClose", func(t *testing.T) {
		conn := &ClientConn{
			readChan: make(chan message, 10),
			done:     make(chan struct{}),
		}
		conn.connected.Store(1)

		close(conn.done)

		_, _, err := conn.ReadMessage()
		if err != adapter.ErrConnClosed {
			t.Errorf("Expected ErrConnClosed, got %v", err)
		}
	})
}

// TestOpcodeConversion tests opcode conversion safety
func TestOpcodeConversion(t *testing.T) {
	testCases := []struct {
		name     string
		op       adapter.Opcode
		expected quickws.Opcode
	}{
		{"Text", adapter.Text, quickws.Text},
		{"Binary", adapter.Binary, quickws.Binary},
		{"Close", adapter.Close, quickws.Close},
		{"Ping", adapter.Ping, quickws.Ping},
		{"Pong", adapter.Pong, quickws.Pong},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			converted := quickws.Opcode(tc.op)
			if converted != tc.expected {
				t.Errorf("Expected opcode %d, got %d", tc.expected, converted)
			}
		})
	}
}

// BenchmarkServerCallback benchmarks the server callback
func BenchmarkServerCallback(b *testing.B) {
	callback := &mockServerCallback{}
	wrapper := &serverCallbackWrapper{
		callback:   callback,
		serverConn: &ServerConn{},
	}

	testData := []byte("benchmark test message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wrapper.OnMessage(nil, quickws.Text, testData)
	}
}

// BenchmarkClientCallback benchmarks the client callback
func BenchmarkClientCallback(b *testing.B) {
	callback := &mockClientCallback{}
	clientConn := &ClientConn{
		readChan: make(chan message, 1000),
		done:     make(chan struct{}),
	}
	clientConn.connected.Store(1)

	wrapper := &clientCallbackWrapper{
		callback:   callback,
		clientConn: clientConn,
	}

	testData := []byte("benchmark test message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wrapper.OnMessage(nil, quickws.Text, testData)
	}
}
