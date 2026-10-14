package quickws

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

// TestIntegrationServerClient tests server and client together
func TestIntegrationServerClient(t *testing.T) {
	serverCallback := &mockServerCallback{}
	sa := NewServerAdapter()

	var serverConnMu sync.Mutex
	var serverConn adapter.ServerConn

	// Create WebSocket server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := sa.Upgrade(w, r, serverCallback)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		serverConnMu.Lock()
		serverConn = conn
		serverConnMu.Unlock()

		go conn.StartReadLoop()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// Connect with gorilla client to test
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer clientConn.Close()

	// Wait for connection
	time.Sleep(100 * time.Millisecond)

	// Test ServerConn WriteTimeout
	t.Run("ServerWriteTimeout", func(t *testing.T) {
		serverConnMu.Lock()
		conn := serverConn
		serverConnMu.Unlock()

		if conn == nil {
			t.Fatal("Server connection is nil")
		}

		testMsg := []byte("server test message")
		err := conn.WriteTimeout(adapter.Text, testMsg, time.Second)
		if err != nil {
			t.Errorf("WriteTimeout failed: %v", err)
		}

		// Read on client side
		clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, msg, err := clientConn.ReadMessage()
		if err != nil {
			t.Errorf("Client read failed: %v", err)
		}

		if string(msg) != string(testMsg) {
			t.Errorf("Expected message '%s', got '%s'", testMsg, msg)
		}
	})

	// Test client sending to server
	t.Run("ClientToServer", func(t *testing.T) {
		serverCallback.reset()

		testMsg := []byte("client test message")
		err := clientConn.WriteMessage(websocket.TextMessage, testMsg)
		if err != nil {
			t.Fatalf("Client write failed: %v", err)
		}

		// Wait for message to be received
		time.Sleep(200 * time.Millisecond)

		serverCallback.mu.Lock()
		messageCount := len(serverCallback.messages)
		var receivedMsg []byte
		if messageCount > 0 {
			receivedMsg = serverCallback.messages[len(serverCallback.messages)-1]
		}
		serverCallback.mu.Unlock()

		if messageCount == 0 {
			t.Error("No message received on server")
		} else if string(receivedMsg) != string(testMsg) {
			t.Errorf("Expected message '%s', got '%s'", testMsg, receivedMsg)
		}
	})

	// Test ServerConn Close
	t.Run("ServerClose", func(t *testing.T) {
		serverConnMu.Lock()
		conn := serverConn
		serverConnMu.Unlock()

		if conn != nil {
			err := conn.Close()
			if err != nil {
				t.Errorf("ServerConn Close failed: %v", err)
			}
		}
	})
}

// TestIntegrationClientAdapter tests the client adapter with real connections
func TestIntegrationClientAdapter(t *testing.T) {
	// Create a WebSocket server using gorilla
	serverCallback := &mockServerCallback{}
	sa := NewServerAdapter()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := sa.Upgrade(w, r, serverCallback)
		if err != nil {
			return
		}
		go conn.StartReadLoop()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// Test client adapter
	ca := NewClientAdapter()
	clientCallback := &mockClientCallback{}

	conn, err := ca.Dial(wsURL, nil, clientCallback)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	time.Sleep(100 * time.Millisecond)

	// Test Write method
	t.Run("ClientWrite", func(t *testing.T) {
		testMsg := []byte("test write")
		err := conn.Write(adapter.Text, testMsg)
		if err != nil {
			t.Errorf("Write failed: %v", err)
		}

		time.Sleep(200 * time.Millisecond)

		serverCallback.mu.Lock()
		messageCount := len(serverCallback.messages)
		serverCallback.mu.Unlock()

		if messageCount == 0 {
			t.Error("Message not received on server")
		}
	})

	// Test WriteTimeout method
	t.Run("ClientWriteTimeout", func(t *testing.T) {
		serverCallback.reset()

		testMsg := []byte("test write timeout")
		err := conn.WriteTimeout(adapter.Text, testMsg, time.Second)
		if err != nil {
			t.Errorf("WriteTimeout failed: %v", err)
		}

		time.Sleep(200 * time.Millisecond)

		serverCallback.mu.Lock()
		messageCount := len(serverCallback.messages)
		serverCallback.mu.Unlock()

		if messageCount == 0 {
			t.Error("Message not received on server")
		}
	})
}
