package auth

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/web3-coder-hub/cache"
	"github.com/web3-coder-hub/logger"
	"github.com/web3-coder-hub/websocket/constant"
)

// TestNewAuthenticator tests authenticator creation
func TestNewAuthenticator(t *testing.T) {
	testCache, _ := cache.NewBigCache(10, 5*time.Minute)
	defer testCache.Close()

	t.Run("TokenAuth", func(t *testing.T) {
		auth := NewAuthenticator(testCache, constant.AuthTypeToken, 10, logger.Default())
		if auth == nil {
			t.Fatal("NewAuthenticator returned nil")
		}
		if auth.authType != constant.AuthTypeToken {
			t.Errorf("Expected auth type %s, got %s", constant.AuthTypeToken, auth.authType)
		}
		if auth.strategy == nil {
			t.Error("Strategy should not be nil for token auth")
		}
	})

	t.Run("NoAuth", func(t *testing.T) {
		auth := NewAuthenticator(testCache, constant.AuthTypeNone, 10, logger.Default())
		if auth == nil {
			t.Fatal("NewAuthenticator returned nil")
		}
		if auth.authType != constant.AuthTypeNone {
			t.Errorf("Expected auth type %s, got %s", constant.AuthTypeNone, auth.authType)
		}
		if auth.strategy != nil {
			t.Error("Strategy should be nil for no auth")
		}
	})

	t.Run("DefaultTimeout", func(t *testing.T) {
		auth := NewAuthenticator(testCache, constant.AuthTypeNone, 0, logger.Default())
		if auth.pendingAuthTimeoutSecs != 10 {
			t.Errorf("Expected default timeout 10, got %d", auth.pendingAuthTimeoutSecs)
		}
	})
}

// TestAuthenticatorGetters tests getter methods
func TestAuthenticatorGetters(t *testing.T) {
	testCache, _ := cache.NewBigCache(10, 5*time.Minute)
	defer testCache.Close()

	auth := NewAuthenticator(testCache, constant.AuthTypeToken, 15, logger.Default())

	if auth.GetAuthType() != constant.AuthTypeToken {
		t.Errorf("Expected auth type %s, got %s", constant.AuthTypeToken, auth.GetAuthType())
	}

	if auth.GetPendingAuthTimeout() != 15 {
		t.Errorf("Expected timeout 15, got %d", auth.GetPendingAuthTimeout())
	}

	strategy := auth.GetStrategy()
	if strategy == nil {
		t.Error("Strategy should not be nil")
	}
}

// TestGenerateClientId tests client ID generation
func TestGenerateClientId(t *testing.T) {
	req := httptest.NewRequest("GET", "/ws", nil)
	req.Header.Set("User-Agent", "TestAgent/1.0")
	req.RemoteAddr = "192.168.1.1:12345"

	clientId1 := GenerateClientId(req)
	clientId2 := GenerateClientId(req)

	if clientId1 == "" {
		t.Error("ClientId should not be empty")
	}

	if clientId1 != clientId2 {
		t.Error("Same request should generate same client ID")
	}

	// Test with different request
	req2 := httptest.NewRequest("GET", "/ws", nil)
	req2.Header.Set("User-Agent", "DifferentAgent/1.0")
	req2.RemoteAddr = "192.168.1.2:12345"

	clientId3 := GenerateClientId(req2)
	if clientId1 == clientId3 {
		t.Error("Different requests should generate different client IDs")
	}
}

// TestGenerateSessionId tests session ID generation
func TestGenerateSessionId(t *testing.T) {
	sessionId1 := GenerateSessionId()

	// Sleep to ensure different timestamp
	time.Sleep(1 * time.Millisecond)

	sessionId2 := GenerateSessionId()

	if sessionId1 == "" {
		t.Error("SessionId should not be empty")
	}

	if sessionId1 == sessionId2 {
		t.Error("Different calls should generate different session IDs")
	}

	if len(sessionId1) > 32 {
		t.Errorf("SessionId should be at most 32 chars, got %d", len(sessionId1))
	}
}

// TestGenerateSessionIdFromClientId tests session ID generation from client ID
func TestGenerateSessionIdFromClientId(t *testing.T) {
	clientId := "test-client-123"

	sessionId1 := GenerateSessionIdFromClientId(clientId)
	sessionId2 := GenerateSessionIdFromClientId(clientId)

	if sessionId1 == "" {
		t.Error("SessionId should not be empty")
	}

	if sessionId1 != sessionId2 {
		t.Error("Same client ID should generate same session ID")
	}

	// Different client ID should generate different session ID
	sessionId3 := GenerateSessionIdFromClientId("different-client")
	if sessionId1 == sessionId3 {
		t.Error("Different client IDs should generate different session IDs")
	}
}

// TestExtractRealIP tests real IP extraction
func TestExtractRealIP(t *testing.T) {
	testCases := []struct {
		name           string
		headers        map[string]string
		remoteAddr     string
		expectedPrefix string
	}{
		{
			name:           "X-Forwarded-For",
			headers:        map[string]string{"X-Forwarded-For": "203.0.113.1, 198.51.100.1"},
			remoteAddr:     "192.168.1.1:12345",
			expectedPrefix: "203.0.113.1",
		},
		{
			name:           "X-Real-IP",
			headers:        map[string]string{"X-Real-IP": "203.0.113.2"},
			remoteAddr:     "192.168.1.1:12345",
			expectedPrefix: "203.0.113.2",
		},
		{
			name:           "RemoteAddr",
			headers:        map[string]string{},
			remoteAddr:     "192.168.1.1:12345",
			expectedPrefix: "192.168.1.1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/ws", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			req.RemoteAddr = tc.remoteAddr

			ip := ExtractRealIP(req)
			if ip != tc.expectedPrefix {
				t.Errorf("Expected IP %s, got %s", tc.expectedPrefix, ip)
			}
		})
	}
}

// TestExtractHeaderOnly tests header extraction
func TestExtractHeaderOnly(t *testing.T) {
	req := httptest.NewRequest("GET", "/ws", nil)
	req.Header.Set("Test-Header", "test-value")

	value := ExtractHeaderOnly(req, "Test-Header", "default")
	if value != "test-value" {
		t.Errorf("Expected 'test-value', got %s", value)
	}

	value = ExtractHeaderOnly(req, "Missing-Header", "default")
	if value != "default" {
		t.Errorf("Expected 'default', got %s", value)
	}
}

// TestTokenAuthStrategy tests token authentication strategy
func TestTokenAuthStrategy(t *testing.T) {
	testCache, _ := cache.NewBigCache(10, 5*time.Minute)
	defer testCache.Close()

	strategy := NewTokenAuthStrategy(testCache, logger.Default())

	if strategy == nil {
		t.Fatal("NewTokenAuthStrategy returned nil")
	}

	if strategy.cache != testCache {
		t.Error("Cache not set correctly")
	}
}

// TestVerifyHmacSign tests HMAC signature verification
func TestVerifyHmacSign(t *testing.T) {
	clientType := "APP"
	clientId := "test-client-123"
	nonce := "1234567890"
	eventData := `{"key":"value"}`
	secret := "test-secret-key"

	t.Run("ValidSignature", func(t *testing.T) {
		// Generate valid signature
		sign := GenerateHmacSign(clientType, clientId, nonce, eventData, secret)

		valid := VerifyHmacSign(clientType, clientId, nonce, eventData, secret, sign)
		if !valid {
			t.Error("Valid signature should pass verification")
		}
	})

	t.Run("InvalidSignature", func(t *testing.T) {
		valid := VerifyHmacSign(clientType, clientId, nonce, eventData, secret, "invalid-sign")
		if valid {
			t.Error("Invalid signature should fail verification")
		}
	})

	t.Run("DifferentData", func(t *testing.T) {
		sign := GenerateHmacSign(clientType, clientId, nonce, eventData, secret)

		valid := VerifyHmacSign(clientType, clientId, nonce, "different-data", secret, sign)
		if valid {
			t.Error("Signature should not match different data")
		}
	})
}

// TestSendAuthError tests auth error response
func TestSendAuthError(t *testing.T) {
	w := httptest.NewRecorder()
	err := &AuthError{
		StatusCode: http.StatusUnauthorized,
		Code:       "TEST_ERROR",
		Message:    "Test error message",
	}

	SendAuthError(w, err)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status %d, got %d", http.StatusUnauthorized, w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		t.Errorf("Expected Content-Type to contain application/json, got %s", contentType)
	}

	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)

	if response["code"] != "TEST_ERROR" {
		t.Errorf("Expected code 'TEST_ERROR', got %v", response["code"])
	}

	if response["message"] != "Test error message" {
		t.Errorf("Expected message 'Test error message', got %v", response["message"])
	}
}

// TestAuthError tests AuthError implementation
func TestAuthError(t *testing.T) {
	err := &AuthError{
		StatusCode: http.StatusUnauthorized,
		Code:       "AUTH_FAILED",
		Message:    "Authentication failed",
	}

	errString := err.Error()
	if errString != "Authentication failed" {
		t.Errorf("Expected 'Authentication failed', got %s", errString)
	}
}

// TestSha256Hex tests sha256 hashing utility
func TestSha256Hex(t *testing.T) {
	input := "test-string"
	hash1 := sha256Hex(input)
	hash2 := sha256Hex(input)

	if hash1 != hash2 {
		t.Error("Same input should produce same hash")
	}

	if len(hash1) != 32 {
		t.Errorf("SHA256 hex should be 32 characters (truncated), got %d", len(hash1))
	}

	// Different input should produce different hash
	hash3 := sha256Hex("different-string")
	if hash1 == hash3 {
		t.Error("Different inputs should produce different hashes")
	}
}

// TestToString tests toString utility
func TestToString(t *testing.T) {
	testCases := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"String", "hello", "hello"},
		{"Int", 123, "123"},
		{"Float", 45.67, "45.67"},
		{"Bool", true, "true"},
		{"Nil", nil, ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := toString(tc.input)
			if result != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}
		})
	}
}

// TestSortAndSerializeParam tests parameter sorting
func TestSortAndSerializeParam(t *testing.T) {
	t.Run("ValidJSON", func(t *testing.T) {
		param := `{"z":"value3","a":"value1","m":"value2"}`
		result := SortAndSerializeParam(param)

		// Should be sorted alphabetically
		if !json.Valid([]byte(result)) {
			t.Error("Result should be valid JSON")
		}

		// Verify it contains all keys
		if !strings.Contains(result, "a") || !strings.Contains(result, "m") || !strings.Contains(result, "z") {
			t.Error("Result should contain all keys")
		}
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		param := "not-json"
		result := SortAndSerializeParam(param)
		if result != param {
			t.Error("Invalid JSON should return original string")
		}
	})

	t.Run("EmptyString", func(t *testing.T) {
		result := SortAndSerializeParam("")
		if result != "" {
			t.Error("Empty string should return empty string")
		}
	})
}

// TestExtractToken tests token extraction from request
func TestExtractToken(t *testing.T) {
	t.Run("FromAccessTokenHeader", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/ws", nil)
		req.Header.Set("Access-Token", "token-from-header")

		token := extractToken(req)
		if token != "token-from-header" {
			t.Errorf("Expected 'token-from-header', got %s", token)
		}
	})

	t.Run("WithBearerPrefix", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/ws", nil)
		req.Header.Set("Access-Token", "Bearer token-with-bearer")

		token := extractToken(req)
		if token != "token-with-bearer" {
			t.Errorf("Expected 'token-with-bearer', got %s", token)
		}
	})

	t.Run("NoToken", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/ws", nil)

		token := extractToken(req)
		if token != "" {
			t.Errorf("Expected empty string, got %s", token)
		}
	})

	t.Run("PriorityOrder", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/ws?access_token=query-token", nil)
		req.Header.Set("Access-Token", "header-token")

		token := extractToken(req)
		// Access-Token header should be extracted (query params not supported)
		if token != "header-token" {
			t.Errorf("Expected 'header-token', got %s", token)
		}
	})
}

// TestTokenAuthStrategyNilLogger tests TokenAuthStrategy with nil logger
func TestTokenAuthStrategyNilLogger(t *testing.T) {
	testCache, _ := cache.NewBigCache(10, 5*time.Minute)
	defer testCache.Close()

	strategy := NewTokenAuthStrategy(testCache, nil)

	if strategy == nil {
		t.Fatal("NewTokenAuthStrategy should handle nil logger")
	}
}

// TestGenerateHmacSign tests HMAC signature generation
func TestGenerateHmacSign(t *testing.T) {
	clientType := "APP"
	clientId := "test-client"
	nonce := "123456"
	param := `{"key":"value"}`
	secret := "test-secret"

	sign := GenerateHmacSign(clientType, clientId, nonce, param, secret)

	if sign == "" {
		t.Error("Generated signature should not be empty")
	}

	// Verify the signature length (HMAC-SHA256 produces 64 hex characters)
	if len(sign) != 64 {
		t.Errorf("Expected signature length 64, got %d", len(sign))
	}
}

// BenchmarkGenerateClientId benchmarks client ID generation
func BenchmarkGenerateClientId(b *testing.B) {
	req := httptest.NewRequest("GET", "/ws", nil)
	req.Header.Set("User-Agent", "TestAgent/1.0")
	req.RemoteAddr = "192.168.1.1:12345"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GenerateClientId(req)
	}
}

// BenchmarkGenerateSessionId benchmarks session ID generation
func BenchmarkGenerateSessionId(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GenerateSessionId()
	}
}

// BenchmarkVerifyHmacSign benchmarks HMAC signature verification
func BenchmarkVerifyHmacSign(b *testing.B) {
	clientType := "APP"
	clientId := "test-client-123"
	nonce := "1234567890"
	eventData := `{"key":"value"}`
	secret := "test-secret-key"
	signature := GenerateHmacSign(clientType, clientId, nonce, eventData, secret)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VerifyHmacSign(clientType, clientId, nonce, eventData, secret, signature)
	}
}

// TestParseJWT tests JWT parsing
func TestParseJWT(t *testing.T) {
	t.Run("ValidJWT", func(t *testing.T) {
		// Create a valid JWT (header.payload.signature format)
		// For testing, we'll create a simple JWT with unverified signature
		header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

		content := loginUser{
			DataCacheKey: "test-cache-key",
			ClientType:   "APP",
		}
		contentJSON, _ := json.Marshal(content)

		payload := jwtPayload{
			Content: string(contentJSON),
			Exp:     time.Now().Add(1 * time.Hour).Unix(),
		}
		payloadJSON, _ := json.Marshal(payload)
		payloadEncoded := base64.RawURLEncoding.EncodeToString(payloadJSON)

		signature := base64.RawURLEncoding.EncodeToString([]byte("fake-signature"))

		token := header + "." + payloadEncoded + "." + signature

		lu, exp, err := parseJWT(token)
		if err != nil {
			t.Errorf("Failed to parse valid JWT: %v", err)
		}
		if lu.DataCacheKey != "test-cache-key" {
			t.Errorf("Expected DataCacheKey 'test-cache-key', got %s", lu.DataCacheKey)
		}
		if lu.ClientType != "APP" {
			t.Errorf("Expected ClientType 'APP', got %s", lu.ClientType)
		}
		if exp == 0 {
			t.Error("Expected non-zero expiry")
		}
	})

	t.Run("InvalidFormat", func(t *testing.T) {
		_, _, err := parseJWT("invalid-token")
		if err == nil {
			t.Error("Should fail with invalid format")
		}
	})

	t.Run("InvalidPayload", func(t *testing.T) {
		token := "header.invalid-base64!@#.$%^.signature"
		_, _, err := parseJWT(token)
		if err == nil {
			t.Error("Should fail with invalid payload")
		}
	})

	t.Run("EmptyContent", func(t *testing.T) {
		header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256"}`))
		payload := jwtPayload{
			Content: "",
			Exp:     0,
		}
		payloadJSON, _ := json.Marshal(payload)
		payloadEncoded := base64.RawURLEncoding.EncodeToString(payloadJSON)
		signature := base64.RawURLEncoding.EncodeToString([]byte("sig"))

		token := header + "." + payloadEncoded + "." + signature

		_, _, err := parseJWT(token)
		if err == nil {
			t.Error("Should fail with empty content")
		}
	})
}

// TestTokenAuthStrategyAuthenticate tests Authenticate method
func TestTokenAuthStrategyAuthenticate(t *testing.T) {
	testCache, _ := cache.NewBigCache(10, 5*time.Minute)
	defer testCache.Close()

	strategy := NewTokenAuthStrategy(testCache, nil)

	t.Run("NoToken", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/ws", nil)

		mockClient := &mockAuthClient{
			clientId: "test-client",
			clientIp: "127.0.0.1",
		}

		authenticated, err := strategy.Authenticate(req, mockClient)
		if err != nil {
			t.Errorf("Should not error with no token: %v", err)
		}
		if authenticated {
			t.Error("Should not be authenticated without token")
		}
	})

	t.Run("InvalidToken", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/ws", nil)
		req.Header.Set("Access-Token", "invalid-token")

		mockClient := &mockAuthClient{
			clientId: "test-client",
			clientIp: "127.0.0.1",
		}

		authenticated, err := strategy.Authenticate(req, mockClient)
		if err == nil {
			t.Error("Should error with invalid token")
		}
		if authenticated {
			t.Error("Should not be authenticated with invalid token")
		}
	})
}

// TestTokenAuthStrategyVerify tests Verify method
func TestTokenAuthStrategyVerify(t *testing.T) {
	testCache, _ := cache.NewBigCache(10, 5*time.Minute)
	defer testCache.Close()

	strategy := NewTokenAuthStrategy(testCache, nil)

	t.Run("EmptyCredential", func(t *testing.T) {
		mockClient := &mockAuthClient{}

		err := strategy.Verify("", mockClient)
		if err == nil {
			t.Error("Should error with empty credential")
		}
	})

	t.Run("InvalidToken", func(t *testing.T) {
		mockClient := &mockAuthClient{}

		err := strategy.Verify("invalid-token", mockClient)
		if err == nil {
			t.Error("Should error with invalid token")
		}
	})
}

// mockAuthClient implements AuthClient interface for testing
type mockAuthClient struct {
	sessionId  string
	userId     string
	accountId  string
	clientId   string
	clientType string
	clientIp   string
	accountIds string
	state      int32
}

func (m *mockAuthClient) SetSessionId(id string)      { m.sessionId = id }
func (m *mockAuthClient) SetUserId(id string)         { m.userId = id }
func (m *mockAuthClient) SetAccountId(id string)      { m.accountId = id }
func (m *mockAuthClient) SetClientId(id string)       { m.clientId = id }
func (m *mockAuthClient) SetClientType(t string)      { m.clientType = t }
func (m *mockAuthClient) SetClientIp(ip string)       { m.clientIp = ip }
func (m *mockAuthClient) SetAccountIds(ids string)    { m.accountIds = ids }
func (m *mockAuthClient) SetState(state int32)        { m.state = state }
func (m *mockAuthClient) SetAuthDeadline(t time.Time) {}
func (m *mockAuthClient) GetState() int32             { return m.state }
func (m *mockAuthClient) GetClientId() string         { return m.clientId }
func (m *mockAuthClient) GetClientType() string       { return m.clientType }
func (m *mockAuthClient) GetClientIp() string         { return m.clientIp }
func (m *mockAuthClient) GetAccountIdsString() string { return m.accountIds }
