package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRequestManager_GenerateRequestID(t *testing.T) {
	rm := NewRequestManager()

	// 生成多个ID，确保唯一性
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := rm.GenerateRequestID()
		if ids[id] {
			t.Errorf("duplicate request ID: %s", id)
		}
		ids[id] = true
	}
}

func TestRequestManager_RegisterRequest(t *testing.T) {
	rm := NewRequestManager()

	// 注册请求
	requestID := "test-req-1"
	pr, err := rm.RegisterRequest(requestID)
	if err != nil {
		t.Fatalf("failed to register request: %v", err)
	}
	if pr.requestID != requestID {
		t.Errorf("expected requestID %s, got %s", requestID, pr.requestID)
	}

	// 重复注册应该失败
	_, err = rm.RegisterRequest(requestID)
	if err == nil {
		t.Error("expected error for duplicate requestID, got nil")
	}
}

func TestRequestManager_WaitForResponse_Success(t *testing.T) {
	rm := NewRequestManager()
	requestID := "test-req-1"

	pr, err := rm.RegisterRequest(requestID)
	if err != nil {
		t.Fatalf("failed to register request: %v", err)
	}

	// 模拟响应
	expectedData := []byte("test response")
	go func() {
		time.Sleep(100 * time.Millisecond)
		rm.NotifyResponse(requestID, expectedData)
	}()

	// 等待响应
	ctx := context.Background()
	data, err := rm.WaitForResponse(ctx, pr)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if string(data) != string(expectedData) {
		t.Errorf("expected data %s, got %s", expectedData, data)
	}

	// 验证已清理
	if rm.PendingCount() != 0 {
		t.Errorf("expected 0 pending requests, got %d", rm.PendingCount())
	}
}

func TestRequestManager_WaitForResponse_Timeout(t *testing.T) {
	rm := NewRequestManager()
	requestID := "test-req-1"

	pr, err := rm.RegisterRequest(requestID)
	if err != nil {
		t.Fatalf("failed to register request: %v", err)
	}

	// 设置超时
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// 等待响应（应该超时）
	_, err = rm.WaitForResponse(ctx, pr)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded error, got %v", err)
	}

	// 验证已清理
	if rm.PendingCount() != 0 {
		t.Errorf("expected 0 pending requests, got %d", rm.PendingCount())
	}
}

func TestRequestManager_WaitForResponse_Error(t *testing.T) {
	rm := NewRequestManager()
	requestID := "test-req-1"

	pr, err := rm.RegisterRequest(requestID)
	if err != nil {
		t.Fatalf("failed to register request: %v", err)
	}

	// 模拟错误
	expectedErr := errors.New("test error")
	go func() {
		time.Sleep(100 * time.Millisecond)
		rm.NotifyError(requestID, expectedErr)
	}()

	// 等待响应
	ctx := context.Background()
	_, err = rm.WaitForResponse(ctx, pr)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != expectedErr.Error() {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	// 验证已清理
	if rm.PendingCount() != 0 {
		t.Errorf("expected 0 pending requests, got %d", rm.PendingCount())
	}
}

func TestRequestManager_NotifyResponse_NotFound(t *testing.T) {
	rm := NewRequestManager()

	// 通知不存在的请求
	ok := rm.NotifyResponse("non-existent", []byte("data"))
	if ok {
		t.Error("expected false for non-existent requestID, got true")
	}
}

func TestRequestManager_ClearAll(t *testing.T) {
	rm := NewRequestManager()

	// 注册多个请求
	requestIDs := []string{"req-1", "req-2", "req-3"}
	prs := make([]*pendingRequest, len(requestIDs))
	for i, id := range requestIDs {
		pr, err := rm.RegisterRequest(id)
		if err != nil {
			t.Fatalf("failed to register request %s: %v", id, err)
		}
		prs[i] = pr
	}

	if rm.PendingCount() != 3 {
		t.Errorf("expected 3 pending requests, got %d", rm.PendingCount())
	}

	// 清理所有请求
	clearErr := errors.New("connection closed")
	rm.ClearAll(clearErr)

	// 验证所有请求都收到错误
	for i, pr := range prs {
		select {
		case err := <-pr.errChan:
			if err.Error() != clearErr.Error() {
				t.Errorf("request %d: expected error %v, got %v", i, clearErr, err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("request %d: timeout waiting for error", i)
		}
	}

	// 验证已清理
	if rm.PendingCount() != 0 {
		t.Errorf("expected 0 pending requests, got %d", rm.PendingCount())
	}
}

func TestRequestManager_Concurrent(t *testing.T) {
	rm := NewRequestManager()
	concurrency := 100
	var wg sync.WaitGroup

	// 并发注册和响应
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			requestID := rm.GenerateRequestID()
			pr, err := rm.RegisterRequest(requestID)
			if err != nil {
				t.Errorf("failed to register request: %v", err)
				return
			}

			// 模拟响应
			go func() {
				time.Sleep(10 * time.Millisecond)
				rm.NotifyResponse(requestID, []byte("response"))
			}()

			// 等待响应
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			_, err = rm.WaitForResponse(ctx, pr)
			if err != nil {
				t.Errorf("request %d failed: %v", n, err)
			}
		}(i)
	}

	wg.Wait()

	// 验证已清理
	if rm.PendingCount() != 0 {
		t.Errorf("expected 0 pending requests, got %d", rm.PendingCount())
	}
}

func TestRequestManager_CancelRequest(t *testing.T) {
	rm := NewRequestManager()
	requestID := "test-req-1"

	_, err := rm.RegisterRequest(requestID)
	if err != nil {
		t.Fatalf("failed to register request: %v", err)
	}

	if rm.PendingCount() != 1 {
		t.Errorf("expected 1 pending request, got %d", rm.PendingCount())
	}

	// 取消请求
	rm.CancelRequest(requestID)

	if rm.PendingCount() != 0 {
		t.Errorf("expected 0 pending requests, got %d", rm.PendingCount())
	}

	// 再次取消不应该崩溃
	rm.CancelRequest(requestID)
}
