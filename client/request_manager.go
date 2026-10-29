package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// RequestManager 管理同步请求的等待和响应匹配
type RequestManager struct {
	pendingRequests sync.Map      // map[string]*pendingRequest
	requestIDSeq    atomic.Uint64 // 请求ID生成器
}

// pendingRequest 待响应的请求
type pendingRequest struct {
	requestID string
	respChan  chan []byte // 响应数据通道（容量1，避免阻塞）
	errChan   chan error  // 错误通道（容量1）
}

// NewRequestManager 创建请求管理器
func NewRequestManager() *RequestManager {
	return &RequestManager{}
}

// GenerateRequestID 生成唯一请求ID
func (rm *RequestManager) GenerateRequestID() string {
	seq := rm.requestIDSeq.Add(1)
	return fmt.Sprintf("req-%d", seq)
}

// RegisterRequest 注册请求并返回等待通道
func (rm *RequestManager) RegisterRequest(requestID string) (*pendingRequest, error) {
	pr := &pendingRequest{
		requestID: requestID,
		respChan:  make(chan []byte, 1), // 容量1，避免通知时阻塞
		errChan:   make(chan error, 1),
	}

	// 检查是否已存在（避免ID冲突）
	if _, loaded := rm.pendingRequests.LoadOrStore(requestID, pr); loaded {
		return nil, fmt.Errorf("request ID already exists: %s", requestID)
	}

	return pr, nil
}

// WaitForResponse 等待响应（阻塞直到响应到达、超时或取消）
func (rm *RequestManager) WaitForResponse(ctx context.Context, pr *pendingRequest) ([]byte, error) {
	// 确保清理资源
	defer rm.CancelRequest(pr.requestID)

	select {
	case data := <-pr.respChan:
		return data, nil
	case err := <-pr.errChan:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// NotifyResponse 通知响应到达
// 返回 true 表示找到了对应的pending请求并通知成功
func (rm *RequestManager) NotifyResponse(requestID string, data []byte) bool {
	value, ok := rm.pendingRequests.Load(requestID)
	if !ok {
		return false
	}

	pr := value.(*pendingRequest)

	// 非阻塞发送（channel容量为1）
	select {
	case pr.respChan <- data:
	default:
		// 不应该发生（说明已经有响应了）
	}

	return true
}

// NotifyError 通知请求发生错误
func (rm *RequestManager) NotifyError(requestID string, err error) bool {
	value, ok := rm.pendingRequests.Load(requestID)
	if !ok {
		return false
	}

	pr := value.(*pendingRequest)

	select {
	case pr.errChan <- err:
	default:
	}

	return true
}

// CancelRequest 取消并清理单个请求
func (rm *RequestManager) CancelRequest(requestID string) {
	rm.pendingRequests.Delete(requestID)
}

// ClearAll 清理所有pending请求（用于连接断开等场景）
func (rm *RequestManager) ClearAll(err error) {
	rm.pendingRequests.Range(func(key, value interface{}) bool {
		pr := value.(*pendingRequest)

		// 通知所有等待的请求
		select {
		case pr.errChan <- err:
		default:
		}

		// 删除请求
		rm.pendingRequests.Delete(key)
		return true
	})
}

// PendingCount 返回当前pending请求数量（用于监控）
func (rm *RequestManager) PendingCount() int {
	count := 0
	rm.pendingRequests.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}
