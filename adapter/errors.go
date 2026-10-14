package adapter

import "errors"

var (
	// ErrConnClosed 连接已关闭
	ErrConnClosed = errors.New("connection closed")
	// ErrNotConnected 未连接
	ErrNotConnected = errors.New("not connected")
	// ErrWriteTimeout 写入超时
	ErrWriteTimeout = errors.New("write timeout")
	// ErrReadTimeout 读取超时
	ErrReadTimeout = errors.New("read timeout")
)
