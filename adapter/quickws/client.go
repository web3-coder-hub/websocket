package quickws

import (
	"bytes"
	"compress/flate"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antlabs/quickws"
	"github.com/web3-coder-hub/websocket/adapter"
	"github.com/web3-coder-hub/websocket/constant"
)

// ClientAdapter quickws 客户端适配器
type ClientAdapter struct{}

// NewClientAdapter 创建 quickws 客户端适配器
func NewClientAdapter() adapter.ClientAdapter {
	return &ClientAdapter{}
}

func (a *ClientAdapter) Name() string {
	return "quickws-client"
}

func (a *ClientAdapter) Dial(url string, headers http.Header, callback adapter.ClientCallback) (adapter.ClientConn, error) {
	return a.DialWithOptions(url, headers, callback)
}

func (a *ClientAdapter) DialWithOptions(url string, headers http.Header, callback adapter.ClientCallback, options ...adapter.ClientOption) (adapter.ClientConn, error) {
	// 解析适配器选项
	opts := &adapter.ClientOptions{}
	for _, opt := range options {
		opt(opts)
	}

	clientConn := &ClientConn{
		url:                url,
		headers:            headers,
		callback:           callback,
		readChan:           make(chan message, 100), // 增大缓冲区避免阻塞
		done:               make(chan struct{}),
		compressionEnabled: opts.Compression, // 记录是否启用压缩
	}
	clientConn.connected.Store(1)

	wrapper := &clientCallbackWrapper{
		callback:   callback,
		clientConn: clientConn,
	}

	// 构建 quickws 选项
	var dialOptions []quickws.ClientOption
	dialOptions = append(dialOptions, quickws.WithClientCallback(wrapper))

	// 启用自动回复ping帧 - 这对于维持长连接至关重要
	dialOptions = append(dialOptions, quickws.WithClientReplyPing())

	// 如果启用压缩，需要同时设置压缩和解压缩
	// quickws 要求两者都设置才会协商 permessage-deflate
	if opts.Compression {
		dialOptions = append(dialOptions, quickws.WithClientCompression())
		dialOptions = append(dialOptions, quickws.WithClientDecompression())
	}

	conn, err := quickws.Dial(url, dialOptions...)
	if err != nil {
		clientConn.connected.Store(0)
		return nil, err
	}

	clientConn.conn = conn

	// 启动读取循环
	// quickws 的 StartReadLoop 会自动调用 callback，不需要额外的 readLoop
	go conn.StartReadLoop()

	return clientConn, nil
}

// ClientConn quickws 客户端连接实现
type ClientConn struct {
	conn               *quickws.Conn
	url                string
	headers            http.Header
	callback           adapter.ClientCallback
	connected          atomic.Int32
	readChan           chan message
	done               chan struct{}
	closeOnce          sync.Once
	compressionEnabled bool // 是否启用了压缩
}

type message struct {
	op   adapter.Opcode
	data []byte
	err  error
}

func (c *ClientConn) Write(op adapter.Opcode, data []byte) error {
	if c.conn == nil {
		return errors.New(constant.GetErrorMessage(constant.ErrCodeConnNotConnected))
	}
	// #nosec G115 - Opcode 转换是安全的，值域在 0-10 范围内
	return c.conn.WriteMessage(quickws.Opcode(op), data)
}

func (c *ClientConn) WriteTimeout(op adapter.Opcode, data []byte, timeout time.Duration) error {
	if c.conn == nil {
		return errors.New(constant.GetErrorMessage(constant.ErrCodeConnWriteTimeout))
	}
	// quickws 的 WriteMessage 不支持超时，使用默认写入
	// #nosec G115 - Opcode 转换是安全的，值域在 0-10 范围内
	return c.conn.WriteMessage(quickws.Opcode(op), data)
}

func (c *ClientConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.connected.Store(0)
		if c.conn != nil {
			err = c.conn.Close()
		}
		close(c.done)
	})
	return err
}

func (c *ClientConn) ReadMessage() (adapter.Opcode, []byte, error) {
	select {
	case msg := <-c.readChan:
		return msg.op, msg.data, msg.err
	case <-c.done:
		return 0, nil, errors.New(constant.GetErrorMessage(constant.ErrCodeConnClose))
	}
}

func (c *ClientConn) IsConnected() bool {
	return c.connected.Load() == 1
}

// clientCallbackWrapper quickws 客户端回调适配器
type clientCallbackWrapper struct {
	callback   adapter.ClientCallback
	clientConn *ClientConn
}

func (w *clientCallbackWrapper) OnOpen(conn *quickws.Conn) {
	if w.callback != nil {
		w.callback.OnOpen(w.clientConn)
	}
}

func (w *clientCallbackWrapper) OnMessage(conn *quickws.Conn, op quickws.Opcode, data []byte) {
	if w.clientConn == nil {
		return
	}

	// 跳过空消息
	if len(data) == 0 {
		return
	}

	// 跳过控制帧（ping/pong/close等），这些帧由底层库自动处理
	if op.IsControl() {
		return
	}

	// 仅在启用压缩时检测并解压缩数据
	if w.clientConn.compressionEnabled && len(data) > 0 {
		// JSON 数据必定以 '{' 或 '[' 开头
		// 压缩数据通常以非可打印字符开头
		if data[0] != '{' && data[0] != '[' {
			if decompressed, err := decompressData(data); err == nil {
				data = decompressed
			} else {
				// Decompression failed, report error but continue
				if w.callback != nil {
					w.callback.OnError(w.clientConn, err)
				}
				return // 解压失败就不继续处理
			}
		}
	}

	// 将消息放入通道供 ReadMessage 读取
	select {
	case w.clientConn.readChan <- message{op: adapter.Opcode(op), data: data}:
	case <-w.clientConn.done:
	default:
		// 如果通道满了，丢弃消息并记录（避免阻塞）
	}

	// 同时直接调用用户回调（如果存在）
	if w.callback != nil {
		w.callback.OnMessage(w.clientConn, adapter.Opcode(op), data)
	}
}

// decompressData 手动解压缩 DEFLATE 数据
// 优化版本：减少内存分配，提高性能
func decompressData(data []byte) ([]byte, error) {
	dataLen := len(data)
	if dataLen == 0 {
		return data, nil
	}

	// WebSocket permessage-deflate 在数据末尾添加 0x00 0x00 0xff 0xff
	// 检查是否已经有尾部标记
	needsTail := dataLen < 4 ||
		data[dataLen-4] != 0x00 || data[dataLen-3] != 0x00 ||
		data[dataLen-2] != 0xff || data[dataLen-1] != 0xff

	var reader io.ReadCloser
	var err error

	if needsTail {
		// 使用临时缓冲区避免修改原始数据
		// 预分配容量以避免扩容
		tailBuf := make([]byte, dataLen+4)
		copy(tailBuf, data)
		tailBuf[dataLen] = 0x00
		tailBuf[dataLen+1] = 0x00
		tailBuf[dataLen+2] = 0xff
		tailBuf[dataLen+3] = 0xff
		reader = flate.NewReader(bytes.NewReader(tailBuf))
	} else {
		reader = flate.NewReader(bytes.NewReader(data))
	}

	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close() // #nosec G110 - WebSocket message size is bounded by protocol
	}()

	// 预估解压后大小通常是压缩数据的 2-10 倍
	// 为 WebSocket 消息预分配合理的初始容量
	// 限制最大解压大小以防止解压炸弹攻击
	const maxDecompressedSize = 10 * 1024 * 1024 // 10MB
	estimatedSize := dataLen * 4
	if estimatedSize > 65536 {
		estimatedSize = 65536 // 最大预分配 64KB
	}
	buf := bytes.NewBuffer(make([]byte, 0, estimatedSize))

	// #nosec G110 - 使用 io.LimitReader 限制解压大小以防止解压炸弹
	limited := io.LimitReader(reader, maxDecompressedSize)
	if _, err := io.Copy(buf, limited); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (w *clientCallbackWrapper) OnClose(conn *quickws.Conn, err error) {
	if w.clientConn != nil {
		w.clientConn.connected.Store(0)
	}
	if w.callback != nil {
		// If there's an error, call OnError first before OnClose
		// This matches the behavior of other WebSocket libraries like gorilla
		if err != nil {
			w.callback.OnError(w.clientConn, err)
		}
		w.callback.OnClose(w.clientConn, err)
	}
}
