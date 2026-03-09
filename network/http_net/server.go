package http_net

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yaice-rx/yaice/config"
	"github.com/yaice-rx/yaice/logger"
	"go.uber.org/zap"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Context 封装请求上下文
type Context struct {
	Request  *http.Request
	Response http.ResponseWriter
	Params   map[string]string
}

// WriteJSON 安全地写入JSON响应
func (ctx *Context) WriteJSON(status int, data interface{}) error {
	ctx.Response.Header().Set("Content-Type", "application/json")
	ctx.Response.WriteHeader(status)
	if err := json.NewEncoder(ctx.Response).Encode(data); err != nil {
		logger.Error("WriteJSON error", zap.Error(err))
		return err
	}
	return nil
}

type HandlerFunc func(ctx *Context)

// HTTPServer HTTP服务器实现
type HTTPServer struct {
	sync.RWMutex
	config        *config.HTTPConfig
	server        *http.Server
	isRunning     bool
	listenPort    int
	activeConns   int32
	healthHandler http.HandlerFunc
	ctx           context.Context
	cancel        context.CancelFunc // 用于优雅关闭所有goroutine

	// 路由管理
	routes     map[string]HandlerFunc // path -> handler
	routeMutex sync.RWMutex
	mux        *http.ServeMux

	// CORS 白名单
	corsOrigins []string // 改用切片，配合strings.Contains或正则匹配更灵活
}

// NewHTTPServer 创建HTTP服务器实例
func NewHTTPServer(ctx context.Context, cfg interface{}) (*HTTPServer, error) {
	httpConfig, ok := cfg.(*config.HTTPConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type, expected *config.HTTPConfig")
	}

	// 创建可取消的子Context
	serverCtx, cancel := context.WithCancel(ctx)

	// 创建默认健康检查处理器
	healthHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"ok","timestamp":"%s"}`, time.Now().Format(time.RFC3339))
	}

	return &HTTPServer{
		ctx:           serverCtx,
		cancel:        cancel,
		config:        httpConfig,
		isRunning:     false,
		activeConns:   0,
		healthHandler: healthHandler,
		routes:        make(map[string]HandlerFunc),
		mux:           http.NewServeMux(),
		corsOrigins:   httpConfig.CORSOrigins, // 直接引用配置
	}, nil
}

// Listen 启动HTTP监听
func (h *HTTPServer) Listen(isAllowConnFunc func(conn interface{}) bool) int {
	h.Lock()
	defer h.Unlock()

	if h.isRunning {
		logger.Warn("HTTP server is already running")
		return -1
	}

	// 初始化路由
	mux := http.NewServeMux()
	h.setupRoutes(mux)
	h.mux = mux

	// 配置服务器
	h.server = &http.Server{
		Addr:           fmt.Sprintf("%s:%d", h.config.Host, h.config.Port),
		Handler:        h.applyMiddleware(mux),
		ReadTimeout:    time.Second * time.Duration(h.config.ReadTimeout),
		WriteTimeout:   time.Second * time.Duration(h.config.WriteTimeout),
		IdleTimeout:    time.Second * time.Duration(h.config.IdleTimeout),
		MaxHeaderBytes: h.config.MaxHeaderBytes,
		// 基础安全配置
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	// 端口扫描逻辑
	var listener net.Listener
	var err error
	for port := h.config.StartPort; port <= h.config.EndPort; port++ {
		addr := fmt.Sprintf("%s:%d", h.config.Host, port)
		listener, err = net.Listen("tcp", addr)
		if err == nil {
			h.listenPort = port
			h.server.Addr = addr
			break
		}
		logger.Debug("Port not available, trying next", zap.Int("port", port), zap.Error(err))
	}

	if err != nil {
		logger.Error("No available ports", zap.Int("start", h.config.StartPort), zap.Int("end", h.config.EndPort))
		return -1
	}

	h.isRunning = true
	go h.serve(listener)

	logger.Info("HTTP server started", zap.String("addr", h.server.Addr), zap.Bool("tls", h.config.EnableTLS))
	return h.listenPort
}

// Close 关闭服务器
func (h *HTTPServer) Close(ctx context.Context) error {
	h.Lock()
	if !h.isRunning {
		h.Unlock()
		return nil
	}
	h.isRunning = false
	h.Unlock() // 尽早释放锁，避免阻塞Shutdown

	// 1. 取消内部Context，通知相关goroutine退出
	h.cancel()

	// 2. 优雅关闭HTTP服务
	logger.Info("Shutting down HTTP server...")
	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := h.server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", zap.Error(err))
		// 强制关闭
		if closeErr := h.server.Close(); closeErr != nil {
			return fmt.Errorf("shutdown failed: %v, force close failed: %v", err, closeErr)
		}
		return err
	}

	logger.Info("HTTP server stopped successfully")
	return nil
}

// RegisterRoute 注册路由
func (h *HTTPServer) RegisterRoute(path string, handler HandlerFunc) error {
	if path == "" || handler == nil {
		return fmt.Errorf("invalid route parameters")
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	h.routeMutex.Lock()
	h.routes[path] = handler
	h.routeMutex.Unlock()

	// 动态注册需要保护 mux
	h.Lock()
	defer h.Unlock()

	if h.isRunning && h.mux != nil {
		h.mux.HandleFunc(path, h.adaptHandler(handler))
		logger.Info("Dynamic route registered", zap.String("path", path))
	}
	return nil
}

// serve 处理连接
func (h *HTTPServer) serve(listener net.Listener) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("HTTP server panic recovered", zap.Any("error", r))
		}
		listener.Close()
		h.Lock()
		h.isRunning = false
		h.Unlock()
	}()

	var err error
	if h.config.EnableTLS {
		if h.config.CertFile == "" || h.config.KeyFile == "" {
			logger.Error("TLS enabled but cert/key missing")
			return
		}
		err = h.server.ServeTLS(listener, h.config.CertFile, h.config.KeyFile)
	} else {
		err = h.server.Serve(listener)
	}

	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("HTTP server serve error", zap.Error(err))
	}
}

// setupRoutes 初始化路由
func (h *HTTPServer) setupRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", h.healthHandler)

	if h.config.EnablePprof {
		// 建议将pprof放在独立端口或增加鉴权中间件，这里仅做路径隔离
		prefix := h.config.PprofPrefix
		if prefix == "" {
			prefix = "/debug/pprof"
		}
		// 使用HandleFunc包装以增加可能的权限检查
		mux.HandleFunc(prefix+"/", pprof.Index)
		mux.HandleFunc(prefix+"/cmdline", pprof.Cmdline)
		mux.HandleFunc(prefix+"/profile", pprof.Profile)
		mux.HandleFunc(prefix+"/symbol", pprof.Symbol)
		mux.HandleFunc(prefix+"/trace", pprof.Trace)
	}

	mux.HandleFunc("/", h.defaultHandler)

	h.routeMutex.RLock()
	for path, handler := range h.routes {
		mux.HandleFunc(path, h.adaptHandler(handler))
	}
	h.routeMutex.RUnlock()
}

// applyMiddleware 组装中间件
func (h *HTTPServer) applyMiddleware(handler http.Handler) http.Handler {
	// 注意顺序：Logging -> Counter -> Gzip -> CORS -> Handler
	chain := handler
	if h.config.EnableCORS {
		chain = h.corsMiddleware(chain)
	}
	chain = h.connectionCounterMiddleware(chain)
	chain = h.loggingMiddleware(chain)
	return chain
}

// adaptHandler 适配自定义Handler到http.HandlerFunc
func (h *HTTPServer) adaptHandler(handler HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 使用context控制超时
		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(h.config.WriteTimeout)*time.Second)
		defer cancel()

		r = r.WithContext(ctx)

		myCtx := &Context{
			Request:  r,
			Response: w,
			Params:   make(map[string]string),
		}
		handler(myCtx)
	}
}

// corsMiddleware CORS处理
func (h *HTTPServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		allowed := false

		if origin != "" {
			for _, allowedOrigin := range h.corsOrigins {
				if allowedOrigin == "*" || allowedOrigin == origin {
					allowed = true
					w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
					break
				}
			}
		}

		if allowed {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Expose-Headers", "Content-Length")
		}

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// connectionCounterMiddleware 连接计数
func (h *HTTPServer) connectionCounterMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&h.activeConns, 1)
		defer atomic.AddInt32(&h.activeConns, -1)
		next.ServeHTTP(w, r)
	})
}

// loggingMiddleware 日志记录
func (h *HTTPServer) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriterWrapper{ResponseWriter: w}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)
		logger.Info("HTTP Request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", wrapped.statusCode),
			zap.Duration("duration", duration),
		)
	})
}

// defaultHandler 默认响应
func (h *HTTPServer) defaultHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"message": "YAICE HTTP Server", "timestamp": "%s"}`, time.Now().Format(time.RFC3339))
}

// GetActiveConnections 获取活跃连接
func (h *HTTPServer) GetActiveConnections() int {
	return int(atomic.LoadInt32(&h.activeConns))
}

// Health 健康检查
func (h *HTTPServer) Health() error {
	h.RLock()
	running := h.isRunning
	h.RUnlock()

	if !running {
		return fmt.Errorf("server not running")
	}
	return nil
}

// GetConnCount 接口实现
func (h *HTTPServer) GetConnCount() int32 {
	return atomic.LoadInt32(&h.activeConns)
}

// SetMaxConnCount 接口实现
func (h *HTTPServer) SetMaxConnCount(count int32) {
	logger.Debug("SetMaxConnCount called", zap.Int32("count", count))
	// 实际逻辑可根据需求添加限流中间件
}

// --- 辅助类型 ---
type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (w *responseWriterWrapper) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.written = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriterWrapper) Write(b []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// isWritten 检查是否已写入Header
func isWritten(w http.ResponseWriter) bool {
	if rw, ok := w.(*responseWriterWrapper); ok {
		return rw.written
	}
	// 无法确定的情况下返回false，由上层处理
	return false
}
