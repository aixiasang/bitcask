package http

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aixiasang/bitcask"
	_ "github.com/aixiasang/bitcask/docs" // 导入Swagger文档
	"github.com/gorilla/mux"
	httpSwagger "github.com/swaggo/http-swagger"
)

// @title Bitcask API
// @version 1.0
// @description Bitcask的RESTful API服务
// @BasePath /api

// Server 表示HTTP服务器实例
type Server struct {
	bc        *bitcask.Bitcask
	addr      string
	server    *http.Server
	router    *mux.Router
	scanLimit int
}

// NewServer 创建新的HTTP服务器实例
func NewServer(bc *bitcask.Bitcask, addr string, scanLimit int) *Server {
	s := &Server{
		bc:        bc,
		addr:      addr,
		scanLimit: scanLimit,
	}

	// 初始化路由
	s.setupRouter()

	return s
}

// setupRouter 设置路由
func (s *Server) setupRouter() {
	router := mux.NewRouter()

	// API路由
	apiRouter := router.PathPrefix("/api").Subrouter()

	// 键值操作API
	keyRouter := apiRouter.PathPrefix("/keys").Subrouter()

	// 获取指定key的值
	keyRouter.HandleFunc("/{key}", s.handleGetKey).Methods("GET")

	// 设置key的值
	keyRouter.HandleFunc("/{key}", s.handlePutKey).Methods("PUT")

	// 删除指定key
	keyRouter.HandleFunc("/{key}", s.handleDeleteKey).Methods("DELETE")

	// 列出所有键值对
	keyRouter.HandleFunc("", s.handleListKeys).Methods("GET")

	// 范围查询
	keyRouter.HandleFunc("/range/{start}/{end}", s.handleRangeQuery).Methods("GET")

	// 管理员操作API
	adminRouter := apiRouter.PathPrefix("/admin").Subrouter()

	// 执行合并操作
	adminRouter.HandleFunc("/merge", s.handleMerge).Methods("POST")

	// 生成hint文件
	adminRouter.HandleFunc("/hint", s.handleHint).Methods("POST")

	// 添加Swagger文档路由
	router.PathPrefix("/swagger/").Handler(httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"), // The URL pointing to API definition
		httpSwagger.DeepLinking(true),
		httpSwagger.DocExpansion("none"),
		httpSwagger.DomID("swagger-ui"),
	))

	// 添加重定向到Swagger UI的路由
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger/index.html", http.StatusMovedPermanently)
	})

	// 添加中间件来记录请求
	router.Use(s.loggingMiddleware)

	// 保存路由器
	s.router = router
}

// 中间件：记录HTTP请求
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

// @Summary 获取指定key的值
// @Description 获取存储在系统中的指定key的值
// @Tags keys
// @Accept json
// @Produce text/plain
// @Param key path string true "查询的键名"
// @Success 200 {string} string "键值内容"
// @Failure 404 {string} string "获取值失败"
// @Router /keys/{key} [get]
func (s *Server) handleGetKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := []byte(vars["key"])

	value, ok := s.bc.Get(key)
	if !ok {
		http.Error(w, "获取值失败", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write(value)
}

// @Summary 设置key的值
// @Description 存储或更新键值对
// @Tags keys
// @Accept text/plain
// @Produce text/plain
// @Param key path string true "设置的键名"
// @Param value body string true "存储的值"
// @Success 200 {string} string "存储成功"
// @Failure 400 {string} string "请求错误"
// @Failure 500 {string} string "存储失败"
// @Router /keys/{key} [put]
func (s *Server) handlePutKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := []byte(vars["key"])

	// 读取请求体作为值
	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("读取请求体失败: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := s.bc.Put(key, value); err != nil {
		http.Error(w, fmt.Sprintf("存储值失败: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "存储成功")
}

// @Summary 删除指定key
// @Description 从系统中删除指定的键值对
// @Tags keys
// @Produce text/plain
// @Param key path string true "要删除的键名"
// @Success 200 {string} string "删除成功"
// @Failure 500 {string} string "删除失败"
// @Router /keys/{key} [delete]
func (s *Server) handleDeleteKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := []byte(vars["key"])

	if err := s.bc.Delete(key); err != nil {
		http.Error(w, fmt.Sprintf("删除失败: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "删除成功")
}

// KVPair 用于JSON序列化的键值对结构
type KVPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// @Summary 列出所有键值对
// @Description 获取系统中所有键值对
// @Tags keys
// @Produce json
// @Success 200 {array} KVPair "键值对列表"
// @Failure 500 {string} string "扫描失败"
// @Router /keys [get]
func (s *Server) handleListKeys(w http.ResponseWriter, r *http.Request) {
	// 收集所有键值对
	var results []KVPair

	err := s.bc.Scan(func(key []byte, value []byte) error {
		results = append(results, KVPair{
			Key:   string(key),
			Value: string(value),
		})
		return nil
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("扫描失败: %v", err), http.StatusInternalServerError)
		return
	}

	// 返回JSON格式
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// RangeQueryResult 范围查询结果
type RangeQueryResult struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// @Summary 范围查询键值对
// @Description 查询指定键范围内的键值对
// @Tags keys
// @Produce json
// @Param start path string true "起始键"
// @Param end path string true "结束键"
// @Param limit query int false "最大返回数量" default(100)
// @Success 200 {array} RangeQueryResult "范围内的键值对"
// @Failure 500 {string} string "范围扫描失败"
// @Router /keys/range/{start}/{end} [get]
func (s *Server) handleRangeQuery(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	startKey := []byte(vars["start"])
	endKey := []byte(vars["end"])

	// 获取limit参数
	limitStr := r.URL.Query().Get("limit")
	limit := s.scanLimit // 默认使用全局limit
	if limitStr != "" {
		fmt.Sscanf(limitStr, "%d", &limit)
	}

	results, err := s.bc.ScanRangeLimit(startKey, endKey, limit)
	if err != nil && err != bitcask.ErrReachLimit && err != bitcask.ErrExceedEndRange {
		http.Error(w, fmt.Sprintf("范围扫描失败: %v", err), http.StatusInternalServerError)
		return
	}

	// 转换为JSON友好的格式
	jsonResults := make([]RangeQueryResult, len(results))
	for i, result := range results {
		jsonResults[i] = RangeQueryResult{
			Key:   string(result.Key),
			Value: string(result.Value),
		}
	}

	// 返回JSON格式
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jsonResults)
}

// @Summary 执行合并操作
// @Description 合并数据文件，删除过时记录
// @Tags admin
// @Produce text/plain
// @Success 200 {string} string "合并成功"
// @Failure 500 {string} string "合并失败"
// @Router /admin/merge [post]
func (s *Server) handleMerge(w http.ResponseWriter, r *http.Request) {
	if err := s.bc.Merge(); err != nil {
		http.Error(w, fmt.Sprintf("合并失败: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "合并成功")
}

// @Summary 生成hint文件
// @Description 生成hint文件，加速下次启动
// @Tags admin
// @Produce text/plain
// @Success 200 {string} string "生成hint文件成功"
// @Failure 500 {string} string "生成hint文件失败"
// @Router /admin/hint [post]
func (s *Server) handleHint(w http.ResponseWriter, r *http.Request) {
	if err := s.bc.Hint(); err != nil {
		http.Error(w, fmt.Sprintf("生成hint文件失败: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "生成hint文件成功")
}

// Start 启动HTTP服务
func (s *Server) Start() error {
	// 创建HTTP服务器
	s.server = &http.Server{
		Addr:         s.addr,
		Handler:      s.router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// 设置信号处理
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// 优雅关闭服务
	go func() {
		<-sigChan
		fmt.Println("\n接收到中断信号，正在优雅关闭服务...")

		// 关闭服务器
		s.server.Close()

		// 服务器已经关闭，程序将自动退出
	}()

	// 启动HTTP服务
	fmt.Printf("HTTP服务已启动，监听地址: %s\n", s.addr)
	fmt.Printf("Swagger文档地址: http://localhost%s/swagger/index.html\n", s.addr)
	fmt.Println("按 Ctrl+C 可安全退出服务")

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP服务错误: %v", err)
	}

	return nil
}

// Stop 停止HTTP服务
func (s *Server) Stop() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}
