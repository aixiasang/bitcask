package http

import (
	"fmt"

	"github.com/aixiasang/bitcask"
	"github.com/spf13/cobra"
)

var (
	// HTTP服务标志
	httpAddr string
)

// RegisterCommand 向Cobra CLI添加HTTP命令
func RegisterCommand(rootCmd *cobra.Command, createBitcaskFn func() (*bitcask.Bitcask, error), scanLimit *int) {
	httpCmd := &cobra.Command{
		Use:   "http",
		Short: "启动HTTP服务",
		Long: `启动HTTP RESTful API服务，提供通过HTTP接口访问Bitcask功能。

REST API端点:
  GET    /api/keys/{key}         - 获取指定key的值
  PUT    /api/keys/{key}         - 设置key的值 (请求体为值内容)
  DELETE /api/keys/{key}         - 删除指定key
  GET    /api/keys               - 列出所有键值对
  GET    /api/keys/range/{start}/{end} - 范围查询
  POST   /api/admin/merge        - 执行合并操作
  POST   /api/admin/hint         - 生成hint文件`,
		Run: func(cmd *cobra.Command, args []string) {
			// 创建一个bitcask实例并保持打开状态
			bc, err := createBitcaskFn()
			if err != nil {
				fmt.Printf("创建 Bitcask 实例失败: %v\n", err)
				return
			}
			defer bc.Close()

			// 创建并启动HTTP服务器
			server := NewServer(bc, httpAddr, *scanLimit)

			// 启动服务器并阻塞
			if err := server.Start(); err != nil {
				fmt.Printf("HTTP服务错误: %v\n", err)
			}
		},
	}

	// 添加HTTP特定的标志
	httpCmd.Flags().StringVar(&httpAddr, "addr", ":8080", "HTTP服务监听地址")

	// 将命令添加到根命令
	rootCmd.AddCommand(httpCmd)
}
