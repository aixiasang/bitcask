package redis

import (
	"github.com/aixiasang/bitcask"
	"github.com/spf13/cobra"
)

var (
	// Redis服务器地址标志
	redisAddr string

	// 创建Bitcask实例的函数
	createBitcaskFunc func() (*bitcask.Bitcask, error)
)

// redisCmd 表示 redis 命令
var redisCmd = &cobra.Command{
	Use:   "redis",
	Short: "启动Redis协议兼容服务器",
	Long: `启动一个Redis协议兼容的服务器，允许使用标准Redis客户端直接连接到Bitcask。
支持的Redis命令: GET, SET, DEL, KEYS, INFO, PING

使用示例:
  bitcask redis --addr :6379 --data-dir ./mydata`,
	Run: func(cmd *cobra.Command, args []string) {
		// 使用全局变量中存储的createBitcask函数
		bc, err := createBitcaskFunc()
		if err != nil {
			cmd.PrintErrf("创建Bitcask实例失败: %v\n", err)
			return
		}
		defer bc.Close()

		// 创建并启动Redis服务器
		server := NewServer(bc, redisAddr)
		if err := server.Start(); err != nil {
			cmd.PrintErrf("启动Redis服务器失败: %v\n", err)
		}
	},
}

// RegisterCommand 注册Redis命令到root命令
func RegisterCommand(rootCmd *cobra.Command, createBitcask func() (*bitcask.Bitcask, error)) {
	// 存储createBitcask函数到全局变量
	createBitcaskFunc = createBitcask

	// 添加Redis特定标志
	redisCmd.Flags().StringVar(&redisAddr, "addr", ":6379", "Redis服务器监听地址")

	// 添加命令到root
	rootCmd.AddCommand(redisCmd)
}
