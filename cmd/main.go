package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/aixiasang/bitcask"
	"github.com/aixiasang/bitcask/config"
	"github.com/aixiasang/bitcask/http"
	"github.com/aixiasang/bitcask/redis"
	"github.com/spf13/cobra"
)

var (
	// 全局标志
	dataDir     string
	maxFileSize uint32
	btreeOrder  int
	autoSync    bool
	debug       bool
)

// rootCmd 表示没有子命令时调用的基础命令
var rootCmd = &cobra.Command{
	Use:   "bitcask",
	Short: "Bitcask 是一个高性能的 key-value 存储引擎",
	Long: `Bitcask 是一个基于日志结构的 key-value 存储引擎，
提供高性能的读写操作，支持事务以及各种高级功能。

使用示例:
  bitcask put mykey myvalue --data-dir ./mydata
  bitcask get mykey --data-dir ./mydata
  bitcask delete mykey --data-dir ./mydata
  bitcask shell --data-dir ./mydata  # 进入交互式模式
  bitcask http --addr :8080 --data-dir ./mydata  # 启动HTTP服务`,
}

// 执行adds所有子命令到根命令并适当设置标志。
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// 全局标志
	rootCmd.PersistentFlags().StringVar(&dataDir, "data-dir", "./data", "数据存储目录")
	rootCmd.PersistentFlags().Uint32Var(&maxFileSize, "max-file-size", 1024, "数据文件最大大小(字节)")
	rootCmd.PersistentFlags().IntVar(&btreeOrder, "btree-order", 128, "B树阶数")
	rootCmd.PersistentFlags().BoolVar(&autoSync, "auto-sync", true, "自动同步写入")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "开启调试模式")

	// 添加所有命令
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(putCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(scanCmd)
	rootCmd.AddCommand(scanRangeCmd)
	rootCmd.AddCommand(mergeCmd)
	rootCmd.AddCommand(hintCmd)
	rootCmd.AddCommand(shellCmd)

	// 设置scanRange的limit标志
	scanRangeCmd.Flags().IntVar(&scanLimit, "limit", 100, "最大扫描记录数")

	// 注册HTTP命令
	http.RegisterCommand(rootCmd, createBitcask, &scanLimit)

	// 注册Redis命令
	redis.RegisterCommand(rootCmd, createBitcask)
}

// 创建并配置 Bitcask 实例
func createBitcask() (*bitcask.Bitcask, error) {
	conf := config.NewConfig()
	conf.DataDir = dataDir
	conf.MaxFileSize = maxFileSize
	conf.BTreeOrder = btreeOrder
	conf.AutoSync = autoSync
	conf.Debug = debug

	// 创建数据目录
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("创建数据目录失败: %v", err)
	}

	bc, err := bitcask.NewBitcask(conf)
	if err != nil {
		// 检查是否是hint文件导致的错误
		if strings.Contains(err.Error(), "从hint文件加载索引失败") {
			fmt.Println("警告: hint文件加载失败，将创建新的存储实例")

			// 尝试删除可能损坏的hint文件
			hintFile := filepath.Join(dataDir, "hint")
			if _, err := os.Stat(hintFile); err == nil {
				if err := os.Remove(hintFile); err != nil {
					fmt.Printf("警告: 无法删除hint文件: %v\n", err)
				}
			}

			// 重新尝试创建实例，但这次不会尝试加载hint文件
			return bitcask.NewBitcask(conf)
		}
		return nil, err
	}
	return bc, nil
}

// getCmd 表示 get 命令
var getCmd = &cobra.Command{
	Use:   "get [key]",
	Short: "获取指定 key 的值",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		bc, err := createBitcask()
		if err != nil {
			fmt.Printf("创建 Bitcask 实例失败: %v\n", err)
			return
		}
		defer bc.Close()

		key := []byte(args[0])
		value, ok := bc.Get(key)
		if !ok {
			fmt.Printf("获取值失败: %v\n", err)
			return
		}
		fmt.Printf("%s\n", value)
	},
}

// putCmd 表示 put 命令
var putCmd = &cobra.Command{
	Use:   "put [key] [value]",
	Short: "存储 key-value 对",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		bc, err := createBitcask()
		if err != nil {
			fmt.Printf("创建 Bitcask 实例失败: %v\n", err)
			return
		}
		defer bc.Close()

		key := []byte(args[0])
		value := []byte(args[1])
		if err := bc.Put(key, value); err != nil {
			fmt.Printf("存储值失败: %v\n", err)
			return
		}
		fmt.Println("存储成功")
	},
}

// deleteCmd 表示 delete 命令
var deleteCmd = &cobra.Command{
	Use:   "delete [key]",
	Short: "删除指定 key",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		bc, err := createBitcask()
		if err != nil {
			fmt.Printf("创建 Bitcask 实例失败: %v\n", err)
			return
		}
		defer bc.Close()

		key := []byte(args[0])
		if err := bc.Delete(key); err != nil {
			fmt.Printf("删除失败: %v\n", err)
			return
		}
		fmt.Println("删除成功")
	},
}

// scanCmd 表示 scan 命令
var scanCmd = &cobra.Command{
	Use:   "scan",
	Short: "扫描所有 key-value 对",
	Run: func(cmd *cobra.Command, args []string) {
		bc, err := createBitcask()
		if err != nil {
			fmt.Printf("创建 Bitcask 实例失败: %v\n", err)
			return
		}
		defer bc.Close()

		count := 0
		err = bc.Scan(func(key []byte, value []byte) error {
			fmt.Printf("Key: %s, Value: %s\n", key, value)
			count++
			return nil
		})
		if err != nil {
			fmt.Printf("扫描失败: %v\n", err)
			return
		}
		fmt.Printf("共扫描到 %d 条记录\n", count)
	},
}

// scanRangeCmd 表示 scanrange 命令
var (
	scanLimit int
)

var scanRangeCmd = &cobra.Command{
	Use:   "scanrange [startKey] [endKey]",
	Short: "扫描指定范围内的 key-value 对",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		bc, err := createBitcask()
		if err != nil {
			fmt.Printf("创建 Bitcask 实例失败: %v\n", err)
			return
		}
		defer bc.Close()

		startKey := []byte(args[0])
		endKey := []byte(args[1])

		results, err := bc.ScanRangeLimit(startKey, endKey, scanLimit)
		if err != nil && err != bitcask.ErrReachLimit && err != bitcask.ErrExceedEndRange {
			fmt.Printf("范围扫描失败: %v\n", err)
			return
		}

		for _, result := range results {
			fmt.Printf("Key: %s, Value: %s\n", result.Key, result.Value)
		}
		fmt.Printf("共扫描到 %d 条记录\n", len(results))
	},
}

// mergeCmd 表示 merge 命令
var mergeCmd = &cobra.Command{
	Use:   "merge",
	Short: "合并数据文件，删除过时记录",
	Run: func(cmd *cobra.Command, args []string) {
		bc, err := createBitcask()
		if err != nil {
			fmt.Printf("创建 Bitcask 实例失败: %v\n", err)
			return
		}
		defer bc.Close()

		if err := bc.Merge(); err != nil {
			fmt.Printf("合并失败: %v\n", err)
			return
		}
		fmt.Println("合并成功")
	},
}

// hintCmd 表示 hint 命令
var hintCmd = &cobra.Command{
	Use:   "hint",
	Short: "生成 hint 文件，加速下次启动",
	Run: func(cmd *cobra.Command, args []string) {
		bc, err := createBitcask()
		if err != nil {
			fmt.Printf("创建 Bitcask 实例失败: %v\n", err)
			return
		}
		defer bc.Close()

		if err := bc.Hint(); err != nil {
			fmt.Printf("生成 hint 文件失败: %v\n", err)
			return
		}
		fmt.Println("生成 hint 文件成功")
	},
}

// shellCmd 表示交互式命令行模式
var shellCmd = &cobra.Command{
	Use:   "shell",
	Short: "启动交互式命令行模式",
	Long: `启动交互式命令行模式，可以连续执行命令。
支持的命令格式与命令行模式相同，但无需输入 'bitcask' 前缀。

示例:
  > get mykey
  > put mykey newvalue
  > delete mykey
  > scan
  > quit 或 exit (退出交互式模式)
  > help (显示帮助信息)`,
	Run: func(cmd *cobra.Command, args []string) {
		// 创建一个bitcask实例并保持打开状态
		bc, err := createBitcask()
		if err != nil {
			fmt.Printf("创建 Bitcask 实例失败: %v\n", err)
			return
		}

		// 设置一个通道，用于接收用户退出信号
		exitChan := make(chan struct{})

		// 设置信号处理
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		// Windows下的信号处理需要在主goroutine中进行，
		// 避免使用os.Exit直接退出，而是通过通道通知主循环结束
		go func() {
			<-sigChan
			fmt.Println("\n接收到中断信号，正在安全关闭...")
			// 通知主循环退出
			close(exitChan)
		}()

		// 确保程序结束时关闭实例
		defer func() {
			signal.Stop(sigChan)
			bc.Close()
			fmt.Println("已安全关闭 Bitcask 实例")
		}()

		fmt.Println("Bitcask 交互式模式已启动。输入 'help' 查看可用命令，输入 'exit' 或 'quit' 退出。")
		fmt.Println("按 Ctrl+C 可安全退出程序。")
		fmt.Print("> ")

		// 启动一个单独的goroutine来读取用户输入
		inputChan := make(chan string)
		go func() {
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				input := scanner.Text()
				inputChan <- input
			}
			// 如果scanner.Scan()返回false，可能是因为标准输入被关闭
			if err := scanner.Err(); err != nil {
				fmt.Printf("\n读取输入错误: %v\n", err)
			}
			close(inputChan)
		}()

		// 主循环
		for {
			select {
			case <-exitChan:
				// 接收到退出信号
				return
			case input, ok := <-inputChan:
				// 接收到用户输入或输入通道关闭
				if !ok {
					// 输入通道已关闭
					return
				}

				input = strings.TrimSpace(input)

				if input == "" {
					fmt.Print("> ")
					continue
				}

				tokens := strings.Fields(input)
				command := tokens[0]
				cmdArgs := tokens[1:]

				switch strings.ToLower(command) {
				case "exit", "quit":
					fmt.Println("再见!")
					return
				case "help":
					printShellHelp()
				case "get":
					if len(cmdArgs) != 1 {
						fmt.Println("用法: get [key]")
						break
					}
					key := []byte(cmdArgs[0])
					value, ok := bc.Get(key)
					if !ok {
						fmt.Printf("获取值失败: %v\n", err)
					} else {
						fmt.Printf("%s\n", value)
					}
				case "put":
					if len(cmdArgs) < 2 {
						fmt.Println("用法: put [key] [value]")
						break
					}
					key := []byte(cmdArgs[0])
					// 将剩余的所有token作为value，支持带空格的值
					value := []byte(strings.Join(cmdArgs[1:], " "))
					if err := bc.Put(key, value); err != nil {
						fmt.Printf("存储值失败: %v\n", err)
					} else {
						fmt.Println("存储成功")
					}
				case "delete":
					if len(cmdArgs) != 1 {
						fmt.Println("用法: delete [key]")
						break
					}
					key := []byte(cmdArgs[0])
					if err := bc.Delete(key); err != nil {
						fmt.Printf("删除失败: %v\n", err)
					} else {
						fmt.Println("删除成功")
					}
				case "scan":
					count := 0
					err = bc.Scan(func(key []byte, value []byte) error {
						fmt.Printf("Key: %s, Value: %s\n", key, value)
						count++
						return nil
					})
					if err != nil {
						fmt.Printf("扫描失败: %v\n", err)
					} else {
						fmt.Printf("共扫描到 %d 条记录\n", count)
					}
				case "scanrange":
					if len(cmdArgs) < 2 {
						fmt.Println("用法: scanrange [startKey] [endKey] [limit]")
						break
					}
					startKey := []byte(cmdArgs[0])
					endKey := []byte(cmdArgs[1])
					limit := scanLimit // 使用全局scanLimit

					if len(cmdArgs) > 2 {
						fmt.Sscanf(cmdArgs[2], "%d", &limit)
					}

					results, err := bc.ScanRangeLimit(startKey, endKey, limit)
					if err != nil && err != bitcask.ErrReachLimit && err != bitcask.ErrExceedEndRange {
						fmt.Printf("范围扫描失败: %v\n", err)
					} else {
						for _, result := range results {
							fmt.Printf("Key: %s, Value: %s\n", result.Key, result.Value)
						}
						fmt.Printf("共扫描到 %d 条记录\n", len(results))
					}
				case "merge":
					if err := bc.Merge(); err != nil {
						fmt.Printf("合并失败: %v\n", err)
					} else {
						fmt.Println("合并成功")
					}
				case "hint":
					if err := bc.Hint(); err != nil {
						fmt.Printf("生成 hint 文件失败: %v\n", err)
					} else {
						fmt.Println("生成 hint 文件成功")
					}
				default:
					fmt.Printf("未知命令: %s\n", command)
					fmt.Println("输入 'help' 查看可用命令")
				}
				fmt.Print("> ")
			}
		}
	},
}

// 打印交互式模式的帮助信息
func printShellHelp() {
	fmt.Println("可用命令:")
	fmt.Println("  get [key]                 - 获取指定 key 的值")
	fmt.Println("  put [key] [value]         - 存储 key-value 对")
	fmt.Println("  delete [key]              - 删除指定 key")
	fmt.Println("  scan                      - 扫描所有 key-value 对")
	fmt.Println("  scanrange [start] [end]   - 扫描指定范围内的 key-value 对")
	fmt.Println("  merge                     - 合并数据文件，删除过时记录")
	fmt.Println("  hint                      - 生成 hint 文件，加速下次启动")
	fmt.Println("  help                      - 显示此帮助信息")
	fmt.Println("  exit, quit                - 退出交互式模式")
	fmt.Println("")
	fmt.Println("快捷键:")
	fmt.Println("  Ctrl+C                    - 安全关闭并退出程序")
}

func main() {
	Execute()
}
