package main

import (
	"fmt"
	"os"
	"path/filepath"

	bitcask "github.com/aixiasang/bitcask/inner"
	"github.com/aixiasang/bitcask/inner/config"
)

func main() {
	// 创建一个临时目录用于测试
	testDir, err := os.MkdirTemp("", "bitcask-test-*")
	if err != nil {
		fmt.Printf("创建测试目录失败: %v\n", err)
		return
	}
	defer os.RemoveAll(testDir)

	fmt.Printf("使用临时目录: %s\n", testDir)

	// 创建配置
	conf := config.NewConfig()
	conf.DataDir = testDir
	conf.MaxFileSize = 100 // 设置小的文件大小，以便测试文件轮转

	// 创建Bitcask实例
	db, err := bitcask.NewBitcask(conf)
	if err != nil {
		fmt.Printf("创建Bitcask实例失败: %v\n", err)
		return
	}

	// 1. 写入一些数据
	fmt.Println("正在写入数据...")
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		if err := db.Put(key, value); err != nil {
			fmt.Printf("写入数据失败: %v\n", err)
			return
		}
	}

	// 2. 更新一些数据，确保创建多个文件
	fmt.Println("正在更新数据...")
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		updatedValue := []byte(fmt.Sprintf("updated-value-%d", i))

		if err := db.Put(key, updatedValue); err != nil {
			fmt.Printf("更新数据失败: %v\n", err)
			return
		}
	}

	// 3. 测试生成Hint文件
	fmt.Println("正在生成Hint文件...")
	if err := db.Hint(); err != nil {
		fmt.Printf("生成Hint文件失败: %v\n", err)
		return
	}

	// 验证Hint文件是否存在
	hintPath := filepath.Join(testDir, conf.HintDir, "keys.hint")
	if _, err := os.Stat(hintPath); os.IsNotExist(err) {
		fmt.Printf("Hint文件未创建: %v\n", err)
	} else {
		fmt.Printf("Hint文件已创建: %s\n", hintPath)
		// 读取hint文件内容
		data, err := os.ReadFile(hintPath)
		if err != nil {
			fmt.Printf("读取Hint文件失败: %v\n", err)
		} else {
			fmt.Printf("Hint文件内容: %s\n", string(data))
		}
	}

	// 4. 获取合并前的WAL文件数量
	walDir := filepath.Join(testDir, conf.WalDir)
	beforeFiles, err := os.ReadDir(walDir)
	if err != nil {
		fmt.Printf("读取WAL目录失败: %v\n", err)
		return
	}
	fmt.Printf("合并前WAL文件数量: %d\n", len(beforeFiles))

	// 5. 测试Merge功能
	fmt.Println("正在执行Merge操作...")
	if err := db.Merge(); err != nil {
		fmt.Printf("执行Merge操作失败: %v\n", err)
		return
	}

	// 6. 获取合并后的WAL文件数量
	afterFiles, err := os.ReadDir(walDir)
	if err != nil {
		fmt.Printf("读取WAL目录失败: %v\n", err)
		return
	}
	fmt.Printf("合并后WAL文件数量: %d\n", len(afterFiles))

	// 7. 验证数据完整性
	fmt.Println("正在验证数据完整性...")
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value, err := db.Get(key)
		if err != nil {
			fmt.Printf("读取键 %s 失败: %v\n", string(key), err)
			continue
		}

		expectedValue := "value-" + fmt.Sprintf("%d", i)
		if i < 10 {
			expectedValue = "updated-value-" + fmt.Sprintf("%d", i)
		}

		if string(value) != expectedValue {
			fmt.Printf("键 %s 的值不匹配: 期望=%s, 实际=%s\n",
				string(key), expectedValue, string(value))
		} else {
			fmt.Printf("键 %s = %s (验证通过)\n", string(key), string(value))
		}
	}

	// 8. 关闭数据库
	if err := db.Close(); err != nil {
		fmt.Printf("关闭数据库失败: %v\n", err)
		return
	}

	fmt.Println("测试完成!")
}
