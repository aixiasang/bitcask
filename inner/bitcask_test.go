package bitcask

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aixiasang/bitcask/inner/config"
	"github.com/aixiasang/bitcask/inner/utils"
)

// 测试前的准备工作：创建临时目录
func setupTestDir(t *testing.T) (string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "bitcask-test-*")
	if err != nil {
		t.Fatalf("无法创建测试目录: %v", err)
	}

	// 返回清理函数
	cleanup := func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Logf("清理测试目录失败: %v", err)
		}
	}

	return dir, cleanup
}

// 获取测试配置
func getTestConfig(dataDir string) *config.Config {
	conf := config.NewConfig()
	conf.DataDir = dataDir
	conf.MaxFileSize = 4096 // 使用较小的文件大小以便测试文件轮转
	return conf
}

// 原有的 Put 测试
func TestBitcask_Put(t *testing.T) {
	conf := config.NewConfig()
	// 使用临时目录
	testDir, cleanup := setupTestDir(t)
	defer cleanup()
	conf.DataDir = testDir

	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatal(err)
	}

	m := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := utils.GetKey(i)
		value := utils.GetValue(10)
		t.Logf("写入: %s = %s", string(key), string(value))
		m[string(key)] = string(value)
		if err := db.Put(key, value); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}

	for i, v := range m {
		value, err := db.Get([]byte(i))
		if err != nil {
			t.Fatal(err)
		}
		if string(value) != v {
			t.Fatalf("value mismatch: %s != %s", string(value), v)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("关闭数据库失败: %v", err)
	}
	t.Log("关闭ok！")

	// 重新打开数据库
	db, err = NewBitcask(conf)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i, v := range m {
		value, err := db.Get([]byte(i))
		if err != nil {
			t.Fatal(err)
		}
		if string(value) != v {
			t.Fatalf("重启后value不匹配: %s != %s", string(value), v)
		}
	}
}

// 原有的 Get 测试改进版
func TestBitcask_Get(t *testing.T) {
	// 使用临时目录
	testDir, cleanup := setupTestDir(t)
	defer cleanup()

	conf := config.NewConfig()
	conf.DataDir = testDir
	conf.MaxFileSize = 4096 // 使用更大的文件大小，减少轮转次数

	// 首次写入并读取
	func() {
		db, err := NewBitcask(conf)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		t.Log("写入100条数据(第一次)...")
		for i := 0; i < 100; i++ {
			key := utils.GetKey(i)
			value := []byte("first-value-" + strconv.Itoa(i))
			if err := db.Put(key, value); err != nil {
				t.Fatalf("第一轮写入失败: %v", err)
			}
		}

		t.Log("读取并验证数据(第一次)...")
		for i := 0; i < 100; i++ {
			key := utils.GetKey(i)
			expectedValue := []byte("first-value-" + strconv.Itoa(i))

			value, err := db.Get(key)
			if err != nil {
				t.Fatalf("读取key[%s]失败: %v", key, err)
			}

			if !bytes.Equal(value, expectedValue) {
				t.Fatalf("值不匹配: key=%s, 期望=%s, 得到=%s",
					string(key), string(expectedValue), string(value))
			}
		}
	}()

	// 重新打开，更新数据
	func() {
		db, err := NewBitcask(conf)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		t.Log("重新打开并写入100条数据(更新)...")
		for i := 0; i < 100; i++ {
			key := utils.GetKey(i)
			value := []byte("second-value-" + strconv.Itoa(i))
			if err := db.Put(key, value); err != nil {
				t.Fatalf("第二轮写入失败: %v", err)
			}
		}

		t.Log("读取并验证更新后的数据...")
		for i := 0; i < 100; i++ {
			key := utils.GetKey(i)
			expectedValue := []byte("second-value-" + strconv.Itoa(i))

			value, err := db.Get(key)
			if err != nil {
				t.Fatalf("更新后读取key[%s]失败: %v", key, err)
			}

			if !bytes.Equal(value, expectedValue) {
				t.Fatalf("更新后值不匹配: key=%s, 期望=%s, 得到=%s",
					string(key), string(expectedValue), string(value))
			}
		}
	}()

	// 第三次打开，验证持久化
	func() {
		db, err := NewBitcask(conf)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		t.Log("第三次打开，验证之前的更新...")
		for i := 0; i < 100; i++ {
			key := utils.GetKey(i)
			expectedValue := []byte("second-value-" + strconv.Itoa(i))

			value, err := db.Get(key)
			if err != nil {
				t.Fatalf("第三次读取key[%s]失败: %v", key, err)
			}

			if !bytes.Equal(value, expectedValue) {
				t.Fatalf("第三次值不匹配: key=%s, 期望=%s, 得到=%s",
					string(key), string(expectedValue), string(value))
			}
		}
	}()
}

// 新增测试：删除操作测试
func TestBitcask_Delete_Case(t *testing.T) {
	testDir, cleanup := setupTestDir(t)
	defer cleanup()

	bc, err := NewBitcask(getTestConfig(testDir))
	if err != nil {
		t.Fatalf("创建 Bitcask 实例失败: %v", err)
	}
	defer bc.Close()

	// 写入数据
	key := []byte("delete-test-key")
	value := []byte("delete-test-value")
	if err := bc.Put(key, value); err != nil {
		t.Fatalf("写入数据失败: %v", err)
	}

	// 确认数据已写入
	readValue, err := bc.Get(key)
	if err != nil {
		t.Fatalf("读取数据失败: %v", err)
	}
	if !bytes.Equal(readValue, value) {
		t.Fatalf("读取的值与写入的值不匹配")
	}

	// 删除数据
	if err := bc.Delete(key); err != nil {
		t.Fatalf("删除数据失败: %v", err)
	}

	// 确认数据已删除
	_, err = bc.Get(key)
	if err == nil {
		t.Fatalf("期望删除后读取返回错误，但未返回错误")
	}
}

// 新增测试：文件轮转测试
func TestFileRotation(t *testing.T) {
	testDir, cleanup := setupTestDir(t)
	defer cleanup()

	conf := getTestConfig(testDir)
	conf.MaxFileSize = 100 // 设置非常小的文件大小以便快速触发轮转

	bc, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建 Bitcask 实例失败: %v", err)
	}
	defer bc.Close()

	// 写入足够多的数据以触发多次文件轮转
	for i := 0; i < 50; i++ {
		key := []byte("rotation-key-" + strconv.Itoa(i))
		value := []byte("rotation-value-" + strconv.Itoa(i) + "-" + strconv.Itoa(i))
		if err := bc.Put(key, value); err != nil {
			t.Fatalf("写入数据失败: %v", err)
		}
	}

	// 验证可以读取所有数据
	for i := 0; i < 50; i++ {
		key := []byte("rotation-key-" + strconv.Itoa(i))
		expectedValue := []byte("rotation-value-" + strconv.Itoa(i) + "-" + strconv.Itoa(i))

		value, err := bc.Get(key)
		if err != nil {
			t.Errorf("文件轮转后读取键 '%s' 失败: %v", key, err)
			continue
		}

		if !bytes.Equal(value, expectedValue) {
			t.Errorf("文件轮转后键 '%s' 的值不匹配", key)
		}
	}

	// 检查是否创建了多个 WAL 文件
	walDir := filepath.Join(testDir, conf.WalDir)
	files, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("读取 WAL 目录失败: %v", err)
	}

	// 至少应该有一个 WAL 文件
	if len(files) < 2 {
		t.Errorf("期望创建多个 WAL 文件，但只找到 %d 个", len(files))
	}
}

// 并发测试
func TestConcurrentAccess(t *testing.T) {
	testDir, cleanup := setupTestDir(t)
	defer cleanup()

	bc, err := NewBitcask(getTestConfig(testDir))
	if err != nil {
		t.Fatalf("创建 Bitcask 实例失败: %v", err)
	}
	defer bc.Close()

	const numGoroutines = 10
	const numOperationsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			// 每个 goroutine 写入自己的一组键
			baseKey := "concurrent-key-" + strconv.Itoa(goroutineID) + "-"

			// 写入操作
			for i := 0; i < numOperationsPerGoroutine; i++ {
				key := []byte(baseKey + strconv.Itoa(i))
				value := []byte("value-" + strconv.Itoa(goroutineID) + "-" + strconv.Itoa(i))

				if err := bc.Put(key, value); err != nil {
					t.Errorf("Goroutine %d 写入键 '%s' 失败: %v", goroutineID, key, err)
				}

				// 随机延迟，增加并发冲突的可能性
				if i%10 == 0 {
					time.Sleep(time.Millisecond)
				}
			}

			// 读取操作
			for i := 0; i < numOperationsPerGoroutine; i++ {
				key := []byte(baseKey + strconv.Itoa(i))
				expectedValue := []byte("value-" + strconv.Itoa(goroutineID) + "-" + strconv.Itoa(i))

				value, err := bc.Get(key)
				if err != nil {
					t.Errorf("Goroutine %d 读取键 '%s' 失败: %v", goroutineID, key, err)
					continue
				}

				if !bytes.Equal(value, expectedValue) {
					t.Errorf("Goroutine %d 键 '%s' 的值不匹配", goroutineID, key)
				}
			}
		}(g)
	}

	wg.Wait()
}

// 简化的批处理操作测试
func TestBitcaskBatchOperations(t *testing.T) {
	testDir, cleanup := setupTestDir(t)
	defer cleanup()

	conf := config.NewConfig()
	conf.DataDir = testDir
	conf.MaxFileSize = 1024 // 较小的文件大小以便快速测试

	// 定义两个测试键
	key1 := []byte("test-key-1")
	key2 := []byte("test-key-2")

	// 第一阶段：创建Bitcask并写入初始数据
	{
		db, err := NewBitcask(conf)
		if err != nil {
			t.Fatalf("创建Bitcask失败: %v", err)
		}

		// 写入初始数据
		if err := db.Put(key1, []byte("value-1-initial")); err != nil {
			t.Fatalf("写入key1失败: %v", err)
		}
		if err := db.Put(key2, []byte("value-2-initial")); err != nil {
			t.Fatalf("写入key2失败: %v", err)
		}

		// 验证数据
		v1, err := db.Get(key1)
		if err != nil || string(v1) != "value-1-initial" {
			t.Fatalf("验证key1失败: err=%v, value=%v", err, string(v1))
		}

		v2, err := db.Get(key2)
		if err != nil || string(v2) != "value-2-initial" {
			t.Fatalf("验证key2失败: err=%v, value=%v", err, string(v2))
		}

		// 关闭
		if err := db.Close(); err != nil {
			t.Fatalf("关闭Bitcask失败: %v", err)
		}
	}

	// 第二阶段：重新打开并更新数据
	{
		db, err := NewBitcask(conf)
		if err != nil {
			t.Fatalf("重新打开Bitcask失败: %v", err)
		}

		// 先验证初始数据还在
		v1, err := db.Get(key1)
		if err != nil || string(v1) != "value-1-initial" {
			t.Fatalf("重新打开后验证key1失败: err=%v, value=%v", err, string(v1))
		}

		// 更新数据
		if err := db.Put(key1, []byte("value-1-updated")); err != nil {
			t.Fatalf("更新key1失败: %v", err)
		}

		// 验证更新
		v1Updated, err := db.Get(key1)
		if err != nil || string(v1Updated) != "value-1-updated" {
			t.Fatalf("验证更新后的key1失败: err=%v, value=%v", err, string(v1Updated))
		}

		// 关闭
		if err := db.Close(); err != nil {
			t.Fatalf("关闭Bitcask失败: %v", err)
		}
	}

	// 第三阶段：再次打开并验证持久化
	{
		db, err := NewBitcask(conf)
		if err != nil {
			t.Fatalf("第三次打开Bitcask失败: %v", err)
		}
		defer db.Close()

		// 验证更新的值被持久化
		v1, err := db.Get(key1)
		if err != nil {
			t.Fatalf("第三次验证key1出错: %v", err)
		}
		if string(v1) != "value-1-updated" {
			t.Fatalf("持久化验证失败: 期望=%s, 实际=%s", "value-1-updated", string(v1))
		}

		// 验证未更新的值仍然正确
		v2, err := db.Get(key2)
		if err != nil {
			t.Fatalf("第三次验证key2出错: %v", err)
		}
		if string(v2) != "value-2-initial" {
			t.Fatalf("未更新的值验证失败: 期望=%s, 实际=%s", "value-2-initial", string(v2))
		}
	}
}

// 新增测试：删除操作测试
func TestBitcask_Delete(t *testing.T) {
	testDir, cleanup := setupTestDir(t)
	defer cleanup()

	bc, err := NewBitcask(getTestConfig(testDir))
	if err != nil {
		t.Fatalf("创建 Bitcask 实例失败: %v", err)
	}
	defer bc.Close()

	// 写入数据
	key := []byte("delete-test-key")
	value := []byte("delete-test-value")
	if err := bc.Put(key, value); err != nil {
		t.Fatalf("写入数据失败: %v", err)
	}

	// 确认数据已写入
	readValue, err := bc.Get(key)
	if err != nil {
		t.Fatalf("读取数据失败: %v", err)
	}
	if !bytes.Equal(readValue, value) {
		t.Fatalf("读取的值与写入的值不匹配")
	}

	// 删除数据
	if err := bc.Delete(key); err != nil {
		t.Fatalf("删除数据失败: %v", err)
	}

	// 确认数据已删除
	_, err = bc.Get(key)
	if err == nil {
		t.Fatalf("期望删除后读取返回错误，但未返回错误")
	}
}

// 新增测试：文件轮转测试
func TestFileDelete(t *testing.T) {
	testDir, cleanup := setupTestDir(t)
	defer cleanup()

	conf := getTestConfig(testDir)
	conf.MaxFileSize = 128 // 设置非常小的文件大小以便快速触发轮转

	bc, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建 Bitcask 实例失败: %v", err)
	}
	defer bc.Close()

	m := make(map[string]string)
	// 写入足够多的数据以触发多次文件轮转
	for i := 0; i < 100; i++ {
		key, value := utils.GetKey(i), utils.GetValue(19)
		if err := bc.Put(key, value); err != nil {
			t.Fatalf("写入数据失败: %v", err)
		}
		m[string(key)] = string(value)
	}

	// 验证可以读取所有数据
	for i := 0; i < 200; i++ {
		key := utils.GetKey(i)
		if i < 100 {
			value, err := bc.Get(key)
			if err != nil {
				t.Fatalf("读取数据失败: %v", err)
			}
			if string(value) != m[string(key)] {
				t.Fatalf("读取的值与写入的值不匹配")
			}
		}
		newValue := utils.GetValue(19)
		m[string(key)] = string(newValue)
		if err := bc.Put(key, newValue); err != nil {
			t.Fatalf("更新数据失败: %v", err)
		}
	}
	for i := 0; i < 200; i++ {
		key := utils.GetKey(i)
		value, err := bc.Get(key)
		if err != nil {
			t.Fatalf("读取数据失败: %v", err)
		}
		if string(value) != m[string(key)] {
			t.Fatalf("读取的值与写入的值不匹配")
		}
	}
	if err := bc.Merge(); err != nil {
		t.Fatalf("合并失败: %v", err)
	}
}

// 测试Hint文件功能
func TestBitcask_Hint(t *testing.T) {
	// 创建临时测试目录
	testDir, cleanup := setupTestDir(t)
	defer cleanup()

	conf := config.NewConfig()
	conf.DataDir = testDir
	conf.MaxFileSize = 4096 // 使用较小的文件大小以便测试文件轮转

	// 第一阶段：写入数据并生成hint文件
	{
		db, err := NewBitcask(conf)
		if err != nil {
			t.Fatalf("创建Bitcask失败: %v", err)
		}

		// 写入测试数据
		t.Log("写入测试数据...")
		testData := make(map[string]string)
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("test-key-%d", i)
			value := fmt.Sprintf("test-value-%d", i)
			if err := db.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("写入数据失败: %v", err)
			}
			testData[key] = value
		}

		// 生成hint文件
		t.Log("生成hint文件...")
		if err := db.Hint(); err != nil {
			t.Fatalf("生成hint文件失败: %v", err)
		}

		// 验证数据是否正确写入
		for key, expectedValue := range testData {
			value, err := db.Get([]byte(key))
			if err != nil {
				t.Fatalf("读取数据失败: %v", err)
			}
			if string(value) != expectedValue {
				t.Fatalf("数据不匹配: key=%s, 期望=%s, 实际=%s", key, expectedValue, string(value))
			}
		}

		if err := db.Close(); err != nil {
			t.Fatalf("关闭数据库失败: %v", err)
		}
	}

	// 第二阶段：从hint文件加载索引，验证数据
	{
		// 记录启动前的时间
		startTime := time.Now()

		// 重新打开数据库，应该从hint文件加载索引
		t.Log("从hint文件重新加载数据库...")
		db, err := NewBitcask(conf)
		if err != nil {
			t.Fatalf("重新打开数据库失败: %v", err)
		}
		defer db.Close()

		// 计算加载时间
		loadTime := time.Since(startTime)
		t.Logf("数据库加载时间: %v", loadTime)

		// 验证数据是否正确加载
		t.Log("验证数据是否从hint文件正确加载索引后可读取...")
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("test-key-%d", i)
			expectedValue := fmt.Sprintf("test-value-%d", i)

			value, err := db.Get([]byte(key))
			if err != nil {
				t.Fatalf("从hint加载后读取数据失败: key=%s, err=%v", key, err)
			}
			if string(value) != expectedValue {
				t.Fatalf("从hint加载后数据不匹配: key=%s, 期望=%s, 实际=%s",
					key, expectedValue, string(value))
			}
		}

		// 第三阶段：尝试不使用hint文件启动，比较启动时间
		if err := db.Close(); err != nil {
			t.Fatalf("关闭数据库失败: %v", err)
		}

		// 删除hint文件
		hintPath := filepath.Join(testDir, conf.HintDir, "keys.hint")
		if err := os.Remove(hintPath); err != nil {
			t.Fatalf("删除hint文件失败: %v", err)
		}

		// 不使用hint文件启动的时间
		startTimeNoHint := time.Now()

		dbNoHint, err := NewBitcask(conf)
		if err != nil {
			t.Fatalf("不使用hint文件重新打开数据库失败: %v", err)
		}
		defer dbNoHint.Close()

		loadTimeNoHint := time.Since(startTimeNoHint)
		t.Logf("不使用hint文件的数据库加载时间: %v", loadTimeNoHint)

		// 验证不使用hint文件也能正确加载数据
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("test-key-%d", i)
			expectedValue := fmt.Sprintf("test-value-%d", i)

			value, err := dbNoHint.Get([]byte(key))
			if err != nil {
				t.Fatalf("不使用hint文件加载后读取数据失败: key=%s, err=%v", key, err)
			}
			if string(value) != expectedValue {
				t.Fatalf("不使用hint文件加载后数据不匹配: key=%s, 期望=%s, 实际=%s",
					key, expectedValue, string(value))
			}
		}

		// 记录一个预期：使用hint文件应该比不使用hint文件启动更快
		// 但在小数据量的测试中可能差异不明显，所以我们只记录不做硬性验证
		t.Logf("加载时间比较: 使用hint=%v, 不使用hint=%v", loadTime, loadTimeNoHint)
	}
}

// 测试Merge功能
func TestBitcask_Merge(t *testing.T) {
	testDir, cleanup := setupTestDir(t)
	defer cleanup()

	conf := getTestConfig(testDir)
	conf.MaxFileSize = 100 // 设置非常小的文件大小，确保创建多个WAL文件

	// 创建一个新的Bitcask实例
	bc, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建Bitcask实例失败: %v", err)
	}

	// 写入足够多的数据触发文件轮转
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("merge-key-%d", i))
		value := []byte(fmt.Sprintf("merge-value-%d", i))

		// 对每个键写入两次，确保有冗余数据
		if err := bc.Put(key, value); err != nil {
			t.Fatalf("第一次写入失败: %v", err)
		}

		// 写入更新的值
		updatedValue := []byte(fmt.Sprintf("merge-updated-value-%d", i))
		if err := bc.Put(key, updatedValue); err != nil {
			t.Fatalf("第二次写入失败: %v", err)
		}
	}

	// 获取合并前的WAL文件数量
	walPath := filepath.Join(testDir, conf.WalDir)
	beforeFiles, err := os.ReadDir(walPath)
	if err != nil {
		t.Fatalf("读取WAL目录失败: %v", err)
	}
	beforeCount := len(beforeFiles)
	t.Logf("合并前WAL文件数量: %d", beforeCount)

	// 执行合并操作
	if err := bc.Merge(); err != nil {
		t.Fatalf("执行合并操作失败: %v", err)
	}

	// 验证数据完整性
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("merge-key-%d", i))
		expectedValue := []byte(fmt.Sprintf("merge-updated-value-%d", i))

		value, err := bc.Get(key)
		if err != nil {
			t.Fatalf("合并后读取键失败: %v", err)
		}

		if !bytes.Equal(value, expectedValue) {
			t.Fatalf("合并后数据不一致: 期望=%s, 实际=%s", string(expectedValue), string(value))
		}
	}

	// 获取合并后的WAL文件数量
	afterFiles, err := os.ReadDir(walPath)
	if err != nil {
		t.Fatalf("读取WAL目录失败: %v", err)
	}
	afterCount := len(afterFiles)
	t.Logf("合并后WAL文件数量: %d", afterCount)

	// 通常合并后文件数应该减少
	if afterCount >= beforeCount && beforeCount > 1 {
		t.Logf("警告: 合并后文件数未减少 (前=%d, 后=%d)", beforeCount, afterCount)
	}

	// 关闭Bitcask
	if err := bc.Close(); err != nil {
		t.Fatalf("关闭Bitcask失败: %v", err)
	}
}
