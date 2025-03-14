package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aixiasang/bitcask/config"
	"github.com/gofrs/flock"
)

// TestPersistenceAfterReopen 测试关闭并重新打开数据库后数据的持久性
func TestPersistenceAfterReopen(t *testing.T) {
	// 创建临时测试目录
	testDir := t.TempDir()
	t.Logf("创建测试目录: %s", testDir)

	// 确保目录存在
	walDir := filepath.Join(testDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("创建WAL目录失败: %v", err)
	}
	t.Logf("创建WAL目录: %s", walDir)

	// 创建配置
	conf := config.DefaultConfig(testDir)
	t.Log("创建默认配置")

	// 第一阶段：创建数据库并写入数据
	t.Log("第一阶段：创建数据库并写入数据")
	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建Bitcask实例失败: %v", err)
	}

	// 写入数据
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	for k, v := range testData {
		t.Logf("写入数据: key=%s, value=%s", k, v)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("写入数据失败: %v", err)
		}
	}

	// 删除一些数据
	keysToDelete := []string{"key3", "key5"}
	for _, k := range keysToDelete {
		t.Logf("删除数据: key=%s", k)
		if err := db.Delete([]byte(k)); err != nil {
			t.Fatalf("删除数据失败: %v", err)
		}
		delete(testData, k)
	}

	// 验证当前状态
	t.Log("验证初始数据写入")
	for k, v := range testData {
		value, err := db.Get([]byte(k))
		if err != nil {
			t.Errorf("获取数据失败: %v", err)
			continue
		}
		if string(value) != v {
			t.Errorf("数据不匹配: key=%s, 期望=%s, 实际=%s", k, v, string(value))
		}
	}

	// 验证已删除的键
	for _, k := range keysToDelete {
		value, err := db.Get([]byte(k))
		if err != nil {
			t.Errorf("获取已删除的数据应该不返回错误: %v", err)
			continue
		}
		if value != nil {
			t.Errorf("已删除的数据应该返回nil: key=%s, 实际=%s", k, string(value))
		}
	}

	// 正常关闭数据库
	t.Log("关闭数据库")
	if err := db.Close(); err != nil {
		t.Fatalf("关闭数据库失败: %v", err)
	}

	// 第二阶段：重新打开数据库并验证数据
	t.Log("第二阶段：重新打开数据库并验证数据")
	db2, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("重新打开数据库失败: %v", err)
	}
	defer db2.Close()

	// 验证数据一致性
	t.Log("验证数据持久化")
	for k, v := range testData {
		value, err := db2.Get([]byte(k))
		if err != nil {
			t.Errorf("重新打开后获取数据失败: %v", err)
			continue
		}
		if string(value) != v {
			t.Errorf("重新打开后数据不匹配: key=%s, 期望=%s, 实际=%s", k, v, string(value))
		} else {
			t.Logf("验证成功: key=%s, value=%s", k, v)
		}
	}

	// 验证已删除的键
	for _, k := range keysToDelete {
		value, err := db2.Get([]byte(k))
		if err != nil {
			t.Errorf("重新打开后获取已删除的数据应该不返回错误: %v", err)
			continue
		}
		if value != nil {
			t.Errorf("重新打开后已删除的数据应该返回nil: key=%s, 实际=%s", k, string(value))
		} else {
			t.Logf("验证删除成功: key=%s", k)
		}
	}

	t.Log("持久化测试完成")
}

// TestBatchPersistenceAfterReopen 测试批处理操作的持久性
func TestBatchPersistenceAfterReopen(t *testing.T) {
	// 创建临时测试目录
	testDir := t.TempDir()
	t.Logf("创建测试目录: %s", testDir)

	// 确保目录存在
	walDir := filepath.Join(testDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("创建WAL目录失败: %v", err)
	}
	t.Logf("创建WAL目录: %s", walDir)

	// 创建配置
	conf := config.DefaultConfig(testDir)
	t.Log("创建默认配置")

	// 第一阶段：创建数据库并执行批处理操作
	t.Log("第一阶段：创建数据库并执行批处理操作")
	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建Bitcask实例失败: %v", err)
	}

	// 创建批处理
	batch := db.NewBatch()
	if batch == nil {
		t.Fatal("创建批处理失败")
	}

	// 添加操作到批处理
	t.Log("添加操作到批处理")
	testData := map[string]string{
		"batch-key1": "batch-value1",
		"batch-key2": "batch-value2",
		"batch-key3": "batch-value3",
		"batch-key4": "batch-value4",
		"batch-key5": "batch-value5",
	}

	for k, v := range testData {
		t.Logf("添加操作: key=%s, value=%s", k, v)
		if err := batch.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("添加操作到批处理失败: %v", err)
		}
	}

	// 删除一些键
	keysToDelete := []string{"batch-key3", "batch-key5"}
	for _, k := range keysToDelete {
		t.Logf("添加删除操作: key=%s", k)
		if err := batch.Delete([]byte(k)); err != nil {
			t.Fatalf("添加删除操作失败: %v", err)
		}
		delete(testData, k)
	}

	// 提交批处理
	t.Log("提交批处理")
	if err := batch.Commit(); err != nil {
		t.Fatalf("提交批处理失败: %v", err)
	}

	// 验证当前状态
	t.Log("验证批处理操作")
	for k, v := range testData {
		value, err := db.Get([]byte(k))
		if err != nil {
			t.Errorf("获取数据失败: %v", err)
			continue
		}
		if string(value) != v {
			t.Errorf("数据不匹配: key=%s, 期望=%s, 实际=%s", k, v, string(value))
		}
	}

	// 验证已删除的键
	for _, k := range keysToDelete {
		value, err := db.Get([]byte(k))
		if err != nil {
			t.Errorf("获取已删除的数据应该不返回错误: %v", err)
			continue
		}
		if value != nil {
			t.Errorf("已删除的数据应该返回nil: key=%s, 实际=%s", k, string(value))
		}
	}

	// 关闭数据库
	t.Log("关闭数据库")
	if err := db.Close(); err != nil {
		t.Fatalf("关闭数据库失败: %v", err)
	}

	// 第二阶段：重新打开数据库并验证批处理操作
	t.Log("第二阶段：重新打开数据库并验证批处理操作")
	db2, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("重新打开数据库失败: %v", err)
	}
	defer db2.Close()

	// 验证数据一致性
	t.Log("验证批处理持久化")
	for k, v := range testData {
		value, err := db2.Get([]byte(k))
		if err != nil {
			t.Errorf("重新打开后获取数据失败: %v", err)
			continue
		}
		if string(value) != v {
			t.Errorf("重新打开后数据不匹配: key=%s, 期望=%s, 实际=%s", k, v, string(value))
		} else {
			t.Logf("验证成功: key=%s, value=%s", k, v)
		}
	}

	// 验证已删除的键
	for _, k := range keysToDelete {
		value, err := db2.Get([]byte(k))
		if err != nil {
			t.Errorf("重新打开后获取已删除的数据应该不返回错误: %v", err)
			continue
		}
		if value != nil {
			t.Errorf("重新打开后已删除的数据应该返回nil: key=%s, 实际=%s", k, string(value))
		} else {
			t.Logf("验证删除成功: key=%s", k)
		}
	}

	t.Log("批处理持久化测试完成")
}

// TestConcurrentOperations 测试并发操作
func TestConcurrentOperations(t *testing.T) {
	// 创建临时测试目录
	testDir := t.TempDir()
	t.Logf("创建测试目录: %s", testDir)

	// 确保目录存在
	walDir := filepath.Join(testDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("创建WAL目录失败: %v", err)
	}
	t.Logf("创建WAL目录: %s", walDir)

	// 创建配置
	conf := config.DefaultConfig(testDir)
	t.Log("创建默认配置")

	// 创建数据库
	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建Bitcask实例失败: %v", err)
	}
	defer db.Close()

	// 并发写入
	t.Log("测试并发写入")
	concurrency := 10
	opsPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()

			// 每个goroutine写入数据
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("key-go%d-op%d", id, j)
				value := fmt.Sprintf("value-go%d-op%d", id, j)

				err := db.Put([]byte(key), []byte(value))
				if err != nil {
					t.Errorf("并发写入失败: goroutine=%d, op=%d, err=%v", id, j, err)
				}
			}
		}(i)
	}

	wg.Wait()
	t.Log("并发写入完成")

	// 验证写入的数据
	t.Log("验证并发写入的数据")
	for i := 0; i < concurrency; i++ {
		for j := 0; j < opsPerGoroutine; j++ {
			key := fmt.Sprintf("key-go%d-op%d", i, j)
			expectedValue := fmt.Sprintf("value-go%d-op%d", i, j)

			value, err := db.Get([]byte(key))
			if err != nil {
				t.Errorf("获取并发写入的数据失败: key=%s, err=%v", key, err)
				continue
			}

			if string(value) != expectedValue {
				t.Errorf("并发写入的数据不匹配: key=%s, 期望=%s, 实际=%s", key, expectedValue, string(value))
			}
		}
	}

	t.Log("并发操作测试完成")
}

// TestFileLock 测试文件锁功能
func TestFileLock(t *testing.T) {
	// 创建临时测试目录
	testDir := t.TempDir()
	t.Logf("创建测试目录: %s", testDir)

	// 锁文件路径
	lockFile := filepath.Join(testDir, "bitcask.lock")

	// 创建第一个锁
	lock1 := flock.New(lockFile)
	locked, err := lock1.TryLock()
	if err != nil {
		t.Fatalf("获取文件锁失败: %v", err)
	}
	if !locked {
		t.Fatal("应该能获取到锁")
	}
	t.Log("获取第一个文件锁成功")

	// 尝试获取第二个锁（应该失败）
	lock2 := flock.New(lockFile)
	locked, err = lock2.TryLock() // 简单使用TryLock替代TryLockWithTimeout
	if err != nil {
		t.Fatalf("第二个锁应该返回获取锁失败，而不是错误: %v", err)
	}
	if locked {
		t.Fatal("第二个锁应该获取失败")
	}
	t.Log("第二个文件锁获取失败（预期行为）")

	// 释放第一个锁
	err = lock1.Unlock()
	if err != nil {
		t.Fatalf("释放文件锁失败: %v", err)
	}
	t.Log("释放第一个文件锁成功")

	// 稍微等待一下，确保锁被完全释放
	time.Sleep(100 * time.Millisecond)

	// 再次尝试获取第二个锁（应该成功）
	locked, err = lock2.TryLock()
	if err != nil {
		t.Fatalf("释放第一个锁后获取第二个锁失败: %v", err)
	}
	if !locked {
		t.Fatal("释放第一个锁后应该能获取到第二个锁")
	}
	t.Log("释放第一个锁后获取第二个锁成功")

	// 释放第二个锁
	err = lock2.Unlock()
	if err != nil {
		t.Fatalf("释放第二个文件锁失败: %v", err)
	}
	t.Log("文件锁测试完成")
}

// TestWALRecoveryAfterCrash 测试WAL在崩溃后的恢复
func TestWALRecoveryAfterCrash(t *testing.T) {
	// 创建临时测试目录
	testDir := t.TempDir()
	t.Logf("创建测试目录: %s", testDir)

	// 确保目录存在
	walDir := filepath.Join(testDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("创建WAL目录失败: %v", err)
	}
	t.Logf("创建WAL目录: %s", walDir)

	// 创建配置
	conf := config.DefaultConfig(testDir)
	t.Log("创建默认配置")

	// 定义锁文件路径，用于后续清理
	lockFile := filepath.Join(testDir, "bitcask.lock")

	// 第一阶段：创建数据库并写入数据
	t.Log("第一阶段：创建数据库并写入数据")
	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建Bitcask实例失败: %v", err)
	}

	// 写入数据
	testData := map[string]string{
		"crash-key1": "crash-value1",
		"crash-key2": "crash-value2",
		"crash-key3": "crash-value3",
	}

	for k, v := range testData {
		t.Logf("写入数据: key=%s, value=%s", k, v)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("写入数据失败: %v", err)
		}
	}

	// 同步数据到磁盘（确保数据已写入）
	if err := db.activeWal.Sync(); err != nil {
		t.Fatalf("同步数据失败: %v", err)
	}

	// 获取活动WAL文件路径
	walFilePath := filepath.Join(walDir, fmt.Sprintf("%d.%s", db.activeWal.FileId, conf.WalFileExt))
	t.Logf("活动WAL文件: %s", walFilePath)

	// 不正常关闭数据库（模拟崩溃）
	t.Log("模拟崩溃（不调用 Close()）")

	// 手动释放文件锁（在真实崩溃中这一步不会执行，但在测试中需要为了能继续测试）
	if db.fileLock != nil {
		if err := db.fileLock.Unlock(); err != nil {
			t.Logf("释放锁失败（这在真实崩溃中是正常的）: %v", err)
		}
	}

	// 确保锁文件不再被占用
	time.Sleep(100 * time.Millisecond)

	// 强制删除锁文件（在Windows上可能需要）
	if _, err := os.Stat(lockFile); err == nil {
		if err := os.Remove(lockFile); err != nil {
			t.Logf("删除锁文件失败（尝试继续测试）: %v", err)
		}
	}

	// 第二阶段：重新打开数据库，测试WAL恢复
	t.Log("第二阶段：重新打开数据库，测试WAL恢复")
	db2, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("重新打开数据库失败: %v", err)
	}
	defer db2.Close()

	// 验证数据恢复
	t.Log("验证崩溃后数据恢复")
	for k, v := range testData {
		value, err := db2.Get([]byte(k))
		if err != nil {
			t.Errorf("崩溃后获取数据失败: %v", err)
			continue
		}
		if string(value) != v {
			t.Errorf("崩溃后数据不匹配: key=%s, 期望=%s, 实际=%s", k, v, string(value))
		} else {
			t.Logf("验证成功: key=%s, value=%s", k, v)
		}
	}

	t.Log("WAL崩溃恢复测试完成")
}

// TestCorruptedWAL 测试损坏的WAL文件处理
func TestCorruptedWAL(t *testing.T) {
	// 创建临时测试目录
	testDir := t.TempDir()
	t.Logf("创建测试目录: %s", testDir)

	// 确保目录存在
	walDir := filepath.Join(testDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("创建WAL目录失败: %v", err)
	}
	t.Logf("创建WAL目录: %s", walDir)

	// 创建配置
	conf := config.DefaultConfig(testDir)
	t.Log("创建默认配置")

	// 第一阶段：创建数据库并写入数据
	t.Log("第一阶段：创建数据库并写入数据")
	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建Bitcask实例失败: %v", err)
	}

	// 写入数据
	testData := map[string]string{
		"corrupt-key1": "corrupt-value1",
		"corrupt-key2": "corrupt-value2",
		"corrupt-key3": "corrupt-value3",
	}

	for k, v := range testData {
		t.Logf("写入数据: key=%s, value=%s", k, v)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("写入数据失败: %v", err)
		}
	}

	// 正常关闭数据库
	if err := db.Close(); err != nil {
		t.Fatalf("关闭数据库失败: %v", err)
	}

	// 获取WAL文件路径
	walFilePath := filepath.Join(walDir, "0.wal")
	t.Logf("WAL文件路径: %s", walFilePath)

	// 故意损坏WAL文件（截断文件或追加随机数据）
	t.Log("故意损坏WAL文件")
	f, err := os.OpenFile(walFilePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("打开WAL文件失败: %v", err)
	}

	// 获取文件大小
	stat, err := f.Stat()
	if err != nil {
		t.Fatalf("获取文件状态失败: %v", err)
	}

	// 截断文件（删除最后100字节）
	truncateSize := stat.Size() - 100
	if truncateSize <= 0 {
		truncateSize = stat.Size() / 2 // 如果文件太小，则只截断一半
	}

	t.Logf("截断WAL文件，从 %d 字节截断到 %d 字节", stat.Size(), truncateSize)
	if err := f.Truncate(truncateSize); err != nil {
		t.Fatalf("截断文件失败: %v", err)
	}
	f.Close()

	// 第二阶段：尝试重新打开损坏的数据库
	t.Log("第二阶段：尝试重新打开损坏的数据库")

	// 尝试打开数据库，应该能够打开但可能有一些数据丢失
	db2, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("重新打开数据库失败: %v", err)
	}
	defer db2.Close()

	// 检查能恢复多少数据
	t.Log("检查损坏后恢复的数据")
	recoveredCount := 0
	for k, v := range testData {
		value, err := db2.Get([]byte(k))
		if err != nil {
			t.Logf("获取数据出错（可能由于损坏）: key=%s, err=%v", k, err)
			continue
		}

		if value != nil {
			recoveredCount++
			if string(value) == v {
				t.Logf("成功恢复数据: key=%s, value=%s", k, v)
			} else {
				t.Logf("恢复的数据不匹配: key=%s, 期望=%s, 实际=%s", k, v, string(value))
			}
		} else {
			t.Logf("数据丢失: key=%s", k)
		}
	}

	t.Logf("总共 %d 条数据中恢复了 %d 条", len(testData), recoveredCount)
	t.Log("损坏WAL恢复测试完成")
}
