package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aixiasang/bitcask/config"
)

func TestBitcaskBasicOperations(t *testing.T) {
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

	// 创建数据库实例
	t.Log("开始创建Bitcask实例...")
	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建Bitcask实例失败: %v", err)
	}
	if db == nil {
		t.Fatal("创建的Bitcask实例为nil")
	}
	defer func() {
		t.Log("关闭Bitcask实例...")
		if err := db.Close(); err != nil {
			t.Errorf("关闭Bitcask实例失败: %v", err)
		}
	}()
	t.Log("Bitcask实例创建成功")

	// 测试 Put 操作
	key := []byte("test-key")
	value := []byte("test-value")
	t.Logf("测试Put操作: key=%s, value=%s", key, value)
	if err := db.Put(key, value); err != nil {
		t.Fatalf("Put操作失败: %v", err)
	}
	t.Log("Put操作成功")

	// 测试 Get 操作
	t.Logf("测试Get操作: key=%s", key)
	readValue, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get操作失败: %v", err)
	}
	if string(readValue) != string(value) {
		t.Errorf("Get操作返回错误的值: 期望=%s, 实际=%s", value, readValue)
	}
	t.Log("Get操作成功")

	// 测试更新值
	newValue := []byte("updated-value")
	t.Logf("测试更新操作: key=%s, newValue=%s", key, newValue)
	if err := db.Put(key, newValue); err != nil {
		t.Fatalf("更新操作失败: %v", err)
	}
	t.Log("更新操作成功")

	// 验证更新后的值
	t.Log("验证更新后的值...")
	readValue, err = db.Get(key)
	if err != nil {
		t.Fatalf("获取更新后的值失败: %v", err)
	}
	if string(readValue) != string(newValue) {
		t.Errorf("更新后的值不正确: 期望=%s, 实际=%s", newValue, readValue)
	}
	t.Log("更新验证成功")

	// 测试不存在的键
	nonExistKey := []byte("non-exist-key")
	t.Logf("测试获取不存在的键: key=%s", nonExistKey)
	readValue, err = db.Get(nonExistKey)
	if err != nil {
		t.Fatalf("获取不存在的键时发生错误: %v", err)
	}
	if readValue != nil {
		t.Errorf("获取不存在的键应返回nil，实际返回: %s", readValue)
	}
	t.Log("不存在键测试成功")

	// 测试 Delete 操作
	t.Logf("测试Delete操作: key=%s", key)
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete操作失败: %v", err)
	}
	t.Log("Delete操作成功")

	// 验证删除后的值
	t.Log("验证删除后的值...")
	readValue, err = db.Get(key)
	if err != nil {
		t.Fatalf("获取已删除的键时发生错误: %v", err)
	}
	if readValue != nil {
		t.Errorf("已删除的键应返回nil，实际返回: %s", readValue)
	}
	t.Log("删除验证成功")
}

func TestBitcaskMultipleOperations(t *testing.T) {
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

	// 创建数据库实例
	t.Log("开始创建Bitcask实例...")
	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建Bitcask实例失败: %v", err)
	}
	if db == nil {
		t.Fatal("创建的Bitcask实例为nil")
	}
	defer func() {
		t.Log("关闭Bitcask实例...")
		if err := db.Close(); err != nil {
			t.Errorf("关闭Bitcask实例失败: %v", err)
		}
	}()
	t.Log("Bitcask实例创建成功")

	// 测试多个键值对
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	values := []string{"value1", "value2", "value3", "value4", "value5"}

	// 写入键值对
	t.Log("开始写入多个键值对...")
	for i, key := range keys {
		t.Logf("写入: key=%s, value=%s", key, values[i])
		if err := db.Put([]byte(key), []byte(values[i])); err != nil {
			t.Fatalf("写入键值对失败 (key=%s): %v", key, err)
		}
	}
	t.Log("所有键值对写入成功")

	// 读取键值对
	t.Log("开始验证写入的键值对...")
	for i, key := range keys {
		t.Logf("读取: key=%s", key)
		readValue, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("读取键值对失败 (key=%s): %v", key, err)
		}
		if string(readValue) != values[i] {
			t.Errorf("键值对不匹配 (key=%s): 期望=%s, 实际=%s",
				key, values[i], string(readValue))
		}
	}
	t.Log("所有键值对验证成功")

	// 删除部分键
	t.Log("开始删除部分键...")
	for i, key := range keys {
		if i%2 == 0 { // 删除索引为偶数的键
			t.Logf("删除: key=%s", key)
			if err := db.Delete([]byte(key)); err != nil {
				t.Fatalf("删除键失败 (key=%s): %v", key, err)
			}
		}
	}
	t.Log("部分键删除成功")

	// 验证删除结果
	t.Log("开始验证删除结果...")
	for i, key := range keys {
		t.Logf("验证: key=%s", key)
		readValue, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("读取键值对失败 (key=%s): %v", key, err)
		}

		if i%2 == 0 { // 已删除的键
			if readValue != nil {
				t.Errorf("已删除的键仍然存在 (key=%s): %s", key, string(readValue))
			}
		} else { // 未删除的键
			if string(readValue) != values[i] {
				t.Errorf("未删除的键值对不匹配 (key=%s): 期望=%s, 实际=%s",
					key, values[i], string(readValue))
			}
		}
	}
	t.Log("删除结果验证成功")
}

func TestBitcaskReopen(t *testing.T) {
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

	// 测试数据
	keys := []string{"key1", "key2", "key3"}
	values := []string{"value1", "value2", "value3"}

	t.Log("=== 第一次打开数据库 ===")
	{
		// 创建第一个数据库实例
		t.Log("开始创建第一个Bitcask实例...")
		db, err := NewBitcask(conf)
		if err != nil {
			t.Fatalf("创建第一个Bitcask实例失败: %v", err)
		}
		if db == nil {
			t.Fatal("创建的第一个Bitcask实例为nil")
		}

		// 写入键值对
		t.Log("开始写入测试数据...")
		for i, key := range keys {
			t.Logf("写入: key=%s, value=%s", key, values[i])
			if err := db.Put([]byte(key), []byte(values[i])); err != nil {
				t.Fatalf("写入键值对失败 (key=%s): %v", key, err)
			}
		}
		t.Log("测试数据写入成功")

		// 关闭数据库
		t.Log("关闭第一个Bitcask实例...")
		if err := db.Close(); err != nil {
			t.Fatalf("关闭第一个数据库实例失败: %v", err)
		}
		t.Log("第一个Bitcask实例关闭成功")
	}

	t.Log("=== 重新打开数据库 ===")
	{
		// 重新打开数据库
		t.Log("开始创建第二个Bitcask实例...")
		db, err := NewBitcask(conf)
		if err != nil {
			t.Fatalf("创建第二个Bitcask实例失败: %v", err)
		}
		if db == nil {
			t.Fatal("创建的第二个Bitcask实例为nil")
		}
		defer func() {
			t.Log("关闭第二个Bitcask实例...")
			if err := db.Close(); err != nil {
				t.Errorf("关闭第二个数据库实例失败: %v", err)
			}
		}()
		t.Log("第二个Bitcask实例创建成功")

		// 验证键值对
		t.Log("开始验证持久化的数据...")
		for i, key := range keys {
			t.Logf("验证: key=%s", key)
			readValue, err := db.Get([]byte(key))
			if err != nil {
				t.Fatalf("读取键值对失败 (key=%s): %v", key, err)
			}
			if string(readValue) != values[i] {
				t.Errorf("键值对不匹配 (key=%s): 期望=%s, 实际=%s",
					key, values[i], string(readValue))
			}
		}
		t.Log("所有数据验证成功")
	}
}

// TestBitcaskBatchOperations 测试大量数据操作
func TestBitcaskBatchOperations(t *testing.T) {
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

	// 创建数据库实例
	t.Log("开始创建Bitcask实例...")
	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建Bitcask实例失败: %v", err)
	}
	if db == nil {
		t.Fatal("创建的Bitcask实例为nil")
	}
	defer func() {
		t.Log("关闭Bitcask实例...")
		if err := db.Close(); err != nil {
			t.Errorf("关闭Bitcask实例失败: %v", err)
		}
	}()
	t.Log("Bitcask实例创建成功")

	// 第一阶段：批量写入100条记录
	t.Log("=== 第一阶段：批量写入100条记录 ===")
	expectedValues := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("batch-key-%d", i)
		value := fmt.Sprintf("batch-value-%d", i)
		t.Logf("写入: key=%s, value=%s", key, value)

		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("写入记录失败 [%d]: %v", i, err)
		}
		expectedValues[key] = value
	}
	t.Log("成功写入100条记录")

	// 第二阶段：验证所有写入的记录
	t.Log("=== 第二阶段：验证写入的记录 ===")
	for key, expectedValue := range expectedValues {
		t.Logf("验证: key=%s", key)
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("读取记录失败 (key=%s): %v", key, err)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("值不匹配 (key=%s): 期望=%s, 实际=%s",
				key, expectedValue, string(value))
		}
	}
	t.Log("成功验证100条记录")

	// 第三阶段：更新30条记录
	t.Log("=== 第三阶段：更新30条记录 ===")
	for i := 0; i < 30; i++ {
		keyNum := i * 3 // 更新每第三条记录
		key := fmt.Sprintf("batch-key-%d", keyNum)
		value := fmt.Sprintf("batch-value-%d-updated", keyNum)
		t.Logf("更新: key=%s, value=%s", key, value)

		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("更新记录失败 [%d]: %v", i, err)
		}
		expectedValues[key] = value
	}
	t.Log("成功更新30条记录")

	// 第四阶段：删除20条记录
	t.Log("=== 第四阶段：删除20条记录 ===")
	for i := 0; i < 20; i++ {
		keyNum := i * 5 // 删除每第五条记录
		key := fmt.Sprintf("batch-key-%d", keyNum)
		t.Logf("删除: key=%s", key)

		if err := db.Delete([]byte(key)); err != nil {
			t.Fatalf("删除记录失败 [%d]: %v", i, err)
		}
		delete(expectedValues, key)
	}
	t.Log("成功删除20条记录")

	// 第五阶段：验证最终状态
	t.Log("=== 第五阶段：验证最终状态 ===")
	// 验证存在的记录
	for key, expectedValue := range expectedValues {
		t.Logf("验证存在的记录: key=%s", key)
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("读取记录失败 (key=%s): %v", key, err)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("值不匹配 (key=%s): 期望=%s, 实际=%s",
				key, expectedValue, string(value))
		}
	}

	// 验证已删除的记录
	for i := 0; i < 20; i++ {
		keyNum := i * 5
		key := fmt.Sprintf("batch-key-%d", keyNum)
		t.Logf("验证已删除的记录: key=%s", key)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("读取已删除记录失败 (key=%s): %v", key, err)
			continue
		}
		if value != nil {
			t.Errorf("已删除的记录仍然存在 (key=%s): %s", key, string(value))
		}
	}
	t.Log("成功验证最终状态")

	// 第六阶段：重新打开数据库并验证持久化
	t.Log("=== 第六阶段：验证数据持久化 ===")
	// 关闭当前数据库
	if err := db.Close(); err != nil {
		t.Fatalf("关闭数据库失败: %v", err)
	}

	// 重新打开数据库
	t.Log("重新打开数据库...")
	db2, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("重新打开数据库失败: %v", err)
	}
	defer db2.Close()

	// 验证所有数据
	for key, expectedValue := range expectedValues {
		t.Logf("验证持久化数据: key=%s", key)
		value, err := db2.Get([]byte(key))
		if err != nil {
			t.Errorf("读取持久化数据失败 (key=%s): %v", key, err)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("持久化数据不匹配 (key=%s): 期望=%s, 实际=%s",
				key, expectedValue, string(value))
		}
	}
	t.Log("成功验证持久化数据")
}
