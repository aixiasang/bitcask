package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aixiasang/bitcask/config"
)

// TestBatchBasicOperations 测试批处理操作
func TestBatchBasicOperations(t *testing.T) {
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

	// 创建批处理
	t.Log("创建批处理...")
	batch := db.NewBatch()
	if batch == nil {
		t.Fatal("创建批处理失败")
	}

	// 添加操作到批处理
	t.Log("添加操作到批处理...")
	keys := []string{"batch-key1", "batch-key2", "batch-key3", "batch-key4", "batch-key5"}
	values := []string{"batch-value1", "batch-value2", "batch-value3", "batch-value4", "batch-value5"}

	for i, key := range keys {
		t.Logf("添加操作: key=%s, value=%s", key, values[i])
		if err := batch.Put([]byte(key), []byte(values[i])); err != nil {
			t.Fatalf("添加操作到批处理失败: %v", err)
		}
	}

	// 删除一个键
	keyToDelete := keys[2]
	t.Logf("添加删除操作: key=%s", keyToDelete)
	if err := batch.Delete([]byte(keyToDelete)); err != nil {
		t.Fatalf("添加删除操作失败: %v", err)
	}

	// 提交批处理
	t.Log("提交批处理...")
	if err := batch.Commit(); err != nil {
		t.Fatalf("提交批处理失败: %v", err)
	}

	// 验证结果
	t.Log("验证批处理结果...")
	for i, key := range keys {
		if key == keyToDelete {
			// 验证删除的键
			t.Logf("验证已删除的键: key=%s", key)
			value, err := db.Get([]byte(key))
			if err != nil {
				t.Errorf("获取已删除键失败: %v", err)
				continue
			}
			if value != nil {
				t.Errorf("已删除的键仍然存在: key=%s, value=%s", key, string(value))
			}
		} else {
			// 验证其他键
			t.Logf("验证键值对: key=%s", key)
			value, err := db.Get([]byte(key))
			if err != nil {
				t.Errorf("获取键值对失败: %v", err)
				continue
			}
			if string(value) != values[i] {
				t.Errorf("键值对不匹配: key=%s, 期望=%s, 实际=%s", key, values[i], string(value))
			}
		}
	}

	// 测试重新打开数据库后的批处理持久化
	t.Log("关闭数据库以测试持久化...")
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

	// 验证持久化结果
	t.Log("验证批处理持久化结果...")
	for i, key := range keys {
		if key == keyToDelete {
			// 验证删除的键
			t.Logf("验证持久化后的已删除键: key=%s", key)
			value, err := db2.Get([]byte(key))
			if err != nil {
				t.Errorf("获取持久化后的已删除键失败: %v", err)
				continue
			}
			if value != nil {
				t.Errorf("持久化后已删除的键仍然存在: key=%s, value=%s", key, string(value))
			}
		} else {
			// 验证其他键
			t.Logf("验证持久化后的键值对: key=%s", key)
			value, err := db2.Get([]byte(key))
			if err != nil {
				t.Errorf("获取持久化后的键值对失败: %v", err)
				continue
			}
			if string(value) != values[i] {
				t.Errorf("持久化后的键值对不匹配: key=%s, 期望=%s, 实际=%s", key, values[i], string(value))
			}
		}
	}
}

// TestBatchManyOperations 测试大批量操作
func TestBatchManyOperations(t *testing.T) {
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

	// 创建批处理
	t.Log("创建批处理...")
	batch := db.NewBatch()
	if batch == nil {
		t.Fatal("创建批处理失败")
	}

	// 添加大量操作到批处理
	count := 100
	t.Logf("添加%d个操作到批处理...", count)
	expectedValues := make(map[string]string)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("batch-key-%d", i)
		value := fmt.Sprintf("batch-value-%d", i)

		if i%20 == 0 { // 每20个键删除一个
			t.Logf("添加删除操作: key=%s", key)
			if err := batch.Delete([]byte(key)); err != nil {
				t.Fatalf("添加删除操作失败: %v", err)
			}
			// 不添加到预期值映射
		} else {
			t.Logf("添加操作: key=%s, value=%s", key, value)
			if err := batch.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("添加操作到批处理失败: %v", err)
			}
			expectedValues[key] = value
		}
	}

	// 提交批处理
	t.Log("提交批处理...")
	if err := batch.Commit(); err != nil {
		t.Fatalf("提交批处理失败: %v", err)
	}

	// 验证结果
	t.Log("验证批处理结果...")
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("batch-key-%d", i)
		expectedValue, exists := expectedValues[key]

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("获取键失败: %v", err)
			continue
		}

		if i%20 == 0 {
			// 应该被删除
			if value != nil {
				t.Errorf("应该被删除的键仍然存在: key=%s, value=%s", key, string(value))
			}
		} else {
			// 应该存在
			if !exists {
				t.Errorf("预期值映射中缺少键: %s", key)
				continue
			}

			if value == nil {
				t.Errorf("应该存在的键不存在: key=%s", key)
				continue
			}

			if string(value) != expectedValue {
				t.Errorf("键值对不匹配: key=%s, 期望=%s, 实际=%s", key, expectedValue, string(value))
			}
		}
	}

	t.Log("批处理测试成功完成")
}
