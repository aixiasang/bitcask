package index

import (
	"bytes"
	"testing"

	"github.com/aixiasang/bitcask/record"
)

// 测试索引的Put、Get和Delete操作
func TestBTreeIndex_BasicOperations(t *testing.T) {
	// 创建索引
	idx, err := NewBTreeIndex(32)
	if err != nil {
		t.Fatalf("Failed to create BTree index: %v", err)
	}

	// 创建测试数据
	key := []byte("test-key")
	pos := &record.RecordPos{
		FileId: 1,
		Offset: 100,
		Length: 50,
	}

	// 测试 Put 操作
	if err := idx.Put(key, pos); err != nil {
		t.Fatalf("Failed to put key-pos: %v", err)
	}

	// 测试 Get 操作
	readPos, err := idx.Get(key)
	if err != nil {
		t.Fatalf("Failed to get position: %v", err)
	}
	if readPos == nil {
		t.Fatal("Expected position, got nil")
	}

	// 验证位置信息
	if readPos.FileId != pos.FileId {
		t.Errorf("Expected FileId %d, got %d", pos.FileId, readPos.FileId)
	}
	if readPos.Offset != pos.Offset {
		t.Errorf("Expected Offset %d, got %d", pos.Offset, readPos.Offset)
	}
	if readPos.Length != pos.Length {
		t.Errorf("Expected Length %d, got %d", pos.Length, readPos.Length)
	}

	// 测试 Delete 操作
	if err := idx.Delete(key); err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// 验证删除后的结果
	readPos, err = idx.Get(key)
	if err != nil {
		t.Fatalf("Failed to get position after delete: %v", err)
	}
	if readPos != nil {
		t.Errorf("Expected nil after delete, got position: %+v", readPos)
	}
}

// 测试迭代器功能
func TestBTreeIndex_Iterator(t *testing.T) {
	// 创建索引
	idx, err := NewBTreeIndex(32)
	if err != nil {
		t.Fatalf("Failed to create BTree index: %v", err)
	}

	// 添加多个键值对
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for i, key := range keys {
		pos := &record.RecordPos{
			FileId: 1,
			Offset: uint32(i * 100),
			Length: 50,
		}
		if err := idx.Put([]byte(key), pos); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// 测试升序迭代器
	t.Run("Ascending Iterator", func(t *testing.T) {
		iter := idx.Iterator(true)
		defer iter.Close()

		// 重置迭代器
		if err := iter.Rewind(); err != nil {
			t.Fatalf("Failed to rewind iterator: %v", err)
		}

		// 收集迭代结果
		var iteratedKeys []string
		for iter.Valid() {
			keyBytes := iter.Key()
			iteratedKeys = append(iteratedKeys, string(keyBytes))
			iter.Next()
		}

		// 验证迭代顺序
		if len(iteratedKeys) != len(keys) {
			t.Fatalf("Expected %d keys, got %d", len(keys), len(iteratedKeys))
		}
		for i, key := range iteratedKeys {
			if i > 0 && key <= iteratedKeys[i-1] {
				t.Errorf("Keys not in ascending order: %s after %s",
					key, iteratedKeys[i-1])
			}
		}
	})

	// 测试降序迭代器
	t.Run("Descending Iterator", func(t *testing.T) {
		iter := idx.Iterator(false)
		defer iter.Close()

		// 重置迭代器
		if err := iter.Rewind(); err != nil {
			t.Fatalf("Failed to rewind iterator: %v", err)
		}

		// 收集迭代结果
		var iteratedKeys []string
		for iter.Valid() {
			keyBytes := iter.Key()
			iteratedKeys = append(iteratedKeys, string(keyBytes))
			iter.Next()
		}

		// 验证迭代顺序
		if len(iteratedKeys) != len(keys) {
			t.Fatalf("Expected %d keys, got %d", len(keys), len(iteratedKeys))
		}
		for i, key := range iteratedKeys {
			if i > 0 && key >= iteratedKeys[i-1] {
				t.Errorf("Keys not in descending order: %s after %s",
					key, iteratedKeys[i-1])
			}
		}
	})

	// 测试 Seek 功能
	t.Run("Seek Function", func(t *testing.T) {
		iter := idx.Iterator(true)
		defer iter.Close()

		// 查找特定键
		targetKey := []byte("key3")
		if err := iter.Seek(targetKey); err != nil {
			t.Fatalf("Failed to seek to key: %v", err)
		}
		if !iter.Valid() {
			t.Fatal("Iterator should be valid after seek")
		}

		foundKey := iter.Key()
		if !bytes.Equal(foundKey, targetKey) {
			t.Errorf("Expected to find key %s, got %s",
				string(targetKey), string(foundKey))
		}

		// 验证位置信息
		pos := iter.Value()
		if pos == nil {
			t.Fatal("Position should not be nil")
		}
		if pos.FileId != 1 {
			t.Errorf("Expected FileId 1, got %d", pos.FileId)
		}
		if pos.Offset != 200 { // key3 的偏移量是 2*100
			t.Errorf("Expected Offset 200, got %d", pos.Offset)
		}
	})

	// 测试 AllKey 功能
	t.Run("AllKey Function", func(t *testing.T) {
		iter := idx.Iterator(true)
		defer iter.Close()

		allKeys := iter.AllKey()
		if len(allKeys) != len(keys) {
			t.Fatalf("Expected %d keys, got %d", len(keys), len(allKeys))
		}

		// 验证所有键都在结果中
		for _, key := range keys {
			found := false
			for _, k := range allKeys {
				if string(k) == key {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Key %s not found in AllKey result", key)
			}
		}
	})
}
