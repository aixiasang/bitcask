package index

import (
	"fmt"
	"testing"

	"github.com/aixiasang/bitcask/record"
	"github.com/stretchr/testify/assert"
)

func TestNewBTreeIndex(t *testing.T) {
	index := NewBTreeIndex(12)
	assert.NotNil(t, index)
	assert.NotNil(t, index.tree)
}

func TestBTreeIndex_PutAndGet(t *testing.T) {
	index := NewBTreeIndex(12)

	// 测试基本插入和获取
	key := []byte("test_key")
	pos := &record.Pos{
		FileId: 1,
		Offset: 100,
		Length: 50,
	}

	// 测试插入
	err := index.Put(key, pos)
	assert.NoError(t, err)

	// 测试获取
	result, err := index.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, pos, result)

	// 测试获取不存在的键
	nonExistKey := []byte("non_exist_key")
	result, err = index.Get(nonExistKey)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestBTreeIndex_Delete(t *testing.T) {
	index := NewBTreeIndex(12)

	// 插入测试数据
	key := []byte("test_key")
	pos := &record.Pos{
		FileId: 1,
		Offset: 100,
		Length: 50,
	}
	err := index.Put(key, pos)
	assert.NoError(t, err)

	// 测试删除
	err = index.Delete(key)
	assert.NoError(t, err)

	// 验证删除后无法获取
	result, err := index.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestBTreeIndex_Scan(t *testing.T) {
	index := NewBTreeIndex(12)

	// 插入测试数据
	testData := []struct {
		key string
		pos *record.Pos
	}{
		{"a", &record.Pos{FileId: 1, Offset: 100, Length: 10}},
		{"b", &record.Pos{FileId: 1, Offset: 110, Length: 20}},
		{"c", &record.Pos{FileId: 1, Offset: 130, Length: 30}},
		{"d", &record.Pos{FileId: 1, Offset: 160, Length: 40}},
		{"e", &record.Pos{FileId: 1, Offset: 200, Length: 50}},
	}

	for _, data := range testData {
		err := index.Put([]byte(data.key), data.pos)
		assert.NoError(t, err)
	}

	// 测试完整范围扫描
	results, err := index.Scan([]byte("a"), []byte("e"))
	assert.NoError(t, err)
	assert.Len(t, results, 5)

	// 验证返回的数据结构
	for i, result := range results {
		assert.Equal(t, testData[i].key, result.Key)
		assert.Equal(t, *testData[i].pos, result.Pos)
	}

	// 测试部分范围扫描
	results, err = index.Scan([]byte("b"), []byte("d"))
	assert.NoError(t, err)
	assert.Len(t, results, 3)
	assert.Equal(t, "b", results[0].Key)
	assert.Equal(t, "c", results[1].Key)
	assert.Equal(t, "d", results[2].Key)

	// 测试空范围扫描
	results, err = index.Scan([]byte("x"), []byte("z"))
	assert.NoError(t, err)
	assert.Len(t, results, 0)
}

func TestBTreeIndex_Foreach(t *testing.T) {
	index := NewBTreeIndex(12)

	// 插入测试数据
	testData := []struct {
		key string
		pos *record.Pos
	}{
		{"a", &record.Pos{FileId: 1, Offset: 100, Length: 10}},
		{"b", &record.Pos{FileId: 1, Offset: 110, Length: 20}},
		{"c", &record.Pos{FileId: 1, Offset: 130, Length: 30}},
	}

	for _, data := range testData {
		err := index.Put([]byte(data.key), data.pos)
		assert.NoError(t, err)
	}

	// 测试 Foreach
	var count int
	var keys []string
	err := index.Foreach(func(key []byte, pos *record.Pos) error {
		count++
		keys = append(keys, string(key))
		assert.NotNil(t, pos)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
	assert.Equal(t, []string{"a", "b", "c"}, keys)

	// 测试 Foreach 中断
	count = 0
	err = index.Foreach(func(key []byte, pos *record.Pos) error {
		count++
		if count == 2 {
			return fmt.Errorf("中断遍历")
		}
		return nil
	})
	assert.Error(t, err)
	assert.Equal(t, "中断遍历", err.Error())
	assert.Equal(t, 2, count)
}

func TestBTreeIndex_Update(t *testing.T) {
	index := NewBTreeIndex(12)

	// 插入初始数据
	key := []byte("test_key")
	initialPos := &record.Pos{
		FileId: 1,
		Offset: 100,
		Length: 50,
	}
	err := index.Put(key, initialPos)
	assert.NoError(t, err)

	// 更新数据
	updatedPos := &record.Pos{
		FileId: 2,
		Offset: 200,
		Length: 100,
	}
	err = index.Put(key, updatedPos)
	assert.NoError(t, err)

	// 验证更新后的数据
	result, err := index.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, updatedPos, result)
}

func TestBTreeIndex_Concurrent(t *testing.T) {
	index := NewBTreeIndex(12)
	done := make(chan bool)
	concurrent := 10

	// 并发写入测试
	for i := 0; i < concurrent; i++ {
		go func(id int) {
			key := []byte(fmt.Sprintf("key_%d", id))
			pos := &record.Pos{
				FileId: uint32(id),
				Offset: uint32(id * 100),
				Length: uint32(id * 10),
			}
			err := index.Put(key, pos)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// 等待所有写入完成
	for i := 0; i < concurrent; i++ {
		<-done
	}

	// 验证所有数据
	for i := 0; i < concurrent; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		result, err := index.Get(key)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, uint32(i), result.FileId)
	}
}

func TestBTreeIndex_Close(t *testing.T) {
	index := NewBTreeIndex(12)
	err := index.Close()
	assert.NoError(t, err)
}

func TestBTreeIndex_EdgeCases(t *testing.T) {
	index := NewBTreeIndex(12)

	// 测试空键
	emptyKey := []byte("")
	pos := &record.Pos{FileId: 1, Offset: 100, Length: 50}
	err := index.Put(emptyKey, pos)
	assert.NoError(t, err)

	result, err := index.Get(emptyKey)
	assert.NoError(t, err)
	assert.Equal(t, pos, result)

	// 测试特殊字符键
	specialKey := []byte("!@#$%^&*()")
	err = index.Put(specialKey, pos)
	assert.NoError(t, err)

	result, err = index.Get(specialKey)
	assert.NoError(t, err)
	assert.Equal(t, pos, result)

	// 测试长键
	longKey := make([]byte, 1000)
	for i := range longKey {
		longKey[i] = 'a'
	}
	err = index.Put(longKey, pos)
	assert.NoError(t, err)

	result, err = index.Get(longKey)
	assert.NoError(t, err)
	assert.Equal(t, pos, result)
}

func TestBTreeIndex_Performance(t *testing.T) {
	index := NewBTreeIndex(12)
	const numItems = 10000

	// 批量插入测试
	for i := 0; i < numItems; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		pos := &record.Pos{
			FileId: uint32(i),
			Offset: uint32(i * 100),
			Length: uint32(i * 10),
		}
		err := index.Put(key, pos)
		assert.NoError(t, err)
	}

	// 批量查询测试
	for i := 0; i < numItems; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		result, err := index.Get(key)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, uint32(i), result.FileId)
	}

	// 范围扫描性能测试
	results, err := index.Scan([]byte("key_0"), []byte(fmt.Sprintf("key_%d", numItems-1)))
	assert.NoError(t, err)
	assert.Equal(t, numItems, len(results))
}
