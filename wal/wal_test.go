package wal

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/aixiasang/bitcask/config"
	"github.com/aixiasang/bitcask/index"
	"github.com/aixiasang/bitcask/record"
	"github.com/stretchr/testify/assert"
)

// 创建测试用的配置
func createTestConfig(t *testing.T) *config.Config {
	// 创建临时目录用于测试
	tmpDir := filepath.Join(os.TempDir(), "bitcask_test")
	err := os.MkdirAll(filepath.Join(tmpDir, "wal"), 0755)
	assert.NoError(t, err)

	// 注册清理函数，测试完成后删除临时目录
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	return &config.Config{
		DataDir:   tmpDir,
		WalDir:    "wal",
		AutoSync:  true,
		IndexType: config.IndexTypeBTree,
	}
}

// 测试创建新的 WAL
func TestNewWal(t *testing.T) {
	conf := createTestConfig(t)
	wal, err := NewWal(conf, 1)
	assert.NoError(t, err)
	assert.NotNil(t, wal)

	assert.Equal(t, uint32(1), wal.FileId())
	assert.Equal(t, uint32(0), wal.Size())

	// 清理
	err = wal.Close()
	assert.NoError(t, err)
}

// 测试写入数据
func TestWal_Write(t *testing.T) {
	conf := createTestConfig(t)
	wal, err := NewWal(conf, 1)
	assert.NoError(t, err)

	// 写入数据
	key := []byte("test_key")
	value := []byte("test_value")
	pos, err := wal.Write(key, value)
	assert.NoError(t, err)
	assert.NotNil(t, pos)

	// 验证返回的位置信息
	assert.Equal(t, uint32(1), pos.FileId)
	assert.Equal(t, uint32(0), pos.Offset)
	assert.Greater(t, pos.Length, uint32(0))

	// 验证 WAL 大小增加了
	assert.Equal(t, pos.Length, wal.Size())

	// 清理
	err = wal.Close()
	assert.NoError(t, err)
}

// 测试读取数据
func TestWal_ReadPos(t *testing.T) {
	conf := createTestConfig(t)
	wal, err := NewWal(conf, 1)
	assert.NoError(t, err)

	// 写入数据
	key := []byte("test_key")
	value := []byte("test_value")
	pos, err := wal.Write(key, value)
	assert.NoError(t, err)

	// 读取数据
	rec, err := wal.ReadPos(pos)
	assert.NoError(t, err)
	assert.NotNil(t, rec)

	// 验证读取的数据
	assert.Equal(t, key, rec.Key)
	assert.Equal(t, value, rec.Value)
	assert.Equal(t, record.RecordTypePut, rec.RecordType)

	// 清理
	err = wal.Close()
	assert.NoError(t, err)
}

// 测试删除记录
func TestWal_Delete(t *testing.T) {
	conf := createTestConfig(t)
	wal, err := NewWal(conf, 1)
	assert.NoError(t, err)

	// 写入数据
	key := []byte("test_key")
	value := []byte("test_value")
	_, err = wal.Write(key, value)
	assert.NoError(t, err)

	// 写入删除记录（空值表示删除）
	_, err = wal.Write(key, nil)
	assert.NoError(t, err)

	// 使用 memTable 测试恢复
	memTable := index.NewBTreeIndex(2)
	err = wal.ReadAll(memTable, &atomic.Uint32{})
	assert.NoError(t, err)

	// 验证记录已删除
	pos, err := memTable.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, pos)

	// 清理
	err = wal.Close()
	assert.NoError(t, err)
}

// 测试恢复数据到内存索引
func TestWal_ReadAll(t *testing.T) {
	conf := createTestConfig(t)
	wal, err := NewWal(conf, 1)
	assert.NoError(t, err)

	// 写入多条数据
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
	}

	for _, data := range testData {
		_, err := wal.Write([]byte(data.key), []byte(data.value))
		assert.NoError(t, err)
	}

	// 使用 memTable 测试恢复
	memTable := index.NewBTreeIndex(2)
	err = wal.ReadAll(memTable, &atomic.Uint32{})
	assert.NoError(t, err)

	// 验证所有数据都已恢复
	for _, data := range testData {
		pos, err := memTable.Get([]byte(data.key))
		assert.NoError(t, err)
		assert.NotNil(t, pos)

		// 从 WAL 读取记录并验证
		rec, err := wal.ReadPos(pos)
		assert.NoError(t, err)
		assert.Equal(t, []byte(data.key), rec.Key)
		assert.Equal(t, []byte(data.value), rec.Value)
	}

	// 清理
	err = wal.Close()
	assert.NoError(t, err)
}

// 测试同步
func TestWal_Sync(t *testing.T) {
	conf := createTestConfig(t)
	conf.AutoSync = false // 关闭自动同步以测试手动同步

	wal, err := NewWal(conf, 1)
	assert.NoError(t, err)

	// 写入数据
	key := []byte("test_key")
	value := []byte("test_value")
	_, err = wal.Write(key, value)
	assert.NoError(t, err)

	// 手动同步
	err = wal.Sync()
	assert.NoError(t, err)

	// 清理
	err = wal.Close()
	assert.NoError(t, err)
}

// 测试多个 WAL 文件
func TestMultipleWalFiles(t *testing.T) {
	conf := createTestConfig(t)

	// 创建第一个 WAL 文件
	wal1, err := NewWal(conf, 1)
	assert.NoError(t, err)

	// 写入数据到第一个 WAL 文件
	key1 := []byte("key1")
	value1 := []byte("value1")
	pos1, err := wal1.Write(key1, value1)
	assert.NoError(t, err)

	// 关闭第一个 WAL 文件
	err = wal1.Close()
	assert.NoError(t, err)

	// 创建第二个 WAL 文件
	wal2, err := NewWal(conf, 2)
	assert.NoError(t, err)

	// 写入数据到第二个 WAL 文件
	key2 := []byte("key2")
	value2 := []byte("value2")
	pos2, err := wal2.Write(key2, value2)
	assert.NoError(t, err)

	// 验证文件 ID 不同
	assert.Equal(t, uint32(1), pos1.FileId)
	assert.Equal(t, uint32(2), pos2.FileId)

	// 关闭第二个 WAL 文件
	err = wal2.Close()
	assert.NoError(t, err)

	// 重新打开两个 WAL 文件进行验证
	wal1, err = NewWal(conf, 1)
	assert.NoError(t, err)

	wal2, err = NewWal(conf, 2)
	assert.NoError(t, err)

	// 从第一个 WAL 文件读取
	rec1, err := wal1.ReadPos(pos1)
	assert.NoError(t, err)
	assert.Equal(t, key1, rec1.Key)
	assert.Equal(t, value1, rec1.Value)

	// 从第二个 WAL 文件读取
	rec2, err := wal2.ReadPos(pos2)
	assert.NoError(t, err)
	assert.Equal(t, key2, rec2.Key)
	assert.Equal(t, value2, rec2.Value)

	// 清理
	err = wal1.Close()
	assert.NoError(t, err)

	err = wal2.Close()
	assert.NoError(t, err)
}

// 测试并发写入
func TestWal_ConcurrentWrite(t *testing.T) {
	conf := createTestConfig(t)
	wal, err := NewWal(conf, 1)
	assert.NoError(t, err)

	// 并发写入
	concurrency := 10
	done := make(chan bool)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			key := []byte("key" + string(rune('0'+id)))
			value := []byte("value" + string(rune('0'+id)))

			pos, err := wal.Write(key, value)
			assert.NoError(t, err)
			assert.NotNil(t, pos)

			done <- true
		}(i)
	}

	// 等待所有写入完成
	for i := 0; i < concurrency; i++ {
		<-done
	}

	// 验证 WAL 大小大于 0
	assert.Greater(t, wal.Size(), uint32(0))

	// 清理
	err = wal.Close()
	assert.NoError(t, err)
}

// 测试大数据量写入和读取
func TestWal_LargeData(t *testing.T) {
	conf := createTestConfig(t)
	wal, err := NewWal(conf, 1)
	assert.NoError(t, err)

	// 创建大数据
	keyPrefix := "large_key_"
	valueSize := 1024 * 10 // 10KB
	valueData := make([]byte, valueSize)
	for i := range valueData {
		valueData[i] = byte(i % 256)
	}

	// 写入多条大数据
	recordCount := 10
	positions := make([]*record.Pos, recordCount)

	for i := 0; i < recordCount; i++ {
		key := []byte(keyPrefix + string(rune('0'+i)))
		pos, err := wal.Write(key, valueData)
		assert.NoError(t, err)
		positions[i] = pos
	}

	// 读取并验证所有数据
	for i := 0; i < recordCount; i++ {
		rec, err := wal.ReadPos(positions[i])
		assert.NoError(t, err)
		assert.Equal(t, []byte(keyPrefix+string(rune('0'+i))), rec.Key)
		assert.Equal(t, valueData, rec.Value)
	}

	// 清理
	err = wal.Close()
	assert.NoError(t, err)
}
