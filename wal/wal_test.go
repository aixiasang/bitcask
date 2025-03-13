package wal

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/aixiasang/bitcask/config"
	"github.com/aixiasang/bitcask/index"
	"github.com/aixiasang/bitcask/record"
)

// 测试WAL基本写入和读取功能
func TestWalBasicOperations(t *testing.T) {
	dir := t.TempDir()
	conf := config.DefaultConfig(dir)

	// 创建WAL实例
	w, err := NewWal(1, conf)
	if err != nil {
		t.Fatalf("创建WAL失败: %v", err)
	}
	defer w.Close()

	// 写入一条普通记录
	key := []byte("key1")
	value := []byte("value1")
	rec := record.NewRecordUpdate(key, value)
	pos, err := w.Write(rec)
	if err != nil {
		t.Fatalf("写入记录失败: %v", err)
	}

	// 读取记录
	rt, k, v, err := w.Read(pos)
	if err != nil {
		t.Fatalf("读取记录失败: %v", err)
	}
	if rt != record.RecordUpdate {
		t.Errorf("期望记录类型为 %v, 实际为 %v", record.RecordUpdate, rt)
	}
	if !bytes.Equal(k, key) {
		t.Errorf("期望key为 %v, 实际为 %v", key, k)
	}
	if !bytes.Equal(v, value) {
		t.Errorf("期望value为 %v, 实际为 %v", value, v)
	}

	// 写入一条删除记录
	key2 := []byte("key2")
	rec = record.NewRecordDelete(key2)
	pos, err = w.Write(rec)
	if err != nil {
		t.Fatalf("写入删除记录失败: %v", err)
	}

	// 读取删除记录
	rt, k, v, err = w.Read(pos)
	if err != nil {
		t.Fatalf("读取删除记录失败: %v", err)
	}
	if rt != record.RecordDelete {
		t.Errorf("期望记录类型为 %v, 实际为 %v", record.RecordDelete, rt)
	}
	if !bytes.Equal(k, key2) {
		t.Errorf("期望key为 %v, 实际为 %v", key2, k)
	}
	if v != nil {
		t.Errorf("删除记录的value应为nil, 实际为 %v", v)
	}
}

// 生成测试数据
func generateTestData() []struct {
	key       string
	value     string
	operation record.RecordType
} {
	return []struct {
		key       string
		value     string
		operation record.RecordType
	}{
		{"key1", "value1", record.RecordUpdate},
		{"key2", "value2", record.RecordUpdate},
		{"key3", "value3", record.RecordUpdate},
		{"key2", "", record.RecordDelete},
		{"key4", "value4", record.RecordUpdate},
		{"key1", "value1-updated", record.RecordUpdate},
		{"key5", "value5", record.RecordUpdate},
		{"key6", "value6", record.RecordUpdate},
		{"key3", "", record.RecordDelete},
		{"key7", "value7", record.RecordUpdate},
		{"key8", "value8", record.RecordUpdate},
		{"key9", "value9", record.RecordUpdate},
		{"key10", "value10", record.RecordUpdate},
		{"key7", "value7-updated", record.RecordUpdate},
		{"key8", "", record.RecordDelete},
	}
}

// 计算预期的最终状态
func calculateExpectedState(testData []struct {
	key       string
	value     string
	operation record.RecordType
}) map[string]string {
	expectedState := make(map[string]string)
	for _, data := range testData {
		if data.operation == record.RecordUpdate {
			expectedState[data.key] = data.value
		} else {
			delete(expectedState, data.key)
		}
	}
	return expectedState
}

// TestWalRestoreIndex 测试WAL索引恢复功能
func TestWalRestoreIndex(t *testing.T) {
	dir := t.TempDir()
	conf := config.DefaultConfig(dir)

	// 创建WAL实例
	w, err := NewWal(1, conf)
	if err != nil {
		t.Fatalf("创建WAL失败: %v", err)
	}
	defer w.Close()

	// 准备测试数据
	testData := []struct {
		key       []byte
		value     []byte
		operation record.RecordType
	}{
		{[]byte("key1"), []byte("value1"), record.RecordUpdate},
		{[]byte("key2"), []byte("value2"), record.RecordUpdate},
		{[]byte("key3"), []byte("value3"), record.RecordUpdate},
		{[]byte("key2"), nil, record.RecordDelete},
		{[]byte("key4"), []byte("value4"), record.RecordUpdate},
		{[]byte("key1"), []byte("value1-updated"), record.RecordUpdate},
		{[]byte("key5"), []byte("value5"), record.RecordUpdate},
		{[]byte("key6"), []byte("value6"), record.RecordUpdate},
		{[]byte("key3"), nil, record.RecordDelete},
		{[]byte("key7"), []byte("value7"), record.RecordUpdate},
		{[]byte("key8"), []byte("value8"), record.RecordUpdate},
		{[]byte("key9"), []byte("value9"), record.RecordUpdate},
		{[]byte("key10"), []byte("value10"), record.RecordUpdate},
		{[]byte("key7"), []byte("value7-updated"), record.RecordUpdate},
		{[]byte("key8"), nil, record.RecordDelete},
	}

	// 写入测试数据
	for _, td := range testData {
		var rec *record.Record
		if td.operation == record.RecordUpdate {
			rec = record.NewRecordUpdate(td.key, td.value)
		} else {
			rec = record.NewRecordDelete(td.key)
		}
		_, err := w.Write(rec)
		if err != nil {
			t.Fatalf("写入记录失败: %v", err)
		}
	}

	// 同步数据到磁盘
	if err := w.Sync(); err != nil {
		t.Fatalf("同步数据失败: %v", err)
	}

	// 创建索引并恢复
	idx, err := index.NewBTreeIndex(32)
	if err != nil {
		t.Fatalf("创建索引失败: %v", err)
	}
	if err := w.RestoreIndex(idx); err != nil {
		t.Fatalf("恢复索引失败: %v", err)
	}

	// 验证索引状态
	expectedState := make(map[string][]byte)
	for _, td := range testData {
		key := string(td.key)
		if td.operation == record.RecordUpdate {
			expectedState[key] = td.value
		} else {
			delete(expectedState, key)
		}
	}

	// 验证每个key的状态
	for key, expectedValue := range expectedState {
		pos, err := idx.Get([]byte(key))
		if err != nil {
			t.Errorf("获取key %s 失败: %v", key, err)
			continue
		}
		if pos == nil {
			t.Errorf("key %s 应该存在于索引中", key)
			continue
		}

		_, _, value, err := w.Read(pos)
		if err != nil {
			t.Errorf("读取key %s 的值失败: %v", key, err)
			continue
		}

		if !bytes.Equal(value, expectedValue) {
			t.Errorf("key %s 的值不匹配, 期望 %v, 实际 %v", key, expectedValue, value)
		}
	}

	// 验证已删除的key
	deletedKeys := []string{"key2", "key3", "key8"}
	for _, key := range deletedKeys {
		pos, err := idx.Get([]byte(key))
		if err != nil {
			t.Errorf("获取key %s 失败: %v", key, err)
			continue
		}
		if pos != nil {
			t.Errorf("已删除的key %s 不应该存在于索引中", key)
		}
	}
}

// 测试WAL文件大小限制
func TestWalFileSize(t *testing.T) {
	dir := t.TempDir()
	conf := config.DefaultConfig(dir)
	conf.MaxFileSize = 1024 // 1KB

	// 创建WAL实例
	w, err := NewWal(1, conf)
	if err != nil {
		t.Fatalf("创建WAL失败: %v", err)
	}
	defer w.Close()

	// 写入大量数据直到文件大小超过限制
	data := bytes.Repeat([]byte("a"), 512) // 512字节的数据
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		rec := record.NewRecordUpdate(key, data)
		_, err := w.Write(rec)
		if err != nil {
			t.Fatalf("写入记录失败: %v", err)
		}
	}

	// 验证文件大小
	if w.Offset < conf.MaxFileSize {
		t.Errorf("文件大小应该超过限制: 期望 > %d, 实际 %d", conf.MaxFileSize, w.Offset)
	}
}

// TestWalBatchOperations 测试批量操作
func TestWalBatchOperations(t *testing.T) {
	dir := t.TempDir()
	conf := config.DefaultConfig(dir)

	// 创建WAL实例
	w, err := NewWal(1, conf)
	if err != nil {
		t.Fatalf("创建WAL失败: %v", err)
	}
	defer w.Close()

	// 创建索引
	idx, err := index.NewBTreeIndex(32)
	if err != nil {
		t.Fatalf("创建索引失败: %v", err)
	}

	fmt.Println("开始批量写入测试...")
	// 第一阶段：批量写入100条记录
	positions := make(map[string]*record.RecordPos)
	expectedValues := make(map[string][]byte)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("batch-key-%d", i))
		value := []byte(fmt.Sprintf("batch-value-%d", i))
		rec := record.NewRecordUpdate(key, value)

		pos, err := w.Write(rec)
		if err != nil {
			t.Fatalf("写入记录失败 [%d]: %v", i, err)
		}

		positions[string(key)] = pos
		expectedValues[string(key)] = value

		// 更新索引
		if err := idx.Put(key, pos); err != nil {
			t.Fatalf("更新索引失败 [%d]: %v", i, err)
		}
	}
	fmt.Printf("成功写入 100 条记录\n")

	// 第二阶段：随机更新30条记录
	fmt.Println("开始随机更新测试...")
	for i := 0; i < 30; i++ {
		keyNum := i * 3 // 更新每第三条记录
		key := []byte(fmt.Sprintf("batch-key-%d", keyNum))
		value := []byte(fmt.Sprintf("batch-value-%d-updated", keyNum))
		rec := record.NewRecordUpdate(key, value)

		pos, err := w.Write(rec)
		if err != nil {
			t.Fatalf("更新记录失败 [%d]: %v", i, err)
		}

		positions[string(key)] = pos
		expectedValues[string(key)] = value

		// 更新索引
		if err := idx.Put(key, pos); err != nil {
			t.Fatalf("更新索引失败 [%d]: %v", i, err)
		}
	}
	fmt.Printf("成功更新 30 条记录\n")

	// 第三阶段：随机删除20条记录
	fmt.Println("开始随机删除测试...")
	for i := 0; i < 20; i++ {
		keyNum := i * 5 // 删除每第五条记录
		key := []byte(fmt.Sprintf("batch-key-%d", keyNum))
		rec := record.NewRecordDelete(key)

		_, err := w.Write(rec)
		if err != nil {
			t.Fatalf("删除记录失败 [%d]: %v", i, err)
		}

		delete(expectedValues, string(key))

		// 更新索引
		if err := idx.Delete(key); err != nil {
			t.Fatalf("删除索引失败 [%d]: %v", i, err)
		}
	}
	fmt.Printf("成功删除 20 条记录\n")

	// 同步数据到磁盘
	if err := w.Sync(); err != nil {
		t.Fatalf("同步数据失败: %v", err)
	}

	// 第四阶段：验证所有记录
	fmt.Println("开始验证数据...")
	for key, expectedValue := range expectedValues {
		pos, err := idx.Get([]byte(key))
		if err != nil {
			t.Errorf("获取key %s 失败: %v", key, err)
			continue
		}
		if pos == nil {
			t.Errorf("key %s 应该存在于索引中", key)
			continue
		}

		_, _, value, err := w.Read(pos)
		if err != nil {
			t.Errorf("读取key %s 的值失败: %v", key, err)
			continue
		}

		if !bytes.Equal(value, expectedValue) {
			t.Errorf("key %s 的值不匹配, 期望 %v, 实际 %v", key, expectedValue, value)
		}
	}
	fmt.Printf("成功验证现有记录\n")

	// 第五阶段：验证已删除的记录
	fmt.Println("开始验证已删除记录...")
	for i := 0; i < 20; i++ {
		keyNum := i * 5
		key := []byte(fmt.Sprintf("batch-key-%d", keyNum))
		pos, err := idx.Get(key)
		if err != nil {
			t.Errorf("获取已删除的key失败: %v", err)
			continue
		}
		if pos != nil {
			t.Errorf("已删除的key %s 不应该存在于索引中", string(key))
		}
	}
	fmt.Printf("成功验证已删除记录\n")

	// 第六阶段：测试恢复
	fmt.Println("开始测试数据恢复...")
	newIdx, err := index.NewBTreeIndex(32)
	if err != nil {
		t.Fatalf("创建新索引失败: %v", err)
	}

	if err := w.RestoreIndex(newIdx); err != nil {
		t.Fatalf("恢复索引失败: %v", err)
	}

	// 验证恢复后的数据
	for key, expectedValue := range expectedValues {
		pos, err := newIdx.Get([]byte(key))
		if err != nil {
			t.Errorf("恢复后获取key %s 失败: %v", key, err)
			continue
		}
		if pos == nil {
			t.Errorf("恢复后key %s 应该存在于索引中", key)
			continue
		}

		_, _, value, err := w.Read(pos)
		if err != nil {
			t.Errorf("恢复后读取key %s 的值失败: %v", key, err)
			continue
		}

		if !bytes.Equal(value, expectedValue) {
			t.Errorf("恢复后key %s 的值不匹配, 期望 %v, 实际 %v", key, expectedValue, value)
		}
	}
	fmt.Printf("成功完成数据恢复验证\n")
}
