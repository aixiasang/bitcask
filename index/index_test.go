package index

import (
	"testing"

	"github.com/aixiasang/bitcask/record"
	"github.com/stretchr/testify/assert"
)

func TestNewIndex(t *testing.T) {
	// 测试默认创建BTree索引
	index := NewIndex(IndexTypeBTree)
	assert.NotNil(t, index)

	// 测试基本功能是否正常
	testKey := []byte("test_factory_key")
	testPos := &record.Pos{FileId: 100, Offset: 200, Length: 300}

	// 测试插入
	err := index.Put(testKey, testPos)
	assert.NoError(t, err)

	// 测试获取
	pos, err := index.Get(testKey)
	assert.NoError(t, err)
	assert.Equal(t, testPos, pos)

	// 测试不存在的索引类型回退到默认
	defaultIndex := NewIndex(IndexType(99))
	assert.NotNil(t, defaultIndex)

	// 测试SkipList当前返回nil
	skipListIndex := NewIndex(IndexTypeSkipList)
	assert.Nil(t, skipListIndex, "SkipList索引尚未实现")
}

func TestIndexTypeConstants(t *testing.T) {
	// 测试索引类型常量
	assert.Equal(t, IndexType(0), IndexTypeBTree)
	assert.Equal(t, IndexType(1), IndexTypeSkipList)

	// 测试索引类型之间的差异
	assert.NotEqual(t, IndexTypeBTree, IndexTypeSkipList)
}
