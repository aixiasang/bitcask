package index

import (
	"sync"

	"github.com/aixiasang/bitcask/inner/record"
	"github.com/google/btree"
)

// BTreeIndex 使用 Google BTree 实现的索引
type BTreeIndex struct {
	tree *btree.BTree // 使用 Google BTree 实现的 BTree
	mu   sync.RWMutex // 添加读写锁保证并发安全
}

// item 实现 btree.Item 接口
type item struct {
	key []byte
	pos *record.Pos
}

// Less 实现 btree.Item 接口
func (i item) Less(than btree.Item) bool {
	other := than.(item)
	return string(i.key) < string(other.key)
}

// NewBTreeIndex 创建一个新的 BTree 索引
func NewBTreeIndex(order int) *BTreeIndex {
	return &BTreeIndex{
		tree: btree.New(order), // 使用2阶B树，Google BTree 推荐使用2阶
		mu:   sync.RWMutex{},
	}
}

// Put 插入或更新键值对
func (b *BTreeIndex) Put(key []byte, pos *record.Pos) error {
	b.mu.Lock() // 写操作加写锁
	defer b.mu.Unlock()

	b.tree.ReplaceOrInsert(item{key: key, pos: pos})
	return nil
}

// Get 获取键对应的位置信息
func (b *BTreeIndex) Get(key []byte) (*record.Pos, error) {
	b.mu.RLock() // 读操作加读锁
	defer b.mu.RUnlock()

	value := b.tree.Get(item{key: key})
	if value == nil {
		return nil, nil
	}
	return value.(item).pos, nil
}

// Delete 删除键值对
func (b *BTreeIndex) Delete(key []byte) error {
	b.mu.Lock() // 写操作加写锁
	defer b.mu.Unlock()

	b.tree.Delete(item{key: key})
	return nil
}

// Scan 扫描指定范围内的键值对
func (b *BTreeIndex) Scan(startKey, endKey []byte) ([]*Data, error) {
	b.mu.RLock() // 读操作加读锁
	defer b.mu.RUnlock()

	var results []*Data

	// 遍历 B 树
	b.tree.Ascend(func(i btree.Item) bool {
		item := i.(item)
		if string(item.key) >= string(startKey) && string(item.key) <= string(endKey) {
			data := &Data{
				Key: string(item.key),
				Pos: *item.pos,
			}
			results = append(results, data)
		}
		return true
	})

	return results, nil
}

// Foreach 对每个键值对执行指定的函数
func (b *BTreeIndex) Foreach(fn func(key []byte, pos *record.Pos) error) error {
	b.mu.RLock() // 读操作加读锁
	defer b.mu.RUnlock()

	var err error
	b.tree.Ascend(func(i btree.Item) bool {
		item := i.(item)
		err = fn(item.key, item.pos)
		// 如果出现错误，停止遍历
		if err != nil {
			return false
		}
		return true
	})

	return err
}

// ForeachUnSafe 对每个键值对执行指定的函数
func (b *BTreeIndex) ForeachUnSafe(fn func(key []byte, pos *record.Pos) error) error {
	var err error
	b.tree.Ascend(func(i btree.Item) bool {
		item := i.(item)
		err = fn(item.key, item.pos)
		// 如果出现错误，停止遍历
		if err != nil {
			return false
		}
		return true
	})

	return err
}

// Close 关闭索引
func (b *BTreeIndex) Close() error {
	b.mu.Lock() // 写操作加写锁
	defer b.mu.Unlock()

	// Google BTree 不需要特殊的清理操作
	return nil
}
