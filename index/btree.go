package index

import (
	"bytes"
	"sort"
	"sync"

	"github.com/aixiasang/bitcask/record"
	"github.com/google/btree"
)

type IndexItem struct {
	key []byte
	pos *record.RecordPos
}

func (i *IndexItem) Less(than btree.Item) bool {
	return bytes.Compare(i.key, than.(*IndexItem).key) < 0
}

type BTreeIndex struct {
	tree   *btree.BTree // 树
	degree int          // 度
	mu     sync.RWMutex // 互斥锁
}

func (i *BTreeIndex) Iterator(asc bool) Iterator {
	return NewBtreeIterator(i, asc)
}

func NewBTreeIndex(degree int) (Index, error) {
	return &BTreeIndex{tree: btree.New(degree), degree: degree}, nil
}

func (i *BTreeIndex) Put(key []byte, pos *record.RecordPos) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.tree.ReplaceOrInsert(&IndexItem{key: key, pos: pos})
	return nil
}

func (i *BTreeIndex) Get(key []byte) (*record.RecordPos, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	// Create a temporary item with the search key
	item := i.tree.Get(&IndexItem{key: key})
	if item == nil {
		return nil, nil
	}
	return item.(*IndexItem).pos, nil
}

func (i *BTreeIndex) Delete(key []byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.tree.Delete(&IndexItem{key: key})
	return nil
}

type BtreeIterator struct {
	data   []IndexItem
	asc    bool
	idx    int
	length int
	mu     sync.RWMutex
}

func (i *BtreeIterator) AllKey() [][]byte {
	i.mu.RLock()
	defer i.mu.RUnlock()

	keys := make([][]byte, 0, i.length)
	for _, item := range i.data {
		keys = append(keys, item.key)
	}
	return keys
}

func NewBtreeIterator(i *BTreeIndex, asc bool) *BtreeIterator {
	i.mu.RLock()
	defer i.mu.RUnlock()

	length := i.tree.Len()
	data := make([]IndexItem, 0, length)

	iter := func(item btree.Item) bool {
		data = append(data, *item.(*IndexItem))
		return true
	}

	if asc {
		i.tree.Ascend(iter)
	} else {
		i.tree.Descend(iter)
	}
	return &BtreeIterator{
		data:   data,
		asc:    asc,
		idx:    0,
		length: length,
	}
}
func (i *BtreeIterator) Rewind() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.idx = 0
	return nil
}

func (i *BtreeIterator) Seek(key []byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.asc {
		i.idx = sort.Search(len(i.data), func(idx int) bool {
			return bytes.Compare(i.data[idx].key, key) >= 0
		})
	} else {
		i.idx = sort.Search(len(i.data), func(idx int) bool {
			return bytes.Compare(i.data[idx].key, key) <= 0
		})
	}
	return nil
}

func (i *BtreeIterator) Next() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.idx++
}
func (i *BtreeIterator) Prev() {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.idx > 0 {
		i.idx--
	}
}

func (i *BtreeIterator) Key() []byte {
	i.mu.RLock()
	defer i.mu.RUnlock()
	// 由调用者先调用 Valid() 来确保索引有效
	return i.data[i.idx].key
}

func (i *BtreeIterator) Value() *record.RecordPos {
	i.mu.RLock()
	defer i.mu.RUnlock()
	// 由调用者先调用 Valid() 来确保索引有效
	return i.data[i.idx].pos
}

func (i *BtreeIterator) Valid() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.idx >= 0 && i.idx < i.length
}
func (i *BtreeIterator) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.data = nil
	return nil
}
