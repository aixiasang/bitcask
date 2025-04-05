package index

import (
	"github.com/aixiasang/bitcask/record"
)

type Index interface {
	Put(key []byte, pos *record.Pos) error
	Get(key []byte) (*record.Pos, error)
	Delete(key []byte) error
	Scan(startKey, endKey []byte) ([]*Data, error)
	Foreach(fn func(key []byte, pos *record.Pos) error) error
	ForeachUnSafe(fn func(key []byte, pos *record.Pos) error) error
	Close() error
}

type Data struct {
	Key string     `json:"key"`
	Pos record.Pos `json:"pos"`
}
type IndexType uint8

const (
	IndexTypeBTree IndexType = iota
	IndexTypeSkipList
)

// NewIndex 创建一个新的索引实例
func NewIndex(typ IndexType) Index {
	switch typ {
	case IndexTypeBTree:
		return NewBTreeIndex(32) // 默认使用32阶B树
	case IndexTypeSkipList:
		// 待实现
		return nil
	default:
		return NewBTreeIndex(32) // 默认使用BTree索引
	}
}
