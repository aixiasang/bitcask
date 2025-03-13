package index

import (
	"github.com/aixiasang/bitcask/record"
)

type Index interface {
	Put(key []byte, pos *record.RecordPos) error // 插入
	Get(key []byte) (*record.RecordPos, error)   // 获取
	Delete(key []byte) error                     // 删除
	Iterator(asc bool) Iterator                  // 迭代器
}

type Iterator interface {
	Rewind() error            // 重置
	Seek(key []byte) error    // 移动到指定键
	Next()                    // 下一个
	Prev()                    // 上一个
	Key() []byte              // 键
	Value() *record.RecordPos // 值
	Valid() bool              // 有效
	Close() error             // 关闭
	AllKey() [][]byte
}
