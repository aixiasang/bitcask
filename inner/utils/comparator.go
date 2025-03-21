package utils

import (
	"bytes"
)

// KeyComparator 是一个用于比较键的结构体
type KeyComparator struct{}

// NewKeyComparator 创建一个新的键比较器
func NewKeyComparator() *KeyComparator {
	return &KeyComparator{}
}

// Compare 比较两个字节数组，返回:
// -1 如果 a < b
//
//	0 如果 a = b
//	1 如果 a > b
//
// 比较规则：先比较长度，如果长度不同则长度小的更小
// 如果长度相同，则按字节顺序比较内容
func (kc *KeyComparator) Compare(a, b []byte) int {
	// 先比较长度
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	// 长度相同，比较内容
	return bytes.Compare(a, b)
}

// Equal 判断两个字节数组是否相等
func (kc *KeyComparator) Equal(a, b []byte) bool {
	return len(a) == len(b) && bytes.Equal(a, b)
}

// Less 判断 a 是否小于 b
func (kc *KeyComparator) Less(a, b []byte) bool {
	// 先比较长度
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	// 长度相同，比较内容
	return bytes.Compare(a, b) < 0
}

// Greater 判断 a 是否大于 b
func (kc *KeyComparator) Greater(a, b []byte) bool {
	// 先比较长度
	if len(a) != len(b) {
		return len(a) > len(b)
	}
	// 长度相同，比较内容
	return bytes.Compare(a, b) > 0
}

// LessOrEqual 判断 a 是否小于等于 b
func (kc *KeyComparator) LessOrEqual(a, b []byte) bool {
	return kc.Less(a, b) || kc.Equal(a, b)
}

// GreaterOrEqual 判断 a 是否大于等于 b
func (kc *KeyComparator) GreaterOrEqual(a, b []byte) bool {
	return kc.Greater(a, b) || kc.Equal(a, b)
}

// InRange 判断key是否在[start, end]范围内
func (kc *KeyComparator) InRange(key, start, end []byte) bool {
	return kc.GreaterOrEqual(key, start) && kc.LessOrEqual(key, end)
}
