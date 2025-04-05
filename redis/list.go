package redis

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
)

// LPUSH命令处理
func (s *Server) handleLPush(conn redcon.Conn, key []byte, values [][]byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if ok {
		// 键已存在，检查类型
		if string(keyTypeBytes) != TypeList {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return
		}
	} else {
		// 键不存在，设置类型为列表
		s.bc.Put([]byte(encodeKeyType(keyStr)), []byte(TypeList))
	}

	// 获取当前列表长度
	length := s.getListLength(keyStr)

	// 在头部插入元素（前插法）
	for i := len(values) - 1; i >= 0; i-- {
		// 将值插入到索引0
		for j := length; j > 0; j-- {
			// 移动现有元素
			oldValue, ok := s.bc.Get([]byte(encodeListKey(keyStr, j-1)))
			if ok {
				s.bc.Put([]byte(encodeListKey(keyStr, j)), oldValue)
			}
		}

		// 插入新值到索引0
		s.bc.Put([]byte(encodeListKey(keyStr, 0)), values[i])
		length++
	}

	conn.WriteInt(length)
}

// RPUSH命令处理
func (s *Server) handleRPush(conn redcon.Conn, key []byte, values [][]byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if ok {
		// 键已存在，检查类型
		if string(keyTypeBytes) != TypeList {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return
		}
	} else {
		// 键不存在，设置类型为列表
		s.bc.Put([]byte(encodeKeyType(keyStr)), []byte(TypeList))
	}

	// 获取当前列表长度
	length := s.getListLength(keyStr)

	// 在尾部插入元素
	for _, value := range values {
		s.bc.Put([]byte(encodeListKey(keyStr, length)), value)
		length++
	}

	conn.WriteInt(length)
}

// LPOP命令处理
func (s *Server) handleLPop(conn redcon.Conn, key []byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeList {
		conn.WriteNull()
		return
	}

	// 获取当前列表长度
	length := s.getListLength(keyStr)

	if length == 0 {
		conn.WriteNull()
		return
	}

	// 获取第一个元素
	value, ok := s.bc.Get([]byte(encodeListKey(keyStr, 0)))
	if !ok {
		conn.WriteNull()
		return
	}

	// 移动其他元素
	for i := 0; i < length-1; i++ {
		nextValue, ok := s.bc.Get([]byte(encodeListKey(keyStr, i+1)))
		if ok {
			s.bc.Put([]byte(encodeListKey(keyStr, i)), nextValue)
		}
	}

	// 删除最后一个元素
	s.bc.Delete([]byte(encodeListKey(keyStr, length-1)))

	// 如果列表为空，删除类型标记
	if length == 1 {
		s.bc.Delete([]byte(encodeKeyType(keyStr)))
	}

	conn.WriteBulk(value)
}

// RPOP命令处理
func (s *Server) handleRPop(conn redcon.Conn, key []byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeList {
		conn.WriteNull()
		return
	}

	// 获取当前列表长度
	length := s.getListLength(keyStr)

	if length == 0 {
		conn.WriteNull()
		return
	}

	// 获取最后一个元素
	lastIndex := length - 1
	value, ok := s.bc.Get([]byte(encodeListKey(keyStr, lastIndex)))
	if !ok {
		conn.WriteNull()
		return
	}

	// 删除最后一个元素
	s.bc.Delete([]byte(encodeListKey(keyStr, lastIndex)))

	// 如果列表为空，删除类型标记
	if length == 1 {
		s.bc.Delete([]byte(encodeKeyType(keyStr)))
	}

	conn.WriteBulk(value)
}

// LLEN命令处理
func (s *Server) handleLLen(conn redcon.Conn, key []byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeList {
		conn.WriteInt(0)
		return
	}

	// 获取列表长度
	length := s.getListLength(keyStr)
	conn.WriteInt(length)
}

// LRANGE命令处理
func (s *Server) handleLRange(conn redcon.Conn, key, start, stop []byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeList {
		conn.WriteArray(0)
		return
	}

	// 获取列表长度
	length := s.getListLength(keyStr)

	// 解析开始和结束索引
	startIdx, err := strconv.Atoi(string(start))
	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR 无效的开始索引: %v", err))
		return
	}

	stopIdx, err := strconv.Atoi(string(stop))
	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR 无效的结束索引: %v", err))
		return
	}

	// 处理负索引（从尾部计数）
	if startIdx < 0 {
		startIdx = length + startIdx
	}
	if stopIdx < 0 {
		stopIdx = length + stopIdx
	}

	// 确保索引在有效范围内
	if startIdx < 0 {
		startIdx = 0
	}
	if stopIdx >= length {
		stopIdx = length - 1
	}

	// 如果开始索引大于结束索引或超出范围，返回空数组
	if startIdx > stopIdx || startIdx >= length {
		conn.WriteArray(0)
		return
	}

	// 收集范围内的元素
	elements := make([][]byte, 0, stopIdx-startIdx+1)
	for i := startIdx; i <= stopIdx; i++ {
		value, ok := s.bc.Get([]byte(encodeListKey(keyStr, i)))
		if ok {
			elements = append(elements, value)
		}
	}

	// 写入数组响应
	conn.WriteArray(len(elements))
	for _, element := range elements {
		conn.WriteBulk(element)
	}
}

// 获取列表长度的辅助函数
func (s *Server) getListLength(key string) int {
	prefix := ListItemPrefx + key + ":"
	length := 0

	// 扫描计数列表元素
	s.bc.Scan(func(k []byte, _ []byte) error {
		if strings.HasPrefix(string(k), prefix) {
			parts := strings.Split(string(k), ":")
			if len(parts) == 2 {
				idx, err := strconv.Atoi(parts[1])
				if err == nil && idx >= length {
					length = idx + 1
				}
			}
		}
		return nil
	})

	return length
}
