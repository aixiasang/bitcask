package redis

import (
	"strings"

	"github.com/tidwall/redcon"
)

// HSET命令处理
func (s *Server) handleHSet(conn redcon.Conn, key []byte, args [][]byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if ok {
		// 键已存在，检查类型
		if string(keyTypeBytes) != TypeHash {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return
		}
	} else {
		// 键不存在，设置类型为哈希
		s.bc.Put([]byte(encodeKeyType(keyStr)), []byte(TypeHash))
	}

	// 确保参数是偶数个（字段-值对）
	if len(args)%2 != 0 {
		conn.WriteError("ERR wrong number of arguments for HSET")
		return
	}

	// 设置字段-值对
	fieldsSet := 0
	for i := 0; i < len(args); i += 2 {
		field := string(args[i])
		value := args[i+1]

		// 检查字段是否已存在
		_, ok := s.bc.Get([]byte(encodeHashKey(keyStr, field)))
		isNew := !ok

		// 设置哈希字段值
		s.bc.Put([]byte(encodeHashKey(keyStr, field)), value)

		// 仅计数新增字段
		if isNew {
			fieldsSet++
		}
	}

	conn.WriteInt(fieldsSet)
}

// HGET命令处理
func (s *Server) handleHGet(conn redcon.Conn, key []byte, field []byte) {
	keyStr := string(key)
	fieldStr := string(field)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeHash {
		conn.WriteNull()
		return
	}

	// 获取哈希字段值
	value, ok := s.bc.Get([]byte(encodeHashKey(keyStr, fieldStr)))
	if !ok {
		conn.WriteNull()
		return
	}

	conn.WriteBulk(value)
}

// HDEL命令处理
func (s *Server) handleHDel(conn redcon.Conn, key []byte, fields [][]byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeHash {
		conn.WriteInt(0)
		return
	}

	// 删除字段
	deleted := 0
	for _, field := range fields {
		fieldStr := string(field)
		fieldKey := encodeHashKey(keyStr, fieldStr)

		// 检查字段是否存在
		_, ok := s.bc.Get([]byte(fieldKey))
		if ok {
			// 删除字段
			s.bc.Delete([]byte(fieldKey))
			deleted++
		}
	}

	// 如果所有字段都已删除，也删除哈希类型标记
	if s.getHashFieldCount(keyStr) == 0 {
		s.bc.Delete([]byte(encodeKeyType(keyStr)))
	}

	conn.WriteInt(deleted)
}

// HGETALL命令处理
func (s *Server) handleHGetAll(conn redcon.Conn, key []byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeHash {
		conn.WriteArray(0)
		return
	}

	// 收集所有字段和值
	prefix := HashFieldPrefx + keyStr + ":"
	var fieldsAndValues [][]byte

	s.bc.Scan(func(k []byte, v []byte) error {
		kStr := string(k)
		if strings.HasPrefix(kStr, prefix) {
			// 提取字段名
			field := kStr[len(prefix):]

			// 添加字段和值到结果
			fieldsAndValues = append(fieldsAndValues, []byte(field), v)
		}
		return nil
	})

	// 写入数组响应
	conn.WriteArray(len(fieldsAndValues))
	for _, item := range fieldsAndValues {
		conn.WriteBulk(item)
	}
}

// HKEYS命令处理
func (s *Server) handleHKeys(conn redcon.Conn, key []byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeHash {
		conn.WriteArray(0)
		return
	}

	// 收集所有字段
	prefix := HashFieldPrefx + keyStr + ":"
	var fields [][]byte

	s.bc.Scan(func(k []byte, _ []byte) error {
		kStr := string(k)
		if strings.HasPrefix(kStr, prefix) {
			// 提取字段名
			field := kStr[len(prefix):]
			fields = append(fields, []byte(field))
		}
		return nil
	})

	// 写入数组响应
	conn.WriteArray(len(fields))
	for _, field := range fields {
		conn.WriteBulk(field)
	}
}

// HEXISTS命令处理
func (s *Server) handleHExists(conn redcon.Conn, key []byte, field []byte) {
	keyStr := string(key)
	fieldStr := string(field)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeHash {
		conn.WriteInt(0)
		return
	}

	// 检查字段是否存在
	_, ok = s.bc.Get([]byte(encodeHashKey(keyStr, fieldStr)))
	if !ok {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

// 获取哈希表字段数的辅助函数
func (s *Server) getHashFieldCount(key string) int {
	prefix := HashFieldPrefx + key + ":"
	count := 0

	// 扫描计数哈希字段
	s.bc.Scan(func(k []byte, _ []byte) error {
		if strings.HasPrefix(string(k), prefix) {
			count++
		}
		return nil
	})

	return count
}
