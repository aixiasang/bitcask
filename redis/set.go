package redis

import (
	"strings"

	"github.com/tidwall/redcon"
)

// SADD命令处理
func (s *Server) handleSAdd(conn redcon.Conn, key []byte, members [][]byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if ok {
		// 键已存在，检查类型
		if string(keyTypeBytes) != TypeSet {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return
		}
	} else {
		// 键不存在，设置类型为集合
		s.bc.Put([]byte(encodeKeyType(keyStr)), []byte(TypeSet))
	}

	// 添加成员到集合
	added := 0
	for _, member := range members {
		memberStr := string(member)
		memberKey := encodeSetKey(keyStr, memberStr)

		// 检查成员是否已存在
		_, ok := s.bc.Get([]byte(memberKey))
		if !ok {
			// 成员不存在，添加
			s.bc.Put([]byte(memberKey), []byte{1}) // 使用1作为存在标记
			added++
		}
	}

	conn.WriteInt(added)
}

// SREM命令处理
func (s *Server) handleSRem(conn redcon.Conn, key []byte, members [][]byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeSet {
		conn.WriteInt(0)
		return
	}

	// 从集合中删除成员
	removed := 0
	for _, member := range members {
		memberStr := string(member)
		memberKey := encodeSetKey(keyStr, memberStr)

		// 检查成员是否存在
		_, ok := s.bc.Get([]byte(memberKey))
		if ok {
			// 成员存在，删除
			s.bc.Delete([]byte(memberKey))
			removed++
		}
	}

	// 如果集合为空，删除类型标记
	if s.getSetSize(keyStr) == 0 {
		s.bc.Delete([]byte(encodeKeyType(keyStr)))
	}

	conn.WriteInt(removed)
}

// SMEMBERS命令处理
func (s *Server) handleSMembers(conn redcon.Conn, key []byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeSet {
		conn.WriteArray(0)
		return
	}

	// 收集所有集合成员
	prefix := SetMemberPrefx + keyStr + ":"
	var members [][]byte

	s.bc.Scan(func(k []byte, _ []byte) error {
		kStr := string(k)
		if strings.HasPrefix(kStr, prefix) {
			// 提取成员名
			member := kStr[len(prefix):]
			members = append(members, []byte(member))
		}
		return nil
	})

	// 写入数组响应
	conn.WriteArray(len(members))
	for _, member := range members {
		conn.WriteBulk(member)
	}
}

// SISMEMBER命令处理
func (s *Server) handleSIsMember(conn redcon.Conn, key []byte, member []byte) {
	keyStr := string(key)
	memberStr := string(member)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeSet {
		conn.WriteInt(0)
		return
	}

	// 检查成员是否存在
	_, ok = s.bc.Get([]byte(encodeSetKey(keyStr, memberStr)))
	if !ok {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

// 获取集合大小的辅助函数
func (s *Server) getSetSize(key string) int {
	prefix := SetMemberPrefx + key + ":"
	count := 0

	// 扫描计数集合成员
	s.bc.Scan(func(k []byte, _ []byte) error {
		if strings.HasPrefix(string(k), prefix) {
			count++
		}
		return nil
	})

	return count
}
