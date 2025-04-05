package redis

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
)

// ZADD命令处理
func (s *Server) handleZAdd(conn redcon.Conn, key []byte, args [][]byte) {
	keyStr := string(key)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if ok {
		// 键已存在，检查类型
		if string(keyTypeBytes) != TypeZSet {
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			return
		}
	} else {
		// 键不存在，设置类型为有序集合
		s.bc.Put([]byte(encodeKeyType(keyStr)), []byte(TypeZSet))
	}

	// 确保参数是偶数个（分数-成员对）
	if len(args)%2 != 0 {
		conn.WriteError("ERR wrong number of arguments for ZADD")
		return
	}

	// 添加分数-成员对
	added := 0
	for i := 0; i < len(args); i += 2 {
		scoreStr := string(args[i])
		member := string(args[i+1])

		// 解析分数
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			conn.WriteError(fmt.Sprintf("ERR 无效的分数值'%s'", scoreStr))
			return
		}

		// 检查成员是否已存在
		oldScoreBytes, ok := s.bc.Get([]byte(encodeZSetScoreKey(keyStr, member)))
		if ok {
			// 成员已存在，更新分数
			oldScore, _ := strconv.ParseFloat(string(oldScoreBytes), 64)

			// 删除旧的成员与分数的关联
			s.bc.Delete([]byte(encodeZSetMemberKey(keyStr, oldScore)))
		} else {
			// 新成员
			added++
		}

		// 设置成员的分数
		s.bc.Put([]byte(encodeZSetScoreKey(keyStr, member)), []byte(strconv.FormatFloat(score, 'f', 17, 64)))

		// 设置分数对应的成员
		s.bc.Put([]byte(encodeZSetMemberKey(keyStr, score)), []byte(member))
	}

	conn.WriteInt(added)
}

// ZRANGE命令处理
func (s *Server) handleZRange(conn redcon.Conn, args [][]byte) {
	keyStr := string(args[1])

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeZSet {
		conn.WriteArray(0)
		return
	}

	// 解析开始和结束索引
	start, err := strconv.Atoi(string(args[2]))
	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR 无效的起始索引'%s'", string(args[2])))
		return
	}

	stop, err := strconv.Atoi(string(args[3]))
	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR 无效的结束索引'%s'", string(args[3])))
		return
	}

	// 检查是否需要带分数（WITHSCORES选项）
	withScores := false
	if len(args) > 4 && strings.ToUpper(string(args[4])) == "WITHSCORES" {
		withScores = true
	}

	// 获取所有成员及分数
	pairs := s.getSortedZSetMembers(keyStr)

	// 处理负索引（从尾部计数）
	length := len(pairs)
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// 确保索引在有效范围内
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}

	// 如果开始索引大于结束索引或超出范围，返回空数组
	if start > stop || start >= length {
		conn.WriteArray(0)
		return
	}

	// 准备结果
	resultLen := (stop - start + 1)
	if withScores {
		resultLen *= 2
	}

	// 写入数组响应
	conn.WriteArray(resultLen)
	for i := start; i <= stop; i++ {
		conn.WriteBulk([]byte(pairs[i].Member))
		if withScores {
			conn.WriteBulkString(strconv.FormatFloat(pairs[i].Score, 'f', 17, 64))
		}
	}
}

// ZRANK命令处理
func (s *Server) handleZRank(conn redcon.Conn, key []byte, member []byte) {
	keyStr := string(key)
	memberStr := string(member)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeZSet {
		conn.WriteNull()
		return
	}

	// 获取成员的分数
	_, ok = s.bc.Get([]byte(encodeZSetScoreKey(keyStr, memberStr)))
	if !ok {
		conn.WriteNull() // 成员不存在
		return
	}

	// 获取所有成员及分数
	pairs := s.getSortedZSetMembers(keyStr)

	// 查找成员的排名
	for i, pair := range pairs {
		if pair.Member == memberStr {
			conn.WriteInt(i) // 返回排名（0-based）
			return
		}
	}

	conn.WriteNull() // 理论上不应该发生，因为我们已经找到了分数
}

// ZSCORE命令处理
func (s *Server) handleZScore(conn redcon.Conn, key []byte, member []byte) {
	keyStr := string(key)
	memberStr := string(member)

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if !ok || string(keyTypeBytes) != TypeZSet {
		conn.WriteNull()
		return
	}

	// 获取成员的分数
	scoreBytes, ok := s.bc.Get([]byte(encodeZSetScoreKey(keyStr, memberStr)))
	if !ok {
		conn.WriteNull() // 成员不存在
		return
	}

	conn.WriteBulkString(string(scoreBytes))
}

// 获取有序集合的所有成员及分数（已排序）
func (s *Server) getSortedZSetMembers(key string) ZSetPairs {
	var pairs ZSetPairs
	prefix := ZSetScorePrefx + key + ":"

	// 收集所有成员及其分数
	s.bc.Scan(func(k []byte, v []byte) error {
		kStr := string(k)
		if strings.HasPrefix(kStr, prefix) {
			// 提取成员名和分数
			member := kStr[len(prefix):]
			score, _ := strconv.ParseFloat(string(v), 64)

			pairs = append(pairs, ZSetPair{
				Member: member,
				Score:  score,
			})
		}
		return nil
	})

	// 按分数排序
	sort.Sort(pairs)

	return pairs
}
