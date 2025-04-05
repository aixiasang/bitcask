package redis

import (
	"strconv"
	"time"
)

// 定义Redis数据类型
const (
	TypeString = "string"
	TypeList   = "list"
	TypeHash   = "hash"
	TypeSet    = "set"
	TypeZSet   = "zset"
)

// 为不同数据类型定义前缀，用于在Bitcask中存储
const (
	KeyTypePrefx    = "_type_" // 存储键类型
	KeyExpirePrefx  = "_ttl_"  // 存储键过期时间
	ListItemPrefx   = "_list_" // 列表项
	HashFieldPrefx  = "_hash_" // 哈希字段
	SetMemberPrefx  = "_set_"  // 集合成员
	ZSetScorePrefx  = "_zset_" // 有序集合分数
	ZSetMemberPrefx = "_zsm_"  // 有序集合成员
)

// encodeListKey 编码列表键名
func encodeListKey(key string, index int) string {
	return ListItemPrefx + key + ":" + strconv.Itoa(index)
}

// encodeHashKey 编码哈希键名
func encodeHashKey(key string, field string) string {
	return HashFieldPrefx + key + ":" + field
}

// encodeSetKey 编码集合键名
func encodeSetKey(key string, member string) string {
	return SetMemberPrefx + key + ":" + member
}

// encodeZSetScoreKey 编码有序集合分数键名
func encodeZSetScoreKey(key string, member string) string {
	return ZSetScorePrefx + key + ":" + member
}

// encodeZSetMemberKey 编码有序集合成员键名
func encodeZSetMemberKey(key string, score float64) string {
	// 格式化分数确保排序正确
	scoreStr := strconv.FormatFloat(score, 'f', 17, 64)
	return ZSetMemberPrefx + key + ":" + scoreStr
}

// encodeKeyType 编码键类型
func encodeKeyType(key string) string {
	return KeyTypePrefx + key
}

// encodeKeyExpire 编码键过期时间
func encodeKeyExpire(key string) string {
	return KeyExpirePrefx + key
}

// ZSetPair 有序集合的成员-分数对
type ZSetPair struct {
	Member string
	Score  float64
}

// ZSetPairs 有序集合的成员-分数对数组，用于排序
type ZSetPairs []ZSetPair

func (p ZSetPairs) Len() int           { return len(p) }
func (p ZSetPairs) Less(i, j int) bool { return p[i].Score < p[j].Score }
func (p ZSetPairs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// isExpired 检查键是否过期
func isExpired(ttlBytes []byte) bool {
	if len(ttlBytes) == 0 {
		return false
	}

	expireAt, err := strconv.ParseInt(string(ttlBytes), 10, 64)
	if err != nil {
		return false
	}

	return time.Now().Unix() > expireAt
}
