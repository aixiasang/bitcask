package redis

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aixiasang/bitcask"
	"github.com/tidwall/redcon"
)

// Server 表示Redis协议兼容的服务器
type Server struct {
	bc        *bitcask.Bitcask
	addr      string
	redServer *redcon.Server
	closeChan chan struct{}
}

// NewServer 创建新的Redis服务器
func NewServer(bc *bitcask.Bitcask, addr string) *Server {
	return &Server{
		bc:        bc,
		addr:      addr,
		closeChan: make(chan struct{}),
	}
}

// Start 启动Redis服务器
func (s *Server) Start() error {
	// 打印启动信息
	fmt.Printf("Redis兼容服务已启动，监听地址: %s\n", s.addr)
	fmt.Println("可以使用标准Redis客户端进行连接")
	fmt.Println("支持的命令: GET, SET, DEL, KEYS, INFO, PING")
	fmt.Println("以及: EXPIRE, TTL, LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE")
	fmt.Println("哈希命令: HSET, HGET, HDEL, HGETALL, HKEYS, HEXISTS")
	fmt.Println("集合命令: SADD, SREM, SMEMBERS, SISMEMBER")
	fmt.Println("有序集合: ZADD, ZRANGE, ZRANK, ZSCORE")
	fmt.Println("按 Ctrl+C 可安全退出服务")

	// 创建一个redcon服务器
	s.redServer = redcon.NewServer(s.addr, s.handleCommand,
		func(conn redcon.Conn) bool {
			// 连接接受回调
			log.Printf("Redis客户端已连接: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// 连接关闭回调
			if err != nil {
				log.Printf("Redis客户端连接错误: %v", err)
			}
			log.Printf("Redis客户端已断开连接: %s", conn.RemoteAddr())
		},
	)

	// 处理系统信号以优雅关闭
	go s.handleSignals()

	// 启动服务器
	err := s.redServer.ListenAndServe()
	if err != nil {
		return fmt.Errorf("启动Redis服务失败: %v", err)
	}

	return nil
}

// Stop 停止Redis服务器
func (s *Server) Stop() error {
	close(s.closeChan)
	if s.redServer != nil {
		s.redServer.Close()
	}
	return nil
}

// 处理操作系统信号
func (s *Server) handleSignals() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\n接收到中断信号，正在优雅关闭Redis服务...")
	s.Stop()
}

// 处理Redis命令
func (s *Server) handleCommand(conn redcon.Conn, cmd redcon.Command) {
	// 将命令转为大写
	command := strings.ToUpper(string(cmd.Args[0]))

	// 不再在这里检查键是否过期，而是在各个命令处理函数中检查
	// 这样可以避免不必要的检查，并且确保在需要的时候进行检查

	switch command {
	case "PING":
		conn.WriteString("PONG")
	case "QUIT":
		conn.WriteString("OK")
		conn.Close()
	case "INFO":
		s.handleInfo(conn)

	// 字符串命令
	case "GET":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR GET命令需要一个参数")
			return
		}
		s.handleGet(conn, cmd.Args[1])
	case "SET":
		// SET key value [EX seconds|PX milliseconds]
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR SET命令需要至少两个参数")
			return
		}
		s.handleSet(conn, cmd.Args)
	case "DEL":
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR DEL命令需要至少一个参数")
			return
		}
		s.handleDel(conn, cmd.Args[1:])
	case "KEYS":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR KEYS命令需要一个参数")
			return
		}
		s.handleKeys(conn, cmd.Args[1])

	// 过期时间命令
	case "EXPIRE":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR EXPIRE命令需要两个参数")
			return
		}
		s.handleExpire(conn, cmd.Args[1], cmd.Args[2])
	case "TTL":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR TTL命令需要一个参数")
			return
		}
		s.handleTTL(conn, cmd.Args[1])

	// 列表命令
	case "LPUSH":
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR LPUSH命令需要至少两个参数")
			return
		}
		s.handleLPush(conn, cmd.Args[1], cmd.Args[2:])
	case "RPUSH":
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR RPUSH命令需要至少两个参数")
			return
		}
		s.handleRPush(conn, cmd.Args[1], cmd.Args[2:])
	case "LPOP":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR LPOP命令需要一个参数")
			return
		}
		s.handleLPop(conn, cmd.Args[1])
	case "RPOP":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR RPOP命令需要一个参数")
			return
		}
		s.handleRPop(conn, cmd.Args[1])
	case "LLEN":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR LLEN命令需要一个参数")
			return
		}
		s.handleLLen(conn, cmd.Args[1])
	case "LRANGE":
		if len(cmd.Args) != 4 {
			conn.WriteError("ERR LRANGE命令需要三个参数")
			return
		}
		s.handleLRange(conn, cmd.Args[1], cmd.Args[2], cmd.Args[3])

	// 哈希命令
	case "HSET":
		if len(cmd.Args) < 4 || len(cmd.Args)%2 != 0 {
			conn.WriteError("ERR HSET命令格式错误，需要偶数个参数")
			return
		}
		s.handleHSet(conn, cmd.Args[1], cmd.Args[2:])
	case "HGET":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR HGET命令需要两个参数")
			return
		}
		s.handleHGet(conn, cmd.Args[1], cmd.Args[2])
	case "HDEL":
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR HDEL命令需要至少两个参数")
			return
		}
		s.handleHDel(conn, cmd.Args[1], cmd.Args[2:])
	case "HGETALL":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR HGETALL命令需要一个参数")
			return
		}
		s.handleHGetAll(conn, cmd.Args[1])
	case "HKEYS":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR HKEYS命令需要一个参数")
			return
		}
		s.handleHKeys(conn, cmd.Args[1])
	case "HEXISTS":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR HEXISTS命令需要两个参数")
			return
		}
		s.handleHExists(conn, cmd.Args[1], cmd.Args[2])

	// 集合命令
	case "SADD":
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR SADD命令需要至少两个参数")
			return
		}
		s.handleSAdd(conn, cmd.Args[1], cmd.Args[2:])
	case "SREM":
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR SREM命令需要至少两个参数")
			return
		}
		s.handleSRem(conn, cmd.Args[1], cmd.Args[2:])
	case "SMEMBERS":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR SMEMBERS命令需要一个参数")
			return
		}
		s.handleSMembers(conn, cmd.Args[1])
	case "SISMEMBER":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR SISMEMBER命令需要两个参数")
			return
		}
		s.handleSIsMember(conn, cmd.Args[1], cmd.Args[2])

	// 有序集合命令
	case "ZADD":
		if len(cmd.Args) < 4 || (len(cmd.Args)-2)%2 != 0 {
			conn.WriteError("ERR ZADD命令格式错误，需要偶数个参数")
			return
		}
		s.handleZAdd(conn, cmd.Args[1], cmd.Args[2:])
	case "ZRANGE":
		if len(cmd.Args) < 4 {
			conn.WriteError("ERR ZRANGE命令需要至少三个参数")
			return
		}
		s.handleZRange(conn, cmd.Args)
	case "ZRANK":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR ZRANK命令需要两个参数")
			return
		}
		s.handleZRank(conn, cmd.Args[1], cmd.Args[2])
	case "ZSCORE":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR ZSCORE命令需要两个参数")
			return
		}
		s.handleZScore(conn, cmd.Args[1], cmd.Args[2])

	default:
		conn.WriteError(fmt.Sprintf("ERR 不支持的命令: %s", command))
	}
}

// 检查并移除过期键
func (s *Server) checkAndRemoveExpired(key string) bool {
	ttlKey := encodeKeyExpire(key)
	ttlBytes, ok := s.bc.Get([]byte(ttlKey))
	if !ok {
		return false // 键不存在或无过期时间
	}

	if isExpired(ttlBytes) {
		// 键已过期，删除相关数据
		keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(key)))
		if ok {
			keyType := string(keyTypeBytes)

			// 根据键类型执行不同的删除策略
			switch keyType {
			case TypeString:
				s.bc.Delete([]byte(key))
			case TypeList, TypeHash, TypeSet, TypeZSet:
				// 对于复杂数据类型，需要扫描并删除所有相关键
				prefix := ""
				switch keyType {
				case TypeList:
					prefix = ListItemPrefx + key
				case TypeHash:
					prefix = HashFieldPrefx + key
				case TypeSet:
					prefix = SetMemberPrefx + key
				case TypeZSet:
					prefix = ZSetScorePrefx + key
				}

				if prefix != "" {
					s.bc.Scan(func(k []byte, _ []byte) error {
						if strings.HasPrefix(string(k), prefix) {
							s.bc.Delete(k)
						}
						return nil
					})
				}

				// 对于有序集合，还需要删除成员键
				if keyType == TypeZSet {
					prefix = ZSetMemberPrefx + key
					s.bc.Scan(func(k []byte, _ []byte) error {
						if strings.HasPrefix(string(k), prefix) {
							s.bc.Delete(k)
						}
						return nil
					})
				}
			}
		} else {
			// 可能是字符串类型但未设置类型标记
			s.bc.Delete([]byte(key))
		}

		// 删除类型标记和过期时间标记
		s.bc.Delete([]byte(encodeKeyType(key)))
		s.bc.Delete([]byte(ttlKey))
		return true // 键已删除
	}
	return false // 键未过期
}

// GET命令处理
func (s *Server) handleGet(conn redcon.Conn, key []byte) {
	keyStr := string(key)

	// 检查键是否过期
	if s.checkAndRemoveExpired(keyStr) {
		conn.WriteNull()
		return
	}

	// 检查键类型
	keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if ok {
		keyType := string(keyTypeBytes)
		if keyType != TypeString {
			conn.WriteError(fmt.Sprintf("WRONGTYPE Operation against a key holding the wrong kind of value"))
			return
		}
	}

	value, ok := s.bc.Get(key)
	if !ok {
		conn.WriteNull()
		return
	}
	conn.WriteBulk(value)
}

// SET命令处理
func (s *Server) handleSet(conn redcon.Conn, args [][]byte) {
	key := string(args[1])
	value := args[2]

	// 设置键类型为字符串
	s.bc.Put([]byte(encodeKeyType(key)), []byte(TypeString))

	// 写入键值
	if err := s.bc.Put(args[1], value); err != nil {
		conn.WriteError(fmt.Sprintf("ERR 存储值失败: %v", err))
		return
	}

	// 处理可选的过期时间参数
	if len(args) > 3 {
		option := strings.ToUpper(string(args[3]))
		if option == "EX" && len(args) >= 5 {
			// 过期时间（秒）
			seconds, err := strconv.ParseInt(string(args[4]), 10, 64)
			if err != nil {
				conn.WriteError(fmt.Sprintf("ERR 无效的过期时间: %v", err))
				return
			}
			expireAt := time.Now().Unix() + seconds
			s.bc.Put([]byte(encodeKeyExpire(key)), []byte(strconv.FormatInt(expireAt, 10)))
		} else if option == "PX" && len(args) >= 5 {
			// 过期时间（毫秒）
			millis, err := strconv.ParseInt(string(args[4]), 10, 64)
			if err != nil {
				conn.WriteError(fmt.Sprintf("ERR 无效的过期时间: %v", err))
				return
			}
			expireAt := time.Now().Unix() + (millis / 1000)
			s.bc.Put([]byte(encodeKeyExpire(key)), []byte(strconv.FormatInt(expireAt, 10)))
		}
	}

	conn.WriteString("OK")
}

// DEL命令处理
func (s *Server) handleDel(conn redcon.Conn, keys [][]byte) {
	var deleted int
	for _, keyBytes := range keys {
		key := string(keyBytes)

		// 检查键类型
		keyTypeBytes, ok := s.bc.Get([]byte(encodeKeyType(key)))
		if !ok {
			continue // 键不存在
		}

		keyType := string(keyTypeBytes)

		// 根据键类型执行不同的删除策略
		switch keyType {
		case TypeString:
			if err := s.bc.Delete(keyBytes); err == nil {
				deleted++
			}
		case TypeList, TypeHash, TypeSet, TypeZSet:
			// 对于复杂数据类型，需要扫描并删除所有相关键
			prefix := ""
			switch keyType {
			case TypeList:
				prefix = ListItemPrefx + key
			case TypeHash:
				prefix = HashFieldPrefx + key
			case TypeSet:
				prefix = SetMemberPrefx + key
			case TypeZSet:
				prefix = ZSetScorePrefx + key
			}

			if prefix != "" {
				s.bc.Scan(func(k []byte, _ []byte) error {
					if strings.HasPrefix(string(k), prefix) {
						s.bc.Delete(k)
					}
					return nil
				})
				deleted++
			}

			// 对于有序集合，还需要删除成员键
			if keyType == TypeZSet {
				prefix = ZSetMemberPrefx + key
				s.bc.Scan(func(k []byte, _ []byte) error {
					if strings.HasPrefix(string(k), prefix) {
						s.bc.Delete(k)
					}
					return nil
				})
			}
		}

		// 删除类型标记和过期时间标记
		s.bc.Delete([]byte(encodeKeyType(key)))
		s.bc.Delete([]byte(encodeKeyExpire(key)))
	}
	conn.WriteInt(deleted)
}

// KEYS命令处理
func (s *Server) handleKeys(conn redcon.Conn, pattern []byte) {
	patternStr := string(pattern)
	isAllKeys := patternStr == "*"

	// 收集匹配的键
	var matchedKeys [][]byte
	seen := make(map[string]bool)

	// 使用Scan遍历所有键
	err := s.bc.Scan(func(key []byte, _ []byte) error {
		keyStr := string(key)

		// 跳过特殊前缀的键（用于内部存储）
		if strings.HasPrefix(keyStr, KeyTypePrefx) ||
			strings.HasPrefix(keyStr, KeyExpirePrefx) ||
			strings.HasPrefix(keyStr, ListItemPrefx) ||
			strings.HasPrefix(keyStr, HashFieldPrefx) ||
			strings.HasPrefix(keyStr, SetMemberPrefx) ||
			strings.HasPrefix(keyStr, ZSetScorePrefx) ||
			strings.HasPrefix(keyStr, ZSetMemberPrefx) {
			return nil
		}

		// 检查是否已添加过该键
		if !seen[keyStr] {
			// 检查是否过期
			ttlBytes, ok := s.bc.Get([]byte(encodeKeyExpire(keyStr)))
			if ok && isExpired(ttlBytes) {
				// 键已过期，不包含在结果中
				return nil
			}

			// 如果是*或者键包含模式，则添加到结果中
			if isAllKeys || strings.Contains(keyStr, patternStr) {
				matchedKeys = append(matchedKeys, key)
				seen[keyStr] = true
			}
		}
		return nil
	})

	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR 扫描键失败: %v", err))
		return
	}

	// 写入数组响应
	conn.WriteArray(len(matchedKeys))
	for _, key := range matchedKeys {
		conn.WriteBulk(key)
	}
}

// EXPIRE命令处理
func (s *Server) handleExpire(conn redcon.Conn, key, seconds []byte) {
	// 检查键是否存在
	exists := false
	keyStr := string(key)

	_, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if ok {
		exists = true
	} else {
		// 检查是否是原始字符串键
		_, ok := s.bc.Get(key)
		if ok {
			exists = true
			// 设置键类型为字符串
			s.bc.Put([]byte(encodeKeyType(keyStr)), []byte(TypeString))
		}
	}

	if !exists {
		conn.WriteInt(0) // 键不存在
		return
	}

	// 解析过期时间
	secs, err := strconv.ParseInt(string(seconds), 10, 64)
	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR 无效的过期时间: %v", err))
		return
	}

	// 计算过期时间戳
	expireAt := time.Now().Unix() + secs

	// 存储过期时间
	err = s.bc.Put([]byte(encodeKeyExpire(keyStr)), []byte(strconv.FormatInt(expireAt, 10)))
	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR 设置过期时间失败: %v", err))
		return
	}

	conn.WriteInt(1) // 成功设置
}

// TTL命令处理
func (s *Server) handleTTL(conn redcon.Conn, key []byte) {
	keyStr := string(key)

	// 如果键已过期，则删除并返回-2
	if s.checkAndRemoveExpired(keyStr) {
		conn.WriteInt(-2) // 键不存在（已过期）
		return
	}

	// 检查键是否存在
	exists := false
	_, ok := s.bc.Get([]byte(encodeKeyType(keyStr)))
	if ok {
		exists = true
	} else {
		// 检查是否是原始字符串键
		_, ok := s.bc.Get(key)
		if ok {
			exists = true
		}
	}

	if !exists {
		conn.WriteInt(-2) // 键不存在
		return
	}

	// 获取过期时间
	ttlBytes, ok := s.bc.Get([]byte(encodeKeyExpire(keyStr)))
	if !ok {
		conn.WriteInt(-1) // 键永不过期
		return
	}

	// 解析过期时间戳
	expireAt, err := strconv.ParseInt(string(ttlBytes), 10, 64)
	if err != nil {
		conn.WriteInt(-1) // 无法解析过期时间
		return
	}

	// 计算剩余时间
	ttl := expireAt - time.Now().Unix()
	if ttl <= 0 {
		// 键已过期，执行删除
		s.checkAndRemoveExpired(keyStr)
		conn.WriteInt(-2) // 键不存在（已过期）
		return
	}

	conn.WriteInt(int(ttl))
}

// 以下为下一轮实现的更多Redis命令的处理函数...

// INFO命令处理
func (s *Server) handleInfo(conn redcon.Conn) {
	info := fmt.Sprintf(
		"# Server\r\n" +
			"redis_mode:standalone\r\n" +
			"bitcask_compatible:yes\r\n" +
			"redis_version:5.0.0\r\n" +
			"# Stats\r\n" +
			"connected_clients:1\r\n" +
			"# Command Stats\r\n" +
			"# Keyspace\r\n",
	)
	conn.WriteBulkString(info)
}
