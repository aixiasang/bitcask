# 🔄 Redis 适配层

Bitcask 的 Redis 协议兼容模块，允许使用标准 Redis 客户端直接连接到 Bitcask 存储引擎。

## ✨ 功能特性

- 🔄 完全兼容 Redis 协议，支持常用的 Redis 命令
- 🗄️ 基于 Bitcask 存储引擎，提供高性能、持久化的存储服务
- 🏗️ 支持多种数据结构：字符串、列表、哈希表、集合、有序集合
- ⏱️ 支持过期时间设置

## 📋 支持的命令

### 🧪 基础命令
- `PING` - 检测服务器连接状态
- `INFO` - 获取服务器信息
- `KEYS` - 查找所有匹配的键

### 📝 字符串操作
- `GET` - 获取值
- `SET` - 设置值
- `DEL` - 删除键

### ⏰ 过期时间
- `EXPIRE` - 设置键的过期时间
- `TTL` - 获取键的剩余过期时间

### 📋 列表操作
- `LPUSH` - 从列表左侧插入元素
- `RPUSH` - 从列表右侧插入元素
- `LPOP` - 弹出列表最左边的元素
- `RPOP` - 弹出列表最右边的元素
- `LLEN` - 获取列表长度
- `LRANGE` - 获取列表指定范围的元素

### 📑 哈希表操作
- `HSET` - 设置哈希表字段的值
- `HGET` - 获取哈希表指定字段的值
- `HDEL` - 删除哈希表的一个或多个字段
- `HGETALL` - 获取哈希表中所有的字段和值
- `HKEYS` - 获取哈希表中的所有字段
- `HEXISTS` - 检查哈希表中是否存在指定的字段

### 🔢 集合操作
- `SADD` - 添加集合元素
- `SREM` - 移除集合元素
- `SMEMBERS` - 获取集合中的所有元素
- `SISMEMBER` - 判断元素是否是集合的成员

### 📊 有序集合操作
- `ZADD` - 添加有序集合元素
- `ZRANGE` - 通过索引区间返回有序集合的成员
- `ZRANK` - 返回有序集合中成员的排名
- `ZSCORE` - 返回有序集合中成员的分数值

## 🚀 使用方法

### 🏁 作为独立服务启动

```bash
bitcask redis --addr :6379 --data-dir ./data
```

### 💻 在应用程序中嵌入

```go
import (
    "github.com/aixiasang/bitcask"
    "github.com/aixiasang/bitcask/config"
    "github.com/aixiasang/bitcask/redis"
)

// 创建配置
conf := config.NewConfig()
conf.DataDir = "./data"

// 创建 Bitcask 实例
bc, err := bitcask.NewBitcask(conf)
if err != nil {
    // 处理错误
}

// 创建 Redis 服务器
server := redis.NewServer(bc, ":6379")

// 启动服务器
if err := server.Start(); err != nil {
    // 处理错误
}

// 停止服务器
server.Stop()
```

## 📝 使用示例

### 字符串操作
```
SET user:1 "John Doe"
GET user:1                // 返回: "John Doe"
SET user:1 "Jane Smith"   // 更新值
SET visitor:count 42 EX 3600  // 设置1小时过期
DEL user:1                // 删除键
```

### 哈希表操作
```
HSET user:2 name "Alice" age "30" city "New York"
HGET user:2 name          // 返回: "Alice"
HGETALL user:2            // 返回所有字段和值
HDEL user:2 city          // 删除city字段
```

### 列表操作
```
LPUSH messages "Hello"
RPUSH messages "World"
LRANGE messages 0 -1      // 返回: ["Hello", "World"]
LPOP messages             // 返回并删除: "Hello"
```

### 集合和有序集合
```
SADD tags "golang" "database" "nosql"
SMEMBERS tags             // 返回所有元素
SISMEMBER tags "golang"   // 检查元素是否存在

ZADD scores 100 "Alice" 95 "Bob" 80 "Charlie"
ZRANGE scores 0 -1        // 返回所有成员（按分数排序）
ZRANK scores "Bob"        // 返回成员排名
```

## ⚡ 性能特性

- 📝 基于 Bitcask 的日志结构存储，写入性能高
- 🧠 全内存索引，读取性能高
- 💾 数据持久化到磁盘，提供数据安全性
- 🔍 优化的范围查询支持

## ⚠️ 限制

- 🚫 不支持事务类操作（MULTI/EXEC）
- 🚫 不支持发布/订阅功能
- 🚫 不支持Lua脚本

## 🧪 测试

可以使用标准 Redis 客户端（如 redis-cli）或我们提供的测试工具连接和测试服务器：

```bash
go run tools/redis-test.go
```

## 📢 注意事项

- GET 命令现在返回 ([]byte, bool) 而不是 ([]byte, error)，适配了最新的 Bitcask 接口
- 使用 DELETE 命令可能会同时删除键的所有相关元数据（类型标记和过期时间） 