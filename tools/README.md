# 🛠️ Bitcask 测试工具

本目录包含用于测试 Bitcask 各个模块的工具程序。

## 🔄 Redis 测试工具

### 🧪 redis-test.go

Redis 兼容层的测试程序，用于验证 Bitcask 的 Redis 接口功能。

#### ✨ 功能特性

- 🔌 连接到 Bitcask Redis 服务
- 🔍 测试各种 Redis 命令和数据类型
- 📊 展示命令结果和错误信息

#### 📋 支持的测试

- 🧪 基础命令：PING, INFO
- 📝 字符串操作：GET, SET
- 📋 列表操作：LPUSH, LRANGE, LPOP
- 📑 哈希表操作：HSET, HGET, HGETALL
- 🔢 集合操作：SADD, SISMEMBER, SMEMBERS
- ⏱️ 过期时间：EXPIRE, TTL

#### 🚀 使用方法

```bash
# 确保 Bitcask Redis 服务已在 6380 端口运行
go run tools/redis-test.go
```

### ⏰ expire-test

过期键测试工具，专门验证 Redis 过期时间功能。

#### ✨ 功能特性

- ⏱️ 检查键是否正确过期
- 👁️ 监控键的存在状态
- 📊 显示实时剩余的键

#### 🚀 使用方法

```bash
# 确保 Bitcask Redis 服务已在 6380 端口运行
go run tools/expire-test/main.go
```

## 📝 输出示例

redis-test.go 输出示例：

```
已成功连接到Redis服务器
PING结果: PONG
INFO返回的服务器信息:
# Server
redis_mode:standalone
bitcask_compatible:yes
redis_version:5.0.0
# Stats
connected_clients:1
# Command Stats
# Keyspace

=== 测试字符串操作 ===
GET test-key = test-value

=== 测试列表操作 ===
列表包含 3 个元素: [item1 item2 item3]
从列表头部弹出: item1

=== 测试哈希表操作 ===
用户名: 张三
用户信息:
  age: 30
  name: 张三
  email: zhangsan@example.com

=== 测试集合操作 ===
Go 是集合成员: true
集合包含 4 个元素: [Go C++ Java Python]

=== 测试过期时间 ===
temp-key 将在 5 秒后过期

测试完成
```

expire-test 输出示例：

```
已成功连接到Redis服务器
temp-key 已经过期，无法获取

=== 列出所有键 ===
找到1个键:
1: test-key

测试完成
```

## ⚠️ 注意事项

- 测试工具需要 Redis 服务器已经运行
- 默认连接本地的 6380 端口
- 可以修改源码中的连接设置以适应不同环境 