# 🔧 命令行工具

Bitcask 的命令行工具模块，提供了丰富的命令行接口用于管理和操作 Bitcask 存储引擎。

## 🌟 功能概述

本模块基于 [spf13/cobra](https://github.com/spf13/cobra) 构建，提供以下功能：

- 🖥️ 服务启动与管理
- 🗄️ 数据操作与维护
- 🌐 集群管理
- 🧰 实用工具

## 📊 命令结构

```
bitcask
├── server      - 启动各种服务器
│   ├── redis   - 启动 Redis 兼容服务器
│   ├── http    - 启动 HTTP API 服务器
│   └── raft    - 启动 Raft 分布式节点
├── get         - 获取键值
├── set         - 设置键值
├── del         - 删除键值
├── keys        - 列出所有键
├── ttl         - 查看键的剩余过期时间
├── expire      - 设置键的过期时间
├── hint        - 生成 hint 文件
├── merge       - 执行数据文件合并
├── backup      - 备份数据
├── restore     - 恢复数据
├── benchmark   - 性能基准测试
└── admin       - 管理功能
    ├── stats   - 查看存储统计信息
    └── compact - 压缩数据文件
```

## 🚀 主要命令详解

### 🖥️ 服务器命令

#### 🔄 Redis 服务器

启动 Redis 兼容服务器：

```bash
bitcask server redis --addr :6379 --data-dir ./data
```

选项：
- `--addr` - 服务监听地址，默认 `:6379`
- `--data-dir` - 数据存储目录
- `--password` - 可选的认证密码

#### 🌐 HTTP 服务器

启动 HTTP API 服务器：

```bash
bitcask server http --addr :8080 --data-dir ./data
```

选项：
- `--addr` - 服务监听地址，默认 `:8080`
- `--data-dir` - 数据存储目录
- `--cors` - 是否启用跨域请求支持

#### 🔄 Raft 服务器

启动 Raft 分布式节点：

```bash
bitcask server raft --id 1 --addr :9001 --peers "2=:9002,3=:9003" --data-dir ./raft-node1
```

选项：
- `--id` - 节点 ID
- `--addr` - 节点地址
- `--peers` - 集群其他节点地址
- `--data-dir` - 数据存储目录
- `--join` - 是否以加入模式启动

### 🗄️ 数据操作命令

#### ⬆️ 设置键值

```bash
bitcask set mykey "Hello World" --ttl 3600
```

选项：
- `--ttl` - 过期时间（秒）
- `--data-dir` - 数据目录路径

#### ⬇️ 获取键值

```bash
bitcask get mykey
```

选项：
- `--data-dir` - 数据目录路径

#### 🗑️ 删除键值

```bash
bitcask del mykey
```

选项：
- `--data-dir` - 数据目录路径

#### 📋 列出所有键

```bash
bitcask keys
```

选项：
- `--pattern` - 匹配模式（支持通配符）
- `--data-dir` - 数据目录路径

#### ⏱️ 设置过期时间

```bash
bitcask expire mykey 3600
```

选项：
- `--data-dir` - 数据目录路径

### 🧹 数据维护命令

#### 📝 生成 Hint 文件

```bash
bitcask hint --data-dir ./data
```

选项：
- `--data-dir` - 数据目录路径

#### 🔄 合并数据文件

```bash
bitcask merge --data-dir ./data
```

选项：
- `--data-dir` - 数据目录路径
- `--threshold` - 触发合并的文件数阈值

#### 💾 备份数据

```bash
bitcask backup --data-dir ./data --target ./backup.tar.gz
```

选项：
- `--data-dir` - 数据目录路径
- `--target` - 备份文件路径

#### 📥 恢复数据

```bash
bitcask restore --source ./backup.tar.gz --target ./restored-data
```

选项：
- `--source` - 备份文件路径
- `--target` - 恢复目标目录

### 📊 性能测试命令

```bash
bitcask benchmark --ops 100000 --value-size 1024 --threads 4
```

选项：
- `--ops` - 操作次数
- `--value-size` - 值大小（字节）
- `--threads` - 线程数
- `--read-write-ratio` - 读写比例

### ⚙️ 管理命令

#### 📈 查看统计信息

```bash
bitcask admin stats --data-dir ./data
```

输出包括：
- 键数量
- 磁盘占用
- 数据文件数量
- 内存占用

#### 🗜️ 压缩数据文件

```bash
bitcask admin compact --data-dir ./data
```

选项：
- `--data-dir` - 数据目录路径
- `--force` - 强制执行，忽略文件数阈值

## 🌐 全局选项

所有命令支持以下全局选项：

- `--config` - 配置文件路径
- `--verbose` - 启用详细日志
- `--log-level` - 日志级别（debug, info, warn, error）
- `--log-file` - 日志文件路径

## ⚙️ 配置文件

可以通过 YAML 文件提供配置，例如：

```yaml
# bitcask.yaml
data-dir: /var/lib/bitcask
log-level: info
log-file: /var/log/bitcask.log

server:
  redis:
    addr: :6379
    password: ""
  
  http:
    addr: :8080
    cors: true
```

## 📝 使用示例

### 🚀 启动服务并设置数据

```bash
# 启动 Redis 服务
bitcask server redis --data-dir ./data &

# 设置一些测试数据
bitcask set user:1 '{"name":"John","age":30}' --ttl 3600
bitcask set user:2 '{"name":"Alice","age":25}' --ttl 3600

# 获取数据
bitcask get user:1

# 列出所有键
bitcask keys --pattern "user:*"
```

### 🧹 数据维护

```bash
# 合并数据文件
bitcask merge --data-dir ./data

# 备份数据
bitcask backup --data-dir ./data --target ./backup.tar.gz

# 查看统计信息
bitcask admin stats --data-dir ./data
``` 