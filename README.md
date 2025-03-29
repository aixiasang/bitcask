# 🗄️ Bitcask 存储系统

一个基于Bitcask论文实现的高性能键值存储系统，具有简单、高效、持久化和崩溃恢复的特性。

## ✨ 特性

- 🚀 高性能读写操作：写入复杂度O(1)，读取复杂度O(1)
- 💾 数据持久化：所有操作都会被记录到WAL文件
- 🛡️ 崩溃恢复：通过hint文件和WAL文件确保数据不丢失
- 🔄 自动文件轮转：防止单个数据文件过大
- 🔒 并发安全：支持多线程并发访问
- 🧹 数据合并：支持清理过期数据，优化存储空间
- 💼 事务支持：提供批量操作和原子提交功能
- 🔍 范围查询：高效的键范围扫描和结果限制功能
- 📊 SQL支持：内置SQL解析和执行引擎，支持基本SQL语法

## 🏗️ 架构设计

Bitcask存储系统基于以下核心概念：

1. **📝 写入追加(Append-Only)**：所有写操作都以追加方式写入活跃的WAL文件
2. **🧠 内存索引**：键到磁盘位置的映射保存在内存中，确保快速查询
3. **🔒 不可变文件**：旧的WAL文件是不可变的，确保数据一致性
4. **📔 Hint文件**：保存索引信息，加速启动过程
5. **💼 事务处理**：通过批处理和事务ID支持原子性操作
6. **🔠 键比较器**：统一的键比较逻辑，确保范围查询的准确性
7. **🔍 SQL引擎**：将SQL查询转换为底层存储操作，实现关系数据模型


## 📝 使用示例

### 🔑 作为键值存储使用

```go
package main

import (
    "fmt"
    "github.com/aixiasang/bitcask/inner"
    "github.com/aixiasang/bitcask/inner/config"
)

func main() {
    // 创建默认配置
    conf := config.NewConfig()
    conf.DataDir = "./data"
    
    // 创建Bitcask实例
    db, err := bitcask.NewBitcask(conf)
    if err != nil {
        panic(err)
    }
    defer db.Close()
    
    // 写入数据
    if err := db.Put([]byte("hello"), []byte("world")); err != nil {
        panic(err)
    }
    
    // 读取数据
    value, err := db.Get([]byte("hello"))
    if err != nil {
        panic(err)
    }
    fmt.Printf("Value: %s\n", string(value))
    
    // 删除数据
    if err := db.Delete([]byte("hello")); err != nil {
        panic(err)
    }
    
    // 使用事务批量处理
    batch := bitcask.NewBatch(db)
    
    // 批量写入
    batch.Put([]byte("key1"), []byte("value1"))
    batch.Put([]byte("key2"), []byte("value2"))
    batch.Put([]byte("key3"), []byte("value3"))
    
    // 批量删除
    batch.Delete([]byte("key3"))
    
    // 原子提交所有操作
    if err := batch.Commit(); err != nil {
        panic(err)
    }
    
    // 范围查询示例
    start := []byte("key1")
    end := []byte("key9")
    
    // 无限制范围查询
    results, err := db.ScanRange(start, end)
    if err != nil {
        panic(err)
    }
    
    for _, item := range results {
        fmt.Printf("Key: %s, Value: %s\n", string(item.Key), string(item.Value))
    }
    
    // 限制结果数量的范围查询
    limitedResults, err := db.ScanRangeLimit(start, end, 10)
    if err != nil {
        panic(err)
    }
    
    fmt.Printf("Found %d items in range\n", len(limitedResults))
}
```

### 📊 使用SQL接口

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/aixiasang/bitcask/sql"
)

func main() {
    // 创建SQL引擎
    engine := sql.NewEngine()
    
    // 打开数据库
    err := engine.Open("./mydb")
    if err != nil {
        log.Fatal("Failed to open database:", err)
    }
    defer engine.Close()
    
    // 创建表
    result, err := engine.Execute(`
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR NOT NULL,
            age INT,
            is_active BOOLEAN DEFAULT true
        )
    `)
    if err != nil {
        log.Fatal("Failed to create table:", err)
    }
    fmt.Println(result)
    
    // 插入数据
    result, err = engine.Execute(`
        INSERT INTO users (id, name, age) VALUES 
        (1, 'Alice', 30),
        (2, 'Bob', 25),
        (3, 'Charlie', 35)
    `)
    if err != nil {
        log.Fatal("Failed to insert data:", err)
    }
    fmt.Println(result)
    
    // 查询数据
    result, err = engine.Execute(`
        SELECT id, name, age FROM users WHERE age > 25 ORDER BY age ASC
    `)
    if err != nil {
        log.Fatal("Failed to query data:", err)
    }
    fmt.Println(result)
}
```

## 📦 包结构

### 🧠 主包 [`bitcask`](./inner/README.md)

主包实现了Bitcask存储系统的核心功能，包括读取、写入、删除和合并等操作。

主要类型和函数：
- `Bitcask` - 核心存储引擎结构体
- `NewBitcask()` - 创建新的Bitcask实例
- `Put()` - 写入键值对
- `Get()` - 获取键对应的值
- `Delete()` - 删除键值对
- `Close()` - 关闭存储引擎，确保数据持久化
- `Merge()` - 合并数据文件，优化存储空间
- `Batch` - 批处理事务结构体
- `NewBatch()` - 创建新的批处理事务
- `ScanRange()` - 范围查询，返回指定范围内的所有键值对
- `ScanRangeLimit()` - 限制结果数量的范围查询

### ⚙️ 配置包 [`inner/config`](./inner/config/README.md)

提供系统配置相关功能，允许用户自定义Bitcask的行为。

主要类型和函数：
- `Config` - 存储系统配置结构体
- `NewConfig()` - 创建默认配置

配置选项包括：
- 数据目录路径
- WAL文件和Hint文件目录
- 最大文件大小
- BTree索引阶数
- 是否加载Hint文件
- 批处理大小限制
- 调试模式开关

### 🔍 索引包 [`inner/index`](./inner/index/README.md)

实现了高效的内存索引，支持快速查找键值对的磁盘位置。

主要类型和接口：
- `Index` - 索引接口，定义了索引操作
- `BTreeIndex` - 基于BTree的索引实现
- `Data` - 包含键和位置信息的数据结构

接口方法包括：
- `Put()` - 添加或更新索引
- `Get()` - 获取键对应的位置
- `Delete()` - 删除键的索引
- `Scan()` - 范围查询
- `Foreach()` - 遍历所有索引

### 📄 记录包 [`inner/record`](./inner/record/README.md)

定义了数据记录的格式和操作。

主要类型：
- `Record` - 数据记录结构体，包含键、值和记录类型
- `Pos` - 记录在磁盘上的位置信息
- `RecordType` - 记录类型（写入、删除、事务写入、事务删除或事务提交）

### 📝 WAL包 [`inner/wal`](./inner/wal/README.md)

实现了预写日志(Write-Ahead Log)机制，确保数据持久化和崩溃恢复。

主要类型和函数：
- `Wal` - WAL文件管理结构体
- `NewWal()` - 创建新的WAL文件
- `Write()` - 写入数据到WAL文件
- `WriteTxn()` - 写入事务相关数据
- `WriteTxnCommit()` - 写入事务提交标记
- `ReadPos()` - 从特定位置读取数据
- `ReadAll()` - 读取整个WAL文件
- `Close()` - 关闭WAL文件

### 🛠️ 工具包 [`inner/utils`](./inner/utils/README.md)

提供各种辅助函数，简化其他包的实现。

主要组件：
- `KeyComparator` - 键比较器，确保比较逻辑一致性
- 文件操作辅助函数
- 数据转换函数
- 测试辅助函数

### 🔤 SQL包 [`sql`](./sql/README.md)

提供SQL解析和执行功能，构建在Bitcask存储引擎之上。

主要组件：
- `Engine` - SQL引擎，处理SQL语句的解析和执行
- `token` - 定义SQL词法单元
- `lexer` - SQL词法分析器
- `ast` - SQL抽象语法树
- `parser` - SQL解析器
- `types` - SQL数据类型系统
- `plan` - 查询计划生成和优化
- `executor` - 查询执行器

支持的SQL功能：
- 表的创建和删除
- 数据插入、查询、更新和删除
- 条件过滤和排序
- 基本事务支持
- 查询计划解释

## 🔑 关键实现细节

### 🔄 数据恢复机制

系统通过以下步骤确保数据不丢失：

1. 首先尝试从Hint文件加载索引作为基础状态
2. 然后处理所有WAL文件以获取最新更新，即使存在Hint文件也会应用最新变更
3. 在关闭时生成新的Hint文件，用于下次启动

### 📂 文件管理策略

- 系统使用单个活跃WAL文件进行写入
- 当WAL文件大小超过阈值时，会自动轮转，创建新的WAL文件
- 旧的WAL文件保持不变，确保数据一致性
- 通过合并操作可以清理过期数据，优化存储空间

### 💼 事务支持

系统提供了批处理事务支持，具有以下特点：

- 原子性：批处理中的所有操作要么全部成功，要么全部失败
- 事务隔离：每个事务有唯一的事务ID，确保操作的隔离性
- 崩溃恢复：系统重启后能够正确恢复已提交的事务
- 批量操作：支持批量的Put和Delete操作，提高性能
- 大小限制：配置中的BatchSize参数控制单个批处理的最大大小

### 🔍 范围查询机制

系统实现了高效的范围查询功能：

- 统一比较器：使用KeyComparator确保所有地方的键比较逻辑一致
- 优化算法：先收集满足条件的键，然后排序并返回结果
- 结果限制：支持限制返回结果的数量，避免大范围查询消耗过多资源
- 提前终止：当扫描超出范围时及时终止，提高查询效率
- 排序保证：确保返回结果按照键的顺序排列

### 📊 SQL引擎

SQL模块建立在Bitcask键值存储之上，提供结构化数据访问：

- 关系模型：支持表、列和关系的定义与操作
- 词法分析与解析：将SQL文本转换为抽象语法树
- 查询优化：生成高效的查询执行计划
- 数据类型系统：支持整数、字符串、布尔值等基本类型
- 索引管理：通过底层键值存储实现高效索引
- 存储映射：将表和行数据映射到键值对
- 事务集成：与Bitcask事务机制集成，确保数据一致性

## 📜 许可证

[MIT License](LICENSE) 

# 🗄️ Bitcask 存储引擎

[![Go Reference](https://pkg.go.dev/badge/github.com/yourusername/bitcask.svg)](https://pkg.go.dev/github.com/yourusername/bitcask)
[![Go Report Card](https://goreportcard.com/badge/github.com/yourusername/bitcask)](https://goreportcard.com/report/github.com/yourusername/bitcask)
[![License](https://img.shields.io/github/license/yourusername/bitcask.svg)](https://github.com/yourusername/bitcask/blob/main/LICENSE)

Bitcask 是一个高性能、持久化的键值存储引擎，基于 Bitcask 论文实现，并进行了一系列优化和扩展。它提供简单易用的 API、可靠的数据持久化、快速的读写性能以及丰富的接口支持。

## ✨ 主要特性

- 🚀 **高性能**：基于顺序写入和内存索引的设计，提供极高的写入和读取性能
- 💾 **数据持久化**：所有写入操作即时持久化到磁盘，防止数据丢失
- 🔄 **自动恢复**：启动时自动从持久化数据恢复内存索引
- 🔌 **多种接口**：支持本地 Go API、Redis 协议、HTTP REST API
- 🌐 **分布式支持**：通过 Raft 共识算法实现多节点分布式存储
- 🏗️ **数据结构丰富**：支持字符串、列表、哈希表、集合等多种数据结构
- ⏱️ **键过期**：支持设置键的过期时间
- 🔄 **自动合并**：自动进行数据文件合并，节省磁盘空间
- ⚙️ **高度可配置**：丰富的配置选项，适应不同场景需求
- 🔧 **命令行工具**：便捷的命令行管理工具

## 📥 安装

### 🧪 从源码安装

```bash
git clone https://github.com/yourusername/bitcask.git
cd bitcask
go build -o bitcask ./cmd/bitcask
```

### 📦 使用 Go Get

```bash
go get -u github.com/yourusername/bitcask
```

## 🚀 快速开始

### 📚 作为库使用

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/yourusername/bitcask/inner"
)

func main() {
    // 创建一个新的 Bitcask 实例
    config := inner.NewConfig()
    config.DataDir = "./data"
    
    db, err := inner.Open(config)
    if err != nil {
        log.Fatalf("Failed to open DB: %v", err)
    }
    defer db.Close()
    
    // 设置键值对
    err = db.Put([]byte("hello"), []byte("world"))
    if err != nil {
        log.Fatalf("Failed to put: %v", err)
    }
    
    // 获取值
    value, err := db.Get([]byte("hello"))
    if err != nil {
        log.Fatalf("Failed to get: %v", err)
    }
    
    fmt.Printf("Value: %s\n", value)
    
    // 删除键
    err = db.Delete([]byte("hello"))
    if err != nil {
        log.Fatalf("Failed to delete: %v", err)
    }
}
```

### 🔄 启动 Redis 兼容服务器

```bash
bitcask server redis --addr :6379 --data-dir ./data
```

现在你可以使用任何 Redis 客户端连接到 Bitcask:

```bash
redis-cli -p 6379
127.0.0.1:6379> SET mykey "Hello Bitcask"
OK
127.0.0.1:6379> GET mykey
"Hello Bitcask"
127.0.0.1:6379> EXPIRE mykey 3600
(integer) 1
127.0.0.1:6379> TTL mykey
(integer) 3599
```

### 🌐 启动 HTTP API 服务器

```bash
bitcask server http --addr :8080 --data-dir ./data
```

使用 HTTP 客户端访问:

```bash
# 设置键值
curl -X PUT "http://localhost:8080/key/mykey" -H "Content-Type: application/json" -d '{"value":"Hello via HTTP"}'

# 获取键值
curl "http://localhost:8080/key/mykey"
```

## 📦 主要模块

Bitcask 由以下主要模块组成:

- [**🧠 inner**](inner/README.md): 核心存储引擎，实现了基本的键值存储功能
- [**🔄 redis**](redis/README.md): Redis 协议兼容层，实现 Redis 命令和数据结构
- [**🌐 http**](http/README.md): HTTP RESTful API 服务
- [**🔄 raft**](raft/README.md): 基于 Raft 算法的分布式共识实现
- [**🔧 cmd**](cmd/README.md): 命令行工具
- [**🛠️ tools**](tools/README.md): 测试和开发工具

## 🔍 核心概念

### 📝 数据存储模型

Bitcask 使用追加写入的日志结构来存储数据，具有以下特点:

1. **📝 顺序写入**: 所有写入操作都是顺序追加到当前活跃数据文件
2. **🧠 内存索引**: 所有键的位置信息保存在内存中，提供O(1)的查找性能
3. **🔒 不可变文件**: 写满的数据文件成为不可变文件，只进行读取操作
4. **📔 键目录**: 内存中维护从键到 {file_id, value_position, value_size} 的映射
5. **🧹 合并操作**: 定期合并数据文件，清理过期和删除的数据

### 📑 数据文件结构

```
+-------------+-------------+-------------+-------------+
| Record 1    | Record 2    | Record 3    | ...         |
+-------------+-------------+-------------+-------------+

Record:
+--------+--------+------------+-----------+-------+
| CRC    | Size   | Timestamp  | Key Size  | Key   | Value   |
| 4 bytes| 4 bytes| 8 bytes    | 4 bytes   | varies| varies  |
+--------+--------+------------+-----------+-------+
```

### 📝 Hint 文件

为加速启动时的索引重建，Bitcask 支持生成包含键位置信息的 hint 文件。

## ⚡ 性能优化

Bitcask 实现了多项性能优化:

- **📦 批量写入**: 支持批量操作，减少磁盘同步次数
- **🔍 键索引优化**: 支持 BTree 和 SkipList 两种索引实现
- **🔄 文件合并策略**: 智能的合并策略，平衡空间利用率和合并频率
- **🧠 内存映射**: 对只读数据文件采用 mmap 提高读取性能
- **🔒 并发控制**: 细粒度锁和无锁结构提高并发性能
- **📝 写前日志**: WAL 确保数据安全性同时提高性能

## 🚀 应用场景

Bitcask 适用于以下场景:

- **⚡ 高吞吐量键值存储**: 适合写入密集型工作负载
- **👤 会话存储**: 网站会话和用户状态存储
- **🔄 缓存系统**: 可持久化的缓存系统
- **📬 消息队列**: 简单的消息队列和任务队列
- **⚙️ 配置存储**: 分布式系统配置管理
- **🧩 嵌入式数据库**: 嵌入到其他应用程序中

## ⚠️ 限制

- **🧠 内存使用**: 所有键需驻留在内存中，不适合超大键空间
- **🔍 范围查询**: 不适合频繁的范围扫描操作
- **🔄 复杂查询**: 不支持复杂索引和查询

## 👨‍💻 贡献指南

欢迎贡献代码、报告问题或提出功能建议:

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

## 📜 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 🙏 致谢

- Bitcask 论文作者 - 提供了原始设计
- 所有贡献者和使用者

## 📞 联系方式

- 项目维护者: [Your Name](mailto:your.email@example.com)
- Twitter: [@yourhandle](https://twitter.com/yourhandle)
- Blog: [yourblog.com](https://yourblog.com) 