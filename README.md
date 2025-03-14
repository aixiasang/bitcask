# Bitcask

Bitcask是一个高性能、持久化的键值存储系统，基于Basho公司原始Bitcask论文的Go语言实现。它提供简单而强大的API，适合需要高吞吐量和低延迟数据访问的应用程序。

## 特性

- **高性能**：所有读操作仅需一次磁盘寻道，写操作是顺序的，使性能最大化
- **持久化**：数据写入到磁盘，在系统崩溃或重启后仍然可用
- **简单**：简洁的API设计，易于集成和使用
- **可靠**：WAL（Write-Ahead Logging）确保数据不会丢失
- **批处理支持**：支持原子批量操作
- **并发安全**：支持多个goroutine并发访问
- **进程锁**：使用文件锁防止多个进程同时访问同一数据库实例
- **强大的错误恢复**：支持崩溃恢复和损坏的WAL文件处理

## 安装

```bash
go get github.com/aixiasang/bitcask
```

## 快速开始

### 基本操作

```go
package main

import (
    "fmt"
    "log"

    "github.com/aixiasang/bitcask"
    "github.com/aixiasang/bitcask/config"
)

func main() {
    // 创建配置
    conf := config.DefaultConfig("./data")

    // 打开数据库
    db, err := bitcask.NewBitcask(conf)
    if err != nil {
        log.Fatalf("无法打开数据库: %v", err)
    }
    defer db.Close()

    // 写入数据
    err = db.Put([]byte("hello"), []byte("world"))
    if err != nil {
        log.Fatalf("写入失败: %v", err)
    }

    // 读取数据
    value, err := db.Get([]byte("hello"))
    if err != nil {
        log.Fatalf("读取失败: %v", err)
    }
    fmt.Printf("hello = %s\n", value)

    // 删除数据
    err = db.Delete([]byte("hello"))
    if err != nil {
        log.Fatalf("删除失败: %v", err)
    }
}
```

### 批量操作

```go
package main

import (
    "fmt"
    "log"

    "github.com/aixiasang/bitcask"
    "github.com/aixiasang/bitcask/config"
)

func main() {
    // 创建配置
    conf := config.DefaultConfig("./data")

    // 打开数据库
    db, err := bitcask.NewBitcask(conf)
    if err != nil {
        log.Fatalf("无法打开数据库: %v", err)
    }
    defer db.Close()

    // 创建批处理
    batch := db.NewBatch()

    // 添加操作到批处理
    batch.Put([]byte("key1"), []byte("value1"))
    batch.Put([]byte("key2"), []byte("value2"))
    batch.Put([]byte("key3"), []byte("value3"))
    batch.Delete([]byte("key2"))

    // 原子提交所有操作
    err = batch.Commit()
    if err != nil {
        log.Fatalf("批处理提交失败: %v", err)
    }

    // 验证结果
    value, _ := db.Get([]byte("key1"))
    fmt.Printf("key1 = %s\n", value)

    value, _ = db.Get([]byte("key2"))
    if value == nil {
        fmt.Println("key2已被删除")
    }

    value, _ = db.Get([]byte("key3"))
    fmt.Printf("key3 = %s\n", value)
}
```

## 配置选项

```go
// 创建自定义配置
conf := &config.Config{
    DirPath:     "./data",         // 数据目录路径
    WalFolder:   "wal",            // WAL文件夹名称
    WalFileExt:  "wal",            // WAL文件扩展名
    MaxFileSize: 1024 * 1024 * 10, // 最大文件大小（10MB）
}

// 或使用默认配置并修改
conf := config.DefaultConfig("./data")
conf.MaxFileSize = 1024 * 1024 * 20 // 修改为20MB
```

## API参考

### 数据库操作

- `NewBitcask(conf *config.Config) (*Bitcask, error)` - 打开或创建数据库
- `(db *Bitcask) Get(key []byte) ([]byte, error)` - 获取键的值
- `(db *Bitcask) Put(key []byte, value []byte) error` - 存储键值对
- `(db *Bitcask) Delete(key []byte) error` - 删除键
- `(db *Bitcask) Close() error` - 关闭数据库

### 批处理操作

- `(db *Bitcask) NewBatch() *Batch` - 创建新的批处理
- `(b *Batch) Put(key []byte, value []byte) error` - 添加存储操作到批处理
- `(b *Batch) Delete(key []byte) error` - 添加删除操作到批处理
- `(b *Batch) Commit() error` - 提交批处理中的所有操作

## 高级特性

### 持久化与恢复

Bitcask使用WAL（Write-Ahead Logging）确保即使在系统崩溃的情况下也能恢复数据。所有的写操作都会先记录到日志文件中，然后再更新内存索引。在数据库重新启动时，会从日志文件中重建索引。

### 文件锁

为防止多个进程同时访问同一数据库实例，Bitcask使用文件锁机制。当一个进程打开数据库时，会获取文件锁，阻止其他进程访问。这确保了数据的一致性和完整性。

### 并发支持

Bitcask支持多个goroutine并发访问数据库。它使用读写锁来保证线程安全，允许多个读操作同时进行，而写操作则会独占锁。

### 错误恢复

- **崩溃恢复**：在非正常关闭（如崩溃）后，数据库会从WAL文件中恢复数据。
- **损坏WAL处理**：即使WAL文件部分损坏，Bitcask也会尝试恢复尽可能多的数据。

### 文件管理

当WAL文件大小超过配置的最大值时，会创建新的WAL文件，旧的文件会保留用于读取。这确保了写操作始终是顺序的，同时保持了索引的完整性。

## 性能注意事项

- 读操作性能极高，通常只需一次磁盘I/O
- 写操作是顺序的，避免了随机磁盘寻道
- 内存中保存完整索引，因此大数据集可能消耗大量内存
- 定期进行压缩可以减小数据文件大小

## 局限性

- 由于索引存储在内存中，数据集大小受限于可用内存
- 当前实现不支持索引持久化，每次启动都需要从WAL重建索引
- 不支持事务（除了批处理提供的原子操作）
- 不支持范围查询或其他高级查询操作

## 许可证

[MIT](/LICENSE) 