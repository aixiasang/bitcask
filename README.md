# Bitcask键值存储

Bitcask是一种高性能的键值存储系统，基于Riak Bitcask设计模式实现。这个Go语言版本的实现提供了简单且高效的键值存储方案，适合各种需要持久化键值对的应用场景。

## 特性

- 简单高效的键值存储
- 所有键都存储在内存中，提供快速查询
- 数据持久化到磁盘，保证数据安全
- 预写日志(WAL)设计，确保数据不会丢失
- 支持事务操作
- 支持范围查询和迭代

## 设计

Bitcask实现基于以下几个核心组件：

1. **记录(Record)**: 定义了存储在日志文件中的记录格式，支持更新、删除和事务操作
2. **索引(Index)**: 内存索引结构，使用B树实现高效查找
3. **WAL(预写日志)**: 负责数据持久化和恢复
4. **文件处理(FileHandler)**: 底层文件I/O抽象
5. **配置(Config)**: 数据库配置参数

## 使用示例

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

    // 创建Bitcask实例
    db := bitcask.NewBitcask(conf)
    defer db.Close()

    // 存储键值对
    if err := db.Put([]byte("hello"), []byte("world")); err != nil {
        log.Fatalf("Failed to put: %v", err)
    }

    // 读取值
    value, err := db.Get([]byte("hello"))
    if err != nil {
        log.Fatalf("Failed to get: %v", err)
    }
    fmt.Printf("Value: %s\n", string(value))

    // 删除键
    if err := db.Delete([]byte("hello")); err != nil {
        log.Fatalf("Failed to delete: %v", err)
    }
}
```

## 性能

Bitcask设计提供了如下性能特点：

- **读取操作**: O(1)复杂度，因为所有键都存储在内存中
- **写入操作**: O(1)复杂度，因为写入总是顺序追加
- **删除操作**: O(1)复杂度，通过写入删除标记实现

## 测试

项目包含全面的测试用例，涵盖了所有主要组件：

```bash
go test -v ./...
```

## 设计限制

- 所有键必须能够放入内存
- 不支持嵌套事务
- 当前实现没有自动合并过时的数据文件

## 后续改进

- 添加数据文件合并（垃圾回收）
- 实现快照功能
- 支持更复杂的查询操作
- 添加数据压缩
- 实现数据加密 