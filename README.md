# 🗄️ Bitcask 存储系统

一个基于Bitcask论文实现的高性能键值存储系统，具有简单、高效、持久化和崩溃恢复的特性。

## ✨ 特性

- 🚀 高性能读写操作：写入复杂度O(1)，读取复杂度O(1)
- 💾 数据持久化：所有操作都会被记录到WAL文件
- 🛡️ 崩溃恢复：通过hint文件和WAL文件确保数据不丢失
- 🔄 自动文件轮转：防止单个数据文件过大
- 🔒 并发安全：支持多线程并发访问
- 🧹 数据合并：支持清理过期数据，优化存储空间

## 🏗️ 架构设计

Bitcask存储系统基于以下核心概念：

1. **写入追加(Append-Only)**：所有写操作都以追加方式写入活跃的WAL文件
2. **内存索引**：键到磁盘位置的映射保存在内存中，确保快速查询
3. **不可变文件**：旧的WAL文件是不可变的，确保数据一致性
4. **Hint文件**：保存索引信息，加速启动过程


## 📝 使用示例

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
}
```

## 📦 包结构

### 🧠 主包 `bitcask`

主包实现了Bitcask存储系统的核心功能，包括读取、写入、删除和合并等操作。

主要类型和函数：
- `Bitcask` - 核心存储引擎结构体
- `NewBitcask()` - 创建新的Bitcask实例
- `Put()` - 写入键值对
- `Get()` - 获取键对应的值
- `Delete()` - 删除键值对
- `Close()` - 关闭存储引擎，确保数据持久化
- `Merge()` - 合并数据文件，优化存储空间

### ⚙️ 配置包 `inner/config`

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

### 🔍 索引包 `inner/index`

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

### 📄 记录包 `inner/record`

定义了数据记录的格式和操作。

主要类型：
- `Record` - 数据记录结构体
- `Pos` - 记录在磁盘上的位置信息
- `RecordType` - 记录类型（普通记录或删除标记）

### 📝 WAL包 `inner/wal`

实现了预写日志(Write-Ahead Log)机制，确保数据持久化和崩溃恢复。

主要类型和函数：
- `Wal` - WAL文件管理结构体
- `NewWal()` - 创建新的WAL文件
- `Write()` - 写入数据到WAL文件
- `ReadPos()` - 从特定位置读取数据
- `ReadAll()` - 读取整个WAL文件
- `Close()` - 关闭WAL文件

### 🛠️ 工具包 `inner/utils`

提供各种辅助函数，简化其他包的实现。

主要函数：
- 文件操作辅助函数
- 数据转换函数
- 测试辅助函数

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

## 📜 许可证

[MIT License](LICENSE) 