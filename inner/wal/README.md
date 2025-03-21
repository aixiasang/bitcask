# 📝 WAL 预写日志包

本包实现了Bitcask存储系统的预写日志(Write-Ahead Log)机制，确保数据持久化和系统崩溃恢复。

## 📋 主要功能

### 📂 WAL文件管理

负责创建、打开、读取和关闭WAL文件，管理文件的生命周期。

```go
// 创建或打开WAL文件
wal, err := NewWal(config, fileId)

// 同步数据到磁盘
wal.Sync()

// 关闭WAL文件
wal.Close()

// 删除WAL文件
wal.Delete()
```

### ✍️ 数据写入

将键值对或事务操作追加到WAL文件。

```go
// 写入普通键值对
pos, err := wal.Write(key, value)

// 写入事务相关的键值对
pos, err := wal.WriteTxn(key, value)

// 写入事务提交标记
pos, err := wal.WriteTxnCommit(key)
```

### 📖 数据读取

从WAL文件中读取特定位置的记录或扫描整个文件。

```go
// 从指定位置读取记录
record, err := wal.ReadPos(position)

// 读取并处理整个WAL文件
wal.ReadAll(memTable, txnIdPtr)
```

## 🔄 预写日志格式

WAL文件由一系列记录组成，每条记录包含以下字段：

1. **记录类型** (1字节): 普通写入、删除、事务写入、事务删除或事务提交
2. **键长度** (4字节): 键的字节数
3. **值长度** (4字节): 值的字节数
4. **键数据** (变长): 实际的键
5. **值数据** (变长): 实际的值(可为空)
6. **CRC校验和** (4字节): 用于验证记录完整性

## 🔐 事务支持

预写日志支持事务操作，通过特殊的记录类型和事务ID实现：

- **事务写入记录**: 包含事务ID和写入操作
- **事务删除记录**: 包含事务ID和删除操作
- **事务提交记录**: 标记事务的完成

恢复过程中，只有已提交的事务会被应用到内存索引。

## 💡 使用示例

```go
// 创建WAL文件
conf := config.NewConfig()
wal, err := wal.NewWal(conf, 1)
if err != nil {
    panic(err)
}
defer wal.Close()

// 写入数据
key := []byte("user:1001")
value := []byte(`{"name":"John","age":30}`)
pos, err := wal.Write(key, value)
if err != nil {
    panic(err)
}

// 同步到磁盘
if err := wal.Sync(); err != nil {
    panic(err)
}

// 读取数据
record, err := wal.ReadPos(pos)
if err != nil {
    panic(err)
}
fmt.Printf("Read: %s = %s\n", string(record.Key), string(record.Value))

// 删除记录(标记删除)
deletedPos, err := wal.Write(key, nil)
if err != nil {
    panic(err)
}

// 写入事务记录
txnId := uint32(1)
txnKey := []byte("txn:user:1002")
txnValue := []byte(`{"name":"Alice","age":25}`)
txnPos, err := wal.WriteTxn(utils.EncodeTxnId(txnId, txnKey), txnValue)
if err != nil {
    panic(err)
}

// 提交事务
commitPos, err := wal.WriteTxnCommit(utils.EncodeTxnId(txnId, []byte("commit")))
if err != nil {
    panic(err)
}
```

## 🔧 设计原则

1. **持久性**：所有写操作立即记录到磁盘，确保数据不丢失
2. **一致性**：使用CRC校验确保数据完整性
3. **原子性**：通过事务机制确保操作的原子性
4. **隔离性**：不同事务的操作相互隔离
5. **顺序写入**：利用顺序写入的高性能特性 