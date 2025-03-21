# 📄 Record 记录包

本包定义了Bitcask存储系统中的数据记录结构和位置信息，是系统中数据表示的基础。

## 📋 主要组件

### 📝 Record 结构体

表示存储在WAL文件中的完整记录，包含以下字段：

```go
type Record struct {
    RecordType RecordType  // 记录类型
    Key        []byte      // 键
    Value      []byte      // 值(可为空)
}
```

### 📍 Pos 结构体

描述记录在磁盘上的位置信息，用于从WAL文件中快速定位和读取数据：

```go
type Pos struct {
    FileId uint32  // 文件ID
    Offset uint32  // 文件内偏移量
    Length uint32  // 记录长度
}
```

### 🏷️ RecordType 类型

定义不同类型的记录，支持基本操作和事务：

```go
const (
    RecordTypePut       RecordType = iota // 写入
    RecordTypeDelete                      // 删除
    RecordTypeTxnPut                      // 事务写入
    RecordTypeTxnDelete                   // 事务删除
    RecordTypeTxnCommit                   // 事务提交
)
```

## 💡 使用示例

```go
// 创建普通记录
normalRecord := record.NewRecord(
    []byte("user:1001"),
    []byte(`{"name":"John","age":30}`)
)

// 创建删除记录
deleteRecord := record.NewRecord(
    []byte("user:1001"),
    nil  // 空值表示删除
)

// 创建事务记录
txnRecord := record.NewTxnRecord(
    []byte("user:1002"),
    []byte(`{"name":"Alice","age":25}`)
)

// 创建事务提交记录
commitRecord := record.NewTxnCommit(
    []byte("commit")
)

// 创建位置信息
position := &record.Pos{
    FileId: 1,
    Offset: 1024,
    Length: 256,
}

// 判断记录是否为删除标记
isDelete := normalRecord.RecordType == record.RecordTypeDelete

// 判断记录是否为事务记录
isTxn := txnRecord.RecordType == record.RecordTypeTxnPut
```

## 🔧 设计要点

1. **简单性**：记录结构简单明了，易于序列化和反序列化
2. **灵活性**：通过RecordType支持不同类型的操作
3. **完整性**：包含CRC校验确保数据完整性
4. **定位效率**：Pos结构设计紧凑，支持高效的数据定位 