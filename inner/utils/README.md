# 🛠️ Utils 工具包

本包提供Bitcask存储系统中使用的各种辅助工具和实用函数，简化其他包的实现。

## 📋 主要组件

### 🔍 KeyComparator - 键比较器

专门用于比较键的工具类，确保在整个系统中使用一致的比较逻辑。

```go
// 创建比较器
comparator := NewKeyComparator()

// 比较两个键的大小
if comparator.Less(keyA, keyB) {
    // keyA < keyB
}

// 检查键是否在指定范围内
if comparator.InRange(key, startKey, endKey) {
    // key在[startKey, endKey]范围内
}
```

比较策略：
- 先比较长度：长度短的键视为较小
- 长度相同时：按字节顺序比较内容

### 🔄 编码工具 - enc.go

提供事务ID与键的编码和解码功能，用于在WAL文件中存储带有事务ID的记录。

```go
// 将事务ID与键编码为一个字节数组
encodedKey := EncodeTxnId(txnId, key)

// 从编码后的字节数组中提取事务ID和原始键
txnId, originalKey := DecodeTxnId(encodedKey)
```

### 🧪 测试辅助工具 - rand_kv.go

生成随机键值对，用于测试和基准测试。

```go
// 获取固定序号的键
key := GetKey(42)

// 获取指定大小的随机值
value := GetValue(1024) // 生成1KB大小的随机值
```

## 💡 使用示例

```go
// 创建比较器
comparator := utils.NewKeyComparator()

// 比较两个键
keyA := []byte("apple")
keyB := []byte("banana")
if comparator.Less(keyA, keyB) {
    fmt.Println("keyA < keyB")
}

// 检查键是否在范围内
start := []byte("apple")
end := []byte("cherry")
key := []byte("banana")
if comparator.InRange(key, start, end) {
    fmt.Println("Key is in range")
}

// 编码事务ID和键
txnId := uint32(123)
key := []byte("mykey")
encodedKey := utils.EncodeTxnId(txnId, key)

// 生成测试数据
testKey := utils.GetKey(1)
testValue := utils.GetValue(100) // 100字节大小的值
```

## 🔧 设计原则

1. **简单性**：提供简单明了的API，易于使用
2. **一致性**：确保整个系统中的一致行为，特别是键的比较逻辑
3. **可测试性**：提供工具使单元测试和集成测试更加简便
4. **隔离性**：将通用功能从业务逻辑中分离，提高代码复用性 