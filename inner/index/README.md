# 🔍 Index 索引包

本包实现了Bitcask存储系统的内存索引组件，支持快速查找键值对在磁盘上的位置。

## 📋 主要组件

### 🔄 Index 接口

提供统一的索引操作接口，允许不同的索引实现（如BTree、Hash表等）。

```go
type Index interface {
    // 添加或更新索引
    Put(key []byte, pos *record.Pos) error
    
    // 获取键对应的位置
    Get(key []byte) (*record.Pos, error)
    
    // 删除键的索引
    Delete(key []byte) error
    
    // 范围查询
    Scan(startKey, endKey []byte) ([]*Data, error)
    
    // 遍历所有索引
    Foreach(fn func(key []byte, pos *record.Pos) error) error
    
    // 无锁遍历（性能优化用）
    ForeachUnSafe(fn func(key []byte, pos *record.Pos) error) error
    
    // 关闭索引
    Close() error
}
```

### 🌲 BTreeIndex 实现

基于Google的BTree库实现的高效内存索引。

```go
// 创建默认阶数的BTree索引
index := NewBTreeIndex(32)

// 添加或更新索引
index.Put(key, position)

// 获取键对应的位置
position, err := index.Get(key)

// 删除键
index.Delete(key)

// 范围查询
results, err := index.Scan(startKey, endKey)
```

特性：
- 使用BTree数据结构，支持快速查找和范围查询
- 线程安全：所有操作都有适当的锁保护
- 自定义比较器：使用utils.KeyComparator确保比较逻辑一致
- 支持并发访问：读操作使用读锁，写操作使用写锁

### 📊 Data 结构

包含键和位置信息的数据结构，用于范围查询结果。

```go
type Data struct {
    Key string       // 键（字符串形式）
    Pos record.Pos   // 记录在磁盘上的位置
}
```

## 💡 使用示例

```go
// 创建新的BTree索引
index := index.NewBTreeIndex(32)

// 添加索引项
key := []byte("mykey")
pos := &record.Pos{
    FileId: 1,
    Offset: 1024,
    Length: 256,
}
index.Put(key, pos)

// 获取索引项
pos, err := index.Get(key)
if err != nil {
    // 处理错误
}
if pos != nil {
    // 使用位置信息从磁盘读取数据
}

// 范围查询
results, err := index.Scan([]byte("a"), []byte("z"))
for _, data := range results {
    fmt.Printf("Key: %s, Position: %+v\n", data.Key, data.Pos)
}

// 遍历所有键值对
index.Foreach(func(key []byte, pos *record.Pos) error {
    fmt.Printf("Key: %s, Position: %+v\n", string(key), pos)
    return nil
})

// 关闭索引
index.Close()
```

## 🔧 设计原则

1. **抽象**：通过接口隐藏实现细节，便于更换不同的索引实现
2. **一致性**：所有索引操作使用统一的比较逻辑
3. **并发安全**：使用读写锁确保并发安全
4. **性能优化**：提供无锁版本的遍历函数，用于特定场景下的性能优化 