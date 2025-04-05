# 🧠 Bitcask 核心模块

Bitcask 是一个高性能的键值存储引擎，基于日志结构的存储模型，提供了快速的写入和读取操作。这个模块包含了 Bitcask 的核心实现。

## 🏗️ 架构设计

Bitcask 采用简单而高效的设计原则：
- 所有的**写操作**顺序追加到活跃数据文件中
- **内存索引**维护所有键到其最新值位置的映射
- **只读数据文件**存储历史记录，定期合并以回收空间
- **hint文件**加速重启时的索引构建
- **事务支持**确保操作的原子性

## 🧩 核心组件

### 🚀 存储引擎 (Bitcask)

存储引擎是核心接口，提供键值存储的基本操作：
- `Put` - 存储键值对
- `Get` - 获取键对应的值
- `Delete` - 删除键值对
- `Scan` - 全量扫描所有键值对
- `ScanRange` - 范围扫描键值对
- `ScanRangeLimit` - 限制结果数量的范围扫描
- `Merge` - 合并WAL文件，优化存储空间
- `Hint` - 生成hint文件
- `Close` - 安全关闭存储引擎

### 📦 批处理 (Batch)

提供事务性操作支持：
- `NewBatch` - 创建新的批处理
- `Put` - 添加键值对到批处理
- `Delete` - 从批处理中删除键
- `Commit` - 提交批处理，原子性执行所有操作

### ⚙️ 配置 (Config)

提供可配置的选项来自定义 Bitcask 实例的行为：
- `DataDir` - 数据目录路径
- `WalDir` - WAL目录名称
- `HintDir` - hint文件目录名称
- `IndexType` - 索引类型（BTree或SkipList）
- `BTreeOrder` - B树的阶数
- `MaxFileSize` - 数据文件最大大小
- `BatchSize` - 批处理大小
- `AutoSync` - 是否自动同步写入
- `LoadHint` - 是否加载hint文件
- `Debug` - 调试模式

### 🔍 索引 (Index)

提供键到值位置的高效映射：
- BTree索引 - 使用Google的btree实现，提供高效的插入、查找和删除操作
- SkipList索引 - 实现跳表数据结构，提供O(log n)复杂度的操作

### 📁 数据文件 (WAL)

采用日志结构文件：
- 追加写入 - 所有写操作顺序追加到活跃WAL文件
- 不可变文件 - 一旦WAL文件达到大小限制，它将变为只读
- 文件格式 - 简单而高效的二进制格式，包含类型、键长度、值长度、键、值等字段

### 📝 Hint文件

用于加速重启时的索引重建：
- 存储所有有效键的位置信息
- 在启动时加载hint文件，避免扫描所有WAL文件
- 通过`Hint()`命令手动生成

## 📊 数据结构

### 📋 记录格式

每个记录在WAL文件中的存储格式如下：
```
+---------+-----------+-----------+------+-------+
| Type(1) | KeyLen(4) | ValLen(4) | Key  | Value |
+---------+-----------+-----------+------+-------+
```

### 🏷️ 记录类型

- `RecordTypePut (0)` - 正常写入记录
- `RecordTypeDelete (1)` - 删除标记
- `RecordTypeBegin (2)` - 事务开始
- `RecordTypeTxnPut (3)` - 事务写入
- `RecordTypeTxnDelete (4)` - 事务删除
- `RecordTypeTxnCommit (5)` - 事务提交

## 🔄 工作流程

### ✍️ 写入流程

1. 将键值对包装为记录格式
2. 将记录追加到活跃WAL文件
3. 更新内存索引，指向新写入的记录位置
4. 如果活跃WAL文件超过大小限制，创建新的活跃文件

### 📖 读取流程

1. 在内存索引中查找键，获取记录位置（文件ID和偏移量）
2. 打开对应的WAL文件，定位到记录位置
3. 读取并解析记录，返回值

### 🗑️ 删除流程

1. 写入删除标记到WAL文件
2. 从内存索引中移除键

### 🔄 范围查询流程

1. 使用`ScanRange`或`ScanRangeLimit`方法指定起止键
2. 内部通过比较器筛选并排序满足条件的键值对
3. 按顺序返回结果

### 📦 事务流程

1. 创建批处理对象
2. 调用`Put`/`Delete`添加操作到批处理
3. 调用`Commit`原子性提交所有操作
4. 底层通过事务日志确保原子性和一致性

### 🚀 启动流程

1. 尝试加载hint文件构建初始索引
2. 如果hint文件不存在或加载失败，扫描所有WAL文件重建索引
3. 从小到大处理WAL文件，确保索引包含最新的记录

## 📝 使用示例

```go
import (
    "github.com/aixiasang/bitcask"
    "github.com/aixiasang/bitcask/config"
)

// 创建配置
conf := config.NewConfig()
conf.DataDir = "./data"
conf.MaxFileSize = 1024 * 1024 // 1MB
conf.AutoSync = true

// 创建Bitcask实例
bc, err := bitcask.NewBitcask(conf)
if err != nil {
    // 处理错误
}
defer bc.Close()

// 写入键值对
err = bc.Put([]byte("key1"), []byte("value1"))
if err != nil {
    // 处理错误
}

// 读取值
val, ok := bc.Get([]byte("key1"))
if !ok {
    // 键不存在
}

// 删除键
err = bc.Delete([]byte("key1"))
if err != nil {
    // 处理错误
}

// 范围查询
results, err := bc.ScanRange([]byte("key-10"), []byte("key-20"))
if err != nil {
    // 处理错误
}
for _, result := range results {
    fmt.Printf("键: %s, 值: %s\n", string(result.Key), string(result.Value))
}

// 使用事务批量操作
batch := bitcask.NewBatch(bc)
batch.Put([]byte("batch-key-1"), []byte("batch-value-1"))
batch.Put([]byte("batch-key-2"), []byte("batch-value-2"))
batch.Delete([]byte("batch-key-old"))
if err := batch.Commit(); err != nil {
    // 处理错误
}

// 生成hint文件，加速下次启动
err = bc.Hint()
if err != nil {
    // 处理错误
}

// 合并WAL文件，优化存储
err = bc.Merge()
if err != nil {
    // 处理错误
}
```

## ⚡ 性能优化

- **批处理** - 支持批量写入操作，减少磁盘同步次数
- **范围查询** - 高效的范围扫描，支持限制结果数量
- **只读映射** - 使用文件的只读内存映射提高读取性能
- **内存索引** - 全内存索引确保高速查找
- **顺序写入** - 采用追加写入方式，最大化磁盘写入性能
- **文件合并** - 支持合并操作，回收冗余空间 