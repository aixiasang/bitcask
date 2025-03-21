# 🧠 Bitcask 存储引擎内部实现

本目录包含Bitcask存储系统的核心实现代码，包括存储引擎、事务处理、文件管理等核心功能。

## 📁 包结构

- **[主包 - inner](./README.md)**: 核心存储引擎实现，处理读写操作、删除、范围查询和事务
- **[配置 - config](./config/README.md)**: 提供系统配置管理
- **[索引 - index](./index/README.md)**: 实现内存索引，支持键值查找和范围扫描
- **[记录 - record](./record/README.md)**: 定义数据记录格式和位置信息
- **[WAL - wal](./wal/README.md)**: 实现预写日志机制，确保数据持久化
- **[工具 - utils](./utils/README.md)**: 提供各种辅助功能

## 🔑 核心组件

### 📊 Bitcask 结构体

```go
type Bitcask struct {
    conf       *config.Config       // 配置
    activeWal  *wal.Wal             // 活跃的WAL文件
    oldWal     map[uint32]*wal.Wal  // 旧的WAL文件
    memTable   index.Index          // 内存索引
    fileId     uint32               // 当前文件ID
    mu         sync.RWMutex         // 互斥锁
    fileIds    []uint32             // 文件ID列表
    txnId      atomic.Uint32        // 事务ID
    comparator *utils.KeyComparator // 键比较器
}
```

### 🔄 批处理事务

```go
type Batch struct {
    conf  *config.Config    // 配置
    db    *Bitcask          // 数据库
    mu    sync.RWMutex      // 互斥锁
    mp    map[string][]byte // 存储写入的key-value
    keys  [][]byte          // 存储删除的key
    txnId uint32            // 事务id
}
```

## 📝 主要功能

### 🔑 基本操作

- **创建/打开数据库**: `NewBitcask(config) (*Bitcask, error)`
- **写入键值对**: `Put(key, value []byte) error`
- **读取值**: `Get(key []byte) ([]byte, error)`
- **删除键值对**: `Delete(key []byte) error`
- **关闭数据库**: `Close() error`

### 🔎 范围查询

- **范围查询(无限制)**: `ScanRange(start, end []byte) ([]*ScanRangeResult, error)`
- **范围查询(限制数量)**: `ScanRangeLimit(start, end []byte, limit int) ([]*ScanRangeResult, error)`
- **优化的范围查询**: `ScanRangeOptimized(start, end []byte, limit int) ([]*ScanRangeResult, error)`

范围查询实现采用"先收集后排序"策略：
1. 使用`KeyComparator`判断键是否在范围内
2. 收集满足条件的键和值
3. 按照键比较器的规则对键进行排序
4. 按顺序构建结果集

### 💼 事务支持

- **创建批处理**: `NewBatch(db *Bitcask) *Batch`
- **批量写入**: `batch.Put(key, value []byte) error`
- **批量删除**: `batch.Delete(key []byte) error`
- **提交事务**: `batch.Commit() error`

### 🧹 维护操作

- **生成索引快照**: `Hint() error`
- **合并压缩**: `Merge() error`
- **数据恢复**: `LoadHint() error`
- **文件轮转**: `tryRotate() error`

## 🔄 工作流程

1. **启动流程**:
   - 加载配置
   - 创建必要的目录
   - 从Hint文件加载基础索引
   - 处理WAL文件应用最新更新
   - 准备活跃的WAL文件

2. **写入流程**:
   - 检查文件大小是否需要轮转
   - 将记录追加到活跃WAL文件
   - 更新内存索引
   - 返回成功状态

3. **读取流程**:
   - 从内存索引获取记录位置
   - 根据位置信息从对应WAL文件读取记录
   - 验证记录有效性
   - 返回记录值

4. **事务流程**:
   - 在批处理中积累操作
   - 提交时写入带有事务ID的记录
   - 写入事务提交标记
   - 更新系统事务ID计数器

5. **合并流程**:
   - 轮转当前活跃文件
   - 遍历内存索引中的所有键
   - 将有效数据重写到新的活跃文件
   - 删除旧文件并更新索引

## 🔧 设计原则

1. **简单性**: 保持核心实现简单明了
2. **高性能**: 优化常见操作的性能，利用追加写入和内存索引
3. **持久性**: 确保写入的数据不会丢失
4. **可靠性**: 通过WAL和Hint机制确保系统可靠恢复
5. **并发安全**: 使用适当的锁机制保护共享资源 