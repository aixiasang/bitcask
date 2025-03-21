# ⚙️ Config 配置包

本包提供Bitcask存储系统的配置管理功能，允许用户自定义系统的行为和参数。

## 📋 主要组件

### ⚙️ Config 结构体

包含所有可配置选项的中央配置结构：

```go
type Config struct {
    DataDir     string // 数据目录路径
    WalDir      string // WAL文件目录名称
    HintDir     string // Hint文件目录名称
    MaxFileSize int64  // 单个WAL文件的最大大小(字节)
    BTreeOrder  int    // BTree索引的阶数
    LoadHint    bool   // 是否加载Hint文件
    BatchSize   int    // 批处理的最大大小
    Debug       bool   // 是否开启调试模式
}
```

### 🏭 NewConfig 函数

创建带有合理默认值的配置实例：

```go
func NewConfig() *Config {
    return &Config{
        DataDir:     "./data",           // 默认数据目录
        WalDir:      "wal",              // 默认WAL目录名
        HintDir:     "hint",             // 默认Hint目录名
        MaxFileSize: 1024 * 1024 * 100,  // 默认100MB
        BTreeOrder:  32,                 // 默认BTree阶数
        LoadHint:    true,               // 默认加载Hint文件
        BatchSize:   100,                // 默认批处理大小
        Debug:       false,              // 默认关闭调试
    }
}
```

## 💡 使用示例

### 基本用法

```go
// 使用默认配置
conf := config.NewConfig()

// 自定义数据目录
conf.DataDir = "/var/lib/bitcask"

// 增加单个WAL文件的大小限制
conf.MaxFileSize = 1024 * 1024 * 200 // 200MB

// 创建Bitcask实例
db, err := bitcask.NewBitcask(conf)
if err != nil {
    panic(err)
}
```

### 调整性能相关配置

```go
// 优化读取性能的配置
conf := config.NewConfig()
conf.BTreeOrder = 64           // 增加BTree阶数，改善范围查询性能
conf.LoadHint = true           // 确保加载hint文件，加速启动

// 优化写入性能的配置
conf := config.NewConfig()
conf.MaxFileSize = 1024 * 1024 * 256  // 增大文件大小，减少文件切换频率
conf.BatchSize = 1000                 // 增加批处理大小，提高事务吞吐量
```

### 开发/调试配置

```go
// 调试模式配置
conf := config.NewConfig()
conf.Debug = true              // 启用详细日志
conf.DataDir = "./debug_data"  // 使用特定目录便于调试
conf.MaxFileSize = 1024 * 1024 // 1MB，方便观察文件切换
```

## 🔧 配置参数详解

### 关键参数

1. **DataDir**: 存储所有数据的根目录
   - 类型: `string`
   - 默认值: `"./data"`
   - 影响: 确定数据文件的存储位置

2. **MaxFileSize**: 单个WAL文件的最大大小
   - 类型: `int64`
   - 默认值: `104857600` (100MB)
   - 影响: 控制文件轮转频率，较大的值减少轮转次数但增加合并开销

3. **BTreeOrder**: BTree索引的阶数
   - 类型: `int`
   - 默认值: `32`
   - 影响: 影响内存使用和查询性能，较大的值提高范围查询性能

4. **BatchSize**: 单个批处理的最大操作数
   - 类型: `int`
   - 默认值: `100`
   - 影响: 控制事务的大小限制，防止过大的事务导致内存溢出

### 调试参数

1. **Debug**: 启用详细日志输出
   - 类型: `bool`
   - 默认值: `false`
   - 影响: 输出详细的操作和错误信息，用于开发调试 