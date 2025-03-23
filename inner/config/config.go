package config

// 索引类型
type IndexType uint8

const (
	IndexTypeBTree    IndexType = iota // B树索引
	IndexTypeSkipList                  // 跳表索引
)

// 配置
type Config struct {
	DataDir     string    // 数据目录
	IndexType   IndexType // 索引类型
	AutoSync    bool      // 自动同步
	BTreeOrder  int       // B树的阶数
	MaxFileSize uint32    // 最大文件大小
	WalDir      string    // WAL 目录
	HintDir     string    // hint 文件目录
	LoadHint    bool      // 是否加载 hint 文件
	BatchSize   int       // 批处理大小
	Debug       bool      // 是否开启调试模式
}

func NewConfig() *Config {
	return &Config{
		DataDir:     "./data",
		IndexType:   IndexTypeBTree,
		AutoSync:    true,
		BTreeOrder:  128,
		MaxFileSize: 1024,
		WalDir:      "wal",
		HintDir:     "hint",
		LoadHint:    true,
		Debug:       true,
		BatchSize:   200,
	}
}
