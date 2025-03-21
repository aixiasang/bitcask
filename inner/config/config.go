package config

type IndexType uint8

const (
	IndexTypeBTree IndexType = iota
	IndexTypeSkipList
)

type Config struct {
	DataDir     string    // 数据目录
	IndexType   IndexType // 索引类型
	AutoSync    bool      // 自动同步
	BTreeOrder  int       // B树的阶数
	MaxFileSize uint32    // 最大文件大小
	WalDir      string    // WAL 目录
	HintDir     string    // hint 文件目录
	LoadHint    bool      // 是否加载 hint 文件
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
	}
}
