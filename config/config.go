package config

type Config struct {
	DirPath     string
	MaxFileSize uint32
	WalFileExt  string
	WalFolder   string
}

// DefaultConfig 创建一个带有默认值的配置
func DefaultConfig(dirPath string) *Config {
	return &Config{
		DirPath:     dirPath,
		MaxFileSize: 1024 * 1024 * 32, // 32MB
		WalFileExt:  "wal",
		WalFolder:   "wal",
	}
}
