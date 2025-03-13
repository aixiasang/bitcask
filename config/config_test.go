package config

import (
	"os"
	"testing"
)

// TestConfigCreation 测试配置创建
func TestConfigCreation(t *testing.T) {
	// 创建临时测试目录
	testDir := "test_dir"
	defer os.RemoveAll(testDir)

	// 基本配置
	conf := &Config{
		DirPath:     testDir,
		MaxFileSize: 1024 * 1024, // 1MB
		WalFileExt:  "wal",
		WalFolder:   "wal",
	}

	// 验证配置值
	if conf.DirPath != testDir {
		t.Errorf("DirPath expected %s, got %s", testDir, conf.DirPath)
	}

	if conf.MaxFileSize != 1024*1024 {
		t.Errorf("MaxFileSize expected %d, got %d", 1024*1024, conf.MaxFileSize)
	}

	if conf.WalFileExt != "wal" {
		t.Errorf("WalFileExt expected %s, got %s", "wal", conf.WalFileExt)
	}

	if conf.WalFolder != "wal" {
		t.Errorf("WalFolder expected %s, got %s", "wal", conf.WalFolder)
	}
}

// TestDefaultConfig 测试创建默认配置的功能
func TestDefaultConfig(t *testing.T) {
	// 创建临时测试目录
	testDir := "test_default_dir"
	defer os.RemoveAll(testDir)

	// 创建默认配置
	conf := DefaultConfig(testDir)

	// 验证默认值
	if conf.DirPath != testDir {
		t.Errorf("DirPath expected %s, got %s", testDir, conf.DirPath)
	}

	// 验证其他默认值（假设我们定义了这些默认值）
	if conf.MaxFileSize == 0 {
		t.Error("MaxFileSize should have a default value")
	}

	if conf.WalFileExt == "" {
		t.Error("WalFileExt should have a default value")
	}

	if conf.WalFolder == "" {
		t.Error("WalFolder should have a default value")
	}
}
