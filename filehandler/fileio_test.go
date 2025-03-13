package filehandler

import (
	"os"
	"path/filepath"
	"testing"
)

// 测试文件操作
func TestFileIO(t *testing.T) {
	// 创建临时测试目录
	testDir := t.TempDir()
	testFile := filepath.Join(testDir, "test.dat")

	// 打开文件
	fh, err := Open(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer fh.Close()

	// 测试 Append 操作
	data := []byte("hello, world")
	n, err := fh.Append(data)
	if err != nil {
		t.Fatalf("Failed to append data: %v", err)
	}
	if n != uint32(len(data)) {
		t.Errorf("Expected append length %d, got %d", len(data), n)
	}

	// 测试 Sync 操作
	if err := fh.Sync(); err != nil {
		t.Fatalf("Failed to sync file: %v", err)
	}

	// 测试 ReadAt 操作
	buf := make([]byte, len(data))
	n, err = fh.ReadAt(0, buf)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}
	if n != uint32(len(data)) {
		t.Errorf("Expected read length %d, got %d", len(data), n)
	}
	if string(buf) != string(data) {
		t.Errorf("Expected read data %s, got %s", string(data), string(buf))
	}

	// 测试 WriteAt 操作
	newData := []byte("new data")
	n, err = fh.WriteAt(0, newData)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	if n != uint32(len(newData)) {
		t.Errorf("Expected write length %d, got %d", len(newData), n)
	}

	// 验证 WriteAt 操作
	readBuf := make([]byte, len(newData))
	n, err = fh.ReadAt(0, readBuf)
	if err != nil {
		t.Fatalf("Failed to read after write: %v", err)
	}
	if n != uint32(len(newData)) {
		t.Errorf("Expected read length %d, got %d", len(newData), n)
	}
	if string(readBuf) != string(newData) {
		t.Errorf("Expected read data %s, got %s", string(newData), string(readBuf))
	}

	// 测试 Read 操作（io.Reader接口）
	fio, ok := fh.(*FileIO)
	if !ok {
		t.Fatal("Failed to cast FileHandler to FileIO")
	}

	// 重置位置
	fio.position = 0
	readBuf = make([]byte, len(newData))
	n2, err := fio.Read(readBuf)
	if err != nil {
		t.Fatalf("Failed to read with Read method: %v", err)
	}
	if n2 != len(newData) {
		t.Errorf("Expected io.Reader read length %d, got %d", len(newData), n2)
	}
	if string(readBuf) != string(newData) {
		t.Errorf("Expected read data %s, got %s", string(newData), string(readBuf))
	}
}
