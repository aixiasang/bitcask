package filehandler

import (
	"os"
	"sync"
)

type FileIO struct {
	fileName string
	File     *os.File
	position int64      // 当前读取位置
	mu       sync.Mutex // 保护并发访问
}

func Open(path string, flag int, perm os.FileMode) (FileHandler, error) {
	fp, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &FileIO{
		fileName: path,
		File:     fp,
		position: 0,
	}, nil
}

func (f *FileIO) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.File.Close()
}

func (f *FileIO) ReadAt(offset uint32, buf []byte) (uint32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.File.ReadAt(buf, int64(offset))
	return uint32(n), err
}

// Read 实现io.Reader接口
func (f *FileIO) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.File.ReadAt(p, f.position)
	if err == nil {
		f.position += int64(n)
	}
	return n, err
}

func (f *FileIO) WriteAt(offset uint32, buf []byte) (uint32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.File.WriteAt(buf, int64(offset))
	return uint32(n), err
}

func (f *FileIO) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.File.Sync()
}

func (f *FileIO) Append(buf []byte) (uint32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.File.Write(buf)
	return uint32(n), err
}

func (f *FileIO) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pos, err := f.File.Seek(offset, whence)
	if err == nil {
		f.position = pos
	}
	return pos, err
}
