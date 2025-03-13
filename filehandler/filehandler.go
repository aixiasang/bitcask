package filehandler

import "io"

// FileHandler 文件处理接口
type FileHandler interface {
	io.Reader
	io.Seeker
	Close() error
	ReadAt(offset uint32, buf []byte) (uint32, error)
	WriteAt(offset uint32, buf []byte) (uint32, error)
	Append(buf []byte) (uint32, error)
	Sync() error
}
