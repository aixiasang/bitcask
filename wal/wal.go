package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aixiasang/bitcask/config"
	"github.com/aixiasang/bitcask/filehandler"
	"github.com/aixiasang/bitcask/index"
	"github.com/aixiasang/bitcask/record"
)

type Wal struct {
	conf        *config.Config          //配置信息
	Offset      uint32                  // 偏移量
	FileId      uint32                  // 文件ID
	FileHandler filehandler.FileHandler // 文件句柄
}

func NewWal(fileId uint32, conf *config.Config) (*Wal, error) {
	wal := &Wal{
		FileId: fileId,
		conf:   conf,
	}

	// 确保目录存在
	walPath := filepath.Join(conf.DirPath, conf.WalFolder)
	if err := os.MkdirAll(walPath, 0755); err != nil {
		return nil, fmt.Errorf("创建WAL目录失败: %v", err)
	}

	// 构建WAL文件路径
	filePath := filepath.Join(walPath, fmt.Sprintf("%d.%s", fileId, conf.WalFileExt))

	fh, err := filehandler.Open(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("打开WAL文件失败: %v", err)
	}
	wal.FileHandler = fh
	return wal, nil
}

func (w *Wal) Write(rec *record.Record) (*record.RecordPos, error) {
	// 编码并写入记录
	encodedRecord := rec.Encode()
	length := uint32(len(encodedRecord))

	// 创建记录位置信息
	pos := &record.RecordPos{
		FileId: w.FileId,
		Offset: w.Offset,
		Length: length,
	}

	// 写入记录
	n, err := w.FileHandler.Append(encodedRecord)
	if err != nil {
		return nil, fmt.Errorf("写入记录失败: %v", err)
	}
	if n != length {
		return nil, fmt.Errorf("写入记录长度不匹配: 期望 %d, 实际 %d", length, n)
	}

	// 更新偏移量
	w.Offset += length
	return pos, nil
}

func (w *Wal) Read(pos *record.RecordPos) (record.RecordType, []byte, []byte, error) {
	// 分配缓冲区
	buf := make([]byte, pos.Length)

	// 读取记录
	n, err := w.FileHandler.ReadAt(pos.Offset, buf)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("读取记录失败: offset=%d, length=%d, err=%v", pos.Offset, pos.Length, err)
	}
	if uint32(n) != pos.Length {
		return 0, nil, nil, fmt.Errorf("读取记录长度不匹配: 期望 %d, 实际 %d", pos.Length, n)
	}

	// 解码记录
	recType, _, key, value, err := record.Decode(buf)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("解码记录失败: %v", err)
	}

	return recType, key, value, nil
}

func (w *Wal) Close() error {
	if err := w.Sync(); err != nil {
		return err
	}
	return w.FileHandler.Close()
}

func (w *Wal) Sync() error {
	return w.FileHandler.Sync()
}

func (w *Wal) RestoreIndex(index index.Index) error {
	// 重置文件指针到开始位置
	_, err := w.FileHandler.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("重置文件指针失败: %v", err)
	}

	fmt.Println("开始恢复索引...")
	var currentOffset int64 = 0

	// 循环读取记录直到文件结束
	for {
		fmt.Printf("当前位置: %d\n", currentOffset)

		// 读取记录类型字节
		typeBuf := make([]byte, 1)
		n, err := w.FileHandler.ReadAt(uint32(currentOffset), typeBuf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("文件读取完成")
				break
			}
			return fmt.Errorf("读取记录类型失败: %v", err)
		}
		if n != 1 {
			return fmt.Errorf("读取记录类型时获取到错误的字节数: %d", n)
		}

		recordType := record.RecordType(typeBuf[0])

		// 检查是否是事务控制记录类型
		if recordType == record.RecordTxnBegin || recordType == record.RecordTxnCommit || recordType == record.RecordTxnAbort {
			// 事务控制记录只有类型和事务ID
			// 跳过事务ID (4字节)
			currentOffset += 1 + 4 // 记录类型(1字节) + 事务ID(4字节)
			continue
		}

		// 如果不是事务控制记录，使用正常的记录解码
		// 将文件指针重新定位
		_, err = w.FileHandler.Seek(currentOffset, io.SeekStart)
		if err != nil {
			return fmt.Errorf("定位文件指针失败: %v", err)
		}

		// 解码记录
		rec, err := record.DecodeStreamToRecord(w.FileHandler)
		if err != nil {
			if err == io.EOF {
				fmt.Println("文件读取完成")
				break
			}
			return fmt.Errorf("解码记录失败: %v", err)
		}

		// 计算记录大小
		recordSize := record.CalculateRecordSize(rec.Key, rec.Value)
		if rec.RecordType > record.RecordDelete {
			// 如果是事务记录，需要加上事务ID的大小
			recordSize += 4 // 4字节的事务ID
		}

		// 根据记录类型更新索引
		switch rec.RecordType {
		case record.RecordUpdate, record.RecordTxnUpdate:
			pos := &record.RecordPos{
				FileId: w.FileId,
				Offset: uint32(currentOffset),
				Length: recordSize,
			}
			if err := index.Put(rec.Key, pos); err != nil {
				fmt.Printf("更新索引失败: key=%s, pos=%+v, err=%v\n", string(rec.Key), pos, err)
				return err
			}
			fmt.Printf("更新索引成功: key=%s, pos=%+v\n", string(rec.Key), pos)
		case record.RecordDelete, record.RecordTxnDelete:
			if err := index.Delete(rec.Key); err != nil {
				fmt.Printf("删除索引失败: key=%s, err=%v\n", string(rec.Key), err)
				return err
			}
			fmt.Printf("删除索引成功: key=%s\n", string(rec.Key))
		}

		// 更新偏移量
		currentOffset += int64(recordSize)
		w.Offset = uint32(currentOffset)
	}

	return nil
}
