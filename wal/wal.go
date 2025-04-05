package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/aixiasang/bitcask/config"
	"github.com/aixiasang/bitcask/index"
	"github.com/aixiasang/bitcask/record"
	"github.com/aixiasang/bitcask/utils"
)

type Wal struct {
	conf   *config.Config // 配置
	fileId uint32         // 文件ID
	offset uint32         // 偏移量
	fp     *os.File       // 文件
	mu     sync.RWMutex   // 互斥锁
}

func NewWal(conf *config.Config, fileId uint32) (*Wal, error) {
	filePath := filepath.Join(conf.DataDir, conf.WalDir, fmt.Sprintf("wal-%d.log", fileId))
	fp, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &Wal{conf: conf, fileId: fileId, fp: fp}, nil
}

func (w *Wal) Write(key, value []byte) (*record.Pos, error) {
	rec := record.NewRecord(key, value)
	return w.write(rec)
}

func (w *Wal) WriteTxn(key, value []byte) (*record.Pos, error) {
	rec := record.NewTxnRecord(key, value)
	return w.write(rec)
}
func (w *Wal) WriteTxnCommit(key []byte) (*record.Pos, error) {
	rec := record.NewTxnCommit(key)
	return w.write(rec)
}
func (w *Wal) WriteTxnBegin(key []byte) (*record.Pos, error) {
	rec := record.NewTxnBegin(key)
	return w.write(rec)
}

func (w *Wal) write(rec *record.Record) (*record.Pos, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	preOffset := w.offset
	encoded, err := rec.Encode()
	if err != nil {
		return nil, err
	}
	length, err := w.fp.Write(encoded)
	if err != nil {
		return nil, err
	}
	if w.conf.AutoSync {
		if err := w.fp.Sync(); err != nil {
			return nil, err
		}
	}
	w.offset += uint32(length)
	return &record.Pos{
		FileId: w.fileId,
		Offset: preOffset,
		Length: uint32(length),
	}, nil
}
func (w *Wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fp.Sync(); err != nil {
		return err
	}
	return w.fp.Close()
}

func (w *Wal) ReadPos(pos *record.Pos) (*record.Record, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// 检查pos是否有效
	if pos == nil {
		return nil, errors.New("position is nil")
	}

	// 获取文件大小，防止越界读取
	fileInfo, err := w.fp.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	endOffset := int64(pos.Offset) + int64(pos.Length)

	// 检查是否超出文件范围
	if int64(pos.Offset) >= fileSize || endOffset > fileSize {
		return nil, fmt.Errorf("read position out of file range: offset=%d, length=%d, fileSize=%d",
			pos.Offset, pos.Length, fileSize)
	}

	// 读取记录数据
	buf := make([]byte, pos.Length)
	n, err := w.fp.ReadAt(buf, int64(pos.Offset))
	if err != nil {
		if err == io.EOF && n > 0 {
			// 部分读取成功，可能是文件末尾数据不完整
			return nil, fmt.Errorf("incomplete record at file end: read %d of %d bytes", n, pos.Length)
		}
		return nil, err
	}

	// 确保读取了完整的数据
	if uint32(n) < pos.Length {
		return nil, fmt.Errorf("incomplete record read: read %d of %d bytes", n, pos.Length)
	}

	// 解码记录
	rec, err := record.DecodeRecord(buf)
	if err != nil {
		// 记录解码失败但有数据，提供更多细节
		return nil, fmt.Errorf("failed to decode record at offset %d: %v", pos.Offset, err)
	}

	return rec, nil
}

type txnData struct {
	rec *record.Record
	pos *record.Pos
}

func (w *Wal) ReadAll(memTable index.Index, dbTxnId *atomic.Uint32) error {
	// 将文件指针移到开始位置
	if _, err := w.fp.Seek(0, 0); err != nil {
		return err
	}

	if w.conf.Debug {
		fmt.Printf("开始从文件ID=%d读取全部记录\n", w.fileId)
	}

	// 获取文件大小
	fileInfo, err := w.fp.Stat()
	if err != nil {
		return fmt.Errorf("无法获取文件大小: %v", err)
	}
	fileSize := fileInfo.Size()

	// 读取整个文件
	buffer := make([]byte, fileSize)
	n, err := w.fp.ReadAt(buffer, 0)
	if err != nil && err != io.EOF {
		return fmt.Errorf("读取文件内容失败: %v", err)
	}
	if int64(n) < fileSize {
		fmt.Printf("警告：仅读取了文件部分内容: %d 字节，总大小 %d 字节\n", n, fileSize)
	}

	batchData := make(map[uint32][]*txnData)
	txnFlag := false
	curTxnId := uint32(0)
	updatedFunc := func(rec *record.Record, pos *record.Pos) error {
		if rec.RecordType == record.RecordTypeBegin {
			txnFlag = true
			curTxnId, _ = utils.DecodeTxnId(rec.Key)
			return nil
		}
		// 事务开始标志
		if txnFlag {
			if rec.RecordType == record.RecordTypeTxnPut {
				if w.conf.Debug {
					fmt.Printf("处理事务写入记录: key=%s, value=%s\n", string(rec.Key), string(rec.Value))
				}
				txnId, decKey := utils.DecodeTxnId(rec.Key)
				if txnId != curTxnId {
					return fmt.Errorf("事务ID不匹配: %d != %d", txnId, curTxnId)
				}
				rec.Key = decKey
				batchData[txnId] = append(batchData[txnId], &txnData{
					rec: rec,
					pos: pos,
				})
			} else if rec.RecordType == record.RecordTypeTxnDelete {
				if w.conf.Debug {
					fmt.Printf("处理事务删除记录: key=%s\n", string(rec.Key))
				}
				txnId, decKey := utils.DecodeTxnId(rec.Key)
				if txnId != curTxnId {
					return fmt.Errorf("事务ID不匹配: %d != %d", txnId, curTxnId)
				}
				rec.Key = decKey
				batchData[txnId] = append(batchData[txnId], &txnData{
					rec: rec,
					pos: pos,
				})
			} else if rec.RecordType == record.RecordTypeTxnCommit {
				if w.conf.Debug {
					fmt.Printf("处理事务提交记录: key=%s\n", string(rec.Key))
				}
				txnId, _ := utils.DecodeTxnId(rec.Key)
				if txnId != curTxnId {
					return fmt.Errorf("事务ID不匹配: %d != %d", txnId, curTxnId)
				}
				for _, rec := range batchData[txnId] {
					if rec.rec.RecordType == record.RecordTypeTxnPut {
						if err := memTable.Put(rec.rec.Key, rec.pos); err != nil {
							return fmt.Errorf("更新索引失败: %v", err)
						}
					} else if rec.rec.RecordType == record.RecordTypeTxnDelete {
						if err := memTable.Delete(rec.rec.Key); err != nil {
							return fmt.Errorf("删除索引失败: %v", err)
						}
					}
				}
				delete(batchData, txnId) // 删除事务数据
				dbTxnId.Store(curTxnId)  // 更新事务ID
				curTxnId = 0             // 重置事务ID
				txnFlag = false          // 重置事务标志
				return nil
			}
		} else {
			// 基于记录类型处理
			if rec.RecordType == record.RecordTypeDelete {
				if w.conf.Debug {
					fmt.Printf("处理删除记录: key=%s\n", string(rec.Key))
				}
				if err := memTable.Delete(rec.Key); err != nil {
					return fmt.Errorf("删除索引失败: %v", err)
				}
			} else if rec.RecordType == record.RecordTypePut {
				if w.conf.Debug {
					fmt.Printf("处理普通记录: key=%s, value=%s\n", string(rec.Key), string(rec.Value))
				}
				if err := memTable.Put(rec.Key, pos); err != nil {
					return fmt.Errorf("更新索引失败: %v", err)
				}
			}
		}
		return nil
	}
	// 逐条解析记录并保存最新的记录位置
	var offset uint32 = 0
	for offset < uint32(n) {
		// 确保至少能读取头部
		if offset+9 > uint32(n) {
			fmt.Printf("文件末尾不完整，停止解析: 剩余 %d 字节\n", uint32(n)-offset)
			break
		}

		// 记录起始位置
		recordStartOffset := offset

		// 读取记录类型
		recordType := record.RecordType(buffer[offset])

		// 读取 key 长度
		keyLength := binary.BigEndian.Uint32(buffer[offset+1 : offset+5])

		// 读取 value 长度
		valueLength := binary.BigEndian.Uint32(buffer[offset+5 : offset+9])

		// 检查 key 和 value 长度的合理性
		if keyLength > 10*1024*1024 || valueLength > 100*1024*1024 {
			fmt.Printf("警告: 可能的数据损坏 - key长度: %d, value长度: %d\n", keyLength, valueLength)
			break
		}

		// 计算记录总长度
		recordLength := 9 + keyLength + valueLength + 4

		// 确保能读取完整的记录
		if offset+recordLength > uint32(n) {
			fmt.Printf("文件末尾记录不完整，停止解析: 需要 %d 字节，剩余 %d 字节\n",
				recordLength, uint32(n)-offset)
			break
		}

		// 读取 key 和 value
		key := buffer[offset+9 : offset+9+keyLength]
		value := buffer[offset+9+keyLength : offset+9+keyLength+valueLength]

		// 读取 CRC
		crc := binary.BigEndian.Uint32(buffer[offset+9+keyLength+valueLength : offset+recordLength])

		// 计算CRC进行验证
		computedCrc := crc32.ChecksumIEEE(buffer[offset : offset+9+keyLength+valueLength])
		if crc != computedCrc {
			fmt.Printf("警告: CRC校验失败 (offset=%d) - 存储的: %d, 计算的: %d\n",
				offset, crc, computedCrc)
			// 继续处理，但记录警告
		}

		if w.conf.Debug {
			fmt.Printf("解析记录: type=%d, key=%s, keyLen=%d, valueLen=%d, offset=%d, len=%d\n",
				recordType, string(key), keyLength, valueLength, offset, recordLength)
		}

		rec := &record.Record{
			RecordType: recordType,
			Key:        key,
			Value:      value,
		}
		pos := &record.Pos{
			FileId: w.fileId,
			Offset: recordStartOffset, // 使用记录的实际起始位置
			Length: recordLength,
		}
		if err := updatedFunc(rec, pos); err != nil {
			return err
		}
		// 更新偏移量
		offset += recordLength
	}

	if w.conf.Debug {
		fmt.Printf("文件ID=%d读取完成，处理了 %d 字节\n", w.fileId, offset)
	}
	// 更新WAL实例的offset以反映文件的实际大小
	w.offset = offset

	return nil
}

func (w *Wal) Size() uint32 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.offset
}

func (w *Wal) Sync() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.fp.Sync()
}

func (w *Wal) FileId() uint32 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.fileId
}
func (w *Wal) UpdateOffset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	fileInfo, err := w.fp.Stat()
	if err != nil {
		return
	}
	w.offset = uint32(fileInfo.Size())
}
func (w *Wal) Delete() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.fp.Sync(); err != nil {
		return err
	}
	if err := w.fp.Close(); err != nil {
		return err
	}
	return os.Remove(w.fp.Name())
}
