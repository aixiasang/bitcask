package record

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type RecordType uint8

const (
	RecordUpdate    RecordType = iota // 更新
	RecordDelete                      // 删除
	RecordTxnUpdate                   // 事务更新
	RecordTxnDelete                   // 事务删除
	RecordTxnBegin                    // 事务开始
	RecordTxnCommit                   // 事务提交
	RecordTxnAbort                    // 事务回滚
	MaxKeySize      = 1 << 20         // 1MB
	MaxValueSize    = 1 << 30         // 1GB
)

// Record 数据记录结构
type Record struct {
	RecordType  RecordType // 记录类型
	TxnId       uint32     // 事务ID，0表示不属于任何事务
	KeyLength   uint32     // 键长度
	ValueLength uint32     // 值长度
	Key         []byte     // 键
	Value       []byte     // 值
}

// RecordPos 记录在文件中的位置信息
type RecordPos struct {
	FileId uint32 // 文件ID
	Offset uint32 // 偏移量
	Length uint32 // 长度
}

// NewRecordUpdate 创建一个更新类型的记录
func NewRecordUpdate(key []byte, value []byte) *Record {
	return newRecord(RecordUpdate, key, value)
}

// NewRecordDelete 创建一个删除类型的记录
func NewRecordDelete(key []byte) *Record {
	return newRecord(RecordDelete, key, nil)
}

// NewRecordTxnBegin 创建一个事务开始的记录
func NewRecordTxnBegin(txId uint32) *Record {
	return newRecordTxn(RecordTxnBegin, txId)
}

// NewRecordTxnCommit 创建一个事务提交的记录
func NewRecordTxnCommit(txId uint32) *Record {
	return newRecordTxn(RecordTxnCommit, txId)
}

// NewRecordTxnAbort 创建一个事务回滚的记录
func NewRecordTxnAbort(txId uint32) *Record {
	return newRecordTxn(RecordTxnAbort, txId)
}

// NewRecordWithTxn 创建一个带事务ID的记录
func NewRecordWithTxn(recordType RecordType, key []byte, value []byte, txnId uint32) *Record {
	record := newRecord(recordType, key, value)
	record.TxnId = txnId
	return record
}

// newRecord 创建一个新的记录
func newRecord(recordType RecordType, key []byte, value []byte) *Record {
	return &Record{
		RecordType:  recordType,
		Key:         key,
		Value:       value,
		KeyLength:   uint32(len(key)),
		ValueLength: uint32(len(value)),
	}
}

// newRecordTxn 创建一个新的事务记录
func newRecordTxn(recordType RecordType, txId uint32) *Record {
	return &Record{
		RecordType: recordType,
		TxnId:      txId,
	}
}

// Encode 将记录编码为字节流
func (record *Record) Encode() []byte {
	var buf *bytes.Buffer

	switch record.RecordType {
	case RecordUpdate, RecordDelete:
		// 普通记录不需要事务ID
		buf = bytes.NewBuffer(make([]byte, 0, 1+4+4+len(record.Key)+len(record.Value)))
		buf.WriteByte(byte(record.RecordType))
		if err := binary.Write(buf, binary.BigEndian, record.KeyLength); err != nil {
			panic(fmt.Sprintf("写入键长度失败: %v", err))
		}
		if err := binary.Write(buf, binary.BigEndian, record.ValueLength); err != nil {
			panic(fmt.Sprintf("写入值长度失败: %v", err))
		}
	case RecordTxnBegin, RecordTxnCommit, RecordTxnAbort:
		// 事务控制记录只需要记录类型和事务ID
		buf = bytes.NewBuffer(make([]byte, 0, 1+4))
		buf.WriteByte(byte(record.RecordType))
		if err := binary.Write(buf, binary.BigEndian, record.TxnId); err != nil {
			panic(fmt.Sprintf("写入事务ID失败: %v", err))
		}
		return buf.Bytes() // 事务控制记录不包含键值，直接返回
	default:
		// 事务相关记录需要事务ID
		buf = bytes.NewBuffer(make([]byte, 0, 1+4+4+4+len(record.Key)+len(record.Value)))
		buf.WriteByte(byte(record.RecordType))
		if err := binary.Write(buf, binary.BigEndian, record.TxnId); err != nil {
			panic(fmt.Sprintf("写入事务ID失败: %v", err))
		}
		if err := binary.Write(buf, binary.BigEndian, record.KeyLength); err != nil {
			panic(fmt.Sprintf("写入键长度失败: %v", err))
		}
		if err := binary.Write(buf, binary.BigEndian, record.ValueLength); err != nil {
			panic(fmt.Sprintf("写入值长度失败: %v", err))
		}
	}

	if _, err := buf.Write(record.Key); err != nil {
		panic(fmt.Sprintf("写入键失败: %v", err))
	}
	if _, err := buf.Write(record.Value); err != nil {
		panic(fmt.Sprintf("写入值失败: %v", err))
	}
	return buf.Bytes()
}

// Decode 解码数据记录字节流
func Decode(data []byte) (RecordType, uint32, []byte, []byte, error) {
	reader := bytes.NewReader(data)
	return decode(reader)
}

// DecodeTxnControl 解码事务记录字节流
func DecodeTxnControl(data []byte) (RecordType, uint32, error) {
	reader := bytes.NewReader(data)
	return decodeTxnControl(reader)
}

// decode 解码数据记录字节流
func decode(reader io.Reader) (RecordType, uint32, []byte, []byte, error) {
	var recordType RecordType
	var txnId uint32
	var keyLength uint32
	var valueLength uint32

	fmt.Println("开始解码数据记录...")

	// 读取记录类型
	err := binary.Read(reader, binary.BigEndian, &recordType)
	if err != nil {
		if err == io.EOF {
			fmt.Println("遇到EOF，文件结束")
			return 0, 0, nil, nil, io.EOF
		}
		fmt.Printf("读取记录类型失败: %v\n", err)
		return 0, 0, nil, nil, fmt.Errorf("读取记录类型失败: %v", err)
	}
	fmt.Printf("读取到记录类型: %d\n", recordType)

	// 验证记录类型
	if recordType > RecordTxnAbort {
		fmt.Printf("无效的记录类型: %d\n", recordType)
		return 0, 0, nil, nil, fmt.Errorf("无效的记录类型: %d", recordType)
	}

	// 根据记录类型决定是否读取事务ID
	switch recordType {
	case RecordUpdate, RecordDelete:
		txnId = 0 // 普通记录没有事务ID
		fmt.Println("普通记录，不读取事务ID")
	default:
		err = binary.Read(reader, binary.BigEndian, &txnId)
		if err != nil {
			fmt.Printf("读取事务ID失败: %v\n", err)
			return 0, 0, nil, nil, fmt.Errorf("读取事务ID失败: %v", err)
		}
		fmt.Printf("读取到事务ID: %d\n", txnId)
	}

	// 读取键长度
	err = binary.Read(reader, binary.BigEndian, &keyLength)
	if err != nil {
		fmt.Printf("读取键长度失败: %v\n", err)
		return 0, 0, nil, nil, fmt.Errorf("读取键长度失败: %v", err)
	}
	fmt.Printf("读取到键长度: %d\n", keyLength)

	// 读取值长度
	err = binary.Read(reader, binary.BigEndian, &valueLength)
	if err != nil {
		fmt.Printf("读取值长度失败: %v\n", err)
		return 0, 0, nil, nil, fmt.Errorf("读取值长度失败: %v", err)
	}
	fmt.Printf("读取到值长度: %d\n", valueLength)

	// 验证键值大小
	if keyLength > MaxKeySize {
		fmt.Printf("键太大: %d > %d\n", keyLength, MaxKeySize)
		return 0, 0, nil, nil, fmt.Errorf("键太大: %d > %d", keyLength, MaxKeySize)
	}
	if valueLength > MaxValueSize {
		fmt.Printf("值太大: %d > %d\n", valueLength, MaxValueSize)
		return 0, 0, nil, nil, fmt.Errorf("值太大: %d > %d", valueLength, MaxValueSize)
	}

	// 读取键
	key := make([]byte, keyLength)
	n, err := io.ReadFull(reader, key)
	if err != nil {
		fmt.Printf("读取键失败(读取了%d字节): %v\n", n, err)
		return 0, 0, nil, nil, fmt.Errorf("读取键失败(读取了%d字节): %v", n, err)
	}
	fmt.Printf("读取到键: %s\n", string(key))

	// 读取值
	var value []byte
	if valueLength > 0 {
		value = make([]byte, valueLength)
		n, err = io.ReadFull(reader, value)
		if err != nil {
			fmt.Printf("读取值失败(读取了%d字节): %v\n", n, err)
			return 0, 0, nil, nil, fmt.Errorf("读取值失败(读取了%d字节): %v", n, err)
		}
		fmt.Printf("读取到值: %s\n", string(value))
	} else if recordType == RecordDelete {
		value = nil
		fmt.Println("删除记录，值为nil")
	} else {
		value = make([]byte, 0)
		fmt.Println("读取到空值")
	}

	fmt.Println("记录解码完成")
	return recordType, txnId, key, value, nil
}

// decodeTxnControl 解码事务记录字节流
func decodeTxnControl(reader io.Reader) (RecordType, uint32, error) {
	var recordType RecordType
	var txnId uint32

	err := binary.Read(reader, binary.BigEndian, &recordType)
	if err != nil {
		return 0, 0, err
	}

	// 验证是否为事务控制记录类型
	if recordType != RecordTxnBegin && recordType != RecordTxnCommit && recordType != RecordTxnAbort {
		return 0, 0, fmt.Errorf("非事务控制记录类型: %d", recordType)
	}

	err = binary.Read(reader, binary.BigEndian, &txnId)
	if err != nil {
		return 0, 0, err
	}

	return recordType, txnId, nil
}

// RecordCallback 处理数据记录的回调函数类型
type RecordCallback func(recordType RecordType, txnId uint32, key, value []byte) bool

// TxnCallback 处理事务记录的回调函数类型
type TxnCallback func(recordType RecordType, txnId uint32) bool

// DecodeStream 从流中读取并处理所有类型的记录
func DecodeStream(reader io.Reader, recordCallback RecordCallback, txnCallback TxnCallback) error {
	buffer := make([]byte, 1) // 用于读取记录类型的缓冲区
	var currentTxn uint32 = 0 // 当前正在处理的事务ID

	for {
		// 尝试读取记录类型
		n, err := io.ReadFull(reader, buffer)
		if err != nil {
			if err == io.EOF {
				return nil // 正常结束
			}
			if err == io.ErrUnexpectedEOF {
				return fmt.Errorf("读取记录类型时遇到意外的文件结束")
			}
			return fmt.Errorf("读取记录类型失败: %v", err)
		}
		if n != 1 {
			return fmt.Errorf("读取记录类型时获取到错误的字节数: %d", n)
		}

		recordType := RecordType(buffer[0])
		if recordType > RecordTxnAbort {
			return fmt.Errorf("无效的记录类型: %d", recordType)
		}

		// 根据记录类型选择不同的处理方式
		if recordType == RecordTxnBegin || recordType == RecordTxnCommit || recordType == RecordTxnAbort {
			// 事务控制记录
			if txnCallback != nil {
				recordType, txnId, err := decodeTxnControl(reader)
				if err != nil {
					return fmt.Errorf("解码事务控制记录失败: %v", err)
				}

				if recordType == RecordTxnBegin {
					if currentTxn != 0 {
						return fmt.Errorf("在事务 %d 结束前试图开始新事务 %d", currentTxn, txnId)
					}
					currentTxn = txnId
				} else if recordType == RecordTxnCommit || recordType == RecordTxnAbort {
					if currentTxn != txnId {
						return fmt.Errorf("试图结束错误的事务: 期望 %d, 实际 %d", currentTxn, txnId)
					}
					currentTxn = 0
				}

				if !txnCallback(recordType, txnId) {
					return nil
				}
			} else {
				// 跳过事务控制记录
				if err := skipTxnRecord(reader); err != nil {
					return fmt.Errorf("跳过事务控制记录失败: %v", err)
				}
			}
		} else {
			// 数据记录
			if recordCallback != nil {
				recordType, txnId, key, value, err := decode(reader)
				if err != nil {
					return fmt.Errorf("解码数据记录失败: %v", err)
				}

				// 验证事务一致性
				if txnId != 0 && txnId != currentTxn {
					return fmt.Errorf("记录的事务ID (%d) 与当前事务 (%d) 不匹配", txnId, currentTxn)
				}

				if !recordCallback(recordType, txnId, key, value) {
					return nil
				}
			} else {
				if err := skipRecord(reader); err != nil {
					return fmt.Errorf("跳过数据记录失败: %v", err)
				}
			}
		}
	}
}

// skipRecord 跳过流中的一个数据记录
func skipRecord(reader io.Reader) error {
	var recordType RecordType
	var keyLength, valueLength uint32

	// 读取记录类型
	err := binary.Read(reader, binary.BigEndian, &recordType)
	if err != nil {
		return err
	}

	// 根据记录类型决定是否跳过事务ID
	switch recordType {
	case RecordUpdate, RecordDelete:
		// 普通记录不需要跳过事务ID
	default:
		// 跳过事务ID
		_, err = io.ReadFull(reader, make([]byte, 4))
		if err != nil {
			return err
		}
	}

	// 读取键长度
	err = binary.Read(reader, binary.BigEndian, &keyLength)
	if err != nil {
		return err
	}

	// 读取值长度
	err = binary.Read(reader, binary.BigEndian, &valueLength)
	if err != nil {
		return err
	}

	// 跳过键字节
	_, err = io.ReadFull(reader, make([]byte, keyLength))
	if err != nil {
		return err
	}

	// 跳过值字节
	_, err = io.ReadFull(reader, make([]byte, valueLength))
	if err != nil {
		return err
	}

	return nil
}

// skipTxnRecord 跳过流中的一个事务控制记录
func skipTxnRecord(reader io.Reader) error {
	// 跳过记录类型（已经读取）
	_, err := io.ReadFull(reader, make([]byte, 1))
	if err != nil {
		return err
	}

	// 跳过事务ID (4字节)
	_, err = io.ReadFull(reader, make([]byte, 4))
	if err != nil {
		return err
	}

	return nil
}

// DecodeStreamToMap 从流中读取所有记录并构建最新值的映射
// 这对于从一系列更新中重建最新状态非常有用
// 现在还跟踪活动事务及其影响
func DecodeStreamToMap(reader io.Reader) (map[string][]byte, error) {
	result := make(map[string][]byte)
	activeTxns := make(map[uint32]map[string][]byte) // 跟踪每个事务的未决更改
	currentTxnId := uint32(0)                        // 0表示不在事务中

	err := DecodeStream(reader,
		// 数据记录回调函数
		func(recordType RecordType, txnId uint32, key, value []byte) bool {
			keyStr := string(key)

			if currentTxnId > 0 {
				// 在事务内部，将更改存储在事务映射中
				txnMap := activeTxns[currentTxnId]
				switch recordType {
				case RecordUpdate:
					txnMap[keyStr] = value
				case RecordDelete:
					txnMap[keyStr] = nil // nil值表示删除
				}
			} else {
				// 不在事务中，直接应用
				switch recordType {
				case RecordUpdate:
					result[keyStr] = value
				case RecordDelete:
					delete(result, keyStr)
				}
			}
			return true // 继续处理
		},
		// 事务记录回调函数
		func(recordType RecordType, txnId uint32) bool {
			switch recordType {
			case RecordTxnBegin:
				// 初始化新事务的未决更改
				currentTxnId = txnId
				activeTxns[txnId] = make(map[string][]byte)
			case RecordTxnCommit:
				// 应用已提交事务的所有更改
				if txnChanges, exists := activeTxns[txnId]; exists {
					for k, v := range txnChanges {
						if v == nil {
							// 这是一个删除操作
							delete(result, k)
						} else {
							// 这是一个更新操作
							result[k] = v
						}
					}
					delete(activeTxns, txnId)
				}
				currentTxnId = 0 // 退出事务模式
			case RecordTxnAbort:
				// 丢弃中止事务的更改
				delete(activeTxns, txnId)
				currentTxnId = 0 // 退出事务模式
			}
			return true // 继续处理
		})

	// 如果有任何未提交的事务，我们忽略它们

	return result, err
}

// CalculateRecordSize 计算记录的总大小
func CalculateRecordSize(key, value []byte) uint32 {
	keyLen := len(key)
	valueLen := len(value)

	// 记录格式：
	// 普通记录：1字节记录类型 + 4字节键长度 + 4字节值长度 + 键内容 + 值内容
	return 1 + 4 + 4 + uint32(keyLen) + uint32(valueLen)
}

// DecodeStreamToRecord 从字节流中解码数据记录
func DecodeStreamToRecord(reader io.Reader) (*Record, error) {
	fmt.Println("开始解码数据记录...")
	recordType, txnId, key, value, err := decode(reader)
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("解码记录失败: %v", err)
	}

	fmt.Printf("解码完成: 记录类型=%d, 事务ID=%d, 键长度=%d, 值长度=%d\n",
		recordType, txnId, len(key), len(value))

	return &Record{
		RecordType:  recordType,
		TxnId:       txnId,
		Key:         key,
		Value:       value,
		KeyLength:   uint32(len(key)),
		ValueLength: uint32(len(value)),
	}, nil
}
