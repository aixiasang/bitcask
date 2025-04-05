package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/aixiasang/bitcask/utils"
)

type RecordType uint8

const (
	RecordTypePut       RecordType = iota // 写入
	RecordTypeDelete                      // 删除
	RecordTypeBegin                       // 事务开始
	RecordTypeTxnPut                      // 事务写入
	RecordTypeTxnDelete                   // 事务删除
	RecordTypeTxnCommit                   // 事务提交
)

type Record struct {
	RecordType RecordType
	Key        []byte
	Value      []byte
}

func NewRecord(key, value []byte) *Record {
	if value == nil {
		return newRecord(key, nil, RecordTypeDelete)
	}
	return newRecord(key, value, RecordTypePut)
}
func NewTxnRecord(key, value []byte) *Record {
	if value == nil {
		return newRecord(key, nil, RecordTypeTxnDelete)
	}
	return newRecord(key, value, RecordTypeTxnPut)
}
func NewTxnCommit(key []byte) *Record {
	return newRecord(key, nil, RecordTypeTxnCommit)
}
func NewTxnBegin(key []byte) *Record {
	return newRecord(key, nil, RecordTypeBegin)
}
func newRecord(key, value []byte, recordType RecordType) *Record {
	return &Record{
		Key:        key,
		Value:      value,
		RecordType: recordType,
	}
}
func (r *Record) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := buf.WriteByte(byte(r.RecordType)); err != nil {
		return nil, errors.New("failed to write record type")
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(r.Key))); err != nil {
		return nil, errors.New("failed to write key length")
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(r.Value))); err != nil {
		return nil, errors.New("failed to write value length")
	}
	if _, err := buf.Write(r.Key); err != nil {
		return nil, errors.New("failed to write key")
	}
	if _, err := buf.Write(r.Value); err != nil {
		return nil, errors.New("failed to write value")
	}
	crc := crc32.ChecksumIEEE(buf.Bytes())
	if err := binary.Write(buf, binary.BigEndian, crc); err != nil {
		return nil, errors.New("failed to write crc")
	}
	return buf.Bytes(), nil
}
func DecodeRecord(data []byte) (*Record, error) {
	if len(data) < 9 { // 至少需要 1 字节类型 + 4 字节 key 长度 + 4 字节 value 长度
		return nil, errors.New("record data too short")
	}

	recordType := RecordType(data[0])

	var keyLength uint32
	if err := binary.Read(bytes.NewReader(data[1:5]), binary.BigEndian, &keyLength); err != nil {
		return nil, errors.New("failed to read key length")
	}

	var valueLength uint32
	if err := binary.Read(bytes.NewReader(data[5:9]), binary.BigEndian, &valueLength); err != nil {
		return nil, errors.New("failed to read value length")
	}

	// 验证长度合理性
	if keyLength > 10*1024*1024 || valueLength > 100*1024*1024 {
		return nil, fmt.Errorf("key or value length too large: keyLength=%d, valueLength=%d", keyLength, valueLength)
	}

	// 验证数据长度是否足够
	expectedLength := 9 + keyLength + valueLength + 4 // header + key + value + crc
	if uint32(len(data)) < expectedLength {
		return nil, errors.New("record data incomplete")
	}

	// 读取 key 和 value
	key := data[9 : 9+keyLength]
	value := data[9+keyLength : 9+keyLength+valueLength]

	// 验证 CRC
	crcData := data[9+keyLength+valueLength:]
	var storedCrc uint32
	if err := binary.Read(bytes.NewReader(crcData), binary.BigEndian, &storedCrc); err != nil {
		return nil, errors.New("failed to read crc")
	}

	// 计算 CRC
	actualCrc := crc32.ChecksumIEEE(data[:9+keyLength+valueLength])
	if storedCrc != actualCrc {
		return nil, errors.New("crc mismatch")
	}
	if recordType == RecordTypeTxnPut || recordType == RecordTypeTxnDelete {
		_, key = utils.DecodeTxnId(key)
	}
	return &Record{
		RecordType: recordType,
		Key:        key,
		Value:      value,
	}, nil
}
