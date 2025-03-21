package utils

import (
	"bytes"
	"encoding/binary"
)

func EncodeTxnId(txnId uint32, key []byte) []byte {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.BigEndian, txnId)
	buf.Write(key)
	return buf.Bytes()
}
func DecodeTxnId(data []byte) (uint32, []byte) {
	txnId := binary.BigEndian.Uint32(data[:4])
	key := data[4:]
	return txnId, key
}
