package record

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestRecordEncode(t *testing.T) {
	record := NewRecordUpdate([]byte("key"), []byte("value"))
	encoded := record.Encode()
	fmt.Println(encoded)
}

func TestRecordDecode(t *testing.T) {
	record := NewRecordUpdate([]byte("key"), []byte("value"))
	encoded := record.Encode()
	t.Log(encoded)
	recordType, txnId, decodedKey, decodedValue, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(recordType, txnId, decodedKey, decodedValue)
}

// TestAllRecordTypes tests encoding and decoding of all record types
func TestAllRecordTypes(t *testing.T) {
	testCases := []struct {
		name          string
		record        *Record
		expectedType  RecordType
		expectedTxnId uint32 // 添加预期的事务ID
	}{
		{
			name:          "Update Record",
			record:        NewRecordUpdate([]byte("key1"), []byte("value1")),
			expectedType:  RecordUpdate,
			expectedTxnId: 0, // 普通更新记录的事务ID应为0
		},
		{
			name:          "Delete Record",
			record:        NewRecordDelete([]byte("key2")),
			expectedType:  RecordDelete,
			expectedTxnId: 0, // 普通删除记录的事务ID应为0
		},
		{
			name:          "Empty Key",
			record:        NewRecordUpdate([]byte{}, []byte("value")),
			expectedType:  RecordUpdate,
			expectedTxnId: 0,
		},
		{
			name:          "Empty Value",
			record:        NewRecordUpdate([]byte("key"), []byte{}),
			expectedType:  RecordUpdate,
			expectedTxnId: 0,
		},
		{
			name:          "Empty Key and Value",
			record:        NewRecordUpdate([]byte{}, []byte{}),
			expectedType:  RecordUpdate,
			expectedTxnId: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := tc.record.Encode()
			recordType, txnId, key, value, err := Decode(encoded)

			if err != nil {
				t.Fatalf("Failed to decode: %v", err)
			}

			if recordType != tc.expectedType {
				t.Errorf("Expected record type %v, got %v", tc.expectedType, recordType)
			}

			// 验证事务ID
			if txnId != tc.expectedTxnId {
				t.Errorf("Expected transaction ID %d, got %d", tc.expectedTxnId, txnId)
			}

			if !bytes.Equal(key, tc.record.Key) {
				t.Errorf("Expected key %v, got %v", tc.record.Key, key)
			}

			if !bytes.Equal(value, tc.record.Value) {
				t.Errorf("Expected value %v, got %v", tc.record.Value, value)
			}
		})
	}
}

// TestDecodeFrom tests the decode function with a reader
func TestDecodeFrom(t *testing.T) {
	record := NewRecordUpdate([]byte("test-key"), []byte("test-value"))
	encoded := record.Encode()

	reader := bytes.NewReader(encoded)
	recordType, txnId, key, value, err := decode(reader)

	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if recordType != RecordUpdate {
		t.Errorf("Expected type RecordUpdate, got %v", recordType)
	}

	if txnId != 0 {
		t.Errorf("Expected txnId 0, got %d", txnId)
	}

	if !bytes.Equal(key, []byte("test-key")) {
		t.Errorf("Expected key 'test-key', got '%s'", key)
	}

	if !bytes.Equal(value, []byte("test-value")) {
		t.Errorf("Expected value 'test-value', got '%s'", value)
	}

	// Test EOF detection
	_, _, _, _, err = decode(reader)
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

// TestDecodeStream tests processing multiple records from a stream
func TestDecodeStream(t *testing.T) {
	// Create a buffer with multiple records
	records := []*Record{
		NewRecordUpdate([]byte("key1"), []byte("value1")),
		NewRecordUpdate([]byte("key2"), []byte("value2")),
		NewRecordDelete([]byte("key3")),
		NewRecordUpdate([]byte("key4"), []byte("value4")),
	}

	var buf bytes.Buffer
	for _, r := range records {
		buf.Write(r.Encode())
	}

	// Keep track of processed records
	processedRecords := 0

	err := DecodeStream(&buf,
		// 数据记录回调
		func(recordType RecordType, txnId uint32, key, value []byte) bool {
			if processedRecords >= len(records) {
				t.Errorf("Too many records processed")
				return false
			}

			expected := records[processedRecords]
			if recordType != expected.RecordType {
				t.Errorf("Record %d: Expected type %v, got %v",
					processedRecords, expected.RecordType, recordType)
			}

			if !bytes.Equal(key, expected.Key) {
				t.Errorf("Record %d: Expected key %s, got %s",
					processedRecords, expected.Key, key)
			}

			if !bytes.Equal(value, expected.Value) {
				t.Errorf("Record %d: Expected value %s, got %s",
					processedRecords, expected.Value, value)
			}

			processedRecords++
			return true
		},
		// 事务记录回调（此测试不处理事务记录）
		nil)

	if err != nil {
		t.Fatalf("DecodeStream failed: %v", err)
	}

	if processedRecords != len(records) {
		t.Errorf("Expected to process %d records, but processed %d", len(records), processedRecords)
	}
}

// TestDecodeStreamToMap tests rebuilding state from a stream of records
func TestDecodeStreamToMap(t *testing.T) {
	// Create a buffer with a sequence of operations
	var buf bytes.Buffer

	// Series of operations: insert, update, delete
	operations := []*Record{
		NewRecordUpdate([]byte("key1"), []byte("value1")),
		NewRecordUpdate([]byte("key2"), []byte("value2")),
		NewRecordUpdate([]byte("key1"), []byte("updated-value1")), // Update existing
		NewRecordDelete([]byte("key2")),                           // Delete
		NewRecordUpdate([]byte("key3"), []byte("value3")),
	}

	// Expected final state
	expected := map[string][]byte{
		"key1": []byte("updated-value1"),
		"key3": []byte("value3"),
		// key2 should be deleted
	}

	// Write all operations to buffer
	for _, op := range operations {
		buf.Write(op.Encode())
	}

	// Rebuild state using DecodeStreamToMap
	result, err := DecodeStreamToMap(&buf)
	if err != nil {
		t.Fatalf("DecodeStreamToMap failed: %v", err)
	}

	// Verify the result matches expected state
	if len(result) != len(expected) {
		t.Errorf("Expected %d entries, got %d", len(expected), len(result))
	}

	for k, v := range expected {
		resultValue, exists := result[k]
		if !exists {
			t.Errorf("Key %s missing from result", k)
			continue
		}

		if !bytes.Equal(resultValue, v) {
			t.Errorf("Key %s: expected value %s, got %s", k, v, resultValue)
		}
	}

	// Verify deleted key is actually gone
	if _, exists := result["key2"]; exists {
		t.Errorf("Key 'key2' should have been deleted but still exists")
	}
}

// TestEarlyTermination tests that the callback can terminate stream processing
func TestEarlyTermination(t *testing.T) {
	// Create 5 test records
	records := []*Record{
		NewRecordUpdate([]byte("key1"), []byte("value1")),
		NewRecordUpdate([]byte("key2"), []byte("value2")),
		NewRecordDelete([]byte("key3")),
		NewRecordUpdate([]byte("key4"), []byte("value4")),
		NewRecordDelete([]byte("key5")),
	}

	// Create a buffer with all encoded records
	var buf bytes.Buffer
	for _, r := range records {
		buf.Write(r.Encode())
	}

	processedCount := 0

	// Process only the first 3 records
	err := DecodeStream(&buf,
		// 数据记录回调
		func(recordType RecordType, txnId uint32, key, value []byte) bool {
			processedCount++
			return processedCount < 3 // Stop after processing 3 records
		},
		// 事务记录回调（此测试不处理事务记录）
		nil)

	if err != nil {
		t.Fatalf("DecodeStream failed: %v", err)
	}

	if processedCount != 3 {
		t.Errorf("Expected to process 3 records, but processed %d", processedCount)
	}
}

// TestInvalidData tests handling of corrupted or invalid record data
func TestInvalidData(t *testing.T) {
	testCases := []struct {
		name        string
		data        []byte
		expectError bool
	}{
		{
			name:        "Empty data",
			data:        []byte{},
			expectError: true,
		},
		{
			name:        "Incomplete header",
			data:        []byte{0x00, 0x01}, // Just a partial header
			expectError: true,
		},
		{
			name:        "Missing key data",
			data:        []byte{byte(RecordUpdate), 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x05},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, _, err := Decode(tc.data)

			if tc.expectError && err == nil {
				t.Errorf("Expected error for corrupted data, got nil")
			}

			if !tc.expectError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}
		})
	}
}

// TestTransactionEncodeDecode tests the encoding and decoding of transaction records
func TestTransactionEncodeDecode(t *testing.T) {
	testCases := []struct {
		name         string
		txn          *Record
		expectedType RecordType
		expectedId   uint32
	}{
		{
			name:         "Begin Transaction",
			txn:          NewRecordTxnBegin(123),
			expectedType: RecordTxnBegin,
			expectedId:   123,
		},
		{
			name:         "Commit Transaction",
			txn:          NewRecordTxnCommit(456),
			expectedType: RecordTxnCommit,
			expectedId:   456,
		},
		{
			name:         "Abort Transaction",
			txn:          NewRecordTxnAbort(789),
			expectedType: RecordTxnAbort,
			expectedId:   789,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 编码事务记录
			encoded := tc.txn.Encode()

			// 解码事务记录
			recordType, txnId, err := DecodeTxnControl(encoded)
			if err != nil {
				t.Fatalf("Failed to decode transaction record: %v", err)
			}

			// 验证记录类型
			if recordType != tc.expectedType {
				t.Errorf("Expected record type %v, got %v", tc.expectedType, recordType)
			}

			// 验证事务ID
			if txnId != tc.expectedId {
				t.Errorf("Expected transaction ID %d, got %d", tc.expectedId, txnId)
			}
		})
	}
}

// TestMixedRecordTypes tests processing a stream with mixed record types
func TestMixedRecordTypes(t *testing.T) {
	// 创建包含混合记录类型的缓冲区
	var buf bytes.Buffer

	// 添加各种类型的记录到缓冲区
	buf.Write(NewRecordTxnBegin(1).Encode())
	buf.Write(NewRecordUpdate([]byte("key1"), []byte("value1")).Encode())
	buf.Write(NewRecordDelete([]byte("key2")).Encode())
	buf.Write(NewRecordTxnCommit(1).Encode())
	buf.Write(NewRecordTxnBegin(2).Encode())
	buf.Write(NewRecordTxnAbort(2).Encode())

	regularRecords := 0
	txnRecords := 0

	// 解码并计数不同类型的记录
	err := DecodeStream(&buf,
		// 普通记录回调
		func(recordType RecordType, txnId uint32, key, value []byte) bool {
			regularRecords++
			return true
		},
		// 事务记录回调
		func(recordType RecordType, txnId uint32) bool {
			txnRecords++
			return true
		})

	if err != nil {
		t.Fatalf("Failed to decode mixed records: %v", err)
	}

	// 验证计数
	if regularRecords != 2 {
		t.Errorf("Expected 2 regular records, got %d", regularRecords)
	}

	if txnRecords != 4 {
		t.Errorf("Expected 4 transaction records, got %d", txnRecords)
	}
}

// TestTransactionHandling tests that transactions are properly applied or discarded
func TestTransactionHandling(t *testing.T) {
	var buf bytes.Buffer

	// Write a sequence with mixed operations and transactions
	// Transaction 101: committed (should be applied)
	buf.Write(NewRecordUpdate([]byte("key1"), []byte("initial")).Encode())
	buf.Write(NewRecordTxnBegin(101).Encode())
	buf.Write(NewRecordUpdate([]byte("key1"), []byte("tx101-update")).Encode())
	buf.Write(NewRecordUpdate([]byte("key2"), []byte("tx101-value")).Encode())
	buf.Write(NewRecordTxnCommit(101).Encode())

	// Transaction 102: aborted (should be discarded)
	buf.Write(NewRecordTxnBegin(102).Encode())
	buf.Write(NewRecordUpdate([]byte("key1"), []byte("tx102-update")).Encode())
	buf.Write(NewRecordDelete([]byte("key2")).Encode())
	buf.Write(NewRecordTxnAbort(102).Encode())

	// Transaction 103: committed (should be applied)
	buf.Write(NewRecordTxnBegin(103).Encode())
	buf.Write(NewRecordDelete([]byte("key1")).Encode())
	buf.Write(NewRecordTxnCommit(103).Encode())

	// Expected final state after processing all transactions
	expectedState := map[string][]byte{
		// key1 should be deleted due to tx103
		// key2 should have value from tx101 ("tx101-value")
		"key2": []byte("tx101-value"),
	}

	result, err := DecodeStreamToMap(&buf)
	if err != nil {
		t.Fatalf("DecodeStreamToMap failed: %v", err)
	}

	// Verify size
	if len(result) != len(expectedState) {
		t.Errorf("Expected %d entries, got %d", len(expectedState), len(result))
	}

	// Verify each key/value
	for k, expectedValue := range expectedState {
		actualValue, exists := result[k]
		if !exists {
			t.Errorf("Expected key %s not found in result", k)
			continue
		}

		if !bytes.Equal(actualValue, expectedValue) {
			t.Errorf("For key %s: expected value %q, got %q",
				k, string(expectedValue), string(actualValue))
		}
	}

	// Verify key1 is deleted
	if _, exists := result["key1"]; exists {
		t.Errorf("Key 'key1' should have been deleted but still exists with value %q",
			string(result["key1"]))
	}
}
