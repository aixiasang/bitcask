package index

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/aixiasang/bitcask/record"
)

// Test basic functionality of Put and Get
func TestBTreeIndexBasicOperations(t *testing.T) {
	index, err := NewBTreeIndex(32)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	btreeIndex, ok := index.(*BTreeIndex)
	if !ok {
		t.Fatalf("Expected BTreeIndex, got %T", index)
	}

	// Test data
	testData := []struct {
		key    []byte
		fileId uint32
		offset uint32
		length uint32
	}{
		{[]byte("key1"), 1, 100, 200},
		{[]byte("key2"), 2, 200, 300},
		{[]byte("key3"), 3, 300, 400},
	}

	// Test Put and Get
	for _, data := range testData {
		pos := &record.RecordPos{
			FileId: data.fileId,
			Offset: data.offset,
			Length: data.length,
		}

		// Put
		err := btreeIndex.Put(data.key, pos)
		if err != nil {
			t.Errorf("Failed to put key %s: %v", data.key, err)
		}

		// Get
		gotPos, err := btreeIndex.Get(data.key)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", data.key, err)
		}

		// Verify
		if gotPos == nil {
			t.Errorf("Expected RecordPos for key %s, got nil", data.key)
			continue
		}

		if gotPos.FileId != data.fileId || gotPos.Offset != data.offset || gotPos.Length != data.length {
			t.Errorf("Key %s: expected pos {%d, %d, %d}, got {%d, %d, %d}",
				data.key, data.fileId, data.offset, data.length,
				gotPos.FileId, gotPos.Offset, gotPos.Length)
		}
	}

	// Test Delete
	deleteKey := []byte("key2")
	err = btreeIndex.Delete(deleteKey)
	if err != nil {
		t.Errorf("Failed to delete key %s: %v", deleteKey, err)
	}

	// Verify deletion
	gotPos, err := btreeIndex.Get(deleteKey)
	if err != nil {
		t.Errorf("Error getting deleted key %s: %v", deleteKey, err)
	}
	if gotPos != nil {
		t.Errorf("Expected nil for deleted key %s, got %+v", deleteKey, gotPos)
	}
}

// Test the AllKey method of the iterator
func TestBTreeIteratorAllKey(t *testing.T) {
	index, err := NewBTreeIndex(32)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	btreeIndex, ok := index.(*BTreeIndex)
	if !ok {
		t.Fatalf("Expected BTreeIndex, got %T", index)
	}

	// Insert keys in non-sorted order
	testKeys := [][]byte{
		[]byte("c"),
		[]byte("a"),
		[]byte("d"),
		[]byte("b"),
		[]byte("e"),
	}

	// Insert keys
	for _, key := range testKeys {
		pos := &record.RecordPos{FileId: 1, Offset: 100, Length: 100}
		if err := btreeIndex.Put(key, pos); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Get iterator and all keys
	iter := btreeIndex.Iterator(true).(*BtreeIterator)
	allKeys := iter.AllKey()

	// Verify count
	if len(allKeys) != len(testKeys) {
		t.Errorf("Expected %d keys, got %d", len(testKeys), len(allKeys))
	}

	// Helper function to check if a key exists in allKeys
	keyExists := func(key []byte) bool {
		for _, k := range allKeys {
			if bytes.Equal(k, key) {
				return true
			}
		}
		return false
	}

	// Verify all keys are present
	for _, key := range testKeys {
		if !keyExists(key) {
			t.Errorf("Key %s not found in AllKey result", key)
		}
	}
}

// Test the Iterator functionality
func TestBTreeIterator(t *testing.T) {
	index, err := NewBTreeIndex(32)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	btreeIndex, ok := index.(*BTreeIndex)
	if !ok {
		t.Fatalf("Expected BTreeIndex, got %T", index)
	}

	// Insert keys in a specific order
	testKeys := [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
		[]byte("d"),
		[]byte("e"),
	}

	// Insert keys
	for i, key := range testKeys {
		pos := &record.RecordPos{FileId: uint32(i + 1), Offset: 100, Length: 100}
		if err := btreeIndex.Put(key, pos); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Test ascending iterator
	t.Run("AscendingIterator", func(t *testing.T) {
		iter := btreeIndex.Iterator(true)
		defer iter.Close()

		// Count keys
		count := 0
		for iter.Rewind(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()

			if key == nil || value == nil {
				t.Errorf("Got nil key or value at position %d", count)
			}

			// Verify key and value
			expectedKey := testKeys[count]
			if !bytes.Equal(key, expectedKey) {
				t.Errorf("Expected key %s at position %d, got %s", expectedKey, count, key)
			}

			expectedFileId := uint32(count + 1)
			if value.FileId != expectedFileId {
				t.Errorf("Expected FileId %d at position %d, got %d", expectedFileId, count, value.FileId)
			}

			count++
		}

		if count != len(testKeys) {
			t.Errorf("Expected to iterate over %d keys, got %d", len(testKeys), count)
		}
	})

	// Test descending iterator
	t.Run("DescendingIterator", func(t *testing.T) {
		iter := btreeIndex.Iterator(false)
		defer iter.Close()

		// Count keys
		count := 0
		for iter.Rewind(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()

			if key == nil || value == nil {
				t.Errorf("Got nil key or value at position %d", count)
			}

			// In descending order, we expect reverse order of keys
			expectedKey := testKeys[len(testKeys)-1-count]
			if !bytes.Equal(key, expectedKey) {
				t.Errorf("Expected key %s at position %d, got %s", expectedKey, count, key)
			}

			expectedFileId := uint32(len(testKeys) - count)
			if value.FileId != expectedFileId {
				t.Errorf("Expected FileId %d at position %d, got %d", expectedFileId, count, value.FileId)
			}

			count++
		}

		if count != len(testKeys) {
			t.Errorf("Expected to iterate over %d keys, got %d", len(testKeys), count)
		}
	})

	// Test Seek functionality
	t.Run("SeekFunctionality", func(t *testing.T) {
		iter := btreeIndex.Iterator(true)
		defer iter.Close()

		// Seek to a specific key
		seekKey := []byte("c")
		iter.Seek(seekKey)

		if !iter.Valid() {
			t.Fatalf("Iterator should be valid after seeking to %s", seekKey)
		}

		// Verify current key
		currentKey := iter.Key()
		if !bytes.Equal(currentKey, seekKey) {
			t.Errorf("Expected key %s after seek, got %s", seekKey, currentKey)
		}

		// Continue iteration from the seek position
		iter.Next()
		if !iter.Valid() {
			t.Fatalf("Iterator should be valid after Next()")
		}

		// Verify next key
		expectedNextKey := []byte("d")
		nextKey := iter.Key()
		if !bytes.Equal(nextKey, expectedNextKey) {
			t.Errorf("Expected key %s after Next(), got %s", expectedNextKey, nextKey)
		}
	})
}

// Test concurrent operations
func TestBTreeConcurrentOperations(t *testing.T) {
	index, err := NewBTreeIndex(32)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	btreeIndex, ok := index.(*BTreeIndex)
	if !ok {
		t.Fatalf("Expected BTreeIndex, got %T", index)
	}

	// Number of goroutines and operations
	goroutines := 10
	operationsPerGoroutine := 100

	// Wait group to synchronize goroutines
	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Initialize random seed
	rand.Seed(time.Now().UnixNano())

	// Start goroutines
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()

			for i := 0; i < operationsPerGoroutine; i++ {
				// Generate a random key
				key := []byte(fmt.Sprintf("key-%d-%d", id, i))
				pos := &record.RecordPos{
					FileId: uint32(id),
					Offset: uint32(i * 100),
					Length: 100,
				}

				// Randomly choose an operation: put, get, or delete
				op := rand.Intn(3)
				switch op {
				case 0: // Put
					err := btreeIndex.Put(key, pos)
					if err != nil {
						t.Errorf("Goroutine %d: Failed to put key %s: %v", id, key, err)
					}
				case 1: // Get
					_, err := btreeIndex.Get(key)
					if err != nil {
						t.Errorf("Goroutine %d: Failed to get key %s: %v", id, key, err)
					}
				case 2: // Delete
					err := btreeIndex.Delete(key)
					if err != nil {
						t.Errorf("Goroutine %d: Failed to delete key %s: %v", id, key, err)
					}
				}
			}
		}(g)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Verify the tree is still usable
	testKey := []byte("test-final")
	testPos := &record.RecordPos{FileId: 999, Offset: 999, Length: 999}

	err = btreeIndex.Put(testKey, testPos)
	if err != nil {
		t.Errorf("Failed to put test key after concurrent operations: %v", err)
	}

	gotPos, err := btreeIndex.Get(testKey)
	if err != nil {
		t.Errorf("Failed to get test key after concurrent operations: %v", err)
	}

	if gotPos == nil || gotPos.FileId != testPos.FileId ||
		gotPos.Offset != testPos.Offset || gotPos.Length != testPos.Length {
		t.Errorf("Expected pos %+v, got %+v", testPos, gotPos)
	}
}

// Test Prev method of iterator
func TestBTreeIteratorPrev(t *testing.T) {
	index, err := NewBTreeIndex(32)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	btreeIndex, ok := index.(*BTreeIndex)
	if !ok {
		t.Fatalf("Expected BTreeIndex, got %T", index)
	}

	// Insert keys
	testKeys := [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
		[]byte("d"),
		[]byte("e"),
	}

	for i, key := range testKeys {
		pos := &record.RecordPos{FileId: uint32(i), Offset: 100, Length: 100}
		if err := btreeIndex.Put(key, pos); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Test using Next and Prev to navigate
	iter := btreeIndex.Iterator(true)
	defer iter.Close()

	// Start at beginning
	iter.Rewind()

	// Check we're at a valid position
	if !iter.Valid() {
		t.Fatalf("Iterator should be valid after Rewind")
	}

	// Move forward two positions
	iter.Next()
	iter.Next()

	// Check we're at a valid position
	if !iter.Valid() {
		t.Fatalf("Iterator should be valid after two Next calls")
	}

	// Only get key if the iterator is valid
	if iter.Valid() {
		key := iter.Key()
		if !bytes.Equal(key, []byte("c")) {
			t.Errorf("Expected key 'c' after two Next calls, got %s", key)
		}
	} else {
		t.Fatalf("Iterator should be valid before accessing key")
	}

	// Move back one position
	iter.Prev()

	// Check we're at a valid position
	if !iter.Valid() {
		t.Fatalf("Iterator should be valid after Prev call")
	}

	// Only get key if the iterator is valid
	if iter.Valid() {
		key := iter.Key()
		if !bytes.Equal(key, []byte("b")) {
			t.Errorf("Expected key 'b' after Prev call, got %s", key)
		}
	} else {
		t.Fatalf("Iterator should be valid before accessing key")
	}

	// Test boundary conditions

	// Move back to start
	iter.Rewind()

	// Check we're at a valid position
	if !iter.Valid() {
		t.Fatalf("Iterator should be valid after Rewind")
	}

	// Try to go prev at beginning (should stay at beginning)
	iter.Prev()

	// Check we're still at a valid position
	if !iter.Valid() {
		t.Fatalf("Iterator should remain valid after Prev at beginning")
	}

	// Only get key if the iterator is valid
	if iter.Valid() {
		key := iter.Key()
		if !bytes.Equal(key, []byte("a")) {
			t.Errorf("Expected to remain at first key 'a' after Prev at beginning, got %s", key)
		}
	} else {
		t.Fatalf("Iterator should be valid before accessing key")
	}
}

// Test performance of BTree operations
func BenchmarkBTreeOperations(b *testing.B) {
	index, err := NewBTreeIndex(32)
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	btreeIndex, ok := index.(*BTreeIndex)
	if !ok {
		b.Fatalf("Expected BTreeIndex, got %T", index)
	}

	// Pre-generate keys for consistent benchmarking
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("benchmark-key-%d", i))
	}

	// Benchmark Put
	b.Run("Put", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pos := &record.RecordPos{FileId: uint32(i), Offset: uint32(i * 100), Length: 100}
			_ = btreeIndex.Put(keys[i], pos)
		}
	})

	// Ensure keys are inserted for Get and Delete benchmarks
	for i := 0; i < b.N; i++ {
		pos := &record.RecordPos{FileId: uint32(i), Offset: uint32(i * 100), Length: 100}
		_ = btreeIndex.Put(keys[i], pos)
	}

	// Benchmark Get
	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = btreeIndex.Get(keys[i%len(keys)])
		}
	})

	// Benchmark Iterator
	b.Run("IteratorNext", func(b *testing.B) {
		iter := btreeIndex.Iterator(true)
		defer iter.Close()

		b.ResetTimer()
		iter.Rewind()
		for i := 0; i < b.N && iter.Valid(); i++ {
			_ = iter.Key()
			_ = iter.Value()
			iter.Next()
		}
	})

	// Benchmark Delete
	b.Run("Delete", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = btreeIndex.Delete(keys[i%len(keys)])
		}
	})
}
