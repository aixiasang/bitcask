package bitcask

import (
	"sync"

	"github.com/aixiasang/bitcask/record"
	orderedmap "github.com/wk8/go-ordered-map"
)

type Batch struct {
	db     *Bitcask
	mu     sync.Mutex
	txnId  uint32
	batchs *orderedmap.OrderedMap
}

func (db *Bitcask) NewBatch() *Batch {
	return &Batch{
		db:     db,
		txnId:  db.txnId.Load(),
		batchs: orderedmap.New(),
	}
}
func (b *Batch) Put(key, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batchs.Set(string(key), value)
	return nil
}

func (b *Batch) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batchs.Set(string(key), nil)
	return nil
}
func (db *Bitcask) batchPut(key, value []byte, txnId uint32) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	rec := record.NewRecordWithTxn(record.RecordTxnUpdate, key, value, txnId)
	pos, err := db.activeWal.Write(rec)
	if err != nil {
		return err
	}
	db.memTable.Put(key, pos)
	return nil
}
func (db *Bitcask) batchDelete(key []byte, txnId uint32) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	rec := record.NewRecordWithTxn(record.RecordTxnDelete, key, nil, txnId)
	_, err := db.activeWal.Write(rec)
	if err != nil {
		return err
	}
	db.memTable.Delete(key)
	return nil
}
func (db *Bitcask) batchBegin(txnId uint32) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	rec := record.NewRecordTxnBegin(txnId)
	_, err := db.activeWal.Write(rec)
	return err
}
func (db *Bitcask) batchCommit(txnId uint32) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	rec := record.NewRecordTxnCommit(txnId)
	_, err := db.activeWal.Write(rec)
	return err
}
func (b *Batch) Commit() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 更新事务ID
	b.db.txnId.Store(b.txnId + 1)

	if err := b.db.batchBegin(b.txnId); err != nil {
		return err
	}

	// 遍历所有批处理操作，按照插入顺序
	for pair := b.batchs.Oldest(); pair != nil; pair = pair.Next() {
		keyStr := pair.Key.(string)
		value := pair.Value
		key := []byte(keyStr)

		if value == nil {
			// 删除操作
			if err := b.db.batchDelete(key, b.txnId); err != nil {
				return err
			}
		} else {
			// 更新操作
			if err := b.db.batchPut(key, value.([]byte), b.txnId); err != nil {
				return err
			}
		}
	}
	if err := b.db.batchCommit(b.txnId); err != nil {
		return err
	}
	// 同步WAL确保持久化
	if err := b.db.activeWal.Sync(); err != nil {
		return err
	}

	return nil
}
