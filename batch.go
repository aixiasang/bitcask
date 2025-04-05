package bitcask

import (
	"errors"
	"fmt"
	"sync"

	"github.com/aixiasang/bitcask/config"
	"github.com/aixiasang/bitcask/utils"
)

// 批处理
type Batch struct {
	conf  *config.Config    // 配置
	db    *Bitcask          // 数据库
	mu    sync.RWMutex      // 互斥锁
	mp    map[string][]byte // 存储写入的key-value
	keys  [][]byte          // 存储删除的key
	txnId uint32            // 事务id
}

func NewBatch(db *Bitcask) *Batch {
	return &Batch{db: db, txnId: db.txnId.Load(), conf: db.conf, mp: make(map[string][]byte)}
}

func (b *Batch) Put(key, value []byte) error {
	b.log()
	b.mu.Lock()
	defer b.mu.Unlock()

	b.mp[string(key)] = value
	b.keys = append(b.keys, key)
	return nil
}

func (b *Batch) Delete(key []byte) error {
	b.log()
	if _, ok := b.db.Get(key); !ok {
		// 如果在批处理之中 删除 不在就不需要处理
		b.mu.Lock()
		defer b.mu.Unlock()
		if _, ok := b.mp[string(key)]; ok {
			delete(b.mp, string(key))
			return nil
		}
		return nil
	}
	// 如果key存在数据库中 则将key从批处理中删除
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mp[string(key)] = nil
	b.keys = append(b.keys, key)
	return nil
}
func (b *Batch) log() {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.mp) > b.conf.BatchSize {
		fmt.Printf("警告: 批处理大小超过限制, 当前大小: %d, 限制大小: %d\n", len(b.mp), b.conf.BatchSize)
		fmt.Println(b.conf)
	}
}
func (b *Batch) Commit() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conf.Debug {
		fmt.Printf("开始提交事务, 事务ID: %d\n", b.txnId)
	}
	if len(b.mp) >= b.conf.BatchSize {
		if b.conf.Debug {
			fmt.Printf("警告: 批处理大小超过限制, 当前大小: %d, 限制大小: %d\n", len(b.mp), b.conf.BatchSize)
		}
		return errors.New("批处理大小超过限制")
	}
	if len(b.mp) == 0 {
		if b.conf.Debug {
			fmt.Printf("警告: 批处理中没有操作, 事务ID: %d\n", b.txnId)
		}
		return nil
	}
	if err := b.db.putTxnBegin([]byte("txn_begin"), b.txnId); err != nil {
		return err
	}
	for _, key := range b.keys {
		if value, ok := b.mp[string(key)]; ok {
			if value == nil {
				if err := b.db.deleteTxn(key, b.txnId); err != nil {
					return err
				}
			} else {
				if err := b.db.putTxn(key, value, b.txnId); err != nil {
					return err
				}
			}
		}
	}
	if err := b.db.putTxnCommit([]byte("txn_commit"), b.txnId); err != nil {
		return err
	}

	b.db.txnId.Add(1)
	b.keys = nil
	b.mp = nil
	return nil
}
func (bc *Bitcask) putTxn(key, value []byte, txnId uint32) error {
	if key == nil {
		return errors.New("key cannot be nil")
	}
	if err := bc.tryRotate(); err != nil {
		return err
	}
	encKey := utils.EncodeTxnId(txnId, key)
	pos, err := bc.activeWal.WriteTxn(encKey, value)
	if err != nil {
		return err
	}
	if err := bc.memTable.Put(key, pos); err != nil {
		return err
	}
	return nil
}
func (bc *Bitcask) putTxnBegin(key []byte, txnId uint32) error {
	if key == nil {
		return errors.New("key cannot be nil")
	}
	if err := bc.tryRotate(); err != nil {
		return err
	}
	encKey := utils.EncodeTxnId(txnId, key)
	if _, err := bc.activeWal.WriteTxnBegin(encKey); err != nil {
		return err
	}
	return nil
}

func (bc *Bitcask) putTxnCommit(key []byte, txnId uint32) error {
	if key == nil {
		return errors.New("key cannot be nil")
	}
	if err := bc.tryRotate(); err != nil {
		return err
	}
	encKey := utils.EncodeTxnId(txnId, key)
	if _, err := bc.activeWal.WriteTxnCommit(encKey); err != nil {
		return err
	}
	return nil
}
func (bc *Bitcask) deleteTxn(key []byte, txnId uint32) error {

	pos, err := bc.memTable.Get(key)
	if err != nil {
		return err
	}
	if pos == nil {
		return nil
	}
	if err := bc.tryRotate(); err != nil {
		return err
	}
	encKey := utils.EncodeTxnId(txnId, key)
	if _, err = bc.activeWal.WriteTxn(encKey, nil); err != nil {
		return err
	}
	if err := bc.memTable.Delete(key); err != nil {
		return err
	}
	return nil
}
