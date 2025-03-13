package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aixiasang/bitcask/config"
	"github.com/aixiasang/bitcask/index"
	"github.com/aixiasang/bitcask/record"
	"github.com/aixiasang/bitcask/wal"
)

type Bitcask struct {
	conf      *config.Config
	activeWal *wal.Wal
	olderWal  map[uint32]*wal.Wal
	mu        sync.RWMutex
	txnId     atomic.Uint32
	walId     atomic.Uint32
	memTable  index.Index
	// fileLock  flock.Flock
}

func NewBitcask(conf *config.Config) (*Bitcask, error) {
	db := &Bitcask{
		conf:      conf,
		activeWal: nil,
		olderWal:  make(map[uint32]*wal.Wal),
		txnId:     atomic.Uint32{},
		walId:     atomic.Uint32{},
	}

	btreeIndex, err := index.NewBTreeIndex(32)
	if err != nil {
		return nil, fmt.Errorf("创建索引失败: %v", err)
	}
	db.memTable = btreeIndex

	if err := db.loadWal(); err != nil {
		return nil, fmt.Errorf("加载WAL失败: %v", err)
	}
	if db.activeWal == nil {
		var err error
		db.activeWal, err = wal.NewWal(db.walId.Load(), db.conf)
		if err != nil {
			return nil, fmt.Errorf("创建新的WAL失败: %v", err)
		}
	}
	return db, nil
}

func (db *Bitcask) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	pos, err := db.memTable.Get(key)
	if err != nil {
		return nil, err
	}
	if pos == nil {
		return nil, nil
	}

	var wa *wal.Wal
	if pos.FileId == db.activeWal.FileId {
		wa = db.activeWal
	} else {
		wa = db.olderWal[pos.FileId]
	}

	recType, _, value, err := wa.Read(pos)
	if err != nil {
		return nil, err
	}
	if recType == record.RecordDelete || recType == record.RecordTxnDelete {
		return nil, nil
	}
	return value, nil
}

func (db *Bitcask) Put(key []byte, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 检查键值大小
	if len(key) > record.MaxKeySize {
		return fmt.Errorf("key too large: %d > %d", len(key), record.MaxKeySize)
	}
	if len(value) > record.MaxValueSize {
		return fmt.Errorf("value too large: %d > %d", len(value), record.MaxValueSize)
	}

	if err := db.tryUpdateActiveWal(); err != nil {
		return err
	}

	rec := record.NewRecordUpdate(key, value)
	pos, err := db.activeWal.Write(rec)
	if err != nil {
		return err
	}
	db.memTable.Put(key, pos)
	return nil
}

func (db *Bitcask) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.tryUpdateActiveWal(); err != nil {
		return err
	}
	rec := record.NewRecordDelete(key)
	_, err := db.activeWal.Write(rec)
	if err != nil {
		return err
	}
	db.memTable.Delete(key)
	return nil
}

func (db *Bitcask) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, wal := range db.olderWal {
		wal.Close()
	}
	db.activeWal.Close()
	return nil
}

func (db *Bitcask) tryUpdateActiveWal() error {
	if db.activeWal.Offset < db.conf.MaxFileSize {
		return nil
	}
	if err := db.activeWal.Sync(); err != nil {
		return err
	}
	db.olderWal[db.activeWal.FileId] = db.activeWal

	db.walId.Add(1)
	var err error
	db.activeWal, err = wal.NewWal(db.walId.Load(), db.conf)
	if err != nil {
		return fmt.Errorf("创建新的WAL失败: %v", err)
	}
	return nil
}

func (db *Bitcask) loadWal() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	fmt.Println("开始加载WAL文件...")
	walPath := filepath.Join(db.conf.DirPath, db.conf.WalFolder)
	fmt.Printf("WAL目录路径: %s\n", walPath)

	// 确保WAL目录存在
	if err := os.MkdirAll(walPath, 0755); err != nil {
		return fmt.Errorf("创建WAL目录失败: %v", err)
	}

	// 查找所有WAL文件
	pattern := filepath.Join(walPath, "*."+db.conf.WalFileExt)
	fmt.Printf("查找WAL文件: %s\n", pattern)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("查找WAL文件失败: %v", err)
	}

	if len(files) == 0 {
		fmt.Println("没有找到WAL文件，这是正常的（首次运行）")
		return nil
	}

	fmt.Printf("找到 %d 个WAL文件\n", len(files))
	sort.Strings(files)

	// 加载每个WAL文件
	var maxWalId uint32 = 0
	for i, fp := range files {
		fileName := strings.TrimSuffix(filepath.Base(fp), "."+db.conf.WalFileExt)
		fmt.Printf("处理WAL文件: %s\n", fileName)

		walId, err := strconv.ParseUint(fileName, 10, 32)
		if err != nil {
			return fmt.Errorf("解析WAL文件ID失败 (%s): %v", fileName, err)
		}

		if uint32(walId) > maxWalId {
			maxWalId = uint32(walId)
		}

		fmt.Printf("创建WAL实例 (ID=%d)\n", walId)
		wa, err := wal.NewWal(uint32(walId), db.conf)
		if err != nil {
			return fmt.Errorf("创建WAL失败 (ID=%d): %v", walId, err)
		}

		fmt.Printf("恢复WAL索引 (ID=%d)\n", walId)
		if err := wa.RestoreIndex(db.memTable); err != nil {
			wa.Close() // 清理资源
			return fmt.Errorf("恢复索引失败 (ID=%d): %v", walId, err)
		}

		if i == len(files)-1 {
			fmt.Printf("设置活动WAL (ID=%d)\n", walId)
			db.activeWal = wa
		} else {
			fmt.Printf("添加到历史WAL (ID=%d)\n", walId)
			db.olderWal[uint32(walId)] = wa
		}
	}

	// 设置下一个WAL ID
	db.walId.Store(maxWalId + 1)
	fmt.Printf("设置下一个WAL ID: %d\n", maxWalId+1)
	return nil
}
