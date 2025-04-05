package bitcask

import (
	"bytes"
	"testing"

	"github.com/aixiasang/bitcask/config"
	"github.com/aixiasang/bitcask/utils"
)

func TestBatch_Put(t *testing.T) {
	conf := config.NewConfig()
	conf.BatchSize = 200
	conf.Debug = true
	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	defer db.Close()
	mp := make(map[string][]byte)
	batch := NewBatch(db)
	for i := range 100 {
		key, value := utils.GetKey(i), utils.GetValue(19)
		if err := batch.Put(key, value); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
		mp[string(key)] = value
	}

	if err = batch.Commit(); err != nil {
		t.Fatalf("提交事务失败: %v", err)
	}
	for i := range 100 {
		key := utils.GetKey(i)
		value, ok := db.Get(key)
		if !ok {
			t.Fatalf("读取失败: %v, %v, %v", err, string(value), string(mp[string(key)]))
		}
		if !bytes.Equal(value, mp[string(key)]) {
			t.Fatalf("读取失败: %v, %v, %v", err, string(value), string(mp[string(key)]))
		}
	}
}
func TestBatch_Put_And_Delete(t *testing.T) {
	conf := config.NewConfig()
	conf.BatchSize = 200
	conf.Debug = true
	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	mp := make(map[string][]byte)
	batch := NewBatch(db)
	for i := range 100 {
		key, value := utils.GetKey(i), utils.GetValue(19)
		if err := batch.Put(key, value); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
		mp[string(key)] = value
	}

	if err = batch.Commit(); err != nil {
		t.Fatalf("提交事务失败: %v", err)
	}
	for i := range 100 {
		key := utils.GetKey(i)
		value, ok := db.Get(key)

		if !ok {
			t.Fatalf("读取失败: %v, %v, %v", err, string(value), string(mp[string(key)]))
		}
		if !bytes.Equal(value, mp[string(key)]) {
			t.Fatalf("读取失败: %v, %v, %v", err, string(value), string(mp[string(key)]))
		}
	}
	db.Close()

	db, err = NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	for i := range 100 {
		key := utils.GetKey(i)
		value, ok := db.Get(key)
		if !ok {
			t.Fatalf("读取失败: %v, %v, %v", err, string(value), string(mp[string(key)]))
		}
		if !bytes.Equal(value, mp[string(key)]) {
			t.Fatalf("读取失败: %v, %v, %v", err, string(value), string(mp[string(key)]))
		}
	}
	batch = NewBatch(db)
	for i := range 100 {
		key := utils.GetKey(i)
		if err := batch.Delete(key); err != nil {
			t.Fatalf("删除失败: %v", err)
		}
	}

	if err = batch.Commit(); err != nil {
		t.Fatalf("提交事务失败: %v", err)
	}
	db.Close()

	db, err = NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	if err := db.Scan(func(key []byte, value []byte) error {
		t.Fatalf("读取失败: %v, %v, %v", err, string(key), string(value))
		return nil
	}); err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	t.Log("测试通过")

	if err := db.Merge(); err != nil {
		t.Fatalf("合并失败: %v", err)
	}
}

func TestBatch_Get(t *testing.T) {
	conf := config.NewConfig()
	conf.BatchSize = 200
	conf.Debug = true
	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	mp := make(map[string][]byte)
	batch1 := NewBatch(db)
	for i := range 100 {
		key, value := utils.GetKey(i), utils.GetValue(19)
		if err := batch1.Put(key, value); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
		mp[string(key)] = value
	}

	if err = batch1.Commit(); err != nil {
		t.Fatalf("提交事务失败: %v", err)
	}
	batch2 := NewBatch(db)
	for i := range 100 {
		key, value := utils.GetKey(i), utils.GetValue(19)
		if err := batch2.Put(key, value); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
		mp[string(key)] = value
	}

	if err = batch2.Commit(); err != nil {
		t.Fatalf("提交事务失败: %v", err)
	}

	for i := range 100 {
		key := utils.GetKey(i)
		value, ok := db.Get(key)
		if !ok {
			t.Fatalf("读取失败: %v, %v, %v", err, string(value), string(mp[string(key)]))
		}
		if !bytes.Equal(value, mp[string(key)]) {
			t.Fatalf("读取失败: %v, %v, %v", err, string(value), string(mp[string(key)]))
		}
	}
	db.Close()

	db, err = NewBitcask(conf)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	for i := range 100 {
		key := utils.GetKey(i)
		value, ok := db.Get(key)
		if !ok {
			t.Fatalf("读取失败: %v", err)
		}
		if !bytes.Equal(value, mp[string(key)]) {
			t.Fatalf("读取失败: %v, %v, %v", err, string(value), string(mp[string(key)]))
		}
	}
}
