package inner

import (
	"bytes"
	"testing"

	"github.com/aixiasang/bitcask/inner/config"
	"github.com/aixiasang/bitcask/inner/utils"
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
		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("读取失败: %v", err)
		}
		if !bytes.Equal(value, mp[string(key)]) {
			t.Fatalf("读取失败: %v, %v, %v", err, string(value), string(mp[string(key)]))
		}
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
		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("读取失败: %v", err)
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
		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("读取失败: %v", err)
		}
		if !bytes.Equal(value, mp[string(key)]) {
			t.Fatalf("读取失败: %v, %v, %v", err, string(value), string(mp[string(key)]))
		}
	}
}
