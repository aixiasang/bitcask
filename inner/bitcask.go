package bitcask

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aixiasang/bitcask/inner/config"
	"github.com/aixiasang/bitcask/inner/index"
	"github.com/aixiasang/bitcask/inner/record"
	"github.com/aixiasang/bitcask/inner/wal"
)

type Bitcask struct {
	conf      *config.Config
	activeWal *wal.Wal
	oldWal    map[uint32]*wal.Wal
	memTable  index.Index
	fileId    uint32
	mu        sync.RWMutex
	fileIds   []uint32
}

func NewBitcask(conf *config.Config) (*Bitcask, error) {
	// 创建 WAL 目录
	walPath := filepath.Join(conf.DataDir, conf.WalDir)
	if err := os.MkdirAll(walPath, 0755); err != nil {
		return nil, err
	}

	// 创建 hint 目录
	hintPath := filepath.Join(conf.DataDir, conf.HintDir)
	if err := os.MkdirAll(hintPath, 0755); err != nil {
		return nil, err
	}

	bc := &Bitcask{
		conf:     conf,
		oldWal:   make(map[uint32]*wal.Wal),
		memTable: index.NewBTreeIndex(conf.BTreeOrder),
		fileId:   0,
	}

	// 尝试从 hint 文件加载索引作为基础状态
	if err := bc.LoadHint(); err != nil {
		return nil, fmt.Errorf("从hint文件加载索引失败: %v", err)
	}

	// 然后处理所有WAL文件以获取最新更新
	// 这确保即使存在hint文件，也能应用最新的变更
	if err := bc.loadWalFiles(); err != nil {
		return nil, err
	}

	if bc.activeWal == nil {
		activeWal, err := wal.NewWal(bc.conf, bc.fileId)
		if err != nil {
			return nil, err
		}
		bc.activeWal = activeWal
	}
	return bc, nil
}

func (bc *Bitcask) tryRotate() error {
	if bc.activeWal.Size() < bc.conf.MaxFileSize {
		return nil
	}
	return bc.mustRotate()
}
func (bc *Bitcask) mustRotate() error {
	if err := bc.activeWal.Sync(); err != nil {
		return err
	}
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// 保存当前的 fileId
	oldFileId := bc.fileId

	// 将当前的 WAL 文件添加到旧文件列表
	bc.oldWal[oldFileId] = bc.activeWal

	// 创建新的 WAL 文件
	bc.fileIds = append(bc.fileIds, bc.fileId)
	bc.fileId++
	activeWal, err := wal.NewWal(bc.conf, bc.fileId)
	if err != nil {
		return err
	}
	bc.activeWal = activeWal
	return nil
}
func (bc *Bitcask) Put(key, value []byte) error {
	if key == nil {
		return errors.New("key cannot be nil")
	}
	if err := bc.tryRotate(); err != nil {
		return err
	}
	pos, err := bc.activeWal.Write(key, value)
	if err != nil {
		return err
	}
	if err := bc.memTable.Put(key, pos); err != nil {
		return err
	}
	return nil
}

func (bc *Bitcask) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, errors.New("key cannot be nil")
	}

	pos, err := bc.memTable.Get(key)
	if err != nil {
		return nil, err
	}
	if pos == nil {
		return nil, errors.New("key not found")
	}

	var targetWal *wal.Wal
	if pos.FileId == bc.fileId {
		targetWal = bc.activeWal
	} else if w, ok := bc.oldWal[pos.FileId]; ok {
		targetWal = w
	} else {
		return nil, fmt.Errorf("file not found: fileId=%d", pos.FileId)
	}

	rec, err := targetWal.ReadPos(pos)
	if err != nil {
		return nil, fmt.Errorf("error reading from file %d at offset %d: %v",
			pos.FileId, pos.Offset, err)
	}
	if rec.RecordType == record.RecordTypeDelete {
		return nil, errors.New("key not found (deleted)")
	}
	return rec.Value, nil
}

func (bc *Bitcask) Delete(key []byte) error {

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
	if _, err = bc.activeWal.Write(key, nil); err != nil {
		return err
	}
	if err := bc.memTable.Delete(key); err != nil {
		return err
	}
	return nil
}

func (bc *Bitcask) loadWalFiles() error {
	walPath := filepath.Join(bc.conf.DataDir, bc.conf.WalDir)
	files, err := os.ReadDir(walPath)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}

	// 收集所有WAL文件ID
	for _, fp := range files {
		// fmt.Sprintf("wal-%d.log", fileId)
		fileName := fp.Name()
		if !strings.HasPrefix(fileName, "wal-") || !strings.HasSuffix(fileName, ".log") {
			fmt.Printf("跳过非WAL文件: %s\n", fileName)
			continue // 跳过不合规文件，而不是返回错误
		}
		fileName = strings.TrimSuffix(fileName, ".log")
		fileName = strings.TrimPrefix(fileName, "wal-")
		fileId, err := strconv.ParseUint(fileName, 10, 32)
		if err != nil {
			fmt.Printf("无法解析文件ID: %s, 错误: %v\n", fileName, err)
			continue // 跳过无法解析ID的文件
		}
		bc.fileIds = append(bc.fileIds, uint32(fileId))
	}

	// 确保按照ID排序，这样可以按正确顺序处理文件
	sort.Slice(bc.fileIds, func(i, j int) bool {
		return bc.fileIds[i] < bc.fileIds[j]
	})

	fmt.Printf("找到 %d 个WAL文件，按顺序处理: %v\n", len(bc.fileIds), bc.fileIds)

	// 从最旧到最新处理WAL文件
	for i, fileId := range bc.fileIds {
		curWal, err := wal.NewWal(bc.conf, uint32(fileId))
		if err != nil {
			return fmt.Errorf("无法打开WAL文件 %d: %v", fileId, err)
		}

		fmt.Printf("正在处理WAL文件 %d (索引 %d/%d)\n", fileId, i+1, len(bc.fileIds))

		if bc.conf.LoadHint {
			if err := curWal.ReadAll(bc.memTable); err != nil {
				return fmt.Errorf("读取WAL文件 %d 失败: %v", fileId, err)
			}
			curWal.UpdateOffset()
		}
		bc.mu.Lock()
		if i == len(bc.fileIds)-1 {
			// 最后一个文件成为活跃WAL
			fmt.Printf("设置文件 %d 为活跃WAL\n", fileId)
			bc.activeWal = curWal
			bc.fileId = uint32(fileId)
		} else {
			// 其他文件存储为旧WAL
			fmt.Printf("添加文件 %d 到旧WAL映射\n", fileId)
			bc.oldWal[uint32(fileId)] = curWal
		}
		bc.mu.Unlock()
	}
	return nil
}

func (bc *Bitcask) Close() error {
	// 始终在关闭时生成 hint 文件，不再依赖 LoadHint 配置
	// 这样可以确保下次启动时有最新的索引快照
	if err := bc.Hint(); err != nil {
		return err
	}

	// 关闭活跃的 WAL 文件
	if err := bc.activeWal.Sync(); err != nil {
		return err
	}

	if err := bc.activeWal.Close(); err != nil {
		return err
	}

	// 关闭所有旧的 WAL 文件
	for _, w := range bc.oldWal {
		if err := w.Close(); err != nil {
			return err
		}
	}

	return nil
}
func (bc *Bitcask) Hint() error {
	// 创建hint目录
	hintDir := filepath.Join(bc.conf.DataDir, bc.conf.HintDir)
	if err := os.MkdirAll(hintDir, 0755); err != nil {
		return fmt.Errorf("创建hint目录失败: %v", err)
	}

	// 创建hint文件
	hintPath := filepath.Join(hintDir, "keys.hint")
	hintFile, err := os.OpenFile(hintPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("创建hint文件失败: %v", err)
	}
	defer hintFile.Close()

	// 遍历内存索引，将键和位置信息写入hint文件
	var entries uint32 = 0
	err = bc.memTable.Foreach(func(key []byte, pos *record.Pos) error {
		// 写入键长度
		if err := binary.Write(hintFile, binary.BigEndian, uint32(len(key))); err != nil {
			return fmt.Errorf("写入键长度失败: %v", err)
		}

		// 写入文件ID
		if err := binary.Write(hintFile, binary.BigEndian, pos.FileId); err != nil {
			return fmt.Errorf("写入文件ID失败: %v", err)
		}

		// 写入偏移量
		if err := binary.Write(hintFile, binary.BigEndian, pos.Offset); err != nil {
			return fmt.Errorf("写入偏移量失败: %v", err)
		}

		// 写入长度
		if err := binary.Write(hintFile, binary.BigEndian, pos.Length); err != nil {
			return fmt.Errorf("写入记录长度失败: %v", err)
		}

		// 写入键
		if _, err := hintFile.Write(key); err != nil {
			return fmt.Errorf("写入键失败: %v", err)
		}

		entries++
		return nil
	})

	if err != nil {
		return fmt.Errorf("遍历内存索引失败: %v", err)
	}

	// 同步文件确保持久化
	if err := hintFile.Sync(); err != nil {
		return fmt.Errorf("同步hint文件失败: %v", err)
	}

	fmt.Printf("成功生成hint文件，共%d个键值对\n", entries)
	return nil
}

// Merge 合并WAL文件，删除冗余数据，提高效率
func (bc *Bitcask) Merge() error {
	oldFileIds := bc.fileIds
	bc.mu.Lock()
	bc.fileIds = make([]uint32, 0)
	bc.mu.Unlock()
	if err := bc.mustRotate(); err != nil {
		return err
	}
	if err := bc.memTable.ForeachUnSafe(func(key []byte, pos *record.Pos) error {
		var targetWal *wal.Wal
		if pos.FileId == bc.fileId {
			targetWal = bc.activeWal
		} else if w, ok := bc.oldWal[pos.FileId]; ok {
			targetWal = w
		} else {
			return fmt.Errorf("file not found: fileId=%d", pos.FileId)
		}
		rec, err := targetWal.ReadPos(pos)
		if err != nil {
			return fmt.Errorf("读取WAL文件失败: %v", err)
		}
		if err := bc.Put(key, rec.Value); err != nil {
			return fmt.Errorf("写入数据失败: %v", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("合并WAL文件失败: %v", err)
	}

	for _, fileId := range oldFileIds {
		if err := bc.oldWal[fileId].Delete(); err != nil {
			return fmt.Errorf("删除WAL文件失败: %v", err)
		}
		delete(bc.oldWal, fileId)
	}
	return nil
}

// LoadHint 从hint文件加载索引
func (bc *Bitcask) LoadHint() error {
	hintPath := filepath.Join(bc.conf.DataDir, bc.conf.HintDir, "keys.hint")

	// 检查hint文件是否存在
	_, err := os.Stat(hintPath)
	if os.IsNotExist(err) {
		return nil // hint文件不存在，不需要加载
	}
	if err != nil {
		return fmt.Errorf("检查hint文件状态失败: %v", err)
	}

	// 打开hint文件
	hintFile, err := os.Open(hintPath)
	if err != nil {
		return fmt.Errorf("打开hint文件失败: %v", err)
	}
	defer hintFile.Close()

	var entries uint32 = 0
	for {
		// 读取键长度
		var keyLength uint32
		err = binary.Read(hintFile, binary.BigEndian, &keyLength)
		if err == io.EOF {
			break // 读取完毕
		}
		if err != nil {
			return fmt.Errorf("读取键长度失败: %v", err)
		}

		// 读取文件ID
		var fileId uint32
		if err := binary.Read(hintFile, binary.BigEndian, &fileId); err != nil {
			return fmt.Errorf("读取文件ID失败: %v", err)
		}

		// 读取偏移量
		var offset uint32
		if err := binary.Read(hintFile, binary.BigEndian, &offset); err != nil {
			return fmt.Errorf("读取偏移量失败: %v", err)
		}

		// 读取长度
		var length uint32
		if err := binary.Read(hintFile, binary.BigEndian, &length); err != nil {
			return fmt.Errorf("读取记录长度失败: %v", err)
		}

		// 读取键
		key := make([]byte, keyLength)
		if _, err := io.ReadFull(hintFile, key); err != nil {
			return fmt.Errorf("读取键失败: %v", err)
		}

		// 创建位置信息
		pos := &record.Pos{
			FileId: fileId,
			Offset: offset,
			Length: length,
		}

		// 更新内存索引
		if err := bc.memTable.Put(key, pos); err != nil {
			return fmt.Errorf("更新内存索引失败: %v", err)
		}

		// 更新fileId，确保新文件ID大于已有文件ID
		if fileId >= bc.fileId {
			bc.fileId = fileId + 1
		}

		entries++
	}

	fmt.Printf("从hint文件加载了%d个键值对\n", entries)
	return nil
}
