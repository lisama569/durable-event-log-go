package logstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// formatVersionCurrent 是当前实现写入的磁盘格式版本。
// 兼容规则：等于当前版本 -> 接受；小于当前版本 -> 接受并升级；
// 大于当前版本 -> 拒绝（ErrUnsupportedVersion）。
const formatVersionCurrent uint16 = 1

const metaFileName = "meta.json"

// meta 是 meta.json 的磁盘结构（版本化元数据）。
type meta struct {
	Version uint16 `json:"version"`
}

// loadMeta 读取并校验元数据。规则：
//   - 文件不存在：以当前版本创建，动作 "created"；
//   - 内容为空或不可解析：返回 ErrMetaCorrupt（不留下半可用状态，由调用方决定清理）；
//   - 版本 > 当前：返回 ErrUnsupportedVersion；
//   - 版本 < 当前：接受并立即升级落盘，动作 "upgraded"；
//   - 版本 == 当前：动作 "loaded"。
func loadMeta(dir string) (meta, string, error) {
	path := filepath.Join(dir, metaFileName)
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		m := meta{Version: formatVersionCurrent}
		if err := saveMeta(dir, m); err != nil {
			return meta{}, "", err
		}
		return m, "created", nil
	}
	if err != nil {
		return meta{}, "", err
	}
	if len(data) == 0 {
		return meta{}, "", fmt.Errorf("%w: %s is empty", ErrMetaCorrupt, path)
	}
	var m meta
	if err := json.Unmarshal(data, &m); err != nil {
		return meta{}, "", fmt.Errorf("%w: %v", ErrMetaCorrupt, err)
	}
	switch {
	case m.Version > formatVersionCurrent:
		return meta{}, "", fmt.Errorf("%w: on-disk=%d current=%d",
			ErrUnsupportedVersion, m.Version, formatVersionCurrent)
	case m.Version < formatVersionCurrent:
		m.Version = formatVersionCurrent
		if err := saveMeta(dir, m); err != nil {
			return meta{}, "", err
		}
		return m, "upgraded", nil
	default:
		return m, "loaded", nil
	}
}

// saveMeta 原子写入元数据（tmp + rename + fsync 目录）。
func saveMeta(dir string, m meta) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, metaFileName+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, metaFileName)); err != nil {
		return err
	}
	// fsync 目录，保证 rename 结果持久。
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
