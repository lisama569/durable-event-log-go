package logstore

import (
	"fmt"
	"os"
)

// Trim 回收所有记录均严格小于 offset 的非活跃段文件，释放磁盘空间。
//
// 并发规则：
//   - 任一待删段仍有进行中的读取（refs>0）时，整体返回 ErrTrimInUse，
//     不做部分删除（避免半回收状态）；
//   - 活跃写入段永不删除，新写入不受影响；
//   - 回收与读取由引用计数与互斥锁串行化：读取要么先完成，
//     要么 Trim 明确拒绝，二者不会互相污染。
func (l *Log) Trim(offset uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	// 选出候选：非活跃段且段末尾 <= offset（即所有记录严格小于 offset+1）。
	var keep, drop []*segment
	for i, s := range l.segs {
		if i < len(l.segs)-1 && s.end() <= offset {
			drop = append(drop, s)
		} else {
			keep = append(keep, s)
		}
	}
	if len(drop) == 0 {
		return nil
	}
	for _, s := range drop {
		if s.refs > 0 {
			return fmt.Errorf("%w: segment %d has %d active reads",
				ErrTrimInUse, s.base, s.refs)
		}
	}
	var freed int64
	for _, s := range drop {
		freed += s.size
		if err := s.file.Close(); err != nil {
			return err
		}
		if err := os.Remove(segmentPath(l.dir, s.base)); err != nil {
			return err
		}
	}
	l.segs = keep
	l.totalBytes -= freed
	// fsync 目录，保证删除结果持久。
	if d, err := os.Open(l.dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// totalSize 返回当前所有段占用字节数。
func (l *Log) totalSize() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.totalBytes
}
