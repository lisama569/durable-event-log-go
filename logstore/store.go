package logstore

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// defaultMaxSegmentBytes 是未指定时的段大小上限。
const defaultMaxSegmentBytes = 64 << 20

// Log 是可嵌入的追加日志。并发安全。
//
// 持久化语义：Append 返回成功即表示记录已写入段文件；
// 若 Options.SyncOnAppend 为真，返回前已 fsync。
// 已确认的记录在后续崩溃、截断或尾部不完整时都不会静默丢失——
// 恢复过程只截断从未确认过的尾部。
type Log struct {
	dir  string
	opts Options
	info RecoveryInfo

	mu         sync.RWMutex
	closed     bool
	segs       []*segment // 按 base 升序；最后一个为活跃写入段
	totalBytes int64      // 所有段字节数之和
}

// Open 打开（必要时创建）目录中的日志，执行恢复后返回。
// 恢复规则冲突（元数据版本过高、元数据损坏、段中间损坏）时返回错误，
// 不会留下半可用状态。
func Open(dir string, opts Options) (*Log, error) {
	if opts.MaxSegmentBytes <= 0 {
		opts.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	m, action, err := loadMeta(dir)
	if err != nil {
		return nil, err
	}
	segs, info, err := recoverSegments(dir)
	if err != nil {
		return nil, err
	}
	info.MetaAction = action
	info.FormatVersion = m.Version
	l := &Log{dir: dir, opts: opts, info: info, segs: segs}
	for _, s := range segs {
		l.totalBytes += s.size
	}
	if len(l.segs) == 0 {
		if err := l.rollSegmentLocked(); err != nil {
			return nil, err
		}
	}
	return l, nil
}

// RecoveryInfo 返回上次 Open 的恢复诊断信息。
func (l *Log) RecoveryInfo() RecoveryInfo {
	return l.info
}

// Append 追加一条记录，确认成功后返回其偏移。
// 容量不足返回 ErrNoSpace（明确拒绝，不静默覆盖）；日志已关闭返回 ErrClosed。
func (l *Log) Append(payload []byte) (uint64, error) {
	frame := encodeRecord(payload)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	if l.opts.MaxBytes > 0 && l.totalBytes+int64(len(frame)) > l.opts.MaxBytes {
		return 0, fmt.Errorf("%w: need %d bytes, total %d/%d",
			ErrNoSpace, len(frame), l.totalBytes, l.opts.MaxBytes)
	}
	active := l.segs[len(l.segs)-1]
	if active.size > 0 && active.size+int64(len(frame)) > l.opts.MaxSegmentBytes {
		if err := l.rollSegmentLocked(); err != nil {
			return 0, err
		}
		active = l.segs[len(l.segs)-1]
	}
	off := active.end()
	if _, err := active.file.WriteAt(frame, active.size); err != nil {
		return 0, err
	}
	active.size += int64(len(frame))
	active.dirty = true
	l.totalBytes += int64(len(frame))
	if l.opts.SyncOnAppend {
		if err := active.file.Sync(); err != nil {
			return 0, err
		}
		active.dirty = false
	}
	return off, nil
}

// rollSegmentLocked 滚动到新的活跃段。调用方须持有写锁。
func (l *Log) rollSegmentLocked() error {
	var base uint64
	if n := len(l.segs); n > 0 {
		prev := l.segs[n-1]
		base = prev.end()
		if prev.dirty {
			if err := prev.file.Sync(); err != nil {
				return err
			}
			prev.dirty = false
		}
	}
	seg, err := openSegment(l.dir, base)
	if err != nil {
		return err
	}
	l.segs = append(l.segs, seg)
	return nil
}

// findSegmentLocked 返回包含 offset 的段；offset 越界时返回 nil。
// 调用方须持有读锁或写锁。
func (l *Log) findSegmentLocked(offset uint64) *segment {
	// 段数量通常很少，线性查找即可。
	for _, s := range l.segs {
		if offset >= s.base && offset < s.end() {
			return s
		}
	}
	return nil
}

// acquireSegment 定位 offset 所在段并增加其读取引用计数，
// 使并发 Trim 能识别"读取进行中"并明确拒绝回收。
// 引用计数是共享状态，必须在写锁下修改。
func (l *Log) acquireSegment(offset uint64) (*segment, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, ErrClosed
	}
	seg := l.findSegmentLocked(offset)
	if seg == nil {
		return nil, fmt.Errorf("%w: offset %d, valid end %d",
			ErrOffsetOutOfRange, offset, l.nextOffsetLocked())
	}
	seg.refs++
	return seg, nil
}

// releaseSegment 递减读取引用计数。
func (l *Log) releaseSegment(seg *segment) {
	l.mu.Lock()
	seg.refs--
	l.mu.Unlock()
}

// ReadAt 读取从 offset 开始的记录。
func (l *Log) ReadAt(offset uint64) (Record, error) {
	seg, err := l.acquireSegment(offset)
	if err != nil {
		return Record{}, err
	}
	defer l.releaseSegment(seg)
	payload, _, err := readRecordAt(seg.file, int64(offset-seg.base))
	if err != nil {
		return Record{}, fmt.Errorf("read at %d: %w", offset, err)
	}
	return Record{Offset: offset, Payload: payload}, nil
}

// Replay 从 offset 起按顺序回调每条记录；回调返回错误即终止并透传。
// offset 等于 NextOffset（空区间）时立即返回 nil。
func (l *Log) Replay(offset uint64, fn func(Record) error) error {
	for {
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return ErrClosed
		}
		end := l.nextOffsetLocked()
		if offset == end {
			l.mu.Unlock()
			return nil
		}
		seg := l.findSegmentLocked(offset)
		if seg == nil {
			l.mu.Unlock()
			return fmt.Errorf("%w: offset %d, valid end %d",
				ErrOffsetOutOfRange, offset, end)
		}
		seg.refs++
		l.mu.Unlock()

		payload, total, err := readRecordAt(seg.file, int64(offset-seg.base))
		l.releaseSegment(seg)
		if err != nil {
			return fmt.Errorf("replay at %d: %w", offset, err)
		}
		if err := fn(Record{Offset: offset, Payload: payload}); err != nil {
			return err
		}
		offset += uint64(total)
	}
}

// NextOffset 返回下一条待写入记录的偏移（即有效数据末尾）。
func (l *Log) NextOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOffsetLocked()
}

func (l *Log) nextOffsetLocked() uint64 {
	return l.segs[len(l.segs)-1].end()
}

// Sync 将未落盘的写入刷到磁盘。
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	for _, s := range l.segs {
		if !s.dirty {
			continue
		}
		if err := s.file.Sync(); err != nil {
			return err
		}
		s.dirty = false
	}
	return nil
}

// Close 刷盘并关闭日志。重复调用返回 ErrClosed。
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	var err error
	for _, s := range l.segs {
		if s.dirty {
			if serr := s.file.Sync(); serr != nil {
				err = errors.Join(err, serr)
			}
			s.dirty = false
		}
		if cerr := s.file.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}
	l.closed = true
	return err
}
