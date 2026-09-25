package logstore

import "sync"

// Log 是可嵌入的追加日志。并发安全。
type Log struct {
	dir     string
	opts    Options
	info    RecoveryInfo
	mu      sync.RWMutex
	closed  bool
	segments []*segment // TODO: 段管理与索引
}

// Open 打开（必要时创建）目录中的日志，执行恢复后返回。
func Open(dir string, opts Options) (*Log, error) {
	return nil, nil // TODO
}

// RecoveryInfo 返回上次 Open 的恢复诊断信息。
func (l *Log) RecoveryInfo() RecoveryInfo {
	return l.info
}

// Append 追加一条记录，确认成功后返回其偏移。
// 容量不足返回 ErrNoSpace；日志已关闭返回 ErrClosed。
func (l *Log) Append(payload []byte) (uint64, error) {
	return 0, nil // TODO
}

// ReadAt 读取从 offset 开始的记录。
func (l *Log) ReadAt(offset uint64) (Record, error) {
	return Record{}, nil // TODO
}

// Replay 从 offset 起按顺序回调每条记录；回调返回错误即终止并透传。
func (l *Log) Replay(offset uint64, fn func(Record) error) error {
	return nil // TODO
}

// NextOffset 返回下一条待写入记录的偏移（即有效数据末尾）。
func (l *Log) NextOffset() uint64 {
	return 0 // TODO
}

// Sync 将未落盘的写入刷到磁盘。
func (l *Log) Sync() error {
	return nil // TODO
}

// Close 刷盘并关闭日志。
func (l *Log) Close() error {
	return nil // TODO
}
