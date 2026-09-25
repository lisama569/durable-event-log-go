// Package logstore 提供可嵌入的本地追加日志存储组件。
package logstore

import "errors"

// 错误定义：调用方可据此区分容量、格式、并发等不同失败原因。
var (
	// ErrNoSpace 表示达到容量上限且无法回收，写入被明确拒绝。
	ErrNoSpace = errors.New("logstore: no space left")
	// ErrCorrupt 表示记录数据校验失败（CRC 或魔数不匹配）。
	ErrCorrupt = errors.New("logstore: corrupt record")
	// ErrTruncated 表示记录尾部不完整（崩溃截断）。
	ErrTruncated = errors.New("logstore: truncated record")
	// ErrUnsupportedVersion 表示磁盘格式版本无法被当前实现接受。
	ErrUnsupportedVersion = errors.New("logstore: unsupported format version")
	// ErrMetaCorrupt 表示元数据不完整或不可解析。
	ErrMetaCorrupt = errors.New("logstore: corrupt metadata")
	// ErrClosed 表示日志已关闭。
	ErrClosed = errors.New("logstore: log closed")
	// ErrOffsetOutOfRange 表示读取位置不在有效范围内。
	ErrOffsetOutOfRange = errors.New("logstore: offset out of range")
	// ErrTrimInUse 表示待回收段仍有进行中的读取，回收被拒绝。
	ErrTrimInUse = errors.New("logstore: segment in use, trim rejected")
)

// Options 控制日志的容量与分段行为。
type Options struct {
	// MaxSegmentBytes 单个段文件的最大字节数，超过即滚动到新段。
	MaxSegmentBytes int64
	// MaxBytes 整个日志允许占用的最大字节数；达到上限时 Append 返回 ErrNoSpace。
	MaxBytes int64
	// SyncOnAppend 每次 Append 确认前是否 fsync。
	SyncOnAppend bool
}

// Record 是一条已解码的日志记录。
type Record struct {
	// Offset 记录在日志中的起始字节偏移（全局、单调递增）。
	Offset uint64
	// Payload 记录内容。
	Payload []byte
}

// RecoveryInfo 描述一次打开时的恢复过程与结论，供诊断使用。
type RecoveryInfo struct {
	// MetaAction 元数据处理动作："created" / "loaded" / "upgraded"。
	MetaAction string
	// FormatVersion 当前生效的磁盘格式版本。
	FormatVersion uint16
	// ScannedSegments 被扫描的段文件数。
	ScannedSegments int
	// ValidEndOffset 扫描确认的有效数据末尾偏移。
	ValidEndOffset uint64
	// TruncatedBytes 因尾部不完整/损坏被截掉的字节数，0 表示无截断。
	TruncatedBytes int64
	// TruncatedReason 截断原因（"torn-tail" / "corrupt-tail"），无截断时为空。
	TruncatedReason string
}
