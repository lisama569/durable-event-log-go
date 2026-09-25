package logstore

import "os"

// segment 对应一个段文件，持有文件句柄与引用计数，
// 保证回收（删除）时进行中的读取仍可完成。
type segment struct {
	base  uint64   // 段内第一条记录的全局起始偏移
	size  int64    // 当前文件字节数
	file  *os.File // 段文件句柄
	refs  int      // 进行中的读取引用数
	dirty bool     // 自上次 sync 后是否有写入
}

// segmentPath 返回段文件名。
func segmentPath(dir string, base uint64) string {
	return "" // TODO
}

// openSegment 打开（或创建）一个段文件。
func openSegment(dir string, base uint64) (*segment, error) {
	return nil, nil // TODO
}
