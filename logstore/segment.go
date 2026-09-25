package logstore

import (
	"fmt"
	"os"
	"path/filepath"
)

// segment 对应一个段文件，持有文件句柄与引用计数，
// 保证回收（删除）时进行中的读取仍可完成。
type segment struct {
	base  uint64   // 段内第一条记录的全局起始偏移
	size  int64    // 当前文件字节数
	file  *os.File // 段文件句柄
	refs  int      // 进行中的读取引用数
	dirty bool     // 自上次 sync 后是否有写入
}

// end 返回段末尾的全局偏移（不含）。
func (s *segment) end() uint64 {
	return s.base + uint64(s.size)
}

// segmentPath 返回段文件名（20 位零填充起始偏移，保证字典序==偏移序）。
func segmentPath(dir string, base uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.log", base))
}

// openSegment 打开（或创建）一个段文件，size 取当前文件实际大小。
func openSegment(dir string, base uint64) (*segment, error) {
	f, err := os.OpenFile(segmentPath(dir, base),
		os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &segment{base: base, size: fi.Size(), file: f}, nil
}
