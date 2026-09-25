package logstore

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
)

// recoverSegments 扫描目录中的段文件，校验记录链，
// 截断不完整的尾部，返回有效段列表与恢复诊断。
// 规则：
//   - 最末段尾部不完整（ErrTruncated）或校验失败（ErrCorrupt）：
//     截断到最后一条完好记录处，记录原因 "torn-tail"/"corrupt-tail"；
//   - 非最末段出现上述错误、或段间偏移不连续：
//     返回带位置信息的错误，拒绝打开（不留下半可用状态）。
func recoverSegments(dir string) ([]*segment, RecoveryInfo, error) {
	info := RecoveryInfo{}
	bases, err := listSegmentBases(dir)
	if err != nil {
		return nil, info, err
	}
	segs := make([]*segment, 0, len(bases))
	for i, base := range bases {
		seg, err := openSegment(dir, base)
		if err != nil {
			return nil, info, err
		}
		last := i == len(bases)-1
		valid, truncBytes, reason, err := scanSegment(seg, last)
		if err != nil {
			return nil, info, err
		}
		if truncBytes > 0 {
			if err := seg.file.Truncate(valid); err != nil {
				return nil, info, fmt.Errorf("truncate tail: %w", err)
			}
			if err := seg.file.Sync(); err != nil {
				return nil, info, err
			}
			seg.size = valid
			info.TruncatedBytes += truncBytes
			info.TruncatedReason = reason
		}
		seg.size = valid
		segs = append(segs, seg)
		info.ScannedSegments++
	}
	// 校验段间连续性。
	for i := 1; i < len(segs); i++ {
		if segs[i-1].end() != segs[i].base {
			return nil, info, fmt.Errorf("%w: gap between segment %d (end=%d) and %d",
				ErrCorrupt, segs[i-1].base, segs[i-1].end(), segs[i].base)
		}
	}
	if n := len(segs); n > 0 {
		info.ValidEndOffset = segs[n-1].end()
	}
	return segs, info, nil
}

// scanSegment 顺序扫描一个段，返回有效字节数、需截断字节数与原因。
// last 表示是否为最末段；非最末段出现任何解析错误都视为损坏。
func scanSegment(seg *segment, last bool) (valid int64, truncBytes int64, reason string, err error) {
	var off int64
	for off < seg.size {
		_, total, rerr := readRecordAt(seg.file, off)
		if rerr == nil {
			off += total
			continue
		}
		if errors.Is(rerr, io.EOF) {
			break // 干净结尾
		}
		if !last {
			return 0, 0, "", fmt.Errorf("%w: segment %d at offset %d: %v",
				ErrCorrupt, seg.base, seg.base+uint64(off), rerr)
		}
		reason := "torn-tail"
		if errors.Is(rerr, ErrCorrupt) {
			reason = "corrupt-tail"
		}
		return off, seg.size - off, reason, nil
	}
	return off, seg.size - off, reason, nil
}

// listSegmentBases 列出目录中所有段文件的起始偏移，升序。
func listSegmentBases(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var bases []uint64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".log") {
			continue
		}
		base, err := strconv.ParseUint(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: bad segment name %q", ErrCorrupt, name)
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}
