package logstore

import (
	"errors"
	"fmt"
	"testing"
)

// TestCapacityExhaustion 容量耗尽：Append 必须明确拒绝（ErrNoSpace），
// 不得静默覆盖已有数据；Trim 回收后必须能继续写入。
func TestCapacityExhaustion(t *testing.T) {
	dir := t.TempDir()
	// 小段便于滚动；容量约容纳 6 条记录（每帧 16+10=26 字节）。
	l, err := Open(dir, Options{MaxSegmentBytes: 52, MaxBytes: 156})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("0123456789") // 每帧 26 字节
	var offsets []uint64
	for i := 0; ; i++ {
		off, err := l.Append(payload)
		if errors.Is(err, ErrNoSpace) {
			t.Logf("append #%d rejected: %v (关键状态: totalBytes=%d)", i, err, l.totalSize())
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, off)
	}
	if len(offsets) != 6 {
		t.Fatalf("appended %d records, want 6", len(offsets))
	}
	// 已确认数据在拒绝写入后仍然完整。
	for i, off := range offsets {
		rec, err := l.ReadAt(off)
		if err != nil || string(rec.Payload) != "0123456789" {
			t.Fatalf("record %d polluted after ErrNoSpace: rec=%v err=%v", i, rec, err)
		}
	}

	// 回收前两个段（每段 2 条记录），释放 104 字节。
	trimPoint := offsets[4]
	before := l.totalSize()
	if err := l.Trim(trimPoint); err != nil {
		t.Fatal(err)
	}
	after := l.totalSize()
	t.Logf("trim at offset=%d: totalBytes %d -> %d", trimPoint, before, after)
	if after >= before {
		t.Fatalf("trim did not free space: %d -> %d", before, after)
	}
	// 已回收区间的读取必须明确报错，而非返回脏数据。
	if _, err := l.ReadAt(offsets[0]); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read trimmed offset: got %v, want ErrOffsetOutOfRange", err)
	}
	// 未回收区间仍可读。
	if _, err := l.ReadAt(offsets[4]); err != nil {
		t.Fatalf("read live offset: %v", err)
	}
	// 回收后可以继续写入。
	off, err := l.Append(payload)
	if err != nil {
		t.Fatalf("append after trim: %v", err)
	}
	t.Logf("append after trim at offset=%d, totalBytes=%d", off, l.totalSize())
	l.Close()
}

// TestTrimInUseRejected 回收与读取竞争：待删段有进行中读取时必须整体拒绝。
func TestTrimInUseRejected(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{MaxSegmentBytes: 26}) // 每段恰好 1 条记录
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var offsets []uint64
	for i := 0; i < 3; i++ {
		off, err := l.Append([]byte(fmt.Sprintf("rec-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, off)
	}
	// 人为持有一个对首段的读取引用，模拟进行中的读取。
	seg, err := l.acquireSegment(offsets[0])
	if err != nil {
		t.Fatal(err)
	}
	err = l.Trim(offsets[2]) // 请求回收前两个段
	if !errors.Is(err, ErrTrimInUse) {
		t.Fatalf("got %v, want ErrTrimInUse", err)
	}
	t.Logf("trim rejected as expected: %v", err)
	// 拒绝是整体的：首段仍可读，未发生部分删除。
	if _, err := l.ReadAt(offsets[0]); err != nil {
		t.Fatalf("segment lost after rejected trim: %v", err)
	}
	l.releaseSegment(seg)
	// 引用释放后回收成功。
	if err := l.Trim(offsets[2]); err != nil {
		t.Fatal(err)
	}
	t.Logf("trim succeeded after reader released, totalBytes=%d", l.totalSize())
}
