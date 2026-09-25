package logstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// lastSegmentPath 返回目录中偏移最大的段文件路径。
func lastSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var last string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") && e.Name() > last {
			last = e.Name()
		}
	}
	if last == "" {
		t.Fatal("no segment file found")
	}
	return filepath.Join(dir, last)
}

// TestCrashWithoutClose 模拟进程异常退出（不 Close、不 Sync 直接重开）：
// 已确认写入的记录必须全部可恢复。
func TestCrashWithoutClose(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{}) // 不开 SyncOnAppend，模拟崩溃前未 fsync
	if err != nil {
		t.Fatal(err)
	}
	var offsets []uint64
	for i := 0; i < 10; i++ {
		off, err := l.Append([]byte(fmt.Sprintf("crash-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, off)
	}
	end := l.NextOffset()
	t.Logf("simulated crash: nextOffset=%d without Close/Sync", end)
	// 不调用 Close，直接以新实例重开（同机器页缓存等价于崩溃后重启）。
	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	info := l2.RecoveryInfo()
	t.Logf("recovery: validEnd=%d truncated=%d reason=%q",
		info.ValidEndOffset, info.TruncatedBytes, info.TruncatedReason)
	if info.ValidEndOffset != end {
		t.Fatalf("validEnd=%d want %d", info.ValidEndOffset, end)
	}
	for i, off := range offsets {
		rec, err := l2.ReadAt(off)
		if err != nil {
			t.Fatalf("record %d lost after crash: %v", i, err)
		}
		if want := fmt.Sprintf("crash-%d", i); string(rec.Payload) != want {
			t.Fatalf("record %d: got %q want %q", i, rec.Payload, want)
		}
	}
	// 恢复结果可重复：再次重开结论一致。
	l2.Close()
	l3, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	info3 := l3.RecoveryInfo()
	t.Logf("second recovery: validEnd=%d truncated=%d", info3.ValidEndOffset, info3.TruncatedBytes)
	if info3.ValidEndOffset != end || info3.TruncatedBytes != 0 {
		t.Fatalf("recovery not repeatable: %+v", info3)
	}
	l3.Close()
}

// TestTornTailTruncated 模拟崩溃造成的不完整尾部：
// 恢复必须截断坏尾、保留全部已确认记录，且错误类型可区分。
func TestTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SyncOnAppend: true})
	if err != nil {
		t.Fatal(err)
	}
	var offsets []uint64
	for i := 0; i < 5; i++ {
		off, _ := l.Append([]byte(fmt.Sprintf("good-%d", i)))
		offsets = append(offsets, off)
	}
	end := l.NextOffset()
	l.Close()

	// 在尾部追加半帧垃圾（只有部分帧头），模拟撕裂写。
	seg := lastSegmentPath(t, dir)
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	garbage := encodeRecord([]byte("unconfirmed"))[:7]
	if _, err := f.Write(garbage); err != nil {
		t.Fatal(err)
	}
	f.Close()
	t.Logf("injected torn tail: %d bytes at offset %d", len(garbage), end)

	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	info := l2.RecoveryInfo()
	t.Logf("recovery: validEnd=%d truncated=%d reason=%q (判据: 尾部帧头不完整 -> torn-tail)",
		info.ValidEndOffset, info.TruncatedBytes, info.TruncatedReason)
	if info.TruncatedReason != "torn-tail" {
		t.Fatalf("reason=%q want torn-tail", info.TruncatedReason)
	}
	if info.ValidEndOffset != end {
		t.Fatalf("validEnd=%d want %d", info.ValidEndOffset, end)
	}
	for i, off := range offsets {
		if _, err := l2.ReadAt(off); err != nil {
			t.Fatalf("confirmed record %d lost: %v", i, err)
		}
	}
	// 截断后可继续追加，且新记录紧接有效末尾。
	off, err := l2.Append([]byte("after-recovery"))
	if err != nil {
		t.Fatal(err)
	}
	if off != end {
		t.Fatalf("append after recovery at %d, want %d", off, end)
	}
	l2.Close()
}

// TestCorruptTailTruncated 尾部 CRC 校验失败：识别为 corrupt-tail 并截断。
func TestCorruptTailTruncated(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SyncOnAppend: true})
	if err != nil {
		t.Fatal(err)
	}
	off, _ := l.Append([]byte("confirmed"))
	end := l.NextOffset()
	l.Close()
	t.Logf("confirmed record end offset=%d", end)

	// 写入一条 CRC 错误的完整帧。
	seg := lastSegmentPath(t, dir)
	f, _ := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o644)
	bad := encodeRecord([]byte("bad-crc"))
	bad[len(bad)-1] ^= 0xFF // 破坏 payload，CRC 不再匹配
	f.Write(bad)
	f.Close()

	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	info := l2.RecoveryInfo()
	t.Logf("recovery: validEnd=%d truncated=%d reason=%q (判据: 尾部帧 CRC 不匹配 -> corrupt-tail)",
		info.ValidEndOffset, info.TruncatedBytes, info.TruncatedReason)
	if info.TruncatedReason != "corrupt-tail" {
		t.Fatalf("reason=%q want corrupt-tail", info.TruncatedReason)
	}
	rec, err := l2.ReadAt(off)
	if err != nil || string(rec.Payload) != "confirmed" {
		t.Fatalf("confirmed record damaged: rec=%v err=%v", rec, err)
	}
	l2.Close()
}

// TestCorruptMiddleRejected 非尾段中间损坏：拒绝打开并给出可诊断位置。
func TestCorruptMiddleRejected(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{MaxSegmentBytes: 64, SyncOnAppend: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("middle-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()

	// 破坏第一个段（非最末段）中的一条记录。
	first := filepath.Join(dir, fmt.Sprintf("%020d.log", 0))
	data, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	data[headerSize] ^= 0xFF // 破坏第一条记录的 payload
	if err := os.WriteFile(first, data, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("corrupted first segment at record offset 0 (非尾段)")

	_, err = Open(dir, Options{})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
	t.Logf("open rejected with diagnosable error: %v", err)
}
