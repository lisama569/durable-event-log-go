package logstore

import (
	"errors"
	"fmt"
	"testing"
)

func TestAppendReadReplayRoundtrip(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{MaxSegmentBytes: 128, SyncOnAppend: true})
	if err != nil {
		t.Fatal(err)
	}
	var offsets []uint64
	for i := 0; i < 20; i++ {
		off, err := l.Append([]byte(fmt.Sprintf("event-%02d", i)))
		if err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, off)
		t.Logf("append event-%02d at offset=%d", i, off)
	}
	// 指定位置读取。
	for i, off := range offsets {
		rec, err := l.ReadAt(off)
		if err != nil {
			t.Fatalf("ReadAt(%d): %v", off, err)
		}
		if want := fmt.Sprintf("event-%02d", i); string(rec.Payload) != want {
			t.Fatalf("offset %d: got %q want %q", off, rec.Payload, want)
		}
	}
	// 顺序重放。
	var n int
	err = l.Replay(0, func(r Record) error {
		if r.Offset != offsets[n] {
			t.Fatalf("replay #%d: offset=%d want %d", n, r.Offset, offsets[n])
		}
		n++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 20 {
		t.Fatalf("replayed %d records, want 20", n)
	}
	// 越界读取必须可区分。
	if _, err := l.ReadAt(l.NextOffset()); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read past end: got %v, want ErrOffsetOutOfRange", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after close: got %v, want ErrClosed", err)
	}
}

func TestReopenPersistsConfirmedRecords(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SyncOnAppend: true})
	if err != nil {
		t.Fatal(err)
	}
	off, err := l.Append([]byte("durable"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	info := l2.RecoveryInfo()
	t.Logf("recovery: action=%s version=%d segments=%d validEnd=%d truncated=%d reason=%q",
		info.MetaAction, info.FormatVersion, info.ScannedSegments,
		info.ValidEndOffset, info.TruncatedBytes, info.TruncatedReason)
	rec, err := l2.ReadAt(off)
	if err != nil {
		t.Fatal(err)
	}
	if string(rec.Payload) != "durable" {
		t.Fatalf("got %q", rec.Payload)
	}
	if info.TruncatedBytes != 0 {
		t.Fatalf("unexpected truncation: %+v", info)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
}
