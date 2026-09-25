package eventlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
)

func openFresh(t *testing.T, n int, segBytes int64) (string, []Position) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: segBytes})
	if err != nil {
		t.Fatal(err)
	}
	ps := seedRecords(t, s, n)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, ps
}

func activeSegmentFile(t *testing.T, dir string) string {
	t.Helper()
	bases, err := listSegmentFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	return segmentPath(dir, bases[len(bases)-1])
}

// Truncating the tail to land in the middle of a frame simulates a crash that
// lost the last (unacknowledged) write. Without Repair the log must be
// refused; with Repair the torn bytes must be identified and removed while
// every earlier record stays intact and replay is deterministic.
func TestRecoveryTornTail(t *testing.T) {
	const n = 12
	dir, ps := openFresh(t, n, 400)
	path := activeSegmentFile(t, dir)
	info, _ := os.Stat(path)
	orig := info.Size()

	// Chop 3 bytes off the end (partial last frame or missing CRC).
	if err := os.Truncate(path, orig-3); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(dir, Options{SegmentBytes: 400}); !errors.Is(err, ErrIncompleteTail) {
		t.Fatalf("want ErrIncompleteTail without repair, got %v", err)
	} else {
		var re *RecoveryError
		if errors.As(err, &re) {
			t.Logf("refused (no repair): %v", err)
		}
	}

	s, err := Open(dir, Options{SegmentBytes: 400, Repair: true})
	if err != nil {
		t.Fatalf("repair open: %v", err)
	}
	rep := s.LastRecovery()
	t.Logf("recovery decisions:")
	for _, d := range rep.Decisions {
		t.Logf("  - %s", d)
	}
	for _, d := range rep.Damages {
		t.Logf("  damage: %s at offset %d bytes=%d", d.Kind, d.Offset, d.Bytes)
	}
	t.Logf("committed=%s nextSequence=%d repaired=%v", rep.Committed, rep.NextSequence, rep.Repaired)
	if !rep.Repaired || len(rep.Damages) != 1 {
		t.Fatalf("want 1 repaired damage, got %+v", rep.Damages)
	}
	if rep.Damages[0].Kind != "incomplete-frame" {
		t.Fatalf("damage kind = %q", rep.Damages[0].Kind)
	}
	// Replay twice: result must be identical.
	collect := func() []uint64 {
		var seqs []uint64
		if err := s.Replay(context.Background(), Beginning, func(r Record) error {
			seqs = append(seqs, r.Sequence)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return seqs
	}
	a := collect()
	b := collect()
	if len(a) != len(b) {
		t.Fatal("replay not deterministic")
	}
	if uint64(len(a)) != rep.NextSequence {
		t.Fatalf("replayed %d but nextSequence=%d", len(a), rep.NextSequence)
	}
	if len(a) == n {
		t.Fatal("torn record should not be silently kept")
	}
	t.Logf("surviving records: %d/%d", len(a), n)

	// A new append after repair must work and continue sequence numbering.
	if p, err := s.Append(context.Background(), []byte("after-repair")); err != nil {
		t.Fatalf("append after repair: %v", err)
	} else {
		t.Logf("appended after repair at %s", p)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// And a fresh Open must report a clean log.
	s3, err := Open(dir, Options{SegmentBytes: 400})
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if damages := s3.LastRecovery().Damages; len(damages) != 0 {
		t.Fatalf("expected clean second recovery, got %+v", damages)
	}
	_ = ps
}

// A byte flip in a committed frame must be detected as CRC failure, never
// returned to the caller as plausible data.
func TestRecoveryCorruptFrame(t *testing.T) {
	dir, _ := openFresh(t, 8, 400)
	path := activeSegmentFile(t, dir)
	data, _ := os.ReadFile(path)
	// Flip a payload byte somewhere safely after the header.
	flip := int64(segmentHeaderSize + frameHeaderSize + 2)
	data[flip] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{SegmentBytes: 400}); err == nil {
		t.Fatal("corrupt frame should prevent opening")
	} else {
		t.Logf("corrupt open refused (no repair): %v", err)
	}
	if _, err := Open(dir, Options{SegmentBytes: 400, Repair: true}); err == nil {
		t.Fatal("complete-frame CRC failure must not be auto-repaired (would silently drop committed records)")
	} else if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("want ErrCorruptData for a complete bad frame, got %v", err)
	} else {
		t.Logf("corrupt frame refused even with Repair: %v", err)
	}
}

// Garbage bytes after valid frames are not mistaken for a record.
func TestRecoveryGarbageTail(t *testing.T) {
	dir, _ := openFresh(t, 6, 1 << 20)
	path := activeSegmentFile(t, dir)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(f, "NOT-A-FRAME\xff\x00garbage")
	f.Close()

	s, err := Open(dir, Options{Repair: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	count := 0
	if err := s.Replay(context.Background(), Beginning, func(r Record) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 6 {
		t.Fatalf("want 6 valid records, got %d", count)
	}
	rep := s.LastRecovery()
	if rep.Damages[0].Kind != "bad-magic" {
		t.Fatalf("want bad-magic, got %+v", rep.Damages)
	}
	t.Logf("garbage tail identified as %q and truncated, committed=%s",
		rep.Damages[0].Kind, rep.Committed)
}
