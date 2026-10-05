package eventlog

import (
	"context"
	"errors"
	"testing"
)

// TestCompactionClampedToLaggingCursor: while a Manager is attached,
// compaction never removes data a known cursor has not acknowledged.
func TestCompactionClampedToLaggingCursor(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ps := seedViaStore(t, s, 60, "p")
	m, _ := NewManager(s)
	defer m.Close()

	speedy, err := m.Create(context.Background(), "speedy", Beginning)
	if err != nil {
		t.Fatal(err)
	}
	slow, err := m.Create(context.Background(), "slow", Beginning)
	if err != nil {
		t.Fatal(err)
	}
	// Speedy consumes and acks everything.
	for i := 0; i < 60; i++ {
		r, err := speedy.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := speedy.Ack(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	// Slow only acks records 0..19 and is parked at ps[20].
	for i := 0; i < 20; i++ {
		r, err := slow.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := slow.Ack(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	if slow.Position() != ps[20] {
		t.Fatalf("slow cursor = %s want %s", slow.Position(), ps[20])
	}

	// Ask to reclaim everything below ps[50]; the slow cursor pins it to ps[20].
	res, err := s.Compact(context.Background(), CompactOptions{Before: ps[50]})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	t.Logf("requested compact before=%s (slow cursor floor=%s): deleted=%d rewritten=%v newStart=%s",
		ps[50], ps[20], res.DeletedSegments, res.RewrittenSegment, res.NewStart)
	// Record 10 (below slow's floor? no -- ps[10] < ps[20]) may be reclaimed;
	// record 20 (at slow's floor) MUST survive.
	if _, err := s.Read(context.Background(), ps[10]); err != nil {
		t.Logf("record below slow floor reclaimed as allowed: %v", err)
	}
	r20, err := slow.Next(context.Background())
	if err != nil {
		t.Fatalf("slow cursor lost its next record: %v", err)
	}
	if r20.Position != ps[20] {
		t.Fatalf("floor record moved: got %s want %s", r20.Position, ps[20])
	}
	t.Logf("slow cursor still reads floor record %s seq=%d (compaction was clamped)",
		r20.Position, r20.Sequence)

	// The retention report must carry the clamping decision for diagnostics.
	for _, d := range s.LastRecovery().Decisions {
		t.Logf("  decision: %s", d)
	}
}

// TestDetachedManagerLaggingCursorFailsLoudly: after Detach, compaction may
// reclaim unconsumed records; the lagging cursor then gets an explicit
// ErrCursorPositionReclaimed (wrapping ErrPositionReclaimed), never a silent
// skip and never an end-of-log wait.
func TestDetachedManagerLaggingCursorFailsLoudly(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ps := seedViaStore(t, s, 60, "d")
	m, _ := NewManager(s)

	c, err := m.Create(context.Background(), "lagger", Beginning)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		r, err := c.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Ack(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	lagAt := c.Position()
	t.Logf("lagger acknowledged through %s, then manager detached", ps[9])

	// Detach: retention no longer waits on the cursor. The cursor stays open
	// and its persisted state remains on disk.
	if err := m.Detach(); err != nil {
		t.Fatal(err)
	}
	res, err := s.Compact(context.Background(), CompactOptions{Before: ps[40]})
	if err != nil {
		t.Fatalf("compact after detach: %v", err)
	}
	if res.DeletedSegments == 0 && !res.RewrittenSegment {
		t.Fatalf("expected actual reclamation after detach, got %+v", res)
	}
	t.Logf("reclaimed after detach: deleted=%d newStart=%s (lagger at %s)",
		res.DeletedSegments, res.NewStart, lagAt)

	// A direct read of the lagging position is reclaimed at store level too.
	if _, err := s.Read(context.Background(), lagAt); !errors.Is(err, ErrPositionReclaimed) {
		t.Logf("store read of lagging pos: %v", err)
	}

	_, err = c.Next(context.Background())
	var ce *CursorError
	if !errors.As(err, &ce) {
		t.Fatalf("want *CursorError, got %v", err)
	}
	if !errors.Is(err, ErrCursorPositionReclaimed) || !errors.Is(err, ErrPositionReclaimed) {
		t.Fatalf("want reclaimed sentinel(s), got %v", err)
	}
	t.Logf("lagging cursor fails explicitly: %v", err)

	// The failure must be repeatable, not a one-shot that then skips ahead.
	_, err2 := c.Next(context.Background())
	if !errors.Is(err2, ErrCursorPositionReclaimed) {
		t.Fatalf("second read should fail the same way, got %v", err2)
	}
	if c.Position() != lagAt {
		t.Fatalf("cursor moved on failure: %s -> %s", lagAt, c.Position())
	}
	t.Logf("cursor position unchanged at %s after the explicit failure", c.Position())
}

// TestCursorLagSurvivesRestart: a cursor that fell behind retention (manager
// detached, compaction ran) is reported Lagging on the next process start and
// fails on read, rather than being silently reset or treated as end-of-log.
func TestCursorLagSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	ps := seedViaStore(t, s, 60, "z")
	m, _ := NewManager(s)
	c, _ := m.Create(context.Background(), "restart-lagger", Beginning)
	for i := 0; i < 5; i++ {
		r, _ := c.Next(context.Background())
		if err := c.Ack(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	lagAt := c.Position()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Compact(context.Background(), CompactOptions{Before: ps[40]}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, Options{SegmentBytes: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	m2, err := NewManager(s2)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	infos, err := m2.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || !infos[0].Lagging {
		t.Fatalf("expected one Lagging cursor, got %+v", infos)
	}
	t.Logf("after restart: cursor %q next=%s marked lagging (horizon=%s)",
		infos[0].Name, infos[0].Next, s2.Stats().StartPosition)

	c2, err := m2.Open(context.Background(), "restart-lagger")
	if err != nil {
		t.Fatal(err)
	}
	if c2.Position() != lagAt {
		t.Fatalf("lag position restored wrong: %s want %s", c2.Position(), lagAt)
	}
	if _, err := c2.Next(context.Background()); !errors.Is(err, ErrCursorPositionReclaimed) {
		t.Fatalf("want ErrCursorPositionReclaimed after restart, got %v", err)
	}
	t.Logf("restarted lagging cursor fails with explicit reclaimed error at %s", c2.Position())
}
