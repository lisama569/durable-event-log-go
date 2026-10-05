package eventlog

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// seedViaStore appends n records and returns their positions.
func seedViaStore(t *testing.T, s *Store, n int, tag string) []Position {
	t.Helper()
	var ps []Position
	for i := 0; i < n; i++ {
		p, err := s.Append(context.Background(), []byte(fmt.Sprintf("%s-%04d", tag, i)))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		ps = append(ps, p)
	}
	return ps
}

// TestCursorProgressAcrossRestart: acked progress survives a full close+reopen
// of BOTH store and manager; unacked work is redelivered (at-least-once).
func TestCursorProgressAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ps := seedViaStore(t, s, 10, "r")

	m, err := NewManager(s)
	if err != nil {
		t.Fatal(err)
	}
	c, err := m.Create(ctx, "orders", Beginning)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cursor orders created at %s (writePos=%s)", c.Position(), s.Stats().WritePosition)

	// Consume and ack records 0..4.
	for i := 0; i < 5; i++ {
		rec, err := c.Next(ctx)
		if err != nil {
			t.Fatalf("next %d: %v", i, err)
		}
		if rec.Position != ps[i] || rec.Sequence != uint64(i) {
			t.Fatalf("delivered %s seq=%d, want %s seq=%d", rec.Position, rec.Sequence, ps[i], i)
		}
		if err := c.Ack(ctx, rec); err != nil {
			t.Fatalf("ack %d: %v", i, err)
		}
		t.Logf("consumed&acked seq=%d at %s -> cursor now %s", rec.Sequence, rec.Position, c.Position())
	}
	if c.Position() != ps[5] {
		t.Fatalf("cursor after 5 acks = %s, want %s", c.Position(), ps[5])
	}

	// Deliver record 5 but DO NOT ack it, then crash the process simulation by
	// closing the manager and the store and reopening from disk.
	rec5, err := c.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("delivered but NOT acked: seq=%d at %s (simulating crash here)", rec5.Sequence, rec5.Position)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: progress must be restored to ps[5], and record 5 redelivered.
	s2, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	m2, err := NewManager(s2)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	c2, err := m2.Open(ctx, "orders")
	if err != nil {
		t.Fatalf("reopen cursor: %v", err)
	}
	t.Logf("cursor orders reopened at %s", c2.Position())
	if c2.Position() != ps[5] {
		t.Fatalf("restored cursor = %s, want %s (acked high-water)", c2.Position(), ps[5])
	}
	again, err := c2.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.Position != rec5.Position || again.Sequence != rec5.Sequence {
		t.Fatalf("redelivery mismatch: got %s seq=%d want %s seq=%d",
			again.Position, again.Sequence, rec5.Position, rec5.Sequence)
	}
	t.Logf("record %s redelivered after restart (at-least-once), data=%q",
		again.Position, again.Data)

	// Consume the rest and count total deliveries by sequence: seq 5 twice,
	// every other seq once.
	if err := c2.Ack(ctx, again); err != nil {
		t.Fatal(err)
	}
	for i := 6; i < 10; i++ {
		r, err := c2.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.Sequence != uint64(i) {
			t.Fatalf("seq gap: got %d want %d", r.Sequence, i)
		}
		if err := c2.Ack(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if c2.Position() != s2.Stats().WritePosition {
		t.Fatalf("cursor %s not at end %s", c2.Position(), s2.Stats().WritePosition)
	}
	t.Logf("all 10 logical records consumed; cursor caught up to %s", c2.Position())
}

// TestMultipleCursorsIndependent: two named cursors never move each other, and
// an ack on one cannot make the other skip records.
func TestMultipleCursorsIndependent(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ps := seedViaStore(t, s, 8, "m")
	m, err := NewManager(s)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	a, err := m.Create(ctx, "consumer-a", Beginning)
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.Create(ctx, "consumer-b", Beginning)
	if err != nil {
		t.Fatal(err)
	}
	// Duplicate create is rejected, preserving the existing cursor.
	if _, err := m.Create(ctx, "consumer-a", Beginning); !errors.Is(err, ErrCursorExists) {
		t.Fatalf("want ErrCursorExists, got %v", err)
	}
	if _, err := m.Open(ctx, "ghost"); !errors.Is(err, ErrCursorNotFound) {
		t.Fatalf("want ErrCursorNotFound, got %v", err)
	}

	for i := 0; i < 6; i++ {
		r, err := a.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.Sequence != uint64(i) {
			t.Fatalf("a seq %d want %d", r.Sequence, i)
		}
		if err := a.Ack(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	// B must still be at the very beginning regardless of A's progress.
	r0, err := b.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r0.Position != ps[0] {
		t.Fatalf("consumer B drifted to %s, want %s", r0.Position, ps[0])
	}
	t.Logf("consumer A at %s, consumer B still at %s (independent)", a.Position(), b.Position())

	// A cannot ack a position ahead of itself (that would silently skip).
	if err := a.AckPosition(ctx, ps[7]); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument for forward jump, got %v", err)
	}
	t.Log("forward-jump ack rejected: acknowledged records are never silently skipped")
}

// TestCursorListAndDelete exercises lifecycle bookkeeping.
func TestCursorListAndDelete(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedViaStore(t, s, 5, "l")
	m, err := NewManager(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(ctx, "zeta", Beginning); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(ctx, "alpha", CursorAtEnd); err != nil {
		t.Fatal(err)
	}
	infos, err := m.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 2 || infos[0].Name != "alpha" || infos[1].Name != "zeta" {
		t.Fatalf("unexpected list: %+v", infos)
	}
	if infos[1].Backlog == 0 || infos[1].Lagging {
		t.Fatalf("zeta should have a positive backlog and not be lagging: %+v", infos[1])
	}
	t.Logf("alpha tail next=%s backlog=%d lagging=%t; zeta next=%s backlog=%d",
		infos[0].Next, infos[0].Backlog, infos[0].Lagging,
		infos[1].Next, infos[1].Backlog)

	// Deleting an open cursor is refused.
	op, err := m.Open(ctx, "alpha")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Delete(ctx, "alpha"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument deleting open cursor, got %v", err)
	}
	if err := op.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Delete(ctx, "alpha"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Open(ctx, "alpha"); !errors.Is(err, ErrCursorNotFound) {
		t.Fatalf("deleted cursor still opens: %v", err)
	}
	t.Log("open-cursor delete refused, then deleted cleanly")
}
