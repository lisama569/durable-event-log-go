package eventlog

import (
	"context"
	"errors"
	"testing"
)

func TestCursorInvalidNames(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, DefaultOptions())
	defer s.Close()
	m, _ := NewManager(s)
	defer m.Close()
	ctx := context.Background()
	for _, name := range []string{"", "a/b", `a\b`, "..", "x.cursor.json", "with\x00nul"} {
		if _, err := m.Create(ctx, name, Beginning); !errors.Is(err, ErrCursorInvalidName) {
			t.Fatalf("name %q: want ErrCursorInvalidName, got %v", name, err)
		}
		if _, err := m.Open(ctx, name); !errors.Is(err, ErrCursorInvalidName) {
			t.Fatalf("open name %q: want ErrCursorInvalidName, got %v", name, err)
		}
	}
	t.Log("path-traversal/reserved/empty cursor names rejected uniformly")
}

// Cursors must keep working across segment rotations: positions stay logical
// and stable, and a tail cursor is woken for records that land in a new segment.
func TestCursorAcrossSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 300})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m, _ := NewManager(s)
	defer m.Close()
	c, _ := m.Create(context.Background(), "rot", Beginning)

	var ps []Position
	for i := 0; i < 60; i++ {
		p, err := s.Append(context.Background(), []byte("rotate-payload"))
		if err != nil {
			t.Fatal(err)
		}
		ps = append(ps, p)
	}
	if s.Stats().SegmentCount < 2 {
		t.Fatalf("expected rotation into multiple segments, got %d", s.Stats().SegmentCount)
	}
	for i := 0; i < 60; i++ {
		r, err := c.Next(context.Background())
		if err != nil {
			t.Fatalf("next %d: %v", i, err)
		}
		if r.Position != ps[i] || r.Sequence != uint64(i) {
			t.Fatalf("rotation delivery %d: got %s seq=%d want %s", i, r.Position, r.Sequence, ps[i])
		}
		if err := c.Ack(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("cursor consumed across %d segments, stable logical positions, end=%s",
		s.Stats().SegmentCount, c.Position())
}
