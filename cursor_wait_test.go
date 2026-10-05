package eventlog

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestCursorRealtimeTailConsume: a cursor parked at the end is woken by new
// appends (no polling), delivers records promptly and in order.
func TestCursorRealtimeTailConsume(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m, err := NewManager(s)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	c, err := m.Create(context.Background(), "tail", CursorAtEnd)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	// First Next with no data must park; it should not return immediately.
	parked := make(chan struct{})
	got := make(chan Record, 8)
	errCh := make(chan error, 1)
	go func() {
		close(parked)
		for i := 0; i < 5; i++ {
			r, e := c.Next(ctx)
			if e != nil {
				errCh <- e
				return
			}
			got <- r
			if e := c.Ack(ctx, r); e != nil {
				errCh <- e
				return
			}
		}
	}()
	<-parked
	// Give the goroutine time to actually park in the select.
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 5; i++ {
		before := time.Now()
		_, err := s.Append(ctx, []byte(fmt.Sprintf("live-%04d", i)))
		if err != nil {
			t.Fatal(err)
		}
		select {
		case r := <-got:
			latency := time.Since(before)
			if string(r.Data) != fmt.Sprintf("live-%04d", i) {
				t.Fatalf("out-of-order live delivery: %q", r.Data)
			}
			t.Logf("woken for seq=%d at %s, wake latency=%s, cursor=%s",
				r.Sequence, r.Position, latency, c.Position())
			if latency > 2*time.Second {
				t.Fatalf("wake took %s; consumer was not promptly notified", latency)
			}
		case err := <-errCh:
			t.Fatalf("consumer error: %v", err)
		case <-time.After(3 * time.Second):
			t.Fatal("parked consumer was not woken by append (missed edge?)")
		}
	}
	select {
	case err := <-errCh:
		t.Fatalf("unexpected consumer error after stream: %v", err)
	default:
	}
	if c.Position() != s.Stats().WritePosition {
		t.Fatalf("tail cursor %s != end %s", c.Position(), s.Stats().WritePosition)
	}
}

// TestCursorWaitTimeoutLeavesPositionIntact: a parked Next honors a deadline
// and redelivery after the timeout still sees exactly the next record.
func TestCursorWaitTimeoutLeavesPositionIntact(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m, _ := NewManager(s)
	defer m.Close()
	c, _ := m.Create(context.Background(), "tmo", CursorAtEnd)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	_, err = c.Next(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	saved := c.Position()
	t.Logf("parked Next timed out; cursor remains at %s (not advanced)", saved)

	// Next again, then append: must deliver the one new record at the same pos.
	ctx2 := context.Background()
	delivered := make(chan Record, 1)
	go func() {
		r, e := c.Next(ctx2)
		if e != nil {
			delivered <- Record{}
			return
		}
		delivered <- r
	}()
	time.Sleep(40 * time.Millisecond)
	p, err := s.Append(ctx2, []byte("after-timeout"))
	if err != nil {
		t.Fatal(err)
	}
	r := <-delivered
	if r.Position != p {
		t.Fatalf("delivered %s, want newly appended %s", r.Position, p)
	}
	t.Logf("after timeout+append, delivered %s data=%q at the unchanged cursor position",
		r.Position, r.Data)
}

// TestCursorWaitCancel: cancellation exits a parked Next.
func TestCursorWaitCancel(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, DefaultOptions())
	defer s.Close()
	m, _ := NewManager(s)
	defer m.Close()
	c, _ := m.Create(context.Background(), "cancel", CursorAtEnd)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { _, err := c.Next(ctx); errCh <- err }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want Canceled, got %v", err)
		}
		t.Logf("cancellation exited parked Next: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not wake parked consumer")
	}
}

// TestCursorWaitDuringCompaction: a parked consumer must not get stuck when
// compaction runs (it is notified and re-checks), and then receives new writes.
func TestCursorWaitDuringCompaction(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ps := seedViaStore(t, s, 40, "c")
	m, _ := NewManager(s)
	defer m.Close()
	// Cursor starts at record 10 and is parked after consuming 10..39.
	c, err := m.Create(context.Background(), "w", ps[10])
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 10; i < 40; i++ {
		r, err := c.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Ack(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	if c.Position() != s.Stats().WritePosition {
		t.Fatalf("not caught up: %s vs %s", c.Position(), s.Stats().WritePosition)
	}

	// Cursor at end; park a Next, compact, then append. Compaction alone wakes
	// it (re-check finds nothing and it reparks); the later append delivers.
	type result struct {
		r   Record
		err error
	}
	delivered := make(chan result, 1)
	go func() {
		r, e := c.Next(context.Background())
		delivered <- result{r, e}
	}()
	time.Sleep(50 * time.Millisecond)
	// Compaction is clamped to the cursor floor (== end), so it reclaims
	// nothing here; but it still broadcasts a wake to parked consumers.
	res, err := s.Compact(context.Background(), CompactOptions{Before: ps[30]})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	t.Logf("compaction while parked: deleted=%d reclaimed=%d (clamped to cursor floor)",
		res.DeletedSegments, res.ReclaimedBytes)
	time.Sleep(50 * time.Millisecond)
	np, err := s.Append(context.Background(), []byte("post-compact-live"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-delivered:
		if res.err != nil {
			t.Fatalf("next after compaction: %v", res.err)
		}
		if res.r.Position != np {
			t.Fatalf("delivered %s, want %s after compaction", res.r.Position, np)
		}
		t.Logf("consumer survived compaction and received new %s data=%q",
			res.r.Position, res.r.Data)
	case <-time.After(3 * time.Second):
		t.Fatal("parked consumer stuck across compaction + append")
	}
}

// TestCursorWaitWokenOnClose: closing the store releases parked consumers with
// a clear error instead of hanging forever.
func TestCursorWaitWokenOnClose(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, DefaultOptions())
	m, _ := NewManager(s)
	c, _ := m.Create(context.Background(), "closing", CursorAtEnd)

	errCh := make(chan error, 1)
	go func() { _, err := c.Next(context.Background()); errCh <- err }()
	time.Sleep(50 * time.Millisecond)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) && !errors.Is(err, ErrCursorClosed) {
			t.Fatalf("want closed error, got %v", err)
		}
		t.Logf("parked consumer released on close: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("close did not wake parked consumer")
	}
}
