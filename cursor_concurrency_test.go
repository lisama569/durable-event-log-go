package eventlog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCursorConcurrentConsumeCompactAppend: several independent cursors consume
// a live log while appends (and clamped compactions) keep happening. Every
// cursor must observe an in-order, gap-free sequence; parked waits must wake;
// nothing deadlocks or spins.
func TestCursorConcurrentConsumeCompactAppend(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m, _ := NewManager(s)
	defer m.Close()

	const consumers = 4
	const total = 400
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cursors := make([]*Cursor, consumers)
	errCh := make(chan error, consumers+8)
	for i := 0; i < consumers; i++ {
		c, err := m.Create(ctx, fmt.Sprintf("cc-%d", i), Beginning)
		if err != nil {
			t.Fatal(err)
		}
		cursors[i] = c
	}

	var consumed [consumers]int64
	var wg sync.WaitGroup
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var lastSeq uint64
			first := true
			for {
				cctx, c := context.WithTimeout(ctx, 2*time.Second)
				r, err := cursors[idx].Next(cctx)
				c()
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						if atomic.LoadInt64(&consumed[idx]) >= total {
							return
						}
						continue
					}
					if ctx.Err() != nil {
						return
					}
					errCh <- fmt.Errorf("consumer %d next: %w", idx, err)
					return
				}
				if !first && r.Sequence != lastSeq+1 {
					errCh <- fmt.Errorf("consumer %d seq gap %d -> %d",
						idx, lastSeq, r.Sequence)
					return
				}
				first = false
				lastSeq = r.Sequence
				if err := cursors[idx].Ack(ctx, r); err != nil {
					errCh <- fmt.Errorf("consumer %d ack: %w", idx, err)
					return
				}
				n := atomic.AddInt64(&consumed[idx], 1)
				if n >= total {
					return
				}
			}
		}(i)
	}

	// Producer.
	var appended int64
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		for atomic.LoadInt64(&appended) < total {
			if _, err := s.Append(ctx, []byte("x")); err != nil {
				if ctx.Err() == nil {
					errCh <- fmt.Errorf("append: %w", err)
				}
				return
			}
			atomic.AddInt64(&appended, 1)
		}
	}()

	// Compactor: always asks to reclaim far back; the guard clamps it to the
	// slowest cursor, so consumers must never see a reclaimed position.
	compDone := make(chan struct{})
	go func() {
		defer close(compDone)
		for {
			select {
			case <-prodDone:
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := s.Compact(ctx, CompactOptions{KeepBytes: 1}); err != nil {
				if ctx.Err() == nil && !errors.Is(err, ErrCompactionInProgress) {
					errCh <- fmt.Errorf("compact: %w", err)
				}
			}
		}
	}()

	<-prodDone
	wg.Wait()
	cancel()
	<-compDone
	close(errCh)
	for e := range errCh {
		t.Fatal(e)
	}
	for i := 0; i < consumers; i++ {
		if got := atomic.LoadInt64(&consumed[i]); got != total {
			t.Fatalf("consumer %d consumed %d, want %d", i, got, total)
		}
		t.Logf("consumer %d: %d in-order records, final cursor=%s",
			i, consumed[i], cursors[i].Position())
	}
	if cursors[0].Position() != s.Stats().WritePosition {
		t.Fatalf("cursor not at end: %s vs %s", cursors[0].Position(), s.Stats().WritePosition)
	}
	t.Logf("all %d consumers drained %d records under concurrent append+compaction; end=%s",
		consumers, total, s.Stats().WritePosition)
}
