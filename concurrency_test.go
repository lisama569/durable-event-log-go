package eventlog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// Concurrent appends must never interleave bytes: replay after close+reopen
// must yield every record exactly once, sequences contiguous, and CRCs valid.
func TestConcurrentAppendIntegrity(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	const writers = 8
	const perWriter = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	var positions []Position
	errCh := make(chan error, 32)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				p, err := s.Append(context.Background(),
					[]byte(fmt.Sprintf("w%d-m%04d-payload-%06d", w, i, i*7)))
				if err != nil {
					errCh <- err
					return
				}
				mu.Lock()
				positions = append(positions, p)
				mu.Unlock()
			}
		}(w)
	}
	// Concurrent readers must see only committed records, never torn data.
	stop := make(chan struct{})
	var rwg sync.WaitGroup
	rwg.Add(2)
	for r := 0; r < 2; r++ {
		go func() {
			defer rwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var n int
				err := s.Replay(context.Background(), Beginning, func(rec Record) error {
					n++
					return nil
				})
				if err != nil && !errors.Is(err, ErrEndOfLog) {
					errCh <- fmt.Errorf("concurrent replay: %w", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	rwg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatalf("background error: %v", e)
	}
	if len(positions) != writers*perWriter {
		t.Fatalf("acknowledged %d, want %d", len(positions), writers*perWriter)
	}
	st := s.Stats()
	t.Logf("committed=%s physical=%d segments=%d lastSeq=%d",
		st.WritePosition, st.PhysicalBytes, st.SegmentCount, st.LastSequence)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Deterministic recovery after all writers stop.
	s2, err := Open(dir, Options{SegmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	count := 0
	var lastSeq uint64 = ^uint64(0)
	if err := s2.Replay(context.Background(), Beginning, func(rec Record) error {
		if count > 0 && rec.Sequence != lastSeq+1 {
			return fmt.Errorf("sequence gap: %d -> %d", lastSeq, rec.Sequence)
		}
		lastSeq = rec.Sequence
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != writers*perWriter {
		t.Fatalf("recovered %d records, want %d", count, writers*perWriter)
	}
	if rep := s2.LastRecovery(); len(rep.Damages) != 0 {
		t.Fatalf("unexpected damages: %+v", rep.Damages)
	}
	t.Logf("recovered %d records contiguously after concurrent writes", count)
}

// Appends that would exceed MaxBytes must be rejected before touching disk,
// and the rejection must leave the log healthy and fully readable.
func TestCapacityExhaustion(t *testing.T) {
	dir := t.TempDir()
	const maxBytes int64 = 1200
	s, err := Open(dir, Options{SegmentBytes: 300, MaxBytes: maxBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ok, rejected := 0, 0
	for i := 0; i < 200; i++ {
		_, err := s.Append(context.Background(), []byte(fmt.Sprintf("payload-%04d-xxxxxxxxxx", i)))
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrCapacityExceeded):
			rejected++
		default:
			t.Fatalf("append %d: unexpected error %v", i, err)
		}
	}
	if rejected == 0 || ok == 0 {
		t.Fatalf("want some accepted and some rejected, got ok=%d rejected=%d", ok, rejected)
	}
	st := s.Stats()
	if st.PhysicalBytes > maxBytes {
		t.Fatalf("physical %d exceeded hard limit %d", st.PhysicalBytes, maxBytes)
	}
	t.Logf("accepted=%d rejected=%d physical=%d/%d committed=%s",
		ok, rejected, st.PhysicalBytes, maxBytes, st.WritePosition)

	// After a capacity rejection the log must still serve all accepted data
	// and remain writable once space is reclaimed.
	count := 0
	if err := s.Replay(context.Background(), Beginning, func(Record) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("replay after exhaustion: %v", err)
	}
	if count != ok {
		t.Fatalf("replay count %d != accepted %d", count, ok)
	}
	res, err := s.Compact(context.Background(), CompactOptions{KeepBytes: 1})
	if res.ReclaimedBytes == 0 && res.DeletedSegments == 0 {
		t.Fatalf("expected reclaimed space, got %+v", res)
	}
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	t.Logf("compaction after exhaustion: %+v", res)
	// Everything was below writePos; a fresh append should fit again.
	if _, err := s.Append(context.Background(), []byte("post-compact")); err != nil {
		t.Fatalf("append after compaction: %v", err)
	}
}

// A second concurrent compaction is rejected instead of blocking forever.
func TestConcurrentCompactImmediateRejection(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 30; i++ {
		if _, err := s.Append(context.Background(), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	inCompaction := make(chan struct{})
	release := make(chan struct{})
	var hookOnce sync.Once
	s.testCompactHook = func() {
		hookOnce.Do(func() { close(inCompaction) })
		<-release
	}
	errCh := make(chan error, 1)
	go func() {
		_, e := s.Compact(context.Background(), CompactOptions{Before: Position(1)})
		errCh <- e
	}()
	<-inCompaction
	if _, err := s.Compact(context.Background(), CompactOptions{Before: Position(1)});
		!errors.Is(err, ErrCompactionInProgress) {
		t.Fatalf("want ErrCompactionInProgress, got %v", err)
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Logf("compaction finished: %v", err)
	}
	s.testCompactHook = nil
	// After completion the gate is free again.
	if _, err := s.Compact(context.Background(), CompactOptions{Before: Position(1)}); err != nil {
		t.Fatalf("sequential compact failed: %v", err)
	}
}
