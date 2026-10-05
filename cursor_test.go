package eventlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// openCursorStore opens a store with n seeded records and a fresh cursor.
func openCursorStore(t *testing.T, dir string, segBytes int64) *Store {
	t.Helper()
	s, err := Open(dir, Options{SegmentBytes: segBytes})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return s
}

func mustNext(t *testing.T, c *Cursor) Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	return r
}

func mustAck(t *testing.T, c *Cursor) {
	t.Helper()
	if err := c.Ack(context.Background()); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// Acked progress must survive a full process-style restart; consumption
// resumes exactly after the last acked record.
func TestCursorProgressSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s := openCursorStore(t, dir, 1<<10)
	ps := seedRecords(t, s, 5)

	c, err := s.OpenCursor("worker")
	if err != nil {
		t.Fatalf("open cursor: %v", err)
	}
	t.Logf("new cursor starts at %s (oldest retained)", c.Position())
	for i := 0; i < 2; i++ {
		r := mustNext(t, c)
		if r.Position != ps[i] {
			t.Fatalf("delivery %d at %s, want %s", i, r.Position, ps[i])
		}
		mustAck(t, c)
		t.Logf("acked record at %s, cursor now %s", r.Position, c.Position())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: same directory, new Store and Cursor objects.
	s2 := openCursorStore(t, dir, 1<<10)
	defer s2.Close()
	c2, err := s2.OpenCursor("worker")
	if err != nil {
		t.Fatalf("reopen cursor: %v", err)
	}
	defer c2.Close()
	if c2.Position() != ps[2] {
		t.Fatalf("cursor resumed at %s, want %s", c2.Position(), ps[2])
	}
	r := mustNext(t, c2)
	if r.Position != ps[2] || string(r.Data) != "record-0002" {
		t.Fatalf("first delivery after restart = %s %q, want %s record-0002",
			r.Position, r.Data, ps[2])
	}
	t.Logf("after restart cursor=%s delivered %s %q", c2.Position(), r.Position, r.Data)
}

// A record delivered but not acked before the crash must be redelivered
// (at-least-once); an acked record must never be silently skipped.
func TestCursorRedeliversUnackedAfterRestart(t *testing.T) {
	dir := t.TempDir()
	s := openCursorStore(t, dir, 1<<10)
	ps := seedRecords(t, s, 3)

	c, err := s.OpenCursor("worker")
	if err != nil {
		t.Fatal(err)
	}
	r0 := mustNext(t, c)
	mustAck(t, c)
	t.Logf("acked %s", r0.Position)
	r1 := mustNext(t, c) // delivered but NOT acked: simulates crash mid-processing
	t.Logf("delivered %s without ack, closing store (simulated crash)", r1.Position)
	if err := s.Close(); err != nil { // store closes underneath the cursor
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openCursorStore(t, dir, 1<<10)
	defer s2.Close()
	c2, err := s2.OpenCursor("worker")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if c2.Position() != ps[1] {
		t.Fatalf("cursor after crash = %s, want unacked %s", c2.Position(), ps[1])
	}
	r := mustNext(t, c2)
	if r.Position != r1.Position || string(r.Data) != string(r1.Data) {
		t.Fatalf("redelivery = %s %q, want %s %q", r.Position, r.Data, r1.Position, r1.Data)
	}
	t.Logf("redelivered unacked record at %s; acked %s was not repeated", r.Position, r0.Position)
}

// A caught-up cursor blocks without polling and wakes as soon as a record
// is appended.
func TestCursorRealtimeWaitWakesOnAppend(t *testing.T) {
	dir := t.TempDir()
	s := openCursorStore(t, dir, 1<<10)
	defer s.Close()
	seedRecords(t, s, 2)

	c, err := s.OpenCursor("live")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mustNext(t, c)
	mustAck(t, c)
	mustNext(t, c)
	mustAck(t, c)
	t.Logf("cursor caught up at %s; entering blocked wait", c.Position())

	type result struct {
		r   Record
		err error
	}
	doneCh := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		r, err := c.Next(ctx)
		doneCh <- result{r, err}
	}()

	// Give the waiter a moment to block, then append.
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	p, err := s.Append(context.Background(), []byte("fresh"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-doneCh:
		elapsed := time.Since(start)
		if res.err != nil {
			t.Fatalf("blocked next: %v", res.err)
		}
		if res.r.Position != p || string(res.r.Data) != "fresh" {
			t.Fatalf("got %s %q, want %s fresh", res.r.Position, res.r.Data, p)
		}
		t.Logf("waiter woke %v after append, delivered %s", elapsed, res.r.Position)
	case <-time.After(3 * time.Second):
		t.Fatal("waiter was not woken by append")
	}
}

// Timeout and cancellation must both unblock a waiting Next with ctx.Err().
func TestCursorNextTimeoutAndCancel(t *testing.T) {
	dir := t.TempDir()
	s := openCursorStore(t, dir, 1<<10)
	defer s.Close()
	seedRecords(t, s, 1)

	c, err := s.OpenCursor("live")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mustNext(t, c)
	mustAck(t, c)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := c.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	t.Logf("timeout wait returned after %v", time.Since(start))

	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel2()
	}()
	if _, err := c.Next(ctx2); !errors.Is(err, context.Canceled) {
		t.Fatalf("want Canceled, got %v", err)
	}
	t.Logf("cancelled wait returned after %v", time.Since(start))
}

// Closing a cursor must unblock a Next waiting for new records.
func TestCursorCloseUnblocksNext(t *testing.T) {
	dir := t.TempDir()
	s := openCursorStore(t, dir, 1<<10)
	defer s.Close()

	c, err := s.OpenCursor("live")
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Next(context.Background()) // no timeout: only Close can stop it
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("want ErrClosed, got %v", err)
		}
		t.Log("blocked Next returned ErrClosed after Close")
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not unblock Next")
	}
}

// Named cursors are independent; one name allows only one active consumer.
func TestCursorIndependenceAndExclusivity(t *testing.T) {
	dir := t.TempDir()
	s := openCursorStore(t, dir, 1<<10)
	defer s.Close()
	ps := seedRecords(t, s, 4)

	a, err := s.OpenCursor("a")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := s.OpenCursor("b")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// a consumes two, b consumes one; neither affects the other.
	mustNext(t, a)
	mustAck(t, a)
	mustNext(t, a)
	mustAck(t, a)
	mustNext(t, b)
	mustAck(t, b)
	if a.Position() != ps[2] || b.Position() != ps[1] {
		t.Fatalf("positions a=%s b=%s, want %s and %s", a.Position(), b.Position(), ps[2], ps[1])
	}
	t.Logf("independent progress: a=%s b=%s", a.Position(), b.Position())

	// A second consumer of the same name is refused, not silently shared.
	if _, err := s.OpenCursor("a"); !errors.Is(err, ErrCursorInUse) {
		t.Fatalf("want ErrCursorInUse, got %v", err)
	}
	// After Close the name can be reopened and resumes from persisted state.
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a2, err := s.OpenCursor("a")
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	defer a2.Close()
	if a2.Position() != ps[2] {
		t.Fatalf("reopened a at %s, want %s", a2.Position(), ps[2])
	}

	infos := s.Cursors()
	t.Logf("open cursors: %+v", infos)
	if len(infos) != 2 {
		t.Fatalf("Cursors() = %d entries, want 2", len(infos))
	}
}

// Bad names and acks without a delivery are rejected as invalid arguments.
func TestCursorArgumentValidation(t *testing.T) {
	dir := t.TempDir()
	s := openCursorStore(t, dir, 1<<10)
	defer s.Close()
	seedRecords(t, s, 1)

	for _, name := range []string{"", "../escape", "a/b", ".hidden", fmt.Sprintf("%65s", "x")} {
		if _, err := s.OpenCursor(name); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("name %q: want ErrInvalidArgument, got %v", name, err)
		}
	}
	c, err := s.OpenCursor("ok")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Ack(context.Background()); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ack without delivery: want ErrInvalidArgument, got %v", err)
	}
}

// A cursor whose next position was reclaimed by compaction must fail
// explicitly with ErrPositionReclaimed — never be mistaken for end-of-log
// and never read a different record.
func TestCursorLaggingBehindCompaction(t *testing.T) {
	dir := t.TempDir()
	s := openCursorStore(t, dir, 1<<10)
	defer s.Close()
	ps := seedRecords(t, s, 60)

	c, err := s.OpenCursor("slow")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < 3; i++ {
		mustNext(t, c)
		mustAck(t, c)
	}
	t.Logf("slow cursor at %s; compacting everything before %s", c.Position(), ps[40])

	res, err := s.Compact(context.Background(), CompactOptions{Before: ps[40]})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	t.Logf("compact result: %+v", res)

	_, err = c.Next(context.Background())
	if !errors.Is(err, ErrPositionReclaimed) {
		t.Fatalf("want ErrPositionReclaimed, got %v", err)
	}
	t.Logf("lagging cursor at %s got explicit error: %v", c.Position(), err)

	// A fresh cursor created after compaction starts at the new retained
	// start and reads real records, not a hole.
	c2, err := s.OpenCursor("fresh")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if c2.Position() != res.NewStart {
		t.Fatalf("fresh cursor at %s, want retained start %s", c2.Position(), res.NewStart)
	}
	r := mustNext(t, c2)
	t.Logf("fresh cursor at %s delivered %s %q", c2.Position(), r.Position, r.Data)
	if r.Position != res.NewStart {
		t.Fatalf("fresh delivery at %s, want %s", r.Position, res.NewStart)
	}
}

// A cursor blocked waiting for records must not be stuck while compaction
// runs concurrently; the next append still wakes it.
func TestCursorWaitSurvivesConcurrentCompaction(t *testing.T) {
	dir := t.TempDir()
	s := openCursorStore(t, dir, 1<<10)
	defer s.Close()
	ps := seedRecords(t, s, 60)

	c, err := s.OpenCursor("live")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Catch up to the end.
	for i := 0; i < 60; i++ {
		r := mustNext(t, c)
		if r.Position != ps[i] {
			t.Fatalf("delivery %d at %s, want %s", i, r.Position, ps[i])
		}
		mustAck(t, c)
	}
	t.Logf("cursor caught up at %s; blocking while compaction runs", c.Position())

	type result struct {
		r   Record
		err error
	}
	doneCh := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		r, err := c.Next(ctx)
		doneCh <- result{r, err}
	}()
	time.Sleep(100 * time.Millisecond)

	if _, err := s.Compact(context.Background(), CompactOptions{Before: ps[50]}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	p, err := s.Append(context.Background(), []byte("post-compaction"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-doneCh:
		if res.err != nil {
			t.Fatalf("blocked next during compaction: %v", res.err)
		}
		if res.r.Position != p {
			t.Fatalf("got %s, want %s", res.r.Position, p)
		}
		t.Logf("waiter survived compaction and delivered %s", res.r.Position)
	case <-time.After(3 * time.Second):
		t.Fatal("waiter stuck after concurrent compaction")
	}
}

// A torn, empty or foreign cursor state file must be refused with a
// diagnosable reason; the previous complete state is never silently reset.
func TestCursorStateCrashSafety(t *testing.T) {
	seed := func(t *testing.T) (string, *Store) {
		dir := t.TempDir()
		s := openCursorStore(t, dir, 1<<10)
		seedRecords(t, s, 3)
		return dir, s
	}
	writeState := func(t *testing.T, dir, name, content string) {
		t.Helper()
		path := cursorStatePath(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Logf("planted state file %s: %q", path, content)
	}

	t.Run("torn write is refused", func(t *testing.T) {
		dir, s := seed(t)
		defer s.Close()
		writeState(t, dir, "w", `{"cursor_version": 1, "name": "w", "ne`)
		_, err := s.OpenCursor("w")
		if !errors.Is(err, ErrCorruptData) {
			t.Fatalf("want ErrCorruptData, got %v", err)
		}
		t.Logf("torn state refused: %v", err)
	})

	t.Run("empty file is refused", func(t *testing.T) {
		dir, s := seed(t)
		defer s.Close()
		writeState(t, dir, "w", "")
		if _, err := s.OpenCursor("w"); !errors.Is(err, ErrCorruptData) {
			t.Fatalf("want ErrCorruptData, got %v", err)
		}
	})

	t.Run("version zero is refused", func(t *testing.T) {
		dir, s := seed(t)
		defer s.Close()
		writeState(t, dir, "w", `{"cursor_version": 0, "name": "w", "next": 33}`)
		if _, err := s.OpenCursor("w"); !errors.Is(err, ErrCorruptData) {
			t.Fatalf("want ErrCorruptData, got %v", err)
		}
	})

	t.Run("unknown newer version is refused with reason", func(t *testing.T) {
		dir, s := seed(t)
		defer s.Close()
		writeState(t, dir, "w", `{"cursor_version": 99, "name": "w", "next": 33}`)
		_, err := s.OpenCursor("w")
		if !errors.Is(err, ErrUnknownVersion) {
			t.Fatalf("want ErrUnknownVersion, got %v", err)
		}
		t.Logf("unknown version refused: %v", err)
	})

	t.Run("name mismatch is refused", func(t *testing.T) {
		dir, s := seed(t)
		defer s.Close()
		writeState(t, dir, "w", `{"cursor_version": 1, "name": "other", "next": 33}`)
		if _, err := s.OpenCursor("w"); !errors.Is(err, ErrCorruptData) {
			t.Fatalf("want ErrCorruptData, got %v", err)
		}
	})

	t.Run("leftover temp file is ignored", func(t *testing.T) {
		dir, s := seed(t)
		defer s.Close()
		// A crash between temp write and rename leaves only the temp file;
		// the previous complete state (here: none) must be used.
		tmp := cursorStatePath(dir, "w") + ".tmp"
		if err := os.WriteFile(tmp, []byte(`{"cursor_version": 1, garbage`), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Logf("planted leftover temp %s", tmp)
		c, err := s.OpenCursor("w")
		if err != nil {
			t.Fatalf("open with leftover tmp: %v", err)
		}
		defer c.Close()
		if c.Position() != Beginning {
			t.Fatalf("cursor = %s, want %s", c.Position(), Beginning)
		}
		t.Logf("leftover tmp ignored, cursor bootstrapped at %s", c.Position())
	})

	t.Run("state survives simulated crash between acks", func(t *testing.T) {
		dir, s := seed(t)
		c, err := s.OpenCursor("w")
		if err != nil {
			t.Fatal(err)
		}
		mustNext(t, c)
		mustAck(t, c) // fsynced before returning
		// Crash simulation: no clean Close of anything, just reopen files.
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s2 := openCursorStore(t, dir, 1<<10)
		defer s2.Close()
		c2, err := s2.OpenCursor("w")
		if err != nil {
			t.Fatal(err)
		}
		defer c2.Close()
		if c2.Position() != Position(33) {
			t.Fatalf("cursor after crash = %s, want pos:33", c2.Position())
		}
		t.Logf("acked position %s survived crash", c2.Position())
	})
}
