package eventlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestCursorAckWriteFailureKeepsOldProgress: if persisting the new progress
// fails (simulating ENOSPC mid-ack), the in-memory cursor must NOT advance:
// redelivery of the same record follows, and the last complete file is intact.
func TestCursorAckWriteFailureKeepsOldProgress(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ps := seedViaStore(t, s, 6, "crash")
	m, _ := NewManager(s)
	defer m.Close()
	c, _ := m.Create(context.Background(), "durable", Beginning)
	ctx := context.Background()

	// Ack 0,1 successfully.
	for i := 0; i < 2; i++ {
		r, _ := c.Next(ctx)
		if err := c.Ack(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	goodPos := c.Position()
	if goodPos != ps[2] {
		t.Fatalf("setup cursor = %s want %s", goodPos, ps[2])
	}

	// Deliver record 2, then force its durable write to fail like ENOSPC.
	r, err := c.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m.testInjectWriteErr = fmt.Errorf("%w: simulated ENOSPC on cursor state", ErrDiskFull)
	err = c.Ack(ctx, r)
	m.testInjectWriteErr = nil
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("want injected disk-full error, got %v", err)
	}
	t.Logf("ack of seq=%d at %s failed durably: %v", r.Sequence, r.Position, err)

	// Memory must still match the last COMPLETE on-disk state.
	if c.Position() != goodPos {
		t.Fatalf("cursor advanced despite failed write: %s != %s", c.Position(), goodPos)
	}
	// And Next redelivers record 2: at-least-once, never a silent skip.
	again, err := c.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.Position != r.Position {
		t.Fatalf("want redelivery of %s after failed ack, got %s", r.Position, again.Position)
	}
	t.Logf("after failed ack, cursor still at %s and redelivers seq=%d (no skip)",
		c.Position(), again.Sequence)

	// Retrying the ack now succeeds and moves forward.
	if err := c.Ack(ctx, again); err != nil {
		t.Fatalf("retry ack: %v", err)
	}
	if c.Position() != ps[3] {
		t.Fatalf("cursor after retry = %s want %s", c.Position(), ps[3])
	}
}

// TestCursorTornStateDetected: physically damage the cursor file as a
// half-written write would (truncate/garble/corrupt CRC). Reopening the
// manager must diagnose it and refuse rather than reset progress to zero.
func TestCursorTornStateDetected(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, DefaultOptions())
	ps := seedViaStore(t, s, 4, "torn")
	m, _ := NewManager(s)
	c, _ := m.Create(context.Background(), "fragile", Beginning)
	for i := 0; i < 3; i++ {
		r, _ := c.Next(context.Background())
		if err := c.Ack(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	wantPos := c.Position()
	_ = wantPos
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Case A: truncate the file mid-JSON (torn power loss).
	path := filepath.Join(dir, cursorDirName, cursorFileName("fragile"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	half := append([]byte(nil), raw[:len(raw)/2]...)
	if err := os.WriteFile(path, half, 0o600); err != nil {
		t.Fatal(err)
	}
	s2, _ := Open(dir, DefaultOptions())
	_, err = NewManager(s2)
	if !errors.Is(err, ErrCursorStateCorrupt) {
		t.Fatalf("torn state: want ErrCursorStateCorrupt, got %v", err)
	}
	t.Logf("half-written cursor file refused: %v", err)
	s2.Close()

	// Case B: structurally valid JSON with a changed payload byte but stale
	// CRC (simulates a torn/modified body that still parses). Must fail the
	// checksum gate, not be accepted with rewritten progress.
	full := append([]byte(nil), raw...)
	idx := bytesIndex(full, []byte(`"fragile"`))
	if idx < 0 {
		t.Fatal("could not locate payload to corrupt")
	}
	full[idx+1] = 'X' // change 'f' inside the JSON string; JSON stays valid
	if err := os.WriteFile(path, full, 0o600); err != nil {
		t.Fatal(err)
	}
	s3, _ := Open(dir, DefaultOptions())
	_, err = NewManager(s3)
	var ce *CursorError
	if !errors.As(err, &ce) || !errors.Is(err, ErrCursorStateCorrupt) {
		t.Fatalf("bad CRC: want corrupt CursorError, got %v", err)
	}
	if !contains(err.Error(), "checksum mismatch") {
		t.Fatalf("want checksum mismatch diagnostic, got %v", err)
	}
	t.Logf("CRC-corrupt cursor file refused and kept in place: %v", err)
	s3.Close()

	// Case C: empty file (write of zero bytes then crash).
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	s4, _ := Open(dir, DefaultOptions())
	_, err = NewManager(s4)
	if !errors.Is(err, ErrCursorStateCorrupt) {
		t.Fatalf("empty state: want ErrCursorStateCorrupt, got %v", err)
	}
	t.Logf("empty cursor file refused: %v", err)
	s4.Close()

	// Case D: restore the complete file -> everything works at old progress.
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	s5, _ := Open(dir, DefaultOptions())
	defer s5.Close()
	m5, err := NewManager(s5)
	if err != nil {
		t.Fatal(err)
	}
	defer m5.Close()
	c5, err := m5.Open(context.Background(), "fragile")
	if err != nil {
		t.Fatal(err)
	}
	r, err := c5.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.Position != ps[3] {
		t.Fatalf("restored cursor delivered %s, want %s", r.Position, ps[3])
	}
	t.Logf("restored complete cursor resumes at %s seq=%d (no progress loss)",
		r.Position, r.Sequence)
}

// TestCursorFutureVersionRejected: a cursor file from a newer binary is
// rejected with a stable rule (ErrCursorVersionUnknown) and never overwritten.
func TestCursorFutureVersionRejected(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, DefaultOptions())
	seedViaStore(t, s, 2, "ver")
	m, _ := NewManager(s)
	c, _ := m.Create(context.Background(), "future", Beginning)
	r, _ := c.Next(context.Background())
	if err := c.Ack(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	_ = m.Close()
	_ = s.Close()

	path := filepath.Join(dir, cursorDirName, cursorFileName("future"))
	raw, _ := os.ReadFile(path)
	var st cursorState
	if err := jsonUnmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	st.CursorVersion = 42
	st.CRC32 = 0
	encoded, err := encodeCursorState(st)
	if err != nil {
		t.Fatal(err)
	}
	// Re-stamp with an unknown version but a valid-looking CRC over version 42,
	// so the check is the VERSION gate (CRC is computed over whatever version).
	future := reencodeWithVersion(encoded, 42)
	if err := os.WriteFile(path, future, 0o600); err != nil {
		t.Fatal(err)
	}

	s2, _ := Open(dir, DefaultOptions())
	defer s2.Close()
	_, err = NewManager(s2)
	if !errors.Is(err, ErrCursorVersionUnknown) {
		t.Fatalf("want ErrCursorVersionUnknown, got %v", err)
	}
	t.Logf("future cursor version refused with reason preserved: %v", err)

	// The file must not have been clobbered.
	after, _ := os.ReadFile(path)
	if len(after) != len(future) {
		t.Fatalf("future-version file was overwritten (%d -> %d bytes)",
			len(future), len(after))
	}
}

// TestCursorStaleTempCleaned: a leftover .tmp from a crashed ack does not
// block reopening; the previous complete cursor is what loads.
func TestCursorStaleTempCleaned(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, DefaultOptions())
	ps := seedViaStore(t, s, 3, "stale")
	m, _ := NewManager(s)
	c, _ := m.Create(context.Background(), "stale", Beginning)
	r, _ := c.Next(context.Background())
	if err := c.Ack(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	_ = m.Close()
	_ = s.Close()

	tmp := filepath.Join(dir, cursorDirName, cursorTempFileName("stale"))
	if err := os.WriteFile(tmp, []byte(`{"partial":`), 0o600); err != nil {
		t.Fatal(err)
	}
	s2, _ := Open(dir, DefaultOptions())
	defer s2.Close()
	m2, err := NewManager(s2)
	if err != nil {
		t.Fatalf("stale temp should not block load: %v", err)
	}
	defer m2.Close()
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("stale temp %v should be removed, stat err=%v", tmp, err)
	}
	c2, err := m2.Open(context.Background(), "stale")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := c2.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r2.Position != ps[1] {
		t.Fatalf("cursor after stale-temp cleanup = %s want %s", r2.Position, ps[1])
	}
	t.Logf("stale .tmp ignored+removed; complete cursor resumes at %s seq=%d",
		r2.Position, r2.Sequence)
}
