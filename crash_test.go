package eventlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestCrashSIGKILL launches a child process that appends records in a tight
// loop without graceful shutdown, then kills it with SIGKILL. The parent
// reopens the directory: every acknowledged record must be readable and the
// (possibly torn) tail must be identifiable.
func TestCrashSIGKILL(t *testing.T) {
	if os.Getenv("EVENTLOG_CRASH_WRITER") == "1" {
		runCrashWriter()
		return
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashSIGKILL")
	cmd.Env = append(os.Environ(),
		"EVENTLOG_CRASH_WRITER=1",
		"EVENTLOG_CRASH_DIR="+dir,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	_ = cmd.Wait()

	bases, err := listSegmentFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	totalBytes := int64(0)
	for _, b := range bases {
		fi, err := os.Stat(segmentPath(dir, b))
		if err != nil {
			t.Fatal(err)
		}
		totalBytes += fi.Size()
	}
	t.Logf("child killed; segments=%d physical=%d", len(bases), totalBytes)

	// Open without repair: either the tail is clean or we must see a
	// distinguishable refusal.
	s, err := Open(dir, Options{Repair: true})
	if err != nil {
		t.Fatalf("reopen after SIGKILL: %v", err)
	}
	defer s.Close()
	rep := s.LastRecovery()
	for _, d := range rep.Decisions {
		t.Logf("  decision: %s", d)
	}
	for _, d := range rep.Damages {
		t.Logf("  torn tail: %s at %s+%d (%d bytes)", d.Kind, d.Path, d.Offset, d.Bytes)
	}
	t.Logf("recovered boundary: committed=%s nextSequence=%d", rep.Committed, rep.NextSequence)

	var lastSeq uint64 = ^uint64(0)
	count := 0
	if err := s.Replay(context.Background(), Beginning, func(r Record) error {
		if count > 0 && r.Sequence != lastSeq+1 {
			return fmt.Errorf("sequence gap %d -> %d at %s", lastSeq, r.Sequence, r.Position)
		}
		lastSeq = r.Sequence
		// Acknowledged payloads must be intact: payload encodes the sequence.
		want := fmt.Sprintf("crash-record-%08d", r.Sequence)
		if string(r.Data) != want {
			return fmt.Errorf("data mismatch at %s: %q", r.Position, r.Data)
		}
		count++
		return nil
	}); err != nil {
		t.Fatalf("replay after SIGKILL: %v", err)
	}
	if count == 0 {
		t.Fatal("expected at least one durable record after 400ms of writes")
	}
	t.Logf("verified %d intact, contiguous acknowledged records after SIGKILL", count)

	// Recovery must be repeatable: reopen again and get the same boundary.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, Options{Repair: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.LastRecovery().Committed != rep.Committed {
		t.Fatalf("non-repeatable recovery: %s vs %s",
			s2.LastRecovery().Committed, rep.Committed)
	}
	if len(s2.LastRecovery().Damages) != 0 {
		t.Fatalf("second open should be clean, got %+v", s2.LastRecovery().Damages)
	}
}

func runCrashWriter() {
	dir := os.Getenv("EVENTLOG_CRASH_DIR")
	_ = os.MkdirAll(filepath.Dir(filepath.Join(dir, "x")), 0o700)
	s, err := Open(dir, Options{SegmentBytes: 64 * 1024})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(2)
	}
	ctx := context.Background()
	for i := 0; ; i++ {
		_, err := s.Append(ctx, []byte(fmt.Sprintf("crash-record-%08d", i)))
		if err != nil {
			fmt.Fprintln(os.Stderr, "append:", err)
			os.Exit(3)
		}
	}
}

var _ = strings.TrimSpace
var _ = strconv.Itoa
var _ = errors.Is
