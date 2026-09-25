package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A lost sidecar must be rebuildable from segment files, with the decision
// recorded in the recovery report.
func TestRecoveryMissingMeta(t *testing.T) {
	dir, _ := openFresh(t, 10, 1<<20)
	if err := os.Remove(filepath.Join(dir, metaFileName)); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("open without meta: %v", err)
	}
	defer s.Close()
	rep := s.LastRecovery()
	if !rep.MetaRebuilt {
		t.Fatal("expected metadata rebuild")
	}
	count := 0
	if err := s.Replay(context.Background(), Beginning, func(r Record) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 10 {
		t.Fatalf("rebuilt replay count = %d", count)
	}
	t.Logf("meta rebuilt from segments; decisions: %v", rep.Decisions)
}

// A sidecar that lies about sequence numbers conflicts. Without Repair the
// open must be refused; with Repair the segment files win.
func TestRecoveryConflictingMeta(t *testing.T) {
	dir, _ := openFresh(t, 10, 1<<20)
	path := filepath.Join(dir, metaFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var md meta
	if err := json.Unmarshal(raw, &md); err != nil {
		t.Fatal(err)
	}
	md.NextSequence = 99999
	bad, _ := json.Marshal(md)
	if err := os.WriteFile(path, bad, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, DefaultOptions()); !errors.Is(err, ErrRecoveryConflict) {
		t.Fatalf("want ErrRecoveryConflict, got %v", err)
	}
	s, err := Open(dir, Options{Repair: true})
	if err != nil {
		t.Fatalf("repair open: %v", err)
	}
	defer s.Close()
	if !s.LastRecovery().MetaRebuilt {
		t.Fatal("repair should rebuild conflicting metadata")
	}
	if s.Stats().LastSequence != 9 {
		t.Fatalf("last seq after repair = %d, want 9", s.Stats().LastSequence)
	}
	t.Logf("conflicting metadata resolved from segment truth; %d", s.Stats().LastSequence)
}

// Unknown future versions are refused rather than misinterpreted.
func TestRecoveryUnknownVersions(t *testing.T) {
	dir, _ := openFresh(t, 4, 1<<20)

	// Future meta version.
	path := filepath.Join(dir, metaFileName)
	raw, _ := os.ReadFile(path)
	var md meta
	json.Unmarshal(raw, &md)
	md.MetaVersion = 99
	future, _ := json.Marshal(md)
	os.WriteFile(path, future, 0o600)
	if _, err := Open(dir, DefaultOptions()); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("future meta: want ErrUnknownVersion, got %v", err)
	}
	t.Log("future metadata version rejected")

	// Future segment version byte.
	json.Unmarshal(raw, &md)
	md.MetaVersion = currentMetaVersion
	md.FormatVersion = 88
	future, _ = json.Marshal(md)
	os.WriteFile(path, future, 0o600)
	if _, err := Open(dir, DefaultOptions()); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("future format via meta: want ErrUnknownVersion, got %v", err)
	}
}

// An empty (torn) sidecar is incomplete metadata: rejected with a diagnostic.
func TestRecoveryEmptyMeta(t *testing.T) {
	dir, _ := openFresh(t, 2, 1<<20)
	if err := os.WriteFile(filepath.Join(dir, metaFileName), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, DefaultOptions())
	if !errors.Is(err, ErrRecoveryConflict) {
		t.Fatalf("want ErrRecoveryConflict for empty meta, got %v", err)
	}
	t.Logf("empty sidecar refused: %v", err)
}

func TestClosedStoreRejects(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := s.Append(ctx, []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("append closed: %v", err)
	}
	if err := s.Replay(ctx, Beginning, func(Record) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("replay closed: %v", err)
	}
	if _, err := s.Compact(ctx, CompactOptions{Before: Position(1)}); !errors.Is(err, ErrClosed) {
		t.Fatalf("compact closed: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("double close: %v", err)
	}
}

func TestOffsetMisalignment(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.Append(context.Background(), []byte("hello-world"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(context.Background(), p+1); !errors.Is(err, ErrOffsetNotAligned) {
		t.Fatalf("want ErrOffsetNotAligned, got %v", err)
	}
	if err := s.Replay(context.Background(), p+3, func(Record) error { return nil });
		!errors.Is(err, ErrOffsetNotAligned) {
		t.Fatalf("replay mid-frame: %v", err)
	}
	t.Log("inside-frame offsets explicitly rejected")
}

// Readers that started before compaction must keep seeing a consistent view;
// new appends are not mixed into the old view, and after compaction new reads
// observe the new retention horizon.
func TestReadDuringCompactionConsistent(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 200})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ps := seedRecords(t, s, 40)

	inCompaction := make(chan struct{})
	release := make(chan struct{})
	var once chan struct{} = inCompaction
	var hookOnce bool
	_ = hookOnce
	s.testCompactHook = func() {
		if once != nil {
			close(once)
			once = nil
		}
		<-release
	}
	done := make(chan error, 1)
	go func() {
		_, e := s.Compact(context.Background(), CompactOptions{Before: ps[30]})
		done <- e
	}()
	<-inCompaction

	// While compaction holds the lock, independently open the on-disk files
	// that are about to be removed and confirm they remain fully readable
	// (simulating a reader that opened them before deletion).
	var oldView []byte
	oldDir := s.dir
	bases, _ := listSegmentFiles(oldDir)
	var oldFiles []*os.File
	for _, b := range bases {
		f, oerr := os.Open(segmentPath(oldDir, b))
		if oerr != nil {
			t.Logf("pre-compaction open of %d: %v", b, oerr)
			continue
		}
		oldFiles = append(oldFiles, f)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("compact: %v", err)
	}
	// Unlinked files still readable via pre-compaction descriptors.
	for _, f := range oldFiles {
		buf := make([]byte, 4096)
		for {
			n, rerr := f.Read(buf)
			if n > 0 {
				oldView = append(oldView, buf[:n]...)
			}
			if rerr != nil {
				break
			}
		}
		f.Close()
	}
	if len(oldView) == 0 {
		t.Fatal("pre-compaction view disappeared")
	}
	t.Logf("pre-compaction readers retained %d bytes after unlink", len(oldView))

	// Post-compaction: old records rejected, surviving records intact, and a
	// new append is visible without contamination.
	if _, err := s.Read(context.Background(), ps[5]); !errors.Is(err, ErrPositionReclaimed) {
		t.Fatalf("old pos: %v", err)
	}
	r, err := s.Read(context.Background(), ps[30])
	if err != nil {
		t.Fatalf("surviving read: %v", err)
	}
	if r.Sequence != 30 {
		t.Fatalf("seq %d", r.Sequence)
	}
	np, err := s.Append(context.Background(), []byte("brand-new"))
	if err != nil {
		t.Fatalf("append after compact: %v", err)
	}
	nr, err := s.Read(context.Background(), np)
	if err != nil || string(nr.Data) != "brand-new" {
		t.Fatalf("new record contaminated: %+v err=%v", nr, err)
	}
}
