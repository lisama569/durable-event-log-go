package eventlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

// CompactOptions selects how much history to reclaim.
type CompactOptions struct {
	// Before drops every record whose position is strictly less than Before.
	Before Position
	// KeepBytes retains at least the newest KeepBytes of logical log data;
	// 0 disables size-based retention. If Before is also set (non-zero),
	// Before wins.
	KeepBytes int64
}

// CompactResult reports what reclamation did.
type CompactResult struct {
	ReclaimedBytes   int64
	DeletedSegments  int
	RewrittenSegment bool
	NewStart         Position
}

// Compact performs online reclamation.
//
// It is serialized with Append and with another Compact (a concurrent call
// gets ErrCompactionInProgress rather than blocking). Reads/replays that
// started before compaction keep working: POSIX lets an open, unlinked file
// be read to completion. Logical positions of surviving records never change;
// positions that were reclaimed are rejected with ErrPositionReclaimed.
func (s *Store) Compact(ctx context.Context, opts CompactOptions) (CompactResult, error) {
	if !s.compacting.CompareAndSwap(false, true) {
		return CompactResult{}, ErrCompactionInProgress
	}
	defer s.compacting.Store(false)
	if s.testCompactHook != nil {
		s.testCompactHook()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return CompactResult{}, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return CompactResult{}, err
	}

	cutoff, err := s.resolveCutoffLocked(opts)
	if err != nil {
		return CompactResult{}, err
	}
	if cutoff == 0 {
		return CompactResult{}, nil
	}
	// Nothing reclaimed if the cutoff is not past the oldest record.
	oldestStart, ok := s.oldestFrameStartLocked()
	if !ok || cutoff <= oldestStart {
		return CompactResult{}, nil
	}

	var result CompactResult

	// Phase 1: rewrite at most one segment that straddles the cutoff.
	// Whole-segment deletion must remain contiguous, so the segment
	// containing the cutoff keeps its base and loses only prefix frames.
	keepIdx := sort.Search(len(s.segments), func(i int) bool {
		return s.segments[i].end() > cutoff
	})
	if keepIdx >= len(s.segments) {
		return CompactResult{}, nil
	}
	if keepIdx == 0 {
		// Cutoff sits inside the oldest segment: rewrite it in place.
		if err := s.rewriteSegmentPrefixLocked(ctx, s.segments[0], cutoff); err != nil {
			return CompactResult{}, err
		}
		result.RewrittenSegment = true
	} else {
		// Segments [0,keepIdx) are fully below the cutoff and are deleted.
		// If the first surviving record is not at keepIdx's base, the
		// boundary segment must be rewritten as well.
		boundary := s.segments[keepIdx]
		starts := s.frameStarts[boundary.base]
		if len(starts) == 0 || cutoff > boundary.base {
			if err := s.rewriteSegmentPrefixLocked(ctx, boundary, cutoff); err != nil {
				return CompactResult{}, err
			}
			result.RewrittenSegment = true
		}
		for i := 0; i < keepIdx; i++ {
			sg := s.segments[i]
			size := sg.size
			if err := os.Remove(sg.path); err != nil && !os.IsNotExist(err) {
				return result, fmt.Errorf("removing %s: %w", sg.path, err)
			}
			result.ReclaimedBytes += size
			result.DeletedSegments++
		}
		s.segments = s.segments[keepIdx:]
	}
	if err := syncDir(s.dir); err != nil {
		return result, err
	}

	// Recompute start and accounting from frame starts actually retained.
	if start, ok := s.oldestFrameStartLocked(); ok {
		s.startPos = start
		result.NewStart = Position(start)
	} else {
		// Everything was below the cutoff except an empty tail: keep base.
		s.startPos = s.segments[0].base
		result.NewStart = Position(s.startPos)
	}
	if err := s.persistMetaLocked(); err != nil {
		return result, s.markUnhealthyLocked("persisting metadata after compaction", err)
	}
	result.ReclaimedBytes += s.reclaimZero() // no-op keeps intent explicit
	s.lastRecovery.decideCompaction(result)
	return result, nil
}

func (s *Store) reclaimZero() int64 { return 0 }

func (s *Store) resolveCutoffLocked(opts CompactOptions) (uint64, error) {
	if opts.Before != 0 {
		if uint64(opts.Before) > s.writePos {
			return 0, nil // beyond end: nothing to do
		}
		return uint64(opts.Before), nil
	}
	if opts.KeepBytes < 0 {
		return 0, fmt.Errorf("%w: negative KeepBytes", ErrInvalidArgument)
	}
	if opts.KeepBytes == 0 {
		return 0, nil
	}
	if opts.KeepBytes >= int64(s.writePos) {
		return 0, nil
	}
	cutoff := s.writePos - uint64(opts.KeepBytes)
	// Snap the cutoff back to the start of the frame it lands in; we reclaim
	// only whole records, never splitting a frame.
	if start, ok := s.frameStartAtOrBeforeLocked(cutoff); ok && start < cutoff {
		cutoff = start
	}
	return cutoff, nil
}

func (s *Store) oldestFrameStartLocked() (uint64, bool) {
	for _, sg := range s.segments {
		starts := s.frameStarts[sg.base]
		if len(starts) > 0 {
			return starts[0], true
		}
	}
	return 0, false
}

// frameStartAtOrAfterLocked finds the smallest record start >= pos.
func (s *Store) frameStartAtOrAfterLocked(pos uint64) (uint64, bool) {
	for _, sg := range s.segments {
		if sg.end() <= pos {
			continue
		}
		starts := s.frameStarts[sg.base]
		i := sort.Search(len(starts), func(i int) bool { return starts[i] >= pos })
		if i < len(starts) {
			return starts[i], true
		}
	}
	return 0, false
}

// frameStartAtOrBeforeLocked finds the greatest record start <= pos.
func (s *Store) frameStartAtOrBeforeLocked(pos uint64) (uint64, bool) {
	var best uint64
	found := false
	for _, sg := range s.segments {
		if sg.base > pos {
			break
		}
		starts := s.frameStarts[sg.base]
		i := sort.Search(len(starts), func(i int) bool { return starts[i] > pos })
		if i > 0 {
			best = starts[i-1]
			found = true
		}
	}
	return best, found
}

// rewriteSegmentPrefixLocked rewrites sg keeping only frames with logical
// offset >= cutoff. The segment base is preserved: prefix frames become a
// hole in a sparse file, so surviving positions are unchanged.
func (s *Store) rewriteSegmentPrefixLocked(ctx context.Context, sg *segment, cutoff uint64) error {
	starts := s.frameStarts[sg.base]
	i := sort.Search(len(starts), func(i int) bool { return starts[i] >= cutoff })
	keepStarts := starts[i:]

	tmpPath := sg.path + ".compact"
	_ = os.Remove(tmpPath)
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("creating compaction temp: %w", err)
	}
	abort := func(cause error) error {
		out.Close()
		os.Remove(tmpPath)
		return cause
	}
	dataStart := sg.base
	if len(keepStarts) > 0 {
		dataStart = keepStarts[0]
	}
	hdr := encodeSegmentHeader(nil, currentFormatVersion, sg.base, dataStart)
	if _, err := out.WriteAt(hdr, 0); err != nil {
		return abort(classifyIOError(fmt.Errorf("writing rewritten header: %w", err)))
	}

	in, err := os.Open(sg.path)
	if err != nil {
		return abort(fmt.Errorf("opening source segment: %w", err))
	}
	defer in.Close()
	for _, start := range keepStarts {
		if err := ctx.Err(); err != nil {
			return abort(err)
		}
		physical := segmentHeaderSize + int64(start-sg.base)
		fr, n, derr := readFrameAt(in, physical)
		if derr != nil {
			return abort(&CorruptionError{Path: sg.path, Offset: physical,
				Kind: classifyDecodeError(derr), Err: derr})
		}
		encoded := encodeFrame(nil, fr)
		if int64(len(encoded)) != n {
			return abort(fmt.Errorf("%w: rewritten frame size mismatch", ErrRecoveryConflict))
		}
		if _, err := out.WriteAt(encoded, physical); err != nil {
			return abort(classifyIOError(fmt.Errorf("writing rewritten frame: %w", err)))
		}
	}
	if err := out.Sync(); err != nil {
		return abort(classifyIOError(err))
	}
	if err := out.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("closing rewritten segment: %w", err)
	}

	// Swap atomically. The active segment's open descriptor is replaced too;
	// POSIX rename keeps the old inode readable for readers that already
	// opened it.
	if sg.file != nil {
		if err := sg.file.Close(); err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("closing active segment during rewrite: %w", err)
		}
		sg.file = nil
	}
	if err := os.Rename(tmpPath, sg.path); err != nil {
		return s.markUnhealthyLocked("renaming rewritten segment", err)
	}
	var newSize int64 = segmentHeaderSize
	if len(keepStarts) > 0 {
		last := keepStarts[len(keepStarts)-1]
		_, n, derr := readFrameAt(in, segmentHeaderSize+int64(last-sg.base))
		if derr != nil {
			return s.markUnhealthyLocked("measuring rewritten segment", derr)
		}
		newSize = int64(segmentHeaderSize) + int64(last-sg.base) + n
	}
	if sg.file == nil && s.active == sg {
		f, err := reopenActiveSegment(sg.path)
		if err != nil {
			return s.markUnhealthyLocked("reopening rewritten active segment", err)
		}
		if _, err := f.Seek(newSize, io.SeekStart); err != nil {
			f.Close()
			return s.markUnhealthyLocked("seeking rewritten active segment", err)
		}
		sg.file = f
	}
	sg.size = newSize
	sg.dataStart = dataStart
	s.frameStarts[sg.base] = append([]uint64(nil), keepStarts...)
	return nil
}

// ensure imports stay used when the file is edited later.
var _ = errors.Is
