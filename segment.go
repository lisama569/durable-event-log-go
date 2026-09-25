package eventlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

const (
	segmentPrefix = "segment-"
	segmentSuffix = ".log"
)

// segment is one log file. Logical frame offsets are counted without the
// segment header: the first frame of a fresh segment is at logical base,
// stored at physical offset segmentHeaderSize. dataStart is the logical
// offset of the first frame actually present (== base unless prefix-
// reclaimed); bytes [base,dataStart) form a sparse hole.
type segment struct {
	base      uint64 // logical offset of the first frame ever in this segment
	dataStart uint64 // logical offset of the first frame still present
	path      string
	size   int64 // current physical file size (header included)
	file   *os.File // non-nil only for the active (writable) segment
}

// logicalLength is the number of frame bytes contained in the file.
func (sg *segment) logicalLength() int64 { return sg.size - segmentHeaderSize }

// end is the logical offset immediately after the last frame byte.
func (sg *segment) end() uint64 { return sg.base + uint64(sg.logicalLength()) }

// physicalOffset converts a logical offset within this segment.
func (sg *segment) physicalOffset(pos uint64) int64 {
	return segmentHeaderSize + int64(pos-sg.base)
}

func segmentPath(dir string, base uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%020d%s", segmentPrefix, base, segmentSuffix))
}

// parseSegmentBase returns the base offset for a segment file name.
func parseSegmentBase(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	n := name[len(segmentPrefix) : len(name)-len(segmentSuffix)]
	v, err := strconv.ParseUint(n, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// listSegmentFiles returns the base offsets of all segment files, sorted.
func listSegmentFiles(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("listing log directory: %w", err)
	}
	var bases []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if v, ok := parseSegmentBase(e.Name()); ok {
			bases = append(bases, v)
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

// createSegmentFile creates a new segment file with a valid header and fsyncs
// both it and the directory entry.
func createSegmentFile(path string, base, dataStart uint64) (*os.File, error) {
	hdr := encodeSegmentHeader(nil, currentFormatVersion, base, dataStart)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("creating segment %s: %w", path, err)
	}
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		os.Remove(path)
		return nil, classifyIOError(fmt.Errorf("writing header of %s: %w", path, err))
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return nil, classifyIOError(fmt.Errorf("fsyncing new segment %s: %w", path, err))
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	return f, nil
}

// reopenActiveSegment opens the existing last segment for appending.
func reopenActiveSegment(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("reopening active segment %s: %w", path, err)
	}
	return f, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("opening directory %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsyncing directory %s: %w", dir, err)
	}
	return nil
}

// classifyIOError turns ENOSPC/EDQUOT into ErrDiskFull so callers can
// distinguish true exhaustion from other I/O failures.
func classifyIOError(err error) error {
	if err == nil {
		return nil
	}
	var perr *os.PathError
	if errors.As(err, &perr) {
		if errors.Is(perr.Err, syscall.ENOSPC) || errors.Is(perr.Err, syscall.EDQUOT) {
			return fmt.Errorf("%w: %v", ErrDiskFull, err)
		}
	}
	return err
}

func isNoSpace(err error) bool { return errors.Is(err, ErrDiskFull) }
