package eventlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
)

// ErrEndOfLog marks a read at the current committed end.
var ErrEndOfLog = errors.New("eventlog: no record at or after position (end of log)")

const (
	defaultSegmentBytes int64 = 8 << 20 // 8 MiB
)

// Options configures a Store.
type Options struct {
	// SegmentBytes is the roll size for the active segment. Default 8 MiB.
	SegmentBytes int64
	// MaxBytes is the hard on-disk capacity including all segment headers.
	// 0 means unlimited.
	MaxBytes int64
	// Repair permits Open to truncate a torn tail and to rebuild metadata
	// that conflicts with the segment files. Without it, ambiguous logs are
	// refused with an error wrapping ErrRecoveryConflict / ErrIncompleteTail.
	Repair bool
}

// Record is one replayed record.
type Record struct {
	Position Position
	Sequence uint64
	Data     []byte
}

// Stats is a point-in-time view of store state.
type Stats struct {
	SegmentCount  int
	ActiveBase    uint64
	StartPosition Position
	WritePosition Position
	PhysicalBytes int64
	LastSequence  uint64
	LastRecovery  *RecoveryReport
}

// Store is an embeddable, crash-safe append-only log.
//
// Concurrency model:
//   - Append and Compact are serialized through mu.
//   - Read and Replay take a read lock and run concurrently with each other.
//   - Compact additionally rejects a concurrent Compact immediately with
//     ErrCompactionInProgress; it never makes readers or writers wait forever.
type Store struct {
	dir  string
	opts Options

	mu        sync.RWMutex
	closed    bool
	unhealthy bool

	compacting atomic.Bool

	segments []*segment // sorted by base; last element is active
	active   *segment
	writePos uint64 // logical end offset (== active.end())
	startPos uint64 // base of the oldest surviving segment

	nextSequence uint64

	// frameStarts holds every known record start (logical offset) per segment,
	// sorted; used to reject offsets that point inside a frame.
	frameStarts map[uint64][]uint64

	lastRecovery *RecoveryReport

	// hub wakes consumers parked at the committed end (edge-triggered).
	hub *waiterHub
	// guard, when set (by a cursor Manager), clamps compaction so no position
	// a known cursor still needs is reclaimed.
	guard retentionGuard

	// testCompactHook, if set, runs after the compaction gate is acquired.
	testCompactHook func()
}

// DefaultOptions returns the recommended options.
func DefaultOptions() Options {
	return Options{SegmentBytes: defaultSegmentBytes, MaxBytes: 0}
}

func normalizeOptions(o Options) (Options, error) {
	if o.SegmentBytes == 0 {
		o.SegmentBytes = defaultSegmentBytes
	}
	if o.SegmentBytes < 0 || o.MaxBytes < 0 {
		return o, fmt.Errorf("%w: negative size option", ErrInvalidArgument)
	}
	return o, nil
}

// Open recovers (or initializes) a log in dir, creating the directory.
func Open(dir string, opts Options) (*Store, error) {
	no, err := normalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("creating log directory %s: %w", dir, err)
	}
	s, report, err := recoverLog(dir, no)
	if err != nil {
		return nil, err
	}
	s.lastRecovery = report
	return s, nil
}

func newEmptyStore(dir string, opts Options, report *RecoveryReport) (*Store, error) {
	path := segmentPath(dir, 0)
	f, err := createSegmentFile(path, 0, 0)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:          dir,
		opts:         opts,
		writePos:     0,
		nextSequence: 0,
		frameStarts:  map[uint64][]uint64{0: {}},
		lastRecovery: report,
		hub:          newWaiterHub(),
	}
	sg := &segment{base: 0, dataStart: 0, path: path, size: segmentHeaderSize, file: f}
	s.segments = []*segment{sg}
	s.active = sg
	return s, nil
}

func buildRecoveredStore(
	dir string, opts Options, bases []uint64, scans []segmentScan,
	md meta, metaPresent bool, report *RecoveryReport,
) (*Store, error) {
	s := &Store{
		dir:         dir,
		opts:        opts,
		frameStarts: make(map[uint64][]uint64),
		hub:         newWaiterHub(),
	}
	for _, sc := range scans {
		path := segmentPath(dir, sc.base)
		starts := make([]uint64, 0, len(sc.frames))
		for _, fr := range sc.frames {
			starts = append(starts, fr.logicalOffset)
		}
		s.frameStarts[sc.base] = starts
		s.segments = append(s.segments, &segment{
			base: sc.base, dataStart: sc.dataStart, path: path, size: sc.validSize,
		})
	}
	last := s.segments[len(s.segments)-1]
	f, err := reopenActiveSegment(last.path)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(last.size, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("seeking active segment: %w", err)
	}
	last.file = f
	s.active = last
	s.writePos = last.end()
	// Retained start = first frame of the first segment that still has frames.
	s.startPos = s.segments[0].base
	for _, sg := range s.segments {
		if starts := s.frameStarts[sg.base]; len(starts) > 0 {
			s.startPos = starts[0]
			break
		}
	}

	// Sequence from the chain: the scan with frames carries the high water.
	s.nextSequence = 0
	for i := len(scans) - 1; i >= 0; i-- {
		if len(scans[i].frames) > 0 {
			s.nextSequence = scans[i].nextSeq
			break
		}
	}

	if metaPresent {
		conflicts := []string{}
		if md.ActiveBase != last.base {
			conflicts = append(conflicts, fmt.Sprintf(
				"meta active_base=%d but active segment base=%d", md.ActiveBase, last.base))
		}
		if md.NextSequence != s.nextSequence {
			conflicts = append(conflicts, fmt.Sprintf(
				"meta next_sequence=%d but scan derives %d", md.NextSequence, s.nextSequence))
		}
		if len(md.Segments) != len(bases) {
			conflicts = append(conflicts, fmt.Sprintf(
				"meta lists %d segments but %d segment files exist", len(md.Segments), len(bases)))
		} else {
			for i, ms := range md.Segments {
				if ms.Base != bases[i] {
					conflicts = append(conflicts, fmt.Sprintf(
						"meta segment %d base=%d but file base=%d", i, ms.Base, bases[i]))
				}
			}
		}
		if md.FormatVersion < report.FormatVersion {
			report.decide("format upgrade: metadata format %d -> %d",
				md.FormatVersion, report.FormatVersion)
			report.MetaRebuilt = true
		}
		if len(conflicts) > 0 {
			if !opts.Repair {
				return nil, fmt.Errorf("%w: %v", ErrRecoveryConflict, conflicts)
			}
			for _, c := range conflicts {
				report.decide("repair: %s; segment files are source of truth, rebuilding metadata", c)
			}
			report.MetaRebuilt = true
			report.Repaired = true
		}
	} else {
		report.MetaRebuilt = true
		report.decide("no metadata sidecar: derived state from segment files")
	}
	return s, nil
}

func (s *Store) persistMetaLocked() error {
	md := meta{
		FormatVersion: currentFormatVersion,
		NextSequence:  s.nextSequence,
		ActiveBase:    s.active.base,
	}
	for _, sg := range s.segments {
		md.Segments = append(md.Segments, metaSegment{Base: sg.base, Size: sg.size})
	}
	return atomicWriteMeta(s.dir, md)
}

func (s *Store) physicalBytesLocked() int64 {
	var n int64
	for _, sg := range s.segments {
		n += sg.size
	}
	return n
}

// markUnhealthyLocked is called after an I/O failure whose on-disk result is
// uncertain. The store refuses all further writes until reopened, so a torn
// frame can never be followed by a good one as if nothing happened.
func (s *Store) markUnhealthyLocked(reason string, err error) error {
	s.unhealthy = true
	return fmt.Errorf("%w: %s: %v", ErrUnhealthy, reason, err)
}

// Append writes one record and returns its logical position. The position is
// acknowledged only after the frame bytes and the segment file have been
// fsynced.
func (s *Store) Append(ctx context.Context, data []byte) (Position, error) {
	if len(data) > maxPayloadLen {
		return 0, fmt.Errorf("%w: payload %d exceeds limit %d",
			ErrInvalidArgument, len(data), maxPayloadLen)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writableLocked(); err != nil {
		return 0, err
	}

	encoded := encodeFrame(nil, frame{
		version:  currentFormatVersion,
		sequence: s.nextSequence,
		payload:  data,
	})
	frameLen := int64(len(encoded))

	// Capacity is checked before touching disk: never overwrite or partially
	// write when the configured limit would be crossed.
	needHeader := int64(0)
	rotating := s.active.logicalLength() > 0 &&
		s.opts.SegmentBytes > 0 && s.active.size+frameLen > s.opts.SegmentBytes
	if rotating {
		needHeader = segmentHeaderSize
	}
	if s.opts.MaxBytes > 0 && s.physicalBytesLocked()+frameLen+needHeader > s.opts.MaxBytes {
		return 0, fmt.Errorf("%w: need %d bytes, limit %d, used %d",
			ErrCapacityExceeded, frameLen+needHeader, s.opts.MaxBytes, s.physicalBytesLocked())
	}

	if rotating {
		if err := s.rotateLocked(ctx); err != nil {
			return 0, err
		}
	}

	pos := s.writePos
	n, err := s.active.file.Write(encoded)
	if err != nil {
		return 0, s.markUnhealthyLocked("writing frame", classifyIOError(err))
	}
	if n != len(encoded) {
		return 0, s.markUnhealthyLocked("short write", io.ErrShortWrite)
	}
	if err := s.active.file.Sync(); err != nil {
		return 0, s.markUnhealthyLocked("fsyncing frame", classifyIOError(err))
	}
	// Durability point reached: frame is on platter.
	s.active.size += frameLen
	s.writePos += uint64(frameLen)
	s.frameStarts[s.active.base] = append(s.frameStarts[s.active.base], pos)
	s.nextSequence++

	if err := s.persistMetaLocked(); err != nil {
		// Data is already durable; only the rebuildable sidecar failed. Go
		// unhealthy so the ambiguity is explicit until reopen.
		s.hub.notify()
		return Position(pos), s.markUnhealthyLocked("persisting metadata after durable frame", err)
	}
	// Wake every consumer parked at the committed end. Edge-triggered: the
	// record above is durable and visible before the channels close.
	s.hub.notify()
	return Position(pos), nil
}

func (s *Store) writableLocked() error {
	switch {
	case s.closed:
		return ErrClosed
	case s.unhealthy:
		return ErrUnhealthy
	}
	return nil
}

// rotateLocked creates the next segment, fsyncs it and its directory entry,
// then commits the new active segment via the metadata sidecar.
func (s *Store) rotateLocked(_ context.Context) error {
	newBase := s.writePos
	path := segmentPath(s.dir, newBase)
	f, err := createSegmentFile(path, newBase, newBase)
	if err != nil {
		return classifyIOError(err)
	}
	sg := &segment{base: newBase, path: path, size: segmentHeaderSize, file: f}
	// Commit the structural change before writing the next frame.
	s.segments = append(s.segments, sg)
	s.active = sg
	s.frameStarts[newBase] = nil
	if err := s.persistMetaLocked(); err != nil {
		return s.markUnhealthyLocked("persisting metadata after rotation", err)
	}
	return nil
}

// findSegmentLocked returns the segment whose [base,end) contains pos.
func (s *Store) findSegmentLocked(pos uint64) (*segment, error) {
	idx := sort.Search(len(s.segments), func(i int) bool {
		return s.segments[i].end() > pos
	})
	if idx == len(s.segments) {
		return nil, fmt.Errorf("%w: %d >= end %d", ErrEndOfLog, pos, s.writePos)
	}
	sg := s.segments[idx]
	if pos < s.startPos || pos < sg.base {
		// The record used to live below the current retention horizon.
		return nil, fmt.Errorf("%w: %d before retained start %d",
			ErrPositionReclaimed, pos, s.startPos)
	}
	return sg, nil
}

func (s *Store) isFrameStartLocked(sg *segment, pos uint64) bool {
	starts := s.frameStarts[sg.base]
	i := sort.Search(len(starts), func(i int) bool { return starts[i] >= pos })
	return i < len(starts) && starts[i] == pos
}

// readFrameAt reads and validates one frame at a physical file offset.
func readFrameAt(f *os.File, physical int64) (frame, int64, error) {
	info, statErr := f.Stat()
	head := make([]byte, frameHeaderSize)
	if _, err := f.ReadAt(head, physical); err != nil {
		if errors.Is(err, io.EOF) {
			return frame{}, 0, errTruncatedFrame
		}
		return frame{}, 0, err
	}
	pLen := int(be32(head[14:18]))
	if uint64(pLen) > maxPayloadLen {
		return frame{}, 0, errFrameLength
	}
	total := frameHeaderSize + pLen + crcSize
	buf := make([]byte, total)
	copy(buf[:frameHeaderSize], head)
	if statErr == nil && physical+int64(total) > info.Size() {
		return frame{}, 0, errTruncatedFrame
	}
	if _, err := f.ReadAt(buf[frameHeaderSize:], physical+frameHeaderSize); err != nil {
		if errors.Is(err, io.EOF) {
			return frame{}, 0, errTruncatedFrame
		}
		return frame{}, 0, err
	}
	fr, n, err := decodeFrame(buf)
	return fr, int64(n), err
}

func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// Read returns the record exactly at pos. A position inside a frame is
// ErrOffsetNotAligned; a compacted position is ErrPositionReclaimed; reading
// at the committed end is ErrEndOfLog.
func (s *Store) Read(ctx context.Context, pos Position) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return Record{}, ErrClosed
	}
	sg, err := s.findSegmentLocked(uint64(pos))
	if err != nil {
		return Record{}, err
	}
	if !s.isFrameStartLocked(sg, uint64(pos)) {
		return Record{}, fmt.Errorf("%w: %s is inside a frame", ErrOffsetNotAligned, pos)
	}
	f, err := os.Open(sg.path)
	if err != nil {
		return Record{}, fmt.Errorf("opening %s: %w", sg.path, err)
	}
	defer f.Close()
	fr, _, err := readFrameAt(f, sg.physicalOffset(uint64(pos)))
	if err != nil {
		return Record{}, &CorruptionError{
			Path: sg.path, Offset: sg.physicalOffset(uint64(pos)),
			Kind: classifyDecodeError(err), Err: err,
		}
	}
	return Record{Position: pos, Sequence: fr.sequence, Data: fr.payload}, nil
}

// Replay calls fn for every committed record at or after start, in order.
// It holds a read lock for the duration, so compaction waits rather than
// deleting a file out from under it.
func (s *Store) Replay(ctx context.Context, start Position, fn func(Record) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	startOff := uint64(start)
	if start == Beginning {
		startOff = s.startPos
	}
	if startOff < s.startPos {
		return fmt.Errorf("%w: %s before retained start %d",
			ErrPositionReclaimed, start, s.startPos)
	}
	if startOff > s.writePos {
		return fmt.Errorf("%w: %d > end %d", ErrEndOfLog, startOff, s.writePos)
	}
	startSG, err := s.findSegmentLocked(startOff)
	if err != nil {
		return err
	}
	if !s.isFrameStartLocked(startSG, startOff) {
		return fmt.Errorf("%w: %s is inside a frame", ErrOffsetNotAligned, start)
	}
	started := false
	for _, sg := range s.segments {
		if sg.end() <= startOff {
			continue
		}
		f, err := os.Open(sg.path)
		if err != nil {
			return fmt.Errorf("opening %s: %w", sg.path, err)
		}
		off := int64(segmentHeaderSize)
		if !started {
			off = sg.physicalOffset(startOff)
			started = true
		}
		for off < sg.size {
			if err := ctx.Err(); err != nil {
				f.Close()
				return err
			}
			fr, n, derr := readFrameAt(f, off)
			if derr != nil {
				f.Close()
				return &CorruptionError{Path: sg.path, Offset: off,
					Kind: classifyDecodeError(derr), Err: derr}
			}
			logical := sg.base + uint64(off-segmentHeaderSize)
			if err := fn(Record{
				Position: Position(logical), Sequence: fr.sequence, Data: fr.payload,
			}); err != nil {
				f.Close()
				return err
			}
			off += n
		}
		f.Close()
	}
	return nil
}

// Stats returns a point-in-time snapshot.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := Stats{
		SegmentCount:  len(s.segments),
		ActiveBase:    s.active.base,
		StartPosition: Position(s.startPos),
		WritePosition: Position(s.writePos),
		PhysicalBytes: s.physicalBytesLocked(),
		LastSequence:  s.nextSequence - 1,
		LastRecovery:  s.lastRecovery,
	}
	if s.nextSequence == 0 {
		st.LastSequence = 0
	}
	return st
}

// LastRecovery returns the recovery report produced by the most recent Open.
func (s *Store) LastRecovery() *RecoveryReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastRecovery
}

// Close flushes state and closes the active file. It is idempotent.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	// Wake parked cursor handlers; they must not block forever on a closed store.
	s.hub.notify()
	if s.active != nil && s.active.file != nil {
		err := s.active.file.Close()
		s.active.file = nil
		if err != nil {
			return fmt.Errorf("closing active segment: %w", err)
		}
	}
	return nil
}
