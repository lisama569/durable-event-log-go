package eventlog

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// SegmentInfo describes one recovered segment file.
type SegmentInfo struct {
	Path string
	Base uint64
	Size int64
}

// TailDamage describes one rejected region found during recovery.
type TailDamage struct {
	Path   string
	Offset int64 // physical offset where the rejected region starts
	Bytes  int64 // number of bytes rejected (0 if unknown)
	Kind   string // incomplete-header | incomplete-frame | crc-mismatch | bad-magic | bad-version | bad-segment-header
}

// RecoveryReport records what recovery found, decided and changed.
// Reports are printable so tests and operators can see recovery position,
// decision basis and key state transitions.
type RecoveryReport struct {
	FormatVersion uint8
	MetaVersion   uint8
	Segments      []SegmentInfo
	StartPosition Position
	Committed     Position
	NextSequence  uint64
	Damages       []TailDamage
	Decisions     []string
	Repaired      bool
	MetaPresent   bool
	MetaRebuilt   bool
}

func (r *RecoveryReport) decide(format string, args ...any) {
	r.Decisions = append(r.Decisions, fmt.Sprintf(format, args...))
}

// decideCompaction records an online-reclamation state change in the
// diagnostic trail of the open log.
func (r *RecoveryReport) decideCompaction(res CompactResult) {
	if r == nil {
		return
	}
	r.decide("compaction: deleted=%d rewritten=%t reclaimedBytes=%d newStart=%s",
		res.DeletedSegments, res.RewrittenSegment, res.ReclaimedBytes, res.NewStart)
}

// scannedFrame is one valid frame found during a scan.
type scannedFrame struct {
	logicalOffset uint64
	physicalOff   int64
	size          int64
	seq           uint64
}

// segmentScan is the result of scanning one segment file.
type segmentScan struct {
	base        uint64
	dataStart   uint64
	version     uint8
	validSize   int64 // physical size up to and including the last valid frame
	frames      []scannedFrame
	damage      *TailDamage // non-nil if a bad/torn region was encountered
	nextSeq     uint64 // expected sequence after the last valid frame (0 if none)
}

// scanSegment reads a whole segment file, validates header + every frame,
// and stops at the first bad region. Bytes after a bad region are not
// interpreted; recovery only ever truncates the final segment there.
func scanSegment(path string, declaredBase uint64) (segmentScan, error) {
	f, err := os.Open(path)
	if err != nil {
		return segmentScan{}, fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return segmentScan{}, fmt.Errorf("reading %s: %w", path, err)
	}
	var sc segmentScan
	sc.base = declaredBase
	sc.validSize = segmentHeaderSize
	if len(data) < segmentHeaderSize {
		sc.damage = &TailDamage{Path: path, Offset: int64(len(data)),
			Bytes: int64(segmentHeaderSize - len(data)), Kind: "incomplete-header"}
		return sc, nil
	}
	ver, base, dataStart, err := decodeSegmentHeader(data[:segmentHeaderSize])
	if err != nil {
		kind := "bad-segment-header"
		switch {
		case errors.Is(err, ErrUnknownVersion):
			kind = "bad-version"
		case errors.Is(err, errBadMagic):
			kind = "bad-magic"
		}
		sc.damage = &TailDamage{Path: path, Offset: 0, Bytes: segmentHeaderSize, Kind: kind}
		return sc, nil
	}
	sc.version = ver
	sc.dataStart = dataStart
	if base != declaredBase {
		return sc, fmt.Errorf("%w: %s declares base %d but file name says %d",
			ErrRecoveryConflict, path, base, declaredBase)
	}
	// First retained frame lives after the (possibly reclaimed) prefix hole.
	off := segmentHeaderSize + int64(dataStart-base)
	if int64(len(data)) < off {
		sc.damage = &TailDamage{Path: path, Offset: int64(len(data)),
			Bytes: off - int64(len(data)), Kind: "incomplete-frame"}
		return sc, nil
	}
	var lastSeq uint64
	haveSeq := false
	for off < int64(len(data)) {
		fr, n, derr := decodeFrame(data[off:])
		if derr != nil {
			kind := classifyDecodeError(derr)
			// Any kind of bad region ends the scan; record how many tail
			// bytes are unparseable so repair can reason about reachability.
			bytes := int64(len(data)) - off
			sc.damage = &TailDamage{Path: path, Offset: off, Bytes: bytes, Kind: kind}
			break
		}
		if haveSeq && fr.sequence <= lastSeq {
			return sc, fmt.Errorf("%w: %s frame at physical %d has non-increasing sequence %d after %d",
				ErrRecoveryConflict, path, off, fr.sequence, lastSeq)
		}
		// Contiguous data: sequence must be exactly last+1. A hole (prefix
		// reclaimed by compaction) legitimately skips numbers.
		if haveSeq && fr.sequence != lastSeq+1 && sc.dataStart == sc.base {
			return sc, fmt.Errorf("%w: %s frame at physical %d has sequence %d, expected %d",
				ErrRecoveryConflict, path, off, fr.sequence, lastSeq+1)
		}
		lastSeq = fr.sequence
		haveSeq = true
		sc.frames = append(sc.frames, scannedFrame{
			logicalOffset: base + uint64(off-segmentHeaderSize),
			physicalOff:   off,
			size:          int64(n),
			seq:           fr.sequence,
		})
		off += int64(n)
		sc.validSize = off
	}
	if haveSeq {
		sc.nextSeq = lastSeq + 1
	}
	return sc, nil
}

func classifyDecodeError(err error) string {
	switch {
	case errors.Is(err, ErrIncompleteTail):
		return "incomplete-frame"
	case errors.Is(err, errBadMagic):
		return "bad-magic"
	case errors.Is(err, errFrameCRC):
		return "crc-mismatch"
	case errors.Is(err, ErrUnknownVersion):
		return "bad-version"
	default:
		return "bad-frame"
	}
}

// recoverLog scans all segment files, applies acceptance/downgrade/rejection
// rules and (with Repair) normalizes a torn tail. It never leaves a partially
// usable log: either it returns a store ready for use, or an error carrying a
// RecoveryReport.
func recoverLog(dir string, opts Options) (*Store, *RecoveryReport, error) {
	report := &RecoveryReport{}

	md, metaPresent, err := loadMeta(dir)
	if err != nil {
		return nil, report, &RecoveryError{Reason: "metadata unreadable", Err: err}
	}
	report.MetaPresent = metaPresent
	if metaPresent {
		report.MetaVersion = md.MetaVersion
		report.FormatVersion = md.FormatVersion
		if md.MetaVersion < currentMetaVersion {
			report.decide("metadata version %d older than binary version %d: accepted, will upgrade on next write",
				md.MetaVersion, currentMetaVersion)
		}
	} else {
		report.FormatVersion = currentFormatVersion
	}

	bases, err := listSegmentFiles(dir)
	if err != nil {
		return nil, report, &RecoveryError{Reason: "cannot list segments", Err: err}
	}

	if len(bases) == 0 {
		if metaPresent && len(md.Segments) > 0 {
			return nil, report, &RecoveryError{
				Reason: "metadata lists segments but no segment files exist",
				Err:    ErrRecoveryConflict,
			}
		}
		// Fresh directory: create the first segment at base 0.
		s, cerr := newEmptyStore(dir, opts, report)
		if cerr != nil {
			return nil, report, &RecoveryError{Reason: "initialization failed", Err: cerr}
		}
		report.decide("no segment files: initialized fresh log at position 0")
		if werr := s.persistMetaLocked(); werr != nil {
			return nil, report, &RecoveryError{Reason: "cannot write initial metadata", Err: werr}
		}
		return s, report, nil
	}

	// Scan every segment. A bad region is repairable only on the final
	// segment; a hole or corruption in the middle destroys the contiguous
	// chain and must be refused rather than silently dropping records.
	var scans []segmentScan
	for i, base := range bases {
		path := segmentPath(dir, base)
		sc, serr := scanSegment(path, base)
		if serr != nil {
			attachDamages(report, scans)
			return nil, report, &RecoveryError{Reason: "segment chain broken", Report: report, Err: serr}
		}
		if sc.damage != nil && i != len(bases)-1 {
			report.Damages = append(report.Damages, *sc.damage)
			attachDamages(report, scans)
			return nil, report, &RecoveryError{
				Reason: fmt.Sprintf("damage in non-final segment %s (%s at offset %d); refusing to drop records",
					path, sc.damage.Kind, sc.damage.Offset),
				Report: report,
				Err:    ErrRecoveryConflict,
			}
		}
		scans = append(scans, sc)
	}
	last := &scans[len(scans)-1]
	if last.damage != nil {
		report.Damages = append(report.Damages, *last.damage)
		// Stable repair rule:
		//  - incomplete-frame: declared frame runs past EOF (a torn append of
		//    bytes that were never acknowledged) -> truncatable.
		//  - bad-magic tail reaching EOF: unparseable residue -> truncatable.
		// A complete-sized frame with a CRC mismatch is NOT torn (torn writes
		// are shorter, never longer/wrong-at-end): truncating it would
		// silently discard acknowledged data, so it is always refused.
		// An unknown version is always refused rather than destroyed.
		repairable := last.damage.Kind == "incomplete-frame" || last.damage.Kind == "bad-magic"
		sentinel := ErrCorruptData
		if last.damage.Kind == "incomplete-frame" {
			sentinel = ErrIncompleteTail
		}
		if last.damage.Kind == "bad-version" {
			sentinel = ErrUnknownVersion
		}
		if !repairable || !opts.Repair {
			action := "open with Repair to truncate it"
			if !repairable {
				action = "damage is not a torn tail; manual intervention required (records after it must not be silently discarded)"
			}
			attachDamages(report, scans)
			return nil, report, &RecoveryError{
				Reason: fmt.Sprintf("damaged tail at %s+%d (%s, %d bytes): %s",
					last.damage.Path, last.damage.Offset, last.damage.Kind,
					last.damage.Bytes, action),
				Report: report,
				Err:    sentinel,
			}
		}
		if err := truncateToValid(last, report); err != nil {
			return nil, report, &RecoveryError{Reason: "tail repair truncate failed", Report: report, Err: err}
		}
		last.damage = nil
	}
	attachDamages(report, scans)

	// Contiguity: segments must connect and sequences must continue.
	if err := verifyChain(scans); err != nil {
		return nil, report, &RecoveryError{Reason: "segment chain not contiguous", Report: report, Err: err}
	}

	s, err := buildRecoveredStore(dir, opts, bases, scans, md, metaPresent, report)
	if err != nil {
		return nil, report, &RecoveryError{Reason: "could not reopen recovered segments", Report: report, Err: err}
	}
	report.Committed = Position(s.writePos)
	report.StartPosition = Position(s.segments[0].base)
	report.NextSequence = s.nextSequence
	for _, sg := range s.segments {
		report.Segments = append(report.Segments, SegmentInfo{
			Path: sg.path, Base: sg.base, Size: sg.size,
		})
	}
	report.decide("recovery position: committed=%s start=%s nextSequence=%d",
		report.Committed, report.StartPosition, report.NextSequence)

	// Persist a normalized sidecar when meta was absent, older, or disagreed.
	if !metaPresent || report.MetaRebuilt {
		if werr := s.persistMetaLocked(); werr != nil {
			return nil, report, &RecoveryError{Reason: "cannot persist rebuilt metadata", Report: report, Err: werr}
		}
	}
	return s, report, nil
}

func attachDamages(report *RecoveryReport, scans []segmentScan) {
	for _, sc := range scans {
		if sc.damage != nil {
			report.Damages = append(report.Damages, *sc.damage)
		}
	}
}

func truncateToValid(sc *segmentScan, report *RecoveryReport) error {
	f, err := os.OpenFile(sc.damage.Path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(sc.validSize); err != nil {
		return fmt.Errorf("truncating torn tail: %w", err)
	}
	if err := f.Sync(); err != nil {
		return classifyIOError(err)
	}
	d := sc.damage
	report.Repaired = true
	report.decide("repair: truncated %d torn bytes at %s+%d (%s); valid boundary physical=%d",
		d.Bytes, d.Path, d.Offset, d.Kind, sc.validSize)
	return nil
}

func verifyChain(scans []segmentScan) error {
	for i := range scans {
		if len(scans[i].frames) == 0 {
			if i == 0 {
				continue // empty first segment
			}
			// Non-first segment being empty is legal right after a rotation
			// with no frames appended, but then it must also be the last.
			if i != len(scans)-1 {
				return fmt.Errorf("%w: empty segment %d in the middle of the chain",
					ErrRecoveryConflict, i)
			}
		}
		if i > 0 {
			prev := scans[i-1]
			if scans[i].base != prev.base+uint64(prev.validSize-segmentHeaderSize) {
				return fmt.Errorf("%w: segment %d base %d does not follow previous end %d",
					ErrRecoveryConflict, i, scans[i].base,
					prev.base+uint64(prev.validSize-segmentHeaderSize))
			}
			if len(prev.frames) > 0 && len(scans[i].frames) > 0 {
				firstSeq := scans[i].frames[0].seq
				lastSeq := prev.frames[len(prev.frames)-1].seq
				if firstSeq != lastSeq+1 {
					return fmt.Errorf("%w: sequence gap across segment boundary: %d -> %d",
						ErrRecoveryConflict, lastSeq, firstSeq)
				}
			}
		}
	}
	return nil
}
