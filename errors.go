package eventlog

import (
	"errors"
	"fmt"
)

// Distinguishable sentinel errors. Callers can use errors.Is to tell
// capacity, corruption, version and lifecycle failures apart.
var (
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("eventlog: store is closed")
	// ErrUnhealthy is returned when a previous write/fsync failure left the
	// store unable to guarantee durability. The process must reopen it.
	ErrUnhealthy = errors.New("eventlog: store is in an unhealthy state")
	// ErrCorruptData means a frame's magic, length or CRC did not match.
	ErrCorruptData = errors.New("eventlog: corrupt data")
	// ErrIncompleteTail means a declared frame runs past the end of the file
	// (typically a torn write from a crash).
	ErrIncompleteTail = errors.New("eventlog: incomplete tail")
	// ErrUnknownVersion means a format/meta version is unsupported.
	ErrUnknownVersion = errors.New("eventlog: unknown or unsupported format version")
	// ErrCapacityExceeded means the configured MaxBytes would be crossed.
	ErrCapacityExceeded = errors.New("eventlog: configured capacity exceeded")
	// ErrDiskFull wraps an OS ENOSPC error.
	ErrDiskFull = errors.New("eventlog: no space left on device")
	// ErrCompactionInProgress rejects a second concurrent compaction.
	ErrCompactionInProgress = errors.New("eventlog: compaction already in progress")
	// ErrPositionReclaimed means the requested position was compacted away.
	ErrPositionReclaimed = errors.New("eventlog: position was reclaimed")
	// ErrOffsetNotAligned means a logical offset points inside a frame.
	ErrOffsetNotAligned = errors.New("eventlog: offset is not at a record boundary")
	// ErrInvalidArgument is returned for bad option/argument combinations.
	ErrInvalidArgument = errors.New("eventlog: invalid argument")
	// ErrRecoveryConflict means meta and segment contents disagree and
	// Options.Repair was false.
	ErrRecoveryConflict = errors.New("eventlog: recovery conflict")
)

// CorruptionError describes exactly where and why framed data was rejected.
type CorruptionError struct {
	Path   string
	Offset int64 // physical offset within Path
	Kind   string
	Err    error
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("eventlog: %s at %s+%d: %v", e.Kind, e.Path, e.Offset, e.Err)
}

func (e *CorruptionError) Unwrap() error {
	if e.Err != nil {
		return e.Err
	}
	return ErrCorruptData
}

// RecoveryError explains why opening/recovering a log was refused.
type RecoveryError struct {
	Reason string
	Report *RecoveryReport
	Err    error
}

func (e *RecoveryError) Error() string {
	if e.Report == nil {
		return fmt.Sprintf("eventlog: recovery refused: %s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("eventlog: recovery refused (%s): %v", e.Reason, e.Err)
}

func (e *RecoveryError) Unwrap() error {
	if e.Err != nil {
		return e.Err
	}
	return ErrRecoveryConflict
}
