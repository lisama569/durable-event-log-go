package eventlog

import (
	"errors"
	"fmt"
)

// Cursor-layer sentinel errors. They supplement (and wrap) the store-level
// sentinels so consumers can tell "the log is quiet" apart from "my cursor
// fell behind retention".
var (
	// ErrCursorNotFound is returned when opening a cursor name that was never
	// committed.
	ErrCursorNotFound = errors.New("eventlog: cursor not found")
	// ErrCursorExists is returned by Create when a cursor of that name is
	// already persisted.
	ErrCursorExists = errors.New("eventlog: cursor already exists")
	// ErrCursorInvalidName rejects names that cannot be mapped safely to a
	// file name.
	ErrCursorInvalidName = errors.New("eventlog: invalid cursor name")
	// ErrCursorClosed is returned after Cursor/Manager Close.
	ErrCursorClosed = errors.New("eventlog: cursor (or its manager) is closed")
	// ErrCursorStateCorrupt means a persisted cursor state is torn, fails its
	// checksum or is internally inconsistent. The file is left in place for
	// diagnosis; the cursor is never silently reset.
	ErrCursorStateCorrupt = errors.New("eventlog: cursor state corrupt")
	// ErrCursorPositionReclaimed marks a cursor whose next position was
	// reclaimed by compaction before being acknowledged. It wraps
	// ErrPositionReclaimed: this is a hard skip, not an end-of-log wait.
	ErrCursorPositionReclaimed = errors.New("eventlog: cursor position reclaimed")
	// ErrCursorVersionUnknown rejects cursor state written by a newer binary.
	ErrCursorVersionUnknown = errors.New("eventlog: unknown cursor state version")
)

// CursorError carries cursor name, the position involved and the reason.
type CursorError struct {
	Name     string
	Position Position
	Kind     string
	Err      error
}

func (e *CursorError) Error() string {
	if e.Name == "" {
		return fmt.Sprintf("eventlog: cursor error: %s: %v", e.Kind, e.Err)
	}
	return fmt.Sprintf("eventlog: cursor %q at %s: %s: %v",
		e.Name, e.Position, e.Kind, e.Err)
}

func (e *CursorError) Unwrap() error {
	if e.Err != nil {
		return e.Err
	}
	return ErrCursorStateCorrupt
}
