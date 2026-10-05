package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

// Cursor versioning: cursorVersionV1 is the only written version. Versions
// newer than currentCursorVersion are refused with ErrUnknownVersion; older
// versions (none exist below 1 yet) are accepted and upgraded on next save.
const (
	cursorVersionV1      uint8 = 1
	currentCursorVersion       = cursorVersionV1
)

// ErrCursorInUse is returned when a cursor name is already open in this
// process or locked by another process.
var ErrCursorInUse = errors.New("eventlog: cursor is already in use")

// cursorState is the versioned, atomically-replaced on-disk cursor state.
type cursorState struct {
	CursorVersion uint8  `json:"cursor_version"`
	Name          string `json:"name"`
	Next          uint64 `json:"next"`
}

// validateCursorName rejects names that are empty, too long, or could escape
// the log directory when used as a file name.
func validateCursorName(name string) error {
	if name == "" || len(name) > 64 {
		return fmt.Errorf("%w: cursor name %q must be 1..64 characters", ErrInvalidArgument, name)
	}
	for _, r := range name {
		ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' ||
			r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.'
		if !ok {
			return fmt.Errorf("%w: cursor name %q has illegal character %q",
				ErrInvalidArgument, name, r)
		}
	}
	if strings.HasPrefix(name, ".") {
		return fmt.Errorf("%w: cursor name %q must not start with '.'", ErrInvalidArgument, name)
	}
	return nil
}

func cursorStatePath(dir, name string) string {
	return filepath.Join(dir, "cursor-"+name+".json")
}

// loadCursorState reads the persisted cursor state. A missing file is not an
// error: present is false and the caller bootstraps a fresh cursor. A torn,
// empty or unparseable file is refused with a diagnosable reason instead of
// being silently reset; a newer version is refused with ErrUnknownVersion.
func loadCursorState(dir, name string) (st cursorState, present bool, err error) {
	path := cursorStatePath(dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cursorState{}, false, nil
		}
		return cursorState{}, false, fmt.Errorf("reading %s: %w", path, err)
	}
	if len(data) == 0 {
		return cursorState{}, false, fmt.Errorf("%w: %s is empty (incomplete cursor write)",
			ErrCorruptData, path)
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return cursorState{}, false, fmt.Errorf("%w: %s is not valid cursor state: %v",
			ErrCorruptData, path, err)
	}
	switch {
	case st.CursorVersion == 0:
		return cursorState{}, false, fmt.Errorf("%w: %s has cursor_version 0 (incomplete cursor write)",
			ErrCorruptData, path)
	case st.CursorVersion > currentCursorVersion:
		return cursorState{}, false, fmt.Errorf("%w: cursor_version=%d, binary supports up to %d",
			ErrUnknownVersion, st.CursorVersion, currentCursorVersion)
	case st.CursorVersion < currentCursorVersion:
		// Older accepted versions upgrade in memory; the next save rewrites
		// them at currentCursorVersion. (No versions below v1 exist yet.)
	}
	if st.Name != name {
		return cursorState{}, false, fmt.Errorf("%w: %s belongs to cursor %q, not %q",
			ErrCorruptData, path, st.Name, name)
	}
	return st, true, nil
}

// saveCursorState writes the state to a temp file, fsyncs it and renames it
// over the old one, then fsyncs the directory. A crash at any point leaves
// either the old complete state or the new complete state, never a mixture.
func saveCursorState(dir string, st cursorState) error {
	st.CursorVersion = currentCursorVersion
	data, err := json.MarshalIndent(&st, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding cursor state: %w", err)
	}
	final := cursorStatePath(dir, st.Name)
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return classifyIOError(fmt.Errorf("creating %s: %w", tmp, err))
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return classifyIOError(fmt.Errorf("writing %s: %w", tmp, err))
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return classifyIOError(fmt.Errorf("fsyncing %s: %w", tmp, err))
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return classifyIOError(fmt.Errorf("closing %s: %w", tmp, err))
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return classifyIOError(fmt.Errorf("renaming cursor state: %w", err))
	}
	if err := syncDir(dir); err != nil {
		return classifyIOError(fmt.Errorf("fsyncing directory for cursor state: %w", err))
	}
	return nil
}

// Cursor is a named, durable consumer position over a Store.
//
// Delivery is at-least-once: Next returns the record at the cursor, and the
// position only advances when Ack persists it. A crash between Next and Ack
// replays the record after reopen; an acked record is never redelivered.
type Cursor struct {
	store *Store
	name  string

	mu sync.Mutex
	// next is atomic so Position/Cursors stay readable while a consumer
	// holds mu through a blocking Next.
	next    atomic.Uint64
	last    Record
	hasLast bool
	closed  bool
	done    chan struct{}
	// closeOnce closes done before Close takes mu, so a Next blocked in its
	// wait loop wakes up and releases mu instead of deadlocking with Close.
	closeOnce sync.Once

	lockFile *os.File // held flock guards against cross-process double consumers
}

// CursorInfo is a point-in-time view of one open cursor, useful for checking
// whether a planned compaction cutoff would outrun a consumer.
type CursorInfo struct {
	Name     string
	Position Position
}

// Cursors lists the cursors currently open in this process with the position
// of the next record each will deliver. Cursors persisted but not open in
// this process are not included.
func (s *Store) Cursors() []CursorInfo {
	s.cmu.Lock()
	defer s.cmu.Unlock()
	out := make([]CursorInfo, 0, len(s.cursors))
	for _, c := range s.cursors {
		out = append(out, CursorInfo{Name: c.name, Position: c.Position()})
	}
	return out
}

// OpenCursor opens (or creates) the named durable cursor.
//
// A cursor name has a single active consumer: opening the same name twice in
// one process, or while another process holds it, fails with ErrCursorInUse.
// Distinct names are fully independent. A newly created cursor starts at the
// oldest retained record; a reopened cursor resumes at its persisted position.
func (s *Store) OpenCursor(name string) (*Cursor, error) {
	if err := validateCursorName(name); err != nil {
		return nil, err
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	s.cmu.Lock()
	defer s.cmu.Unlock()
	if s.cursors[name] != nil {
		return nil, fmt.Errorf("%w: cursor %q is open in this process", ErrCursorInUse, name)
	}

	lockPath := cursorStatePath(s.dir, name) + ".lock"
	lf, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, classifyIOError(fmt.Errorf("opening cursor lock %s: %w", lockPath, err))
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lf.Close()
		return nil, fmt.Errorf("%w: cursor %q is locked by another process: %v", ErrCursorInUse, name, err)
	}
	unlock := func() {
		_ = syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)
		_ = lf.Close()
	}

	st, present, err := loadCursorState(s.dir, name)
	if err != nil {
		unlock()
		return nil, err
	}
	if !present {
		// Bootstrap at the oldest retained position so the new cursor can
		// never be mistaken for one that fell behind before it existed.
		s.mu.RLock()
		st = cursorState{Name: name, Next: s.startPos}
		s.mu.RUnlock()
		if err := saveCursorState(s.dir, st); err != nil {
			unlock()
			return nil, err
		}
	}

	c := &Cursor{
		store:    s,
		name:     name,
		done:     make(chan struct{}),
		lockFile: lf,
	}
	c.next.Store(st.Next)
	s.cursors[name] = c
	return c, nil
}

// Name returns the cursor name.
func (c *Cursor) Name() string { return c.name }

// Position returns the position of the next record to be delivered.
func (c *Cursor) Position() Position {
	return Position(c.next.Load())
}

// Next returns the record at the cursor position.
//
// If the cursor is caught up it blocks — without polling — until a record is
// appended, the log is closed, or ctx is done (timeout/cancellation both
// surface as ctx.Err()). If the cursor's position was reclaimed by
// compaction, Next fails with an error wrapping ErrPositionReclaimed; it
// never silently skips ahead or misreads another record.
//
// The returned record is not yet consumed: the cursor only advances when
// Ack succeeds. Next is safe for concurrent use but is designed for a single
// consumer goroutine per cursor.
func (c *Cursor) Next(ctx context.Context) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return Record{}, ErrClosed
	}
	for {
		if err := ctx.Err(); err != nil {
			return Record{}, err
		}
		// Grab the generation channel before reading, so an append that
		// lands between the read and the wait still wakes us.
		ch := c.store.change()
		rec, err := c.store.Read(ctx, Position(c.next.Load()))
		switch {
		case err == nil:
			c.last = rec
			c.hasLast = true
			return rec, nil
		case errors.Is(err, ErrEndOfLog):
			select {
			case <-ch:
				continue
			case <-c.done:
				return Record{}, ErrClosed
			case <-ctx.Done():
				return Record{}, ctx.Err()
			}
		default:
			// Includes ErrPositionReclaimed (lagging cursor met compaction)
			// and ErrClosed; both must reach the caller explicitly.
			return Record{}, err
		}
	}
}

// Ack durably advances the cursor past the record most recently delivered by
// Next. The new position is fsynced before Ack returns; only then is the
// in-memory position updated. If the persist fails (e.g. disk full, wrapped
// as ErrDiskFull), the cursor keeps its old position and the record will be
// redelivered — Ack may simply be retried.
func (c *Cursor) Ack(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if !c.hasLast {
		return fmt.Errorf("%w: Ack without a delivered record", ErrInvalidArgument)
	}
	next := uint64(c.last.Position) + uint64(framePhysicalSize(len(c.last.Data)))
	st := cursorState{Name: c.name, Next: next}
	if err := saveCursorState(c.store.dir, st); err != nil {
		return err
	}
	c.next.Store(next)
	c.hasLast = false
	return nil
}

// Close releases the cursor's in-process registration and inter-process
// lock. The persisted position remains, so a later OpenCursor resumes where
// the last Ack left off. Close is idempotent.
func (c *Cursor) Close() error {
	c.closeOnce.Do(func() { close(c.done) })

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	c.store.cmu.Lock()
	delete(c.store.cursors, c.name)
	c.store.cmu.Unlock()

	if c.lockFile != nil {
		_ = syscall.Flock(int(c.lockFile.Fd()), syscall.LOCK_UN)
		err := c.lockFile.Close()
		c.lockFile = nil
		if err != nil {
			return fmt.Errorf("closing cursor lock: %w", err)
		}
	}
	return nil
}
