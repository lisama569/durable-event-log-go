package eventlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// CursorAtEnd can be passed at cursor creation to start consuming strictly
// after every record committed at that moment (a "tail" consumer).
const CursorAtEnd Position = ^Position(0)

// CursorInfo is one persisted cursor's summary.
type CursorInfo struct {
	Name    string
	Next    Position
	Updated uint64 // sequence of the record that last moved the cursor
	Backlog uint64 // bytes between cursor position and committed end
	Lagging bool   // next position is below the current retention horizon
}

// Cursor is one named, durable consumption position over a Store.
//
// A cursor only moves on Ack; Next never advances it. Delivery is
// at-least-once: a crash between delivery and Ack redelivers the same record.
// Named cursors are fully independent: acknowledging one never moves another.
type Cursor struct {
	m    *Manager
	s    *Store
	name string

	mu      sync.Mutex
	next    uint64
	updated uint64
	closed  atomic.Bool
}

// cursorEntry is the manager registry row for one persisted cursor.
type cursorEntry struct {
	next       uint64
	updatedSeq uint64
	handle     *Cursor // nil while no live handle is open
}

// Manager owns the named cursors of one Store and enforces a retention floor:
// while attached, compaction is clamped so it never reclaims a position that a
// known cursor still needs. The registry is derived from the cursors/ sidecar
// directory on startup; cursor files are the durable truth.
type Manager struct {
	s   *Store
	dir string

	mu      sync.Mutex
	entries map[string]*cursorEntry
	closed  bool

	// testInjectWriteErr, if non-nil, forces the next durable cursor write to
	// fail as if the disk rejected it. Test-only.
	testInjectWriteErr error
}

// NewManager opens (creating if needed) the cursor registry beside the store.
// Any torn or future-version cursor file makes the failure explicit here with
// ErrCursorStateCorrupt / ErrCursorVersionUnknown; the offending file is kept
// in place and the registry is not partially loaded.
func NewManager(s *Store) (*Manager, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: nil store", ErrCursorInvalidName)
	}
	cdir := filepath.Join(s.dir, cursorDirName)
	if err := os.MkdirAll(cdir, 0o700); err != nil {
		return nil, fmt.Errorf("creating cursor directory: %w", classifyIOError(err))
	}
	if err := syncDir(s.dir); err != nil {
		return nil, err
	}
	m := &Manager{s: s, dir: s.dir, entries: map[string]*cursorEntry{}}

	entries, err := os.ReadDir(cdir)
	if err != nil {
		return nil, fmt.Errorf("listing cursor directory: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if stringsHasSuffix(name, cursorStateTempSuffix) {
			// Leftover of a crashed atomic write; the previous complete file
			// (if any) is intact. Remove the stale temp to keep the dir clean.
			if err := os.Remove(filepath.Join(cdir, name)); err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("removing stale cursor temp %s: %w", name, err)
			}
			continue
		}
		if !stringsHasSuffix(name, cursorFileSuffix) {
			continue
		}
		cursorName := name[:len(name)-len(cursorFileSuffix)]
		st, err := readCursorFile(s.dir, cursorName)
		if err != nil {
			// Refuse to start on a half-new/half-old or future cursor.
			return nil, err
		}
		m.entries[st.Name] = &cursorEntry{next: st.NextPosition, updatedSeq: st.UpdatedSeq}
	}
	if err := s.attachGuard(m); err != nil {
		return nil, fmt.Errorf("attaching retention guard: %w", err)
	}
	return m, nil
}

func stringsHasSuffix(s, suf string) bool {
	return len(s) >= len(suf) && s[len(s)-len(suf):] == suf
}

// retentionFloor implements retentionGuard. Called by Compact while holding
// the store lock; it must therefore only touch manager state. The floor is the
// smallest next-position across ALL known cursors: compaction may reclaim
// strictly below it (already acknowledged by everyone) but never the record a
// lagging cursor is about to read.
func (m *Manager) retentionFloor() (Position, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || len(m.entries) == 0 {
		return 0, false
	}
	floor := ^uint64(0)
	for _, e := range m.entries {
		if e.next < floor {
			floor = e.next
		}
	}
	return Position(floor), true
}

// resolveStartLocked-free helper: call without manager lock (takes store RLocks).
func (m *Manager) resolveStart(start Position) (uint64, error) {
	m.s.mu.RLock()
	defer m.s.mu.RUnlock()
	if m.s.closed {
		return 0, ErrClosed
	}
	switch start {
	case Beginning:
		// Oldest frame the store can still serve; reclaimed prefix is gone.
		return m.s.startPos, nil
	case CursorAtEnd:
		return m.s.writePos, nil
	}
	p := uint64(start)
	if p < m.s.startPos {
		return 0, fmt.Errorf("%w: start %s below retention horizon %s",
			ErrCursorPositionReclaimed, start, Position(m.s.startPos))
	}
	if p > m.s.writePos {
		return 0, fmt.Errorf("%w: start %s beyond committed end %s",
			ErrInvalidArgument, start, Position(m.s.writePos))
	}
	if p != m.s.writePos {
		sg, err := m.s.findSegmentLocked(p)
		if err != nil {
			return 0, err
		}
		if !m.s.isFrameStartLocked(sg, p) {
			return 0, fmt.Errorf("%w: start %s is inside a frame", ErrOffsetNotAligned, start)
		}
	}
	return p, nil
}

// Create makes a new named cursor. Beginning starts at the oldest retained
// record, CursorAtEnd starts strictly after the current committed end, and an
// explicit Position must be a record boundary still present.
func (m *Manager) Create(ctx context.Context, name string, start Position) (*Cursor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateCursorName(name); err != nil {
		return nil, err
	}
	startOff, err := m.resolveStart(start)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrCursorClosed
	}
	if _, exists := m.entries[name]; exists {
		return nil, fmt.Errorf("%w: cursor %q", ErrCursorExists, name)
	}
	st := cursorState{Name: name, NextPosition: startOff}
	if err := atomicWriteCursorFile(m.dir, name, st); err != nil {
		return nil, err
	}
	m.entries[name] = &cursorEntry{next: startOff}
	return m.openLocked(name), nil
}

// openLocked builds a handle from an existing registry row.
func (m *Manager) openLocked(name string) *Cursor {
	e := m.entries[name]
	if e.handle != nil {
		return e.handle
	}
	c := &Cursor{m: m, s: m.s, name: name, next: e.next, updated: e.updatedSeq}
	e.handle = c
	return c
}

// Open returns an already-persisted cursor. A missing cursor yields
// ErrCursorNotFound; a torn/future state yields its specific error.
func (m *Manager) Open(ctx context.Context, name string) (*Cursor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateCursorName(name); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrCursorClosed
	}
	if _, ok := m.entries[name]; ok {
		return m.openLocked(name), nil
	}
	// Defensive re-read; NewManager refuses a directory containing an
	// unparseable cursor file, so reaching here genuinely means unknown.
	st, err := readCursorFile(m.dir, name)
	if err != nil {
		return nil, err
	}
	m.entries[name] = &cursorEntry{next: st.NextPosition, updatedSeq: st.UpdatedSeq}
	return m.openLocked(name), nil
}

// OpenOrCreate opens the cursor or creates it at start.
func (m *Manager) OpenOrCreate(ctx context.Context, name string, start Position) (*Cursor, error) {
	c, err := m.Open(ctx, name)
	if err == nil {
		return c, nil
	}
	if !errors.Is(err, ErrCursorNotFound) {
		return nil, err
	}
	return m.Create(ctx, name, start)
}

// List returns summaries of all persisted cursors in name order.
func (m *Manager) List(ctx context.Context) ([]CursorInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.s.mu.RLock()
	startPos := m.s.startPos
	writePos := m.s.writePos
	closed := m.s.closed
	m.s.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrCursorClosed
	}
	if closed {
		return nil, ErrClosed
	}
	names := make([]string, 0, len(m.entries))
	for n := range m.entries {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]CursorInfo, 0, len(names))
	for _, n := range names {
		e := m.entries[n]
		info := CursorInfo{
			Name: n, Next: Position(e.next), Updated: e.updatedSeq,
			Lagging: e.next < startPos,
		}
		if e.next < writePos {
			info.Backlog = writePos - e.next
		}
		out = append(out, info)
	}
	return out, nil
}

// Delete removes a cursor's persisted state. A live handle must be closed
// first; deleting an open cursor is rejected so an in-flight consumer cannot
// lose its progress out from under itself.
func (m *Manager) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateCursorName(name); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrCursorClosed
	}
	e, ok := m.entries[name]
	if !ok {
		return fmt.Errorf("%w: cursor %q", ErrCursorNotFound, name)
	}
	if e.handle != nil && !e.handle.closed.Load() {
		return fmt.Errorf("%w: cursor %q is open; close it before deleting",
			ErrInvalidArgument, name)
	}
	if err := os.Remove(filepath.Join(m.dir, cursorDirName, cursorFileName(name))); err != nil &&
		!os.IsNotExist(err) {
		return fmt.Errorf("deleting cursor %q: %w", name, classifyIOError(err))
	}
	if err := syncDir(filepath.Join(m.dir, cursorDirName)); err != nil {
		return err
	}
	delete(m.entries, name)
	return nil
}

// Detach removes the retention floor. Open cursors remain usable (and still
// durable) until closed, but compaction no longer waits on any of them.
func (m *Manager) Detach() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	// Existing handles keep working: Cursor only consults the manager for
	// persistence, which is still valid. New Create/Open/Delete calls fail, so
	// the registry cannot grow after the floor is gone.
	m.s.detachGuard(m)
	return nil
}

// Close detaches the manager AND closes every cursor handle, waking parked
// Next calls with ErrCursorClosed. Use Detach when cursors must keep running
// without the retention floor.
func (m *Manager) Close() error {
	m.mu.Lock()
	m.closed = true
	handles := make([]*Cursor, 0, len(m.entries))
	for _, e := range m.entries {
		if e.handle != nil {
			handles = append(handles, e.handle)
		}
	}
	m.mu.Unlock()
	m.s.detachGuard(m)
	for _, c := range handles {
		c.markClosed()
	}
	m.s.hub.notify()
	return nil
}

// Name returns the cursor name.
func (c *Cursor) Name() string { return c.name }

func (c *Cursor) cursorLagError(horizon uint64) error {
	return &CursorError{
		Name: c.name, Position: Position(c.next), Kind: "reclaimed",
		Err: fmt.Errorf("%w (%w): next position %d is below retention horizon %d; "+
			"records this cursor has not acknowledged were reclaimed",
			ErrCursorPositionReclaimed, ErrPositionReclaimed, c.next, horizon),
	}
}

// readNextAtLocked returns the record at explicit position at while the
// caller holds the store read lock. found=false with err==nil means "at the
// committed end".
func (c *Cursor) readNextAtLocked(at uint64) (rec Record, found bool, horizon uint64, err error) {
	if at < c.s.startPos {
		return Record{}, false, c.s.startPos, c.cursorLagError(c.s.startPos)
	}
	sg, ferr := c.s.findSegmentLocked(at)
	if ferr != nil {
		if errors.Is(ferr, ErrEndOfLog) {
			return Record{}, false, c.s.startPos, nil
		}
		return Record{}, false, c.s.startPos, ferr
	}
	if !c.s.isFrameStartLocked(sg, at) {
		return Record{}, false, c.s.startPos,
			fmt.Errorf("%w: cursor %q points inside a frame at %d",
				ErrOffsetNotAligned, c.name, at)
	}
	f, oerr := os.Open(sg.path)
	if oerr != nil {
		return Record{}, false, c.s.startPos, fmt.Errorf("opening %s: %w", sg.path, oerr)
	}
	defer f.Close()
	fr, _, derr := readFrameAt(f, sg.physicalOffset(at))
	if derr != nil {
		return Record{}, false, c.s.startPos, &CorruptionError{
			Path: sg.path, Offset: sg.physicalOffset(at),
			Kind: classifyDecodeError(derr), Err: derr}
	}
	return Record{Position: Position(at), Sequence: fr.sequence, Data: fr.payload},
		true, c.s.startPos, nil
}

// Next delivers the record at the cursor's current position without advancing
// it. When the cursor is at the committed end it parks (no polling) until an
// append wakes it, the context deadline/cancel fires, or the store closes.
func (c *Cursor) Next(ctx context.Context) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	for {
		if c.closed.Load() {
			return Record{}, errors.Join(ErrCursorClosed, ErrClosed)
		}
		c.mu.Lock()
		cnext := c.next
		c.mu.Unlock()

		c.s.mu.RLock()
		if c.s.closed {
			c.s.mu.RUnlock()
			return Record{}, ErrClosed
		}
		// Register while holding the read lock: an append cannot both commit
		// and notify between our check and parking.
		ch := c.s.hub.register()
		rec, found, _, err := c.readNextAtLocked(cnext)
		if err != nil || found {
			c.s.hub.unregister(ch)
			c.s.mu.RUnlock()
			if err != nil {
				return Record{}, err
			}
			return rec, nil
		}
		c.s.mu.RUnlock()

		select {
		case <-ch:
			// Re-check on the next loop iteration.
		case <-ctx.Done():
			c.s.hub.unregister(ch)
			return Record{}, ctx.Err()
		}
	}
}

// advanceLocked computes the position and sequence after acknowledging pos and
// validates that pos is a deliverable frame. Caller holds no store lock.
func (s *Store) advanceInfo(pos uint64) (nextPos uint64, seq uint64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, 0, ErrClosed
	}
	if pos < s.startPos {
		return 0, 0, fmt.Errorf("%w: acknowledge %s below retention horizon %s",
			ErrCursorPositionReclaimed, Position(pos), Position(s.startPos))
	}
	sg, ferr := s.findSegmentLocked(pos)
	if ferr != nil {
		return 0, 0, ferr
	}
	if !s.isFrameStartLocked(sg, pos) {
		return 0, 0, fmt.Errorf("%w: acknowledge %s is inside a frame", ErrOffsetNotAligned, Position(pos))
	}
	f, oerr := os.Open(sg.path)
	if oerr != nil {
		return 0, 0, fmt.Errorf("opening %s: %w", sg.path, oerr)
	}
	defer f.Close()
	fr, _, derr := readFrameAt(f, sg.physicalOffset(pos))
	if derr != nil {
		return 0, 0, &CorruptionError{Path: sg.path, Offset: sg.physicalOffset(pos),
			Kind: classifyDecodeError(derr), Err: derr}
	}
	if nxt, ok := s.frameStartAtOrAfterLocked(pos + 1); ok {
		return nxt, fr.sequence, nil
	}
	return s.writePos, fr.sequence, nil
}

// ack commits a new next-position if it is strictly ahead of the current one,
// persisting before touching any in-memory state.
func (c *Cursor) ack(ctx context.Context, pos Position) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return errors.Join(ErrCursorClosed, ErrClosed)
	}
	p := uint64(pos)
	if p < c.next {
		// Idempotent re-acknowledgement of an already-consumed record.
		return nil
	}
	if p > c.next {
		// Would silently skip records the cursor has not acknowledged.
		return fmt.Errorf("%w: cursor %q cannot ack %s ahead of next %s",
			ErrInvalidArgument, c.name, pos, Position(c.next))
	}
	newNext, seq, err := c.s.advanceInfo(p)
	if err != nil {
		return err
	}
	st := cursorState{Name: c.name, NextPosition: newNext, UpdatedSeq: seq}
	if c.m.testInjectWriteErr != nil {
		return c.m.testInjectWriteErr
	}
	if err := atomicWriteCursorFile(c.m.dir, c.name, st); err != nil {
		// Memory is deliberately NOT advanced: after ENOSPC/torn write the
		// cursor still matches the last complete on-disk state.
		return err
	}
	c.next = newNext
	c.updated = seq
	c.m.mu.Lock()
	if e, ok := c.m.entries[c.name]; ok {
		e.next = newNext
		e.updatedSeq = seq
	}
	c.m.mu.Unlock()
	return nil
}

// Ack commits the cursor past r: the next Next returns the following record
// (or waits at the end). It is durable before it returns.
func (c *Cursor) Ack(ctx context.Context, r Record) error {
	return c.ack(ctx, r.Position)
}

// AckPosition is Ack for callers that kept only the position.
func (c *Cursor) AckPosition(ctx context.Context, p Position) error {
	return c.ack(ctx, p)
}

// Position returns the cursor's current next-to-deliver position.
func (c *Cursor) Position() Position {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Position(c.next)
}

// UpdatedSequence returns the sequence of the last acknowledged record.
func (c *Cursor) UpdatedSequence() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.updated
}

func (c *Cursor) markClosed() {
	c.closed.Store(true)
}

// Close releases this handle without deleting its persisted state. It is
// idempotent. Parked Next calls are woken and fail with ErrCursorClosed.
func (c *Cursor) Close() error {
	was := c.closed.Swap(true)
	if !was {
		c.m.mu.Lock()
		if e, ok := c.m.entries[c.name]; ok && e.handle == c {
			e.handle = nil
		}
		c.m.mu.Unlock()
		c.s.hub.notify()
	}
	return nil
}
