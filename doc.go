// Package eventlog implements an embeddable, crash-safe local append log with
// named, durable consumer cursors.
//
// The Store layer provides crash-safe append, positional read, sequential
// replay and online compaction. The cursor layer (Manager, Cursor) adds:
//   - multiple independent named consumption positions that only advance on
//     explicit Ack and survive process restarts (at-least-once delivery);
//   - blocking Next with edge-triggered wake-up on new appends (no polling),
//     plus context timeout/cancel and close support;
//   - a retention floor: while a Manager is attached, compaction is clamped so
//     unacknowledged data is never silently deleted; after Detach, a cursor that
//     fell behind fails with ErrCursorPositionReclaimed instead of skipping or
//     waiting;
//   - versioned, CRC-protected cursor state written temp-file+fsync+rename, so
//     a crash, ENOSPC or interruption leaves either the previous complete
//     state or the new one, never a half-advanced cursor.
//
// Detailed semantics are documented in README.md.
package eventlog
