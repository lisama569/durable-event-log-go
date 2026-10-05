# durable-event-log-go

An embeddable, crash-safe local append log for Go. Records are appended to
segmented, checksummed files; every acknowledged write is fsynced before it
is visible. On top of raw append/read/replay, the package offers **named
consumer cursors** with durable progress and real-time waiting.

## Core log semantics

- `Open(dir, opts)` recovers or initializes a log. Torn tails, missing or
  conflicting metadata are either repaired (`Options.Repair`) or refused with
  a diagnosable error (`ErrRecoveryConflict`, `ErrIncompleteTail`,
  `ErrUnknownVersion`).
- `Append(ctx, data)` returns a logical `Position` after the frame and its
  segment are fsynced. Positions are stable across compaction.
- `Read(ctx, pos)` reads exactly one record; `Replay(ctx, start, fn)` scans
  in order. A compacted position fails with `ErrPositionReclaimed`, an
  offset inside a frame with `ErrOffsetNotAligned`, the committed end with
  `ErrEndOfLog`.
- `Compact(ctx, CompactOptions{Before|KeepBytes})` reclaims space online
  (whole-segment deletes plus one prefix rewrite). Readers that already
  started keep working; surviving positions never move.
- Capacity: `Options.MaxBytes` is checked before any write; exceeding it
  fails with `ErrCapacityExceeded`. ENOSPC surfaces as `ErrDiskFull`.
- Versions: segment frames, the meta sidecar and cursor state files are all
  versioned. Older versions are accepted/upgraded; newer-than-supported
  versions are refused with `ErrUnknownVersion`, never misread.

## Consumer cursors (subscription layer)

A cursor is a **named, durable consumer position** over a store:

```go
c, err := s.OpenCursor("billing-worker")   // create or resume
rec, err := c.Next(ctx)                    // deliver record at cursor
// ... process rec ...
err = c.Ack(ctx)                           // durably advance past rec
```

### Semantics

- **Named and independent.** Each name has its own persisted position;
  cursor `a` never affects cursor `b`. A name has a single active consumer:
  opening it twice in one process, or while another process holds it
  (advisory `flock`), fails with `ErrCursorInUse`. `Close` releases the name.
- **At-least-once delivery.** `Next` delivers the record at the cursor but
  does not move it. Only a successful `Ack` advances the position, and the
  new position is written to a temp file, fsynced, atomically renamed and
  the directory fsynced **before** `Ack` returns. A crash between `Next` and
  `Ack` therefore redelivers the record after restart — duplicates are
  possible, consumers must be idempotent — but an acked record is never
  silently skipped.
- **Real-time waiting without polling.** When the cursor is caught up,
  `Next` blocks on a generation channel that is closed by every committed
  append, by compaction, and by `Close`. There is no busy loop. Timeouts and
  cancellation surface as `ctx.Err()`; `Cursor.Close` from another goroutine
  unblocks a waiting `Next` with `ErrClosed`.
- **New cursors** start at the oldest retained record (the current
  retention horizon), so a cursor can never be born "already reclaimed".

### Cursors vs. compaction

Compaction proceeds regardless of cursor positions (it never blocks on
consumers), but a lagging cursor is never silently skipped:

- If the cursor's next position was reclaimed, `Next` fails with an error
  wrapping `ErrPositionReclaimed`, naming the position and the retained
  start. It is never reported as end-of-log and never reads a different
  record.
- Compaction wakes blocked readers, so a consumer waiting through a
  compaction is never stuck.
- Operators can inspect `s.Cursors()` (name + next position of every cursor
  open in this process) before choosing a `Compact` cutoff to avoid
  outrunning consumers.

### Cursor state crash safety and versioning

Cursor state lives in `cursor-<name>.json` next to the segments, written
with the same temp-file + fsync + rename + directory-fsync protocol as the
meta sidecar. A crash at any point leaves either the old or the new complete
state:

- A torn, empty, or unparseable state file is **refused with a diagnosable
  error** (`ErrCorruptData` with the path and cause) — never silently reset
  to zero. A leftover `cursor-<name>.json.tmp` from an interrupted write is
  ignored.
- `cursor_version` older than the binary's version is accepted and upgraded
  on the next save; a newer version is refused with `ErrUnknownVersion`.
- If `Ack`'s persist fails (e.g. disk full, wrapped as `ErrDiskFull`), the
  in-memory position is left unchanged and the record will be redelivered;
  the caller may retry `Ack`.

## Verifying locally

```sh
go test ./...                 # whole suite: write/replay/compact/recovery/cursors
go test -race ./...
go test -v -run TestCursor ./...   # cursor tests with position/decision logs
```

The cursor tests log every consumed position and the reason for each
outcome: restart recovery (`TestCursorProgressSurvivesRestart`),
at-least-once redelivery (`TestCursorRedeliversUnackedAfterRestart`),
real-time wake-up (`TestCursorRealtimeWaitWakesOnAppend`), timeout/cancel
(`TestCursorNextTimeoutAndCancel`), lagging cursor vs. compaction
(`TestCursorLaggingBehindCompaction`), waiting through concurrent
compaction (`TestCursorWaitSurvivesConcurrentCompaction`), and cursor-state
crash safety including torn/unknown-version files
(`TestCursorStateCrashSafety`).
