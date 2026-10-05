// Package eventlog implements an embeddable, crash-safe local append log
// with named, durable consumer cursors on top.
//
// The log itself offers append, positional read, ordered replay and online
// compaction. Cursors (Store.OpenCursor) add at-least-once subscription
// semantics: durable per-name progress, blocking Next with timeout/cancel,
// and explicit ErrPositionReclaimed when compaction outruns a consumer.
//
// Detailed semantics are documented in README.md.
package eventlog
