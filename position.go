package eventlog

import "fmt"

// Position is a logical byte offset of a record within the log.
// It survives segment rewriting as long as the record itself survives,
// because rewritten segments keep the same logical offsets.
type Position uint64

// Beginning is the position of the first record in an empty/fresh log.
const Beginning Position = 0

// IsZero reports whether p is the zero (beginning) position.
func (p Position) IsZero() bool { return p == Beginning }

// String renders a position, useful in test/diagnostic output.
func (p Position) String() string { return fmt.Sprintf("pos:%d", uint64(p)) }
