package eventlog

// RetentionGuard is consulted by Compact to learn the lowest position that
// still must be preserved for registered consumers. The floor is the minimum
// over all known cursors (open handles and persisted-but-unopened files), so
// a consumer that crashed mid-processing still pins its records.
type retentionGuard interface {
	// retentionFloor returns the position below which no record may be
	// reclaimed, plus false when no constraint exists.
	retentionFloor() (Position, bool)
}

// attachGuard installs a cursor manager's retention floor. Only one guard can
// be attached at a time; attaching again replaces nothing and returns an
// error rather than letting two registries silently over-pin the log.
func (s *Store) attachGuard(g retentionGuard) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.guard != nil && s.guard != g {
		return ErrInvalidArgument
	}
	s.guard = g
	return nil
}

func (s *Store) detachGuard(g retentionGuard) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.guard == g {
		s.guard = nil
	}
}
