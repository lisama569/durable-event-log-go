package eventlog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Metadata versioning:
//   metaMetaV1 is the only written version. Older versions (currently none
//   exist below 1) would be accepted with downgrade handling; versions newer
//   than currentMetaVersion are refused with ErrUnknownVersion so that a
//   downgraded binary never overwrites data it cannot understand.
const (
	currentMetaVersion uint8 = 1
	metaFileName             = "meta.json"
	metaTempName             = "meta.json.tmp"
)

// meta is the versioned, atomically-replaced sidecar metadata.
type meta struct {
	MetaVersion   uint8         `json:"meta_version"`
	FormatVersion uint8         `json:"format_version"`
	NextSequence  uint64        `json:"next_sequence"`
	ActiveBase    uint64        `json:"active_base"`
	Segments      []metaSegment `json:"segments"`
}

type metaSegment struct {
	Base uint64 `json:"base"`
	Size int64  `json:"size"`
}

// loadMeta reads the sidecar. A missing file is not an error: callers treat
// that as a bootstrap situation and derive state from the segment files.
func loadMeta(dir string) (meta, bool, error) {
	path := filepath.Join(dir, metaFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return meta{}, false, nil
		}
		return meta{}, false, fmt.Errorf("reading %s: %w", path, err)
	}
	var m meta
	if len(data) == 0 {
		// An empty meta file means an incomplete (torn) write of the sidecar.
		return meta{}, false, fmt.Errorf("%w: %s is empty (incomplete metadata write)", ErrRecoveryConflict, path)
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return meta{}, false, fmt.Errorf("%w: %s is not valid metadata: %v", ErrRecoveryConflict, path, err)
	}
	if m.MetaVersion == 0 {
		return meta{}, false, fmt.Errorf("%w: %s has meta_version 0 (incomplete metadata)", ErrRecoveryConflict, path)
	}
	if m.MetaVersion > currentMetaVersion {
		return meta{}, false, fmt.Errorf("%w: meta_version=%d, binary supports up to %d",
			ErrUnknownVersion, m.MetaVersion, currentMetaVersion)
	}
	if m.FormatVersion == 0 || m.FormatVersion > currentFormatVersion {
		return meta{}, false, fmt.Errorf("%w: format_version=%d, binary supports up to %d",
			ErrUnknownVersion, m.FormatVersion, currentFormatVersion)
	}
	return m, true, nil
}

// atomicWriteMeta writes to a temp file, fsyncs it and renames over the old
// one. The directory itself is fsynced so the rename is durable.
func atomicWriteMeta(dir string, m meta) error {
	m.MetaVersion = currentMetaVersion
	if m.FormatVersion == 0 {
		m.FormatVersion = currentFormatVersion
	}
	data, err := json.MarshalIndent(&m, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding metadata: %w", err)
	}
	tmp := filepath.Join(dir, metaTempName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("writing %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("fsyncing %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("closing %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, metaFileName)); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("renaming metadata: %w", err)
	}
	// fsync the directory entry so the rename survives a crash.
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("opening directory for fsync: %w", err)
	}
	err = d.Sync()
	_ = d.Close()
	if err != nil {
		return fmt.Errorf("fsyncing directory: %w", err)
	}
	return nil
}
