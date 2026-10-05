package eventlog

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
)

// Cursor state versioning. Persisted cursor files use their own version
// namespace, independent of the segment/meta format version:
//   - versions in [1, currentCursorVersion] are accepted and upgraded to the
//     current version on the next acknowledged write;
//   - newer versions are refused with ErrCursorVersionUnknown and the file is
//     never overwritten, so a downgraded binary cannot clobber new state;
//   - version 0 and missing/zero values are treated as an incomplete
//     (torn/garbage) write, i.e. ErrCursorStateCorrupt.
const (
	cursorV1              uint8 = 1
	currentCursorVersion        = cursorV1
	cursorDirName               = "cursors"
	cursorFileSuffix            = ".cursor.json"
	cursorStateTempSuffix       = ".tmp"
)

// cursorState is the on-disk envelope of one named cursor. Every semantic
// field is covered by CRC32: a torn write, a partial edit or flipped bytes
// fail verification on the next open instead of silently resetting progress.
type cursorState struct {
	CursorVersion uint8  `json:"cursor_version"`
	Name          string `json:"name"`
	NextPosition  uint64 `json:"next_position"`
	UpdatedSeq    uint64 `json:"updated_sequence"`
	CRC32         uint32 `json:"crc32"`
}

// cursorCanon is the fixed-order, CRC-covered projection of cursorState.
type cursorCanon struct {
	CursorVersion uint8  `json:"cursor_version"`
	Name          string `json:"name"`
	NextPosition  uint64 `json:"next_position"`
	UpdatedSeq    uint64 `json:"updated_sequence"`
}

func canonOf(st cursorState) []byte {
	b, _ := json.Marshal(cursorCanon{
		CursorVersion: st.CursorVersion,
		Name:          st.Name,
		NextPosition:  st.NextPosition,
		UpdatedSeq:    st.UpdatedSeq,
	})
	return b
}

// cursorFileName maps a validated cursor name to its sidecar file name.
func cursorFileName(name string) string {
	return name + cursorFileSuffix
}

func cursorTempFileName(name string) string {
	return name + cursorFileSuffix + cursorStateTempSuffix
}

// validateCursorName rejects names that could escape the cursors directory or
// collide with internal temp naming.
func validateCursorName(name string) error {
	if name == "" || len(name) > 200 {
		return fmt.Errorf("%w: name must be 1..200 bytes", ErrCursorInvalidName)
	}
	if name == "." || name == ".." || strings.HasSuffix(name, cursorFileSuffix) ||
		strings.Contains(name, cursorStateTempSuffix) {
		return fmt.Errorf("%w: reserved name %q", ErrCursorInvalidName, name)
	}
	if strings.ContainsAny(name, `/\`+"\x00") {
		return fmt.Errorf("%w: name %q contains a path separator or NUL",
			ErrCursorInvalidName, name)
	}
	return nil
}

// encodeCursorState stamps the current version and a checksum and serializes.
func encodeCursorState(st cursorState) ([]byte, error) {
	st.CursorVersion = currentCursorVersion
	st.CRC32 = 0
	st.CRC32 = crc32.ChecksumIEEE(canonOf(st))
	data, err := json.MarshalIndent(&st, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encoding cursor state: %w", err)
	}
	return data, nil
}

// decodeCursorState parses, version-checks and verifies one cursor state.
// It never returns a "half" state: any structural problem is an error and the
// caller keeps the file untouched for diagnosis.
func decodeCursorState(data []byte) (cursorState, error) {
	if len(data) == 0 {
		return cursorState{}, fmt.Errorf("%w: empty file (incomplete cursor write)",
			ErrCursorStateCorrupt)
	}
	var st cursorState
	if err := json.Unmarshal(data, &st); err != nil {
		return cursorState{}, fmt.Errorf("%w: not valid cursor state: %v",
			ErrCursorStateCorrupt, err)
	}
	if st.CursorVersion == 0 {
		return cursorState{}, fmt.Errorf("%w: cursor_version 0 (incomplete write)",
			ErrCursorStateCorrupt)
	}
	if st.CursorVersion > currentCursorVersion {
		return cursorState{}, fmt.Errorf("%w: cursor_version=%d, binary supports up to %d",
			ErrCursorVersionUnknown, st.CursorVersion, currentCursorVersion)
	}
	want := crc32.ChecksumIEEE(canonOf(st))
	if want != st.CRC32 {
		return cursorState{}, fmt.Errorf("%w: checksum mismatch (stored=%d computed=%d); "+
			"file was torn or modified outside the eventlog",
			ErrCursorStateCorrupt, st.CRC32, want)
	}
	if st.Name == "" {
		return cursorState{}, fmt.Errorf("%w: empty cursor name", ErrCursorStateCorrupt)
	}
	return st, nil
}

// atomicWriteCursorFile replaces one cursor file crash-safely: temp file +
// fsync + atomic rename + directory fsync. A crash, ENOSPC or interruption at
// any point leaves either the previous complete file or the new complete one.
func atomicWriteCursorFile(dir, name string, st cursorState) error {
	if err := validateCursorName(name); err != nil {
		return err
	}
	data, err := encodeCursorState(st)
	if err != nil {
		return err
	}
	final := filepath.Join(dir, cursorDirName, cursorFileName(name))
	tmp := filepath.Join(dir, cursorDirName, cursorTempFileName(name))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating cursor temp %s: %w", tmp, classifyIOError(err))
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("writing cursor state for %q: %w", name, classifyIOError(err))
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("fsyncing cursor state for %q: %w", name, classifyIOError(err))
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("closing cursor state for %q: %w", name, classifyIOError(err))
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("renaming cursor state for %q: %w", name, classifyIOError(err))
	}
	if err := syncDir(filepath.Join(dir, cursorDirName)); err != nil {
		return err
	}
	return nil
}

// readCursorFile loads and verifies one cursor sidecar.
func readCursorFile(dir, name string) (cursorState, error) {
	path := filepath.Join(dir, cursorDirName, cursorFileName(name))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cursorState{}, fmt.Errorf("%w: cursor %q", ErrCursorNotFound, name)
		}
		return cursorState{}, fmt.Errorf("reading %s: %w", path, err)
	}
	st, err := decodeCursorState(data)
	if err != nil {
		return cursorState{}, &CursorError{Name: name, Kind: "state", Err: err}
	}
	return st, nil
}
