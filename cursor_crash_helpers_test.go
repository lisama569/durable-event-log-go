package eventlog

import (
	"bytes"
	"encoding/json"
	"hash/crc32"
)

func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// reencodeWithVersion takes a valid encoded cursor blob and rewrites its
// cursor_version byte, recomputing the CRC so the only rejection reason is the
// unsupported version (rather than a checksum mismatch).
func reencodeWithVersion(encoded []byte, version uint8) []byte {
	var st cursorState
	_ = json.Unmarshal(encoded, &st)
	canon, _ := json.Marshal(cursorCanon{
		CursorVersion: version,
		Name:          st.Name,
		NextPosition:  st.NextPosition,
		UpdatedSeq:    st.UpdatedSeq,
	})
	st.CursorVersion = version
	st.CRC32 = crc32.ChecksumIEEE(canon)
	out, _ := json.MarshalIndent(&st, "", "  ")
	return out
}

func bytesIndex(s, sep []byte) int { return bytes.Index(s, sep) }

func contains(s, sub string) bool { return bytes.Contains([]byte(s), []byte(sub)) }
