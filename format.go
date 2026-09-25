package eventlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// On-disk format. All integers are big-endian.
//
// Segment file layout:
//
//	[ segment header: magic(4) version(1) baseOffset(8) dataStart(8) headerCRC(4) ]  // 25 bytes
//	dataStart is the logical offset of the first frame still present; after
//	prefix compaction it is greater than base and the gap is a sparse hole.
//	[ frame ][ frame ] ...
//
// Frame layout:
//
//	magic(4) "ELOG" | version(1) | flags(1) | sequence(8) |
//	payloadLen(4) | payload(payloadLen) | crc32(4)
//
// crc32 is the IEEE checksum of every byte preceding it.
const (
	magic0 byte = 'E'
	magic1 byte = 'L'
	magic2 byte = 'O'
	magic3 byte = 'G'

	formatV1 uint8 = 1

	currentFormatVersion = formatV1

	segmentHeaderSize = 4 + 1 + 8 + 8 + 4 // magic + version + base + dataStart + crc
	frameHeaderSize   = 4 + 1 + 1 + 8 + 4
	crcSize           = 4

	// maxPayloadLen guards against wild length fields produced by garbage.
	maxPayloadLen = 64 << 20 // 64 MiB
)

var frameMagic = [4]byte{magic0, magic1, magic2, magic3}

// frame is the in-memory view of one framed record.
type frame struct {
	version  uint8
	flags    uint8
	sequence uint64
	payload  []byte
}

var (
	errBadMagic       = errors.New("bad magic")
	errFrameLength    = errors.New("declared payload length exceeds limit")
	errFrameCRC       = errors.New("frame crc mismatch")
	errHeaderCRC      = errors.New("segment header crc mismatch")
	errTruncatedFrame = fmt.Errorf("%w: not enough bytes for frame", ErrIncompleteTail)
	errTruncatedHead  = fmt.Errorf("%w: not enough bytes for segment header", ErrIncompleteTail)
)

func putBE64(b []byte, v uint64) { binary.BigEndian.PutUint64(b, v) }
func putBE32(b []byte, v uint32) { binary.BigEndian.PutUint32(b, v) }

// encodeFrame appends one frame to dst.
func encodeFrame(dst []byte, f frame) []byte {
	pLen := len(f.payload)
	total := frameHeaderSize + pLen + crcSize
	out := append(dst, make([]byte, total)...)
	base := len(out) - total
	out[base+0] = magic0
	out[base+1] = magic1
	out[base+2] = magic2
	out[base+3] = magic3
	out[base+4] = currentFormatVersion
	out[base+5] = f.flags
	putBE64(out[base+6:base+14], f.sequence)
	putBE32(out[base+14:base+18], uint32(pLen))
	copy(out[base+18:base+18+pLen], f.payload)
	crc := crc32.ChecksumIEEE(out[base : base+frameHeaderSize+pLen])
	putBE32(out[base+frameHeaderSize+pLen:], crc)
	return out
}

// decodeFrame decodes exactly one frame from the start of buf.
// n is the total encoded size including the CRC.
func decodeFrame(buf []byte) (f frame, n int, err error) {
	if len(buf) < frameHeaderSize {
		return frame{}, 0, errTruncatedFrame
	}
	if buf[0] != magic0 || buf[1] != magic1 || buf[2] != magic2 || buf[3] != magic3 {
		return frame{}, 0, errBadMagic
	}
	f.version = buf[4]
	if f.version != currentFormatVersion {
		// Future versions are rejected as such, not mistaken for corruption.
		return frame{}, 0, fmt.Errorf("%w: frame version %d", ErrUnknownVersion, f.version)
	}
	f.flags = buf[5]
	f.sequence = binary.BigEndian.Uint64(buf[6:14])
	pLen := binary.BigEndian.Uint32(buf[14:18])
	if uint64(pLen) > maxPayloadLen {
		return frame{}, 0, errFrameLength
	}
	total := frameHeaderSize + int(pLen) + crcSize
	if len(buf) < total {
		return frame{}, 0, errTruncatedFrame
	}
	storedCRC := binary.BigEndian.Uint32(buf[total-crcSize : total])
	wantCRC := crc32.ChecksumIEEE(buf[:total-crcSize])
	if storedCRC != wantCRC {
		return frame{}, 0, errFrameCRC
	}
	f.payload = make([]byte, pLen)
	copy(f.payload, buf[frameHeaderSize:frameHeaderSize+pLen])
	return f, total, nil
}

// encodeSegmentHeader appends a segment header to dst. dataStart is the
// logical offset of the first frame present in the file (== base unless the
// prefix was reclaimed).
func encodeSegmentHeader(dst []byte, version uint8, base, dataStart uint64) []byte {
	out := append(dst, make([]byte, segmentHeaderSize)...)
	b := len(out) - segmentHeaderSize
	copy(out[b:b+4], frameMagic[:])
	out[b+4] = version
	putBE64(out[b+5:b+13], base)
	putBE64(out[b+13:b+21], dataStart)
	crc := crc32.ChecksumIEEE(out[b : b+21])
	putBE32(out[b+21:b+25], crc)
	return out
}

// decodeSegmentHeader validates the header and returns base and dataStart.
func decodeSegmentHeader(buf []byte) (version uint8, base, dataStart uint64, err error) {
	if len(buf) < segmentHeaderSize {
		return 0, 0, 0, errTruncatedHead
	}
	if string(buf[0:4]) != string(frameMagic[:]) {
		return 0, 0, 0, errBadMagic
	}
	version = buf[4]
	if version == 0 || version > currentFormatVersion {
		return 0, 0, 0, fmt.Errorf("%w: segment version %d", ErrUnknownVersion, version)
	}
	stored := binary.BigEndian.Uint32(buf[21:25])
	want := crc32.ChecksumIEEE(buf[:21])
	if stored != want {
		return 0, 0, 0, errHeaderCRC
	}
	base = binary.BigEndian.Uint64(buf[5:13])
	dataStart = binary.BigEndian.Uint64(buf[13:21])
	if dataStart < base {
		return 0, 0, 0, fmt.Errorf("%w: dataStart %d before base %d",
			ErrRecoveryConflict, dataStart, base)
	}
	return version, base, dataStart, nil
}

// framePhysicalSize is the total on-disk size of a frame with payloadLen bytes.
func framePhysicalSize(payloadLen int) int64 {
	return int64(frameHeaderSize + payloadLen + crcSize)
}
