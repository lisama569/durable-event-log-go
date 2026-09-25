package logstore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// 记录帧格式（格式版本 1）：
//
//	magic   uint32 固定魔数，用于识别帧边界与错位数据
//	version uint16 记录格式版本
//	flags   uint16 保留
//	length  uint32 payload 字节数
//	crc32   uint32 payload 的 CRC32(IEEE)
//	payload []byte
const (
	recordMagic     uint32 = 0x4C4F4731 // "LOG1"
	recordVersionV1 uint16 = 1
	headerSize             = 16
)

// encodeRecord 将 payload 编码为一帧。
func encodeRecord(payload []byte) []byte {
	buf := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], recordMagic)
	binary.LittleEndian.PutUint16(buf[4:6], recordVersionV1)
	binary.LittleEndian.PutUint16(buf[6:8], 0) // flags
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[12:16], crc32.ChecksumIEEE(payload))
	copy(buf[headerSize:], payload)
	return buf
}

// decodeHeader 解析帧头；返回 payload 长度与 CRC。
// 错误约定：
//   - io.EOF：buf 为空，表示干净结尾；
//   - ErrTruncated：头不完整（崩溃截断）；
//   - ErrCorrupt：魔数或版本非法（数据错位/损坏）。
func decodeHeader(buf []byte) (payloadLen uint32, crc uint32, err error) {
	if len(buf) == 0 {
		return 0, 0, io.EOF
	}
	if len(buf) < headerSize {
		return 0, 0, ErrTruncated
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != recordMagic {
		return 0, 0, ErrCorrupt
	}
	if v := binary.LittleEndian.Uint16(buf[4:6]); v != recordVersionV1 {
		return 0, 0, ErrCorrupt
	}
	return binary.LittleEndian.Uint32(buf[8:12]),
		binary.LittleEndian.Uint32(buf[12:16]), nil
}

// verifyPayload 校验 payload 的 CRC。
func verifyPayload(payload []byte, crc uint32) error {
	if crc32.ChecksumIEEE(payload) != crc {
		return ErrCorrupt
	}
	return nil
}

// readRecordAt 从 r 的指定位置读取一条完整记录。
// 用于恢复扫描与随机读取，错误语义同 decodeHeader。
func readRecordAt(r io.ReaderAt, off int64) (payload []byte, total int64, err error) {
	hdr := make([]byte, headerSize)
	n, err := r.ReadAt(hdr, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, 0, err
	}
	payloadLen, crc, err := decodeHeader(hdr[:n])
	if err != nil {
		return nil, 0, err
	}
	payload = make([]byte, payloadLen)
	m, err := r.ReadAt(payload, off+headerSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, 0, err
	}
	if m < int(payloadLen) {
		return nil, 0, ErrTruncated
	}
	if err := verifyPayload(payload, crc); err != nil {
		return nil, 0, err
	}
	return payload, int64(headerSize) + int64(payloadLen), nil
}
