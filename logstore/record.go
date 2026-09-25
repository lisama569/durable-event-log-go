package logstore

import "io"

// 记录帧格式（格式版本 1）：
//   magic   uint32 固定魔数，用于识别帧边界与错位数据
//   version uint16 记录格式版本
//   flags   uint16 保留
//   length  uint32 payload 字节数
//   crc32   uint32 payload 的 CRC32(IEEE)
//   payload []byte
const (
	recordMagic     uint32 = 0x4C4F4731 // "LOG1"
	recordVersionV1 uint16 = 1
	headerSize             = 16
)

// encodeRecord 将 payload 编码为一帧，返回帧字节与总长度。
func encodeRecord(payload []byte) []byte {
	return nil // TODO
}

// decodeHeader 解析帧头；返回 payload 长度与 CRC。
// 错误：io.EOF 表示干净结尾；ErrTruncated 表示头不完整；ErrCorrupt 表示魔数/版本非法。
func decodeHeader(buf []byte) (payloadLen uint32, crc uint32, err error) {
	return 0, 0, io.EOF // TODO
}

// verifyPayload 校验 payload 的 CRC。
func verifyPayload(payload []byte, crc uint32) error {
	return nil // TODO
}
