package logstore

// Trim 回收所有最大偏移严格小于 offset 的段文件，释放磁盘空间。
// 若待删段仍有进行中的读取，返回 ErrTrimInUse，不做部分删除。
func (l *Log) Trim(offset uint64) error {
	return nil // TODO
}

// totalSize 返回当前所有段占用字节数。
func (l *Log) totalSize() int64 {
	return 0 // TODO
}
