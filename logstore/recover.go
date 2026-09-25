package logstore

// recoverSegments 扫描目录中的段文件，校验记录链，
// 截断不完整的尾部，返回有效段列表与恢复诊断。
// 规则：尾部不完整/损坏 -> 截断并记录原因；
// 非尾段中间损坏 -> 返回 ErrCorrupt 并携带位置，拒绝打开。
func recoverSegments(dir string) ([]*segment, RecoveryInfo, error) {
	return nil, RecoveryInfo{}, nil // TODO
}
