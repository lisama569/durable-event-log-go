package logstore

// formatVersionCurrent 是当前实现写入的磁盘格式版本。
// 兼容规则：等于当前版本 -> 接受；小于当前版本 -> 接受并升级；
// 大于当前版本 -> 拒绝（ErrUnsupportedVersion）。
const formatVersionCurrent uint16 = 1

// meta 是 meta.json 的磁盘结构（版本化元数据）。
type meta struct {
	Version uint16 `json:"version"`
}

// loadMeta 读取并校验元数据；文件不存在时创建。
// 返回元数据与处理动作（created/loaded/upgraded）。
func loadMeta(dir string) (meta, string, error) {
	return meta{}, "", nil // TODO
}

// saveMeta 原子写入元数据（tmp + rename）。
func saveMeta(dir string, m meta) error {
	return nil // TODO
}
