package conf

type Config struct {
	DirPath string `json:"dir_path"` //数据目录

	DataFileSize int64 `json:"data_file_size"` //每个数据文件的大小

	SyncWrites bool `json:"sync_writes"` //是否每次写入Sync

	IndexType int `json:"index_type"`

	MergeRatio float32 `json:"merge_ratio"` // merge比例

	// 启动时是否使用 MMap 加载数据
	IsMMapAtStartup bool `json:"is_mmap_at_startup"`

	ServeIP string `json:"serve_ip"`
}
