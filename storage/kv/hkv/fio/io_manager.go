package fio

// IOManager IO的接口(普通文件系统接口和mmap)
type IOManager interface {
	Write([]byte) (int, error)
	Read([]byte, int64) (int, error)
	Sync() error
	Close() error
	Size() (int64, error)
}

type FileIOType = byte

const (
	// StandardFIO 标准文件 IO
	StandardFIO FileIOType = iota

	// MemoryMap 内存文件映射
	MemoryMap
)

// NewIOManager 初始化 IOManager，目前只支持标准 FileIO
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
