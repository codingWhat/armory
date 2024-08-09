package data

type ProcessUnit struct {
	MaxRetryNum   int
	CurRetryNum   int
	isExceptRetry bool

	Data      interface{}
	processed map[string]bool
}

func NewDefaultProcessUnit(data interface{}) *ProcessUnit {
	return &ProcessUnit{
		MaxRetryNum: 1,
		Data:        data,
		processed:   make(map[string]bool),
	}
}
func (i *ProcessUnit) IncrRetryNum() {
	i.CurRetryNum++
}
func (i *ProcessUnit) IsCanRetry() bool {
	if i.MaxRetryNum == 0 {
		return false
	}

	return i.CurRetryNum < i.MaxRetryNum
}
func (i *ProcessUnit) IsProcessedBy(name string) bool {
	val, ok := i.processed[name]
	return ok && val
}

func (i *ProcessUnit) RecordProcess(name string) {
	i.processed[name] = true
}

func (i *ProcessUnit) IsReachMaxRetry() bool {
	return i.CurRetryNum >= i.MaxRetryNum
}
