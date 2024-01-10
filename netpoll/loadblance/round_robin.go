package loadblance

type RoundRobin struct {
	container []interface{}
	currIdx   int64
	length    int
}

func NewRoundRobin(items []interface{}) *RoundRobin {
	return &RoundRobin{
		container: items,
		currIdx:   0,
		length:    len(items),
	}
}

func (r *RoundRobin) Next() interface{} {
	nextIdx := (r.currIdx + 1) % int64(r.length)
	return r.container[nextIdx]
}
