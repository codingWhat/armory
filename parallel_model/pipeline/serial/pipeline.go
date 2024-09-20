package serial

type Pipeline interface {
	AddPipe(pipe Pipe)
	Process(input interface{})
	GetResult() interface{}
}

type DefaultPipeline struct {
	head Pipe
}

func (dp *DefaultPipeline) AddPipe(pipe Pipe) {
	if dp.head == nil {
		dp.head = pipe
	} else {
		p := dp.head
		for p.GetNext() != nil {
			p = p.GetNext()
		}
		p.SetNext(pipe)
	}
}

func (dp *DefaultPipeline) Process(input interface{}) {
	cur := dp.head

	for cur != nil {
		out := cur.DoProcess(input)
		cur.SetResult(out)
		cur = cur.GetNext()
		input = out
	}
}

func (dp *DefaultPipeline) GetResult() interface{} {
	if dp.head != nil {
		p := dp.head
		for p.GetNext() != nil {
			p = p.GetNext()
		}
		return p.GetResult()
	}
	return nil
}
