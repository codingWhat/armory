package serial

type Pipe interface {
	DoProcess(input interface{}) interface{}
	Process(input interface{})
	SetNext(pipe Pipe)
	GetNext() Pipe
	GetResult() interface{}
	SetResult(interface{})
}

type BasePipe struct {
	nextPipe Pipe
	out      interface{}
}

func (bp *BasePipe) Process(input interface{}) {

}
func (bp *BasePipe) DoProcess(input interface{}) interface{} {
	panic("sub pie implement DoProcess")
}

func (bp *BasePipe) SetNext(pipe Pipe) {
	bp.nextPipe = pipe
}

func (bp *BasePipe) GetNext() Pipe {
	return bp.nextPipe
}

func (bp *BasePipe) GetResult() interface{} {
	return bp.out
}
func (bp *BasePipe) SetResult(ret interface{}) {
	bp.out = ret
}
