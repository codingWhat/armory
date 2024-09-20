package serial

import (
	"fmt"
	"testing"
)

type AddPipe struct {
	BasePipe
	value int
}

func NewAddPipe(value int) *AddPipe {
	return &AddPipe{
		value: value,
	}
}

func (ap *AddPipe) DoProcess(input interface{}) interface{} {
	in := input.(int)
	output := in + ap.value
	fmt.Printf("add: %d + %d = %d\n", in, ap.value, output)
	return output
}

type MulPipe struct {
	BasePipe
	value int
}

func NewMulPipe(value int) *MulPipe {
	return &MulPipe{
		value: value,
	}
}

func (mp *MulPipe) DoProcess(input interface{}) interface{} {
	in := input.(int)
	output := in * mp.value
	fmt.Printf("mul: %d * %d = %d\n", in, mp.value, output)
	return output
}

func TestSerialPipeline(t *testing.T) {
	addPipe1 := NewAddPipe(1)
	addPipe2 := NewAddPipe(10)
	mulPipe1 := NewMulPipe(2)

	pipeline := &DefaultPipeline{}
	pipeline.AddPipe(addPipe1)
	pipeline.AddPipe(mulPipe1)
	pipeline.AddPipe(addPipe2)

	pipeline.Process(1)
	fmt.Println("final result:", pipeline.GetResult())
}
