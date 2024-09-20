package parallel

import (
	"fmt"
	"github.com/codingWhat/armory/parallel_model/pipeline/serial"
	"testing"
)

func TestNewPipe(t *testing.T) {

	multi := func(values []interface{}) interface{} {
		var ret []interface{}
		for _, val := range values {
			v := val.(int)
			ret = append(ret, v*10)
		}
		fmt.Println("recv:", values, ", multi:", ret)
		return ret
	}

	merge := func(values []interface{}) interface{} {
		var res int
		for _, value := range values {
			value := value.([]interface{})
			for _, val := range value {
				res += val.(int)
			}
		}
		return res
	}

	pp := NewPipe(3, multi, merge)

	pipeline := &serial.DefaultPipeline{}
	pipeline.AddPipe(pp)

	pipeline.Process([]interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	fmt.Println("---->result: ", pipeline.GetResult())

}
