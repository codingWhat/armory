package stream

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

func TestMsgChan_StreamStart(t *testing.T) {
	mc := make(MsgChan)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ret := Union(ctx, Parallelize(ctx, 3, func(i interface{}) interface{} {
		v := i.(int)
		return v * 10
	}, mc.StreamStart(ctx, func() int {
		return rand.Intn(500)
	}).Map)).Take(100)

	for v := range ret {
		fmt.Println("----->", v)
	}

}
