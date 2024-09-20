package stream

import (
	"context"
	"fmt"
	"sync"
)

type MsgChan chan interface{}

func (mc MsgChan) StreamStart(ctx context.Context, fn func() int) MsgChan {

	stream := make(MsgChan)
	go func() {
		defer close(stream)
		for {
			select {
			case <-ctx.Done():
				return
			case stream <- fn():
			}
		}

	}()
	return stream
}
func (mc MsgChan) Filter(ctx context.Context, fun func(interface{}) bool) MsgChan {

	output := make(MsgChan)
	go func() {
		defer func() {
			fmt.Println("--->close Filter")
			close(output)
		}()
		for m := range mc {
			if fun(m) {
				select {
				case <-ctx.Done():
					return
				case output <- m:
				}
			}
		}
	}()
	return output
}
func (mc MsgChan) Map(ctx context.Context, fun func(interface{}) interface{}) MsgChan {

	output := make(MsgChan)
	go func() {
		defer func() {
			fmt.Println("--->close Map")
			close(output)
		}()
		for m := range mc {
			select {
			case <-ctx.Done():
				return
			case output <- fun(m):
			}
		}
	}()
	return output
}
func (mc MsgChan) Take(num int) MsgChan {
	output := make(MsgChan)

	go func() {
		defer close(output)
		for i := 0; i < num; i++ {
			output <- <-mc
		}
	}()

	return output

}
func Parallelize(ctx context.Context, num int, fun func(interface{}) interface{}, exeCmdFunc func(ctx context.Context, fun func(interface{}) interface{}) MsgChan) []MsgChan {

	streams := make([]MsgChan, num)
	for i := range streams {
		streams[i] = exeCmdFunc(ctx, fun)
	}
	return streams
}

func Union(ctx context.Context, inputs []MsgChan) MsgChan {

	output := make(MsgChan)
	var wg sync.WaitGroup
	read := func(ch MsgChan) {
		defer wg.Done()

		for val := range ch {
			select {
			case <-ctx.Done():
				return
			case output <- val:
			}
		}
	}

	for _, input := range inputs {
		wg.Add(1)
		go read(input)
	}

	go func() {
		wg.Wait()
		close(output)
	}()

	return output
}
