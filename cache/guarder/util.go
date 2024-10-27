package guarder

import "fmt"

func WithRecover(fn func()) {

	defer func() {
		if err := recover(); err != nil {
			fmt.Println("async goroutine panic, err:", err)
		}
	}()

	fn()
}
