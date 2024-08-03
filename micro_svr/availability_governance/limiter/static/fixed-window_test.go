package static

import (
	"fmt"
	"testing"
	"time"
)

func TestFixedWindow_Allow(t *testing.T) {

	fw := NewFixedWindow(1*time.Second, 3)

	for i := 0; i < 15; i++ {
		now := time.Now().Format("15:04:05")
		if fw.Allow() {
			fmt.Println(i, now+"请求通过")
		} else {
			fmt.Println(i, now+"请求不通过")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
