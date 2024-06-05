package main

import (
	"fmt"
	"github.com/afex/hystrix-go/hystrix"
	"net/http"
	"time"
)

// hystrix.go
func main() {
	// 设置一个命令名为"getfail"的断路器
	/*
		Timeout：一个远程调用的超时时间。
		MaxConcurrentRequests：最大并发数，超过并发数的请求直接快速失败。
		SleepWindow：断路器从开路状态转入半开状态的睡眠时间，单位：ms。
		RequestVolumeThreshold：为了避免断路器一启动就进入断路，当超过这个请求之后，断路器才开始工作。
		ErrorPercentThreshold：开路阈值，表示触发开路的失败请求的百分比。

	*/
	hystrix.ConfigureCommand("getfail", hystrix.CommandConfig{
		Timeout:                1000, //ms
		MaxConcurrentRequests:  10,
		SleepWindow:            5000,
		RequestVolumeThreshold: 5,
		ErrorPercentThreshold:  30,
	})

	for i := 0; i < 100; i++ {
		idx := i
		//start1 := time.Now()
		// 使用getfail这个断路来保护以下指令，采用同步的方式，hystrix.Go则是异步方式
		err := hystrix.Do("getfail", func() error {
			// 尝试调用远端服务
			_, err := http.Get("https://www.baidu.com")
			if err != nil {
				fmt.Println("get error:%v", err)
				return err
			}

			fmt.Println("----> 调用成功", idx)
			if idx < 9 {
				time.Sleep(2 * time.Second)
			}

			return nil
		}, func(err error) error {
			// 快速失败时的回调函数
			fmt.Printf("handle  error:%v\n", err)
			return nil
		})

		//cbs, _, _ := hystrix.GetCircuit("getfail")
		//fmt.Println("请求次数:", i+1, ";用时:", time.Now().Sub(start1), ";熔断器开启状态:", cbs.IsOpen(), "请求是否允许：", cbs.AllowRequest())
		if err != nil {
			fmt.Println("----> err", err)
		}
		time.Sleep(1 * time.Second)
	}

}
