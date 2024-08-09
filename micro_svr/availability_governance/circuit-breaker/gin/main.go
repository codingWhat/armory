package main

import (
	"errors"
	"fmt"
	sentinel "github.com/alibaba/sentinel-golang/api"
	"github.com/alibaba/sentinel-golang/core/base"
	"github.com/alibaba/sentinel-golang/core/circuitbreaker"
	"github.com/alibaba/sentinel-golang/core/config"
	"github.com/alibaba/sentinel-golang/logging"
	"github.com/alibaba/sentinel-golang/util"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"time"
)

// 状态转移的时候会触发
type stateChangeTestListener struct {
}

func (s *stateChangeTestListener) OnTransformToClosed(prev circuitbreaker.State, rule circuitbreaker.Rule) {
	fmt.Printf("rule.steategy: %+v, From %s to Closed, time: %d\n", rule.Strategy, prev.String(), util.CurrentTimeMillis())
}

func (s *stateChangeTestListener) OnTransformToOpen(prev circuitbreaker.State, rule circuitbreaker.Rule, snapshot interface{}) {
	fmt.Printf("rule.steategy: %+v, From %s to Open, snapshot: %d, time: %d\n", rule.Strategy, prev.String(), snapshot, util.CurrentTimeMillis())
}

func (s *stateChangeTestListener) OnTransformToHalfOpen(prev circuitbreaker.State, rule circuitbreaker.Rule) {
	fmt.Printf("rule.steategy: %+v, From %s to Half-Open, time: %d\n", rule.Strategy, prev.String(), util.CurrentTimeMillis())
}

func InitSentinel() {
	conf := config.NewDefaultConfig()
	// for testing, logging output to console
	conf.Sentinel.Log.Logger = logging.NewConsoleLogger()
	//方式一： 通过配置文件创建
	err := sentinel.InitWithConfig(conf)
	//方式二： 通过默认创建
	//err :sentinel.InitDefault()
	if err != nil {
		fmt.Println("sss")
		log.Fatal(err)
	}

	circuitbreaker.RegisterStateChangeListeners(&stateChangeTestListener{})

	_, err = circuitbreaker.LoadRules([]*circuitbreaker.Rule{
		// Statistic time span=5s, recoveryTimeout=3s, maxErrorCount=50
		{
			Resource:                     "abc",                           // 名字
			Strategy:                     circuitbreaker.SlowRequestRatio, // 慢查询的策略
			RetryTimeoutMs:               10000,                           // 3s后尝试恢复，进入half状态
			MinRequestAmount:             10,                              // 静默数 Open的前置条件
			StatIntervalMs:               5000,                            // 5s钟慢查询比例不超过0.4
			StatSlidingWindowBucketCount: 100,                             //滑动时间窗口是10
			MaxAllowedRtMs:               50,                              // 50毫秒以外算慢查询
			Threshold:                    0.5,                             // 5s钟慢查询比例不超过0.4
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	InitSentinel()
	r := gin.Default()
	//r.Use(sentinelPlugin.SentinelMiddleware())

	r.GET("/index", func(c *gin.Context) {

		v := c.Query("v")
		notErr := len(v) != 0
		// 第二步：创建客户端调用
		// ****限流开始****
		e, b := sentinel.Entry("abc")
		if b != nil {
			// 返回给前端，超过了qps
			fmt.Println("失败")
			c.JSON(http.StatusTooManyRequests, gin.H{
				"msg": "请求过快,请稍后再试," + time.Now().Format("2006-01-02 15:04:05"),
			})
			return
		}
		err := doBuz(notErr)
		e.Exit(base.WithError(err)) // 不要忘了加它
		// ****限流结束****
		if err != nil {
			c.JSON(200, "服务器错误")
			return
		}
		c.JSON(200, "ok")
	})

	r.Run()
}

func doBuz(i bool) error {
	fmt.Println("--->do buz")
	if i {
		return nil
	}
	time.Sleep(60 * time.Millisecond)
	return errors.New("ddd")
}
