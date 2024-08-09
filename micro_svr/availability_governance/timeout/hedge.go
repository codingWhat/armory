package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {

	request, err := http.NewRequest("Get", "http://www.baidu.com", nil)
	if err != nil {
		panic(err)
	}
	hedged, err := retryHedged(request, 3, 10*time.Millisecond, 10*time.Second, Backoff)
	fmt.Println(hedged, err)
}

type RetryStrategy func(int) time.Duration

func Backoff(retryNum int) time.Duration {
	return time.Duration(retryNum*2+2) * time.Millisecond
}

func retryHedged(req *http.Request, maxRetries int, hedgeDelay time.Duration, reqTimeout time.Duration, rs RetryStrategy) (*http.Response, error) {
	var (
		originalBody []byte
		err          error
	)
	if req != nil && req.Body != nil {
		originalBody, err = copyBody(req.Body)
	}
	if err != nil {
		return nil, err
	}

	AttemptLimit := maxRetries
	if AttemptLimit <= 0 {
		AttemptLimit = 1
	}

	client := http.Client{
		Timeout: reqTimeout,
	}

	// 每次请求copy新的request
	copyRequest := func() (request *http.Request) {
		request = req.Clone(req.Context())
		if request.Body != nil {
			resetBody(request, originalBody)
		}
		return
	}

	multiplexCh := make(chan struct {
		resp  *http.Response
		err   error
		retry int
	})

	totalSentRequests := &sync.WaitGroup{}
	allRequestsBackCh := make(chan struct{})
	go func() {
		totalSentRequests.Wait()
		close(allRequestsBackCh)
	}()
	var resp *http.Response

	var (
		canHedge   uint32
		readyHedge = make(chan struct{})
	)
	for i := 0; i < AttemptLimit; i++ {
		totalSentRequests.Add(1)

		go func(i int) {
			if atomic.CompareAndSwapUint32(&canHedge, 0, 1) {
				go func() {
					<-time.After(hedgeDelay)
					readyHedge <- struct{}{}
				}()
			} else {
				<-readyHedge
				time.Sleep(rs(i))
			}
			// 标记已经执行完
			defer totalSentRequests.Done()
			req = copyRequest()
			resp, err = client.Do(req)
			if err != nil {
				fmt.Printf("error sending the first time: %v\n", err)
			}
			// 重试 500 以上的错误码
			if err == nil && resp.StatusCode < 500 {
				multiplexCh <- struct {
					resp  *http.Response
					err   error
					retry int
				}{resp: resp, err: err, retry: i}
				return
			}
			// 如果正在重试，那么释放fd
			if resp != nil {
				resp.Body.Close()
			}
			// 重置body
			if req.Body != nil {
				resetBody(req, originalBody)
			}
		}(i)
	}

	select {
	case res := <-multiplexCh:
		return res.resp, res.err
	case <-allRequestsBackCh:
		// 到这里，说明全部的 goroutine 都执行完毕，但是都请求失败了
		return nil, errors.New("all req finish，but all fail")
	}
}
func copyBody(src io.ReadCloser) ([]byte, error) {
	b, err := io.ReadAll(src)
	if err != nil {
		return nil, err
	}
	src.Close()
	return b, nil
}

func resetBody(request *http.Request, originalBody []byte) {
	request.Body = io.NopCloser(bytes.NewBuffer(originalBody))
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBuffer(originalBody)), nil
	}
}
