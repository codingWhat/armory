package main

import (
	"fmt"
)

// Server 定义了服务器及其权重
type Server struct {
	Name   string
	Weight int
}

// WeightedRoundRobin 定义了加权轮询的结构
type WeightedRoundRobin struct {
	Servers []Server
	Weights []int // 累积权重
	Index   int   // 当前索引
	CW      int   // 当前权重
}

// NewWeightedRoundRobin 创建一个新的加权轮询实例
func NewWeightedRoundRobin(servers []Server) *WeightedRoundRobin {
	wrr := &WeightedRoundRobin{
		Servers: servers,
		Weights: make([]int, len(servers)),
		Index:   -1,
		CW:      0,
	}
	// 初始化累积权重
	sum := 0
	for i, server := range servers {
		sum += server.Weight
		wrr.Weights[i] = sum
	}
	return wrr
}

// Next 返回下一个服务器
func (wrr *WeightedRoundRobin) Next() Server {
	for {
		wrr.Index = (wrr.Index + 1) % len(wrr.Servers)
		if wrr.Index == 0 {
			wrr.CW = wrr.CW - wrr.gcd()
			if wrr.CW <= 0 {
				wrr.CW = wrr.maxWeight()
				if wrr.CW == 0 {
					return Server{} // 所有服务器权重为0
				}
			}
		}
		if wrr.Servers[wrr.Index].Weight >= wrr.CW {
			return wrr.Servers[wrr.Index]
		}
	}
}

// gcd 计算所有权重的最大公约数
func (wrr *WeightedRoundRobin) gcd() int {
	divisor := wrr.Weights[0]
	for _, weight := range wrr.Weights {
		divisor = gcd(divisor, weight)
	}
	return divisor
}

// maxWeight 返回最大权重
func (wrr *WeightedRoundRobin) maxWeight() int {
	max := 0
	for _, weight := range wrr.Weights {
		if weight > max {
			max = weight
		}
	}
	return max
}

// gcd 计算两个数的最大公约数
func gcd(x, y int) int {
	for y != 0 {
		x, y = y, x%y
	}
	return x
}

func main() {
	// 创建服务器列表
	servers := []Server{

		{"Server3", 1},

		{"Server2", 2},
		{"Server1", 3},
	}

	// 创建加权轮询实例
	wrr := NewWeightedRoundRobin(servers)

	// 模拟请求分配
	for i := 0; i < 10; i++ {
		server := wrr.Next()
		fmt.Printf("Dispatch Request to: %s\n", server.Name)
	}
}
