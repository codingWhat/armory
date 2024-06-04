package main

import (
	"fmt"
	"testing"
)

func TestSmoothWeightedRoundRobin_Select(t *testing.T) {
	lb := NewSmoothWeightedRoundRobin()
	lb.Add(NewNode("127.0.0.1", 3))
	lb.Add(NewNode("127.0.0.2", 2))
	lb.Add(NewNode("127.0.0.3", 1))

	cnt := make(map[string]int)
	for i := 0; i < 6; i++ {
		ip := lb.Select()
		fmt.Println(ip)
		if _, ok := cnt[ip]; !ok {
			cnt[ip] = 0
		}
		cnt[ip]++
	}

	for ip, num := range cnt {
		fmt.Println(ip, "<----->", num)
	}
}
