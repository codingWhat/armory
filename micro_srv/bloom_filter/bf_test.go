package bf

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func stringWithCharset(length int, charset string) string {
	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func generateRandomStrings(n int, length int) [][]byte {
	strings := make([][]byte, n)
	for i := 0; i < n; i++ {
		strings[i] = []byte(stringWithCharset(length, charset))
	}
	return strings
}

func BenchmarkNewBloomFilter(b *testing.B) {

	randomStrings := generateRandomStrings(1000000, 32)

	bf := NewBloomFilter(512 * 1024 * 1024)

	start := time.Now()
	fmt.Println("start time:", start.Format("2006-01-02 15:04:05"))
	for i := 0; i < len(randomStrings); i++ {
		k := randomStrings[i]
		bf.Set(k)
	}
	now := time.Now()
	fmt.Println("end time:", now.Format("2006-01-02 15:04:05"))
	fmt.Println("consume time:", now.Sub(start))

	fmt.Println("[Exist] start time:", time.Now().Format("2006-01-02 15:04:05"))
	c := 0
	for i := 0; i < len(randomStrings); i++ {
		k := randomStrings[i]
		r := bf.Exist(k)
		if !r {
			c++
			//fmt.Println("Get ", k, " is false")
		}
	}

	fmt.Println("err_num:", c, ", [Exist] end time:", time.Now().Format("2006-01-02 15:04:05"))
	//b.ResetTimer()
	//for i := 0; i < b.N; i++ {
	//
	//}
}

//func TestNewBloomFilter(t *testing.T) {
//	bf := NewBloomFilter(512 * 1024 * 1024)
//
//	k := []byte("asdccc")
//	bf.Set(k)
//	fmt.Println("------------")
//	fmt.Println(bf.Get(k))
//	fmt.Printf("%b", bf.bitArr[1])
//}
