package idempotent

import (
	"fmt"
	"github.com/willf/bloom"
	"testing"
)

func TestNewIdempotentSrv(t *testing.T) {
	fmt.Println(bloom.EstimateParameters(50000000, 0.0001))
	fmt.Println(bloom.EstimateParameters(50000000, 0.00001))
	fmt.Println(bloom.EstimateParameters(50000000, 0.000001))
}
