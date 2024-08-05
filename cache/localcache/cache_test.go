package localcache

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func Test_cache_LRU(t *testing.T) {
	c := New(WithCapacity(3), WithSetTimout(1*time.Second))

	for i := 0; i < 4; i++ {
		if i == 2 {
			for j := 0; j < 10; j++ {
				c.Get("0")
			}
		}
		assert.Equal(t, true, c.Set(strconv.Itoa(i), i, 10*time.Second))
	}

	for i := 0; i < 4; i++ {
		got, err := c.Get(strconv.Itoa(i))
		if i == 1 {
			assert.Nil(t, got)
			assert.Equal(t, ErrKeyNoExists, err)
		} else {
			assert.Equal(t, i, got)
			assert.Nil(t, err)
		}

	}

}
func Test_cache_Set(t *testing.T) {

	c := New(WithCapacity(3), WithSetTimout(1*time.Second))

	type args struct {
		k   string
		v   any
		ttl time.Duration
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "set", args: args{k: "a", v: 1, ttl: 2 * time.Second}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if got := c.Set(tt.args.k, tt.args.v, tt.args.ttl); got != tt.want {
				t.Errorf("Set() = %v, want %v", got, tt.want)
			}
		})
	}

	tests = []struct {
		name string
		args args
		want bool
	}{
		{name: "get", args: args{k: "a", v: 1, ttl: 2 * time.Second}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := c.Get(tt.args.k); got != tt.want && err != nil {
				t.Errorf("Set() = %v, want %v", got, tt.want)
			}
		})
	}
}
