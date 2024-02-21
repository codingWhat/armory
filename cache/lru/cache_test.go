package lru

import (
	"container/list"
	"fmt"
	"reflect"
	"testing"
)

var c *Cache

func initCache() {
	c = &Cache{
		size:  4,
		ll:    list.New(),
		items: make(map[string]*entry),
	}
}

type A struct {
	Val string
}

func (a *A) Len() int64 {
	return int64(len(a.Val))
}

func TestMain(t *testing.M) {
	initCache()
	t.Run()
}
func TestCache_Add(t *testing.T) {

	type args struct {
		key string
		val Value
	}
	tests := []struct {
		name string
		args args
	}{
		{"1", args{key: "a", val: &A{Val: "aa"}}},
		{"2", args{key: "b", val: &A{Val: "bb"}}},
		{"3", args{key: "c", val: &A{Val: "cc"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c.Add(tt.args.key, tt.args.val)
		})
	}

}

func TestCache_Get(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		args    args
		want    Value
		wantErr bool
	}{
		{"1", args{key: "a"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.Get(tt.args.key)
			fmt.Println("----->", tt.args.key, got, err)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}
