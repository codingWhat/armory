package pq

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestPQ_Pop(t *testing.T) {
	pq := New()

	for i := 0; i < 10; i++ {
		err := pq.Push(&Item{
			Key:      strconv.Itoa(i),
			Value:    i,
			Priority: int64(i),
		})
		assert.Nil(t, err)
	}

	for i := 0; i < 10; i++ {
		item, err := pq.Pop()
		assert.Equal(t, i, item.Value)
		assert.Nil(t, err)
	}

}
