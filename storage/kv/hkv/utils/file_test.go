package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDirSize(t *testing.T) {
	size, err := DirSize("./")
	assert.Nil(t, err)
	fmt.Println("--->size", size)

}

func TestAvailableDiskSpace(t *testing.T) {
	size, err := AvailableDiskSpace()
	assert.Nil(t, err)
	assert.NotNil(t, size)
	fmt.Println("--->", size/1024/1024/1024, err)
}
