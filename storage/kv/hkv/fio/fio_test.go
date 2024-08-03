package fio

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestFileIO_Write(t *testing.T) {

	fileName, _ := os.MkdirTemp("", "Filo")
	fmt.Println(fileName)
	f, err := NewFileIOManager(fileName + "/111.txt")
	if err != nil {
		return
	}
	write, err := f.Write([]byte("1111"))
	fmt.Println("----->", write, err)

	ret := make([]byte, 4)
	n, err := f.Read(ret, 0)
	assert.Nil(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("1111"), ret)

	f.Write([]byte("2222"))

	ret = make([]byte, 4)
	n, err = f.Read(ret, 4)
	assert.Nil(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("2222"), ret)

	os.Remove(fileName)

}
