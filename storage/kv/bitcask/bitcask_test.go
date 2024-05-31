package bitcask

import (
	"errors"
	"fmt"
	"os"
	"testing"
)

func Test_bitCask(t *testing.T) {
	db := Open("./data/")
	for i := 0; i < 1; i++ {
		k := fmt.Sprintf("ii%d", i)
		v := fmt.Sprintf("vvv%d", i)
		err := db.Put(k, []byte(v))
		if err != nil {
			fmt.Println("----->Put", k, v, err.Error())
		}
	}
	k := "ii0"
	err := db.Del(k)
	if err != nil {
		fmt.Println("---->Del", k, err.Error())
	}

	_, err = db.Get(k)
	if !errors.Is(err, ErrKeyNoExist) {
		t.Error("del failed")
	}

	/*
		for i := 0; i < 40; i++ {
			k := fmt.Sprintf("ii%d", i)
			v, err := db.Get(k)
			if err != nil {
				fmt.Println("---->Get-err", k, err.Error())
			} else {
				fmt.Println("---->Get-ok", k, string(v))
			}
		}
	*/

}
func Test_bitCask_Read(t *testing.T) {
	db := Open("./data/")
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("ii%d", i)
		v, err := db.Get(k)
		fmt.Println("---->Get-ok", k, string(v), err)
	}

	//f, err := os.OpenFile("./data/archive_2", os.O_RDWR, 0600)
	//if err != nil {
	//	panic(err)
	//}
	//
	//var offset int64 = 0
	//for {
	//	e, err := Decode(f)
	//	if err == io.EOF {
	//		break
	//	}
	//	if string(e.Key) == "ii26" {
	//		fmt.Println(e, offset)
	//	}
	//	offset += int64(e.Size())
	//}
}
func Test_bitCask_Merge(t *testing.T) {

	db := Open("./data/")
	fmt.Println("before--------------------DB.INDEX----------------")
	for k, info := range db.Index {
		fmt.Println("key:", k, ", pos:", info.FileID, info.Offset)
	}
	fmt.Println("before--------------------DB.INDEX----------------")

	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("ii%d", i)
		v := fmt.Sprintf("vvv%d", i)
		err := db.Put(k, []byte(v))
		if err != nil {
			fmt.Println("----->Put", k, v, err.Error())
		}
	}

	fmt.Println("--------------------DB.INDEX----------------")
	for k, info := range db.Index {
		fmt.Println("key:", k, ", pos:", info.FileID, info.Offset)
	}
	fmt.Println("--------------------DB.INDEX----------------")
	//db.merge()
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("ii%d", i)
		v, err := db.Get(k)
		fmt.Println("[1]---->Get-ok", k, string(v), err)
	}

	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("ii%d", i)
		v := fmt.Sprintf("vvv%d", i)
		err := db.Put(k, []byte(v))
		if err != nil {
			fmt.Println("----->Put", k, v, err.Error())
		}
	}

	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("ii%d", i)
		v, err := db.Get(k)
		fmt.Println("[2]---->Get-ok", k, string(v), err)
	}

	os.RemoveAll("./data/")
}
