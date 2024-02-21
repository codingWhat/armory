package mdb

import (
	"fmt"
	"testing"
)

func Test_MDB(t *testing.T) {
	db, err := NewMDB("./2.log")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("aa%d", i)
		err = db.Put(k, fmt.Sprintf("av%d", i))
		if err != nil {
			panic(err)
		}
	}

	db.Sync()

	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("aa%d", i)
		v, b := db.Get(k)
		fmt.Println("---->Get", k, v, b)
	}
}

//
//func Test_encode(t *testing.T) {
//
//	file, err := os.OpenFile("./1.log", os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
//	if err != nil {
//		return
//	}
//	for i := 0; i < 5; i++ {
//		k := fmt.Sprintf("aa%d", i)
//		err := encode(file, &entry{
//			LogType:   1,
//			KeySize:   int32(len(k)),
//			ValueSize: 2,
//			Key:       k,
//			Value:     "bb",
//		})
//		if err != nil {
//			panic(err)
//		}
//	}
//
//	file.Sync()
//	ret, err := file.Seek(0, 0)
//	fmt.Println("--->seek", ret, err)
//	for i := 0; i < 5; i++ {
//		e, err := decode(file)
//		fmt.Println(i, "--->decode", e, err)
//	}
//
//}
