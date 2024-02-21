package bitcask

import (
	"fmt"
	"testing"
)

func Test_bitCask(t *testing.T) {

	//err := filepath.Walk("./archive", func(path string, info os.FileInfo, err error) error {
	//	if err != nil {
	//		fmt.Println("遍历目录失败:", err)
	//		return err
	//	}
	//
	//	if !info.IsDir() {
	//		fmt.Println("文件:", path)
	//	}
	//
	//	return nil
	//})
	//
	//if err != nil {
	//	fmt.Println("遍历目录失败:", err)
	//}

	db := Open("./")
	//for i := 0; i < 10; i++ {
	//	k := fmt.Sprintf("ii%d", i)
	//	v := fmt.Sprintf("vvv%d", i)
	//	err := db.Put(k, []byte(v))
	//	if err != nil {
	//		fmt.Println("----->Put", k, v, err.Error())
	//	}
	//}

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("ii%d", i)
		v, err := db.Get(k)
		if err != nil {
			fmt.Println("---->Get", k, err.Error())
		}
		fmt.Println("---->Get", k, string(v))

	}

}
func Test_decode(t *testing.T) {

	//f, err := os.OpenFile("./3.log", os.O_CREATE|os.O_RDWR, 0600)
	//if err != nil {
	//	return
	//}
	//fmt.Println("---->", f.Name())
	//statInfo, err := f.Stat()
	//if err != nil {
	//	return
	//}
	//fmt.Println("---->", statInfo.Name())
	////
	//fmt.Println("-", statInfo.Size())
	//f.Seek(5, 0)
	//f.Write([]byte("6"))

	//f, err := os.OpenFile("./1.log", os.O_CREATE|os.O_RDWR, 0600)
	//if err != nil {
	//	return
	//}
	//stat, err := f.Stat()
	//if err != nil {
	//	return
	//}
	//
	//fmt.Println("-->file size", stat.Size())
	//
	//k := "aa"
	//v := "vv"
	//e := &Entry{
	//	Key:       []byte(k),
	//	Value:     []byte(v),
	//	KeySize:   int32(len(k)),
	//	ValueSize: int32(len(v)),
	//	TS:        int32(1222),
	//}
	//buffer := encode(e)
	//
	//_, err = f.Write(buffer.Bytes())
	//if err != nil {
	//	panic(err)
	//}
	//
	//_, err = f.Seek(0, 0)
	//if err != nil {
	//	panic(err)
	//}
	//ee := decode(f)
	//fmt.Println("--->", ee, string(ee.Key), string(ee.Value))
}
