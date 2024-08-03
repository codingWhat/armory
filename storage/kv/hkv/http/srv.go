package main

import (
	"encoding/json"
	"github.com/codingWhat/armory/storage/kv/hkv"
	"github.com/codingWhat/armory/storage/kv/hkv/http/conf"
	"net/http"
)

var db *hkv.DB

func main() {

	//加载配置文件
	config, err := loadConfig()
	if err != nil {
		panic(err)
	}

	if err := checkConfig(config); err != nil {
		panic(err)
	}

	db, err = newDBInstance(config)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	http.HandleFunc("/put", handlePut)
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/delete", handleDelete)

	//启动http服务
	if err := http.ListenAndServe(config.ServeIP, nil); err != nil {
		panic(err)
	}

}

func handleDelete(writer http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	k := req.URL.Query().Get("key")
	err := db.Delete([]byte(k))
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	writer.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(writer).Encode("ok")
}

func handleGet(writer http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	k := req.URL.Query().Get("key")
	val, err := db.Get([]byte(k))
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	writer.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(val))
}

func handlePut(writer http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]string
	decoder := json.NewDecoder(req.Body)
	err := decoder.Decode(&data)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for k, v := range data {
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
	}

	_, _ = writer.Write([]byte("ok"))
}

func newDBInstance(config *conf.Config) (*hkv.DB, error) {
	// 启动存储引擎
	ops := hkv.DefaultOptions
	if config.DirPath != "" {
		ops.DirPath = config.DirPath
	}
	if config.DataFileSize > 0 {
		ops.DataFileSize = config.DataFileSize
	}

	if config.IndexType > 0 {
		ops.IndexType = hkv.IndexType(config.IndexType)
	}
	if config.IsMMapAtStartup {
		ops.MMapAtStartup = config.IsMMapAtStartup
	}
	if config.MergeRatio > 0 {
		ops.MergeRatio = config.MergeRatio
	}
	if config.SyncWrites {
		ops.SyncWrites = config.SyncWrites
	}

	return hkv.Open(ops)
}

func checkConfig(c *conf.Config) error {
	return nil
}

func loadConfig() (*conf.Config, error) {
	config := &conf.Config{
		DirPath: "./tmp",

		DataFileSize: 10 * 1024 * 1024, //每个数据文件的大小

		IndexType: 1,

		MergeRatio: 0.5,

		// 启动时是否使用 MMap 加载数据
		IsMMapAtStartup: true,

		ServeIP: "127.0.0.1:8081",
	}
	return config, nil
}
