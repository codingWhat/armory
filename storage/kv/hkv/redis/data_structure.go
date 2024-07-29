package redis

import (
	"github.com/codingWhat/armory/storage/kv/hkv"
	"time"
)

type redisDataType byte

const (
	String redisDataType = iota + 1
	List
	Hash
	Zset
	Set
)

type DataStructure struct {
	db *hkv.DB
}

func NewDataStructure(db *hkv.DB) *DataStructure {
	//redcon.ListenAndServe("")
	return &DataStructure{db: db}
}

func (ds *DataStructure) Set(key []byte, val []byte, ttl time.Duration) {
	// key -> [type(1) size(32) ttl(64) value]
}

func (ds *DataStructure) Get(key []byte, val []byte, ttl time.Duration) {

}

func (ds *DataStructure) HSet(key []byte, field []byte, value []byte) {

}
