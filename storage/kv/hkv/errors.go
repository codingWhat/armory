package hkv

import "errors"

var (
	ErrKeyIsEmpty                   = errors.New("key is empty")
	ErrKeyIsNotExists               = errors.New("key is not exists")
	ErrIndexUpdateFailed            = errors.New("index update is failed")
	ErrDataFileNotExists            = errors.New("data file is not exists")
	ErrDataFileDamaged              = errors.New("data file is damaged")
	ErrTxnIndexUpdateFailed         = errors.New("txn index update is failed")
	ErrMergeIsProgress              = errors.New("merge is processing")
	ErrMergeIsNotReachRatio         = errors.New("merge is not reach the ratio")
	ErrDiskSpaceIsNotEnoughForMerge = errors.New("disk space is not enough for merge")
	ErrDBPathHasUsed                = errors.New("db path has used by other process")
)
