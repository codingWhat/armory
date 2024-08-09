package models

import (
	"context"
	"github.com/codingWhat/armory/schedule/single/data"
	"time"
)

type FaceModel struct {
}

func (f *FaceModel) Name() string {
	return "face"
}
func (f *FaceModel) Quota() int {
	return 100
}

func (f *FaceModel) Request(ctx context.Context, pu *data.ProcessUnit) error {
	time.Sleep(time.Second)
	//todo 请求AI模型实例
	//fmt.Println("----->FaceModel request handle, data:", pu.Data)
	return nil
}

func (f *FaceModel) RespBatchHandle(context.Context, []map[string]interface{}) error {
	//todo 批量写入DB
	return nil
}
