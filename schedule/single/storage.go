package main

import "context"

type Storage interface {
	Save(context.Context, map[string]interface{}) error
	BatchSave(context.Context, []map[string]interface{}) error
}
