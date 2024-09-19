package main

import (
	"gorm.io/gorm"
	"time"
)

type Comments struct {
	gorm.Model

	ID        uint64    `gorm:"column:id;primary_key;AUTO_INCREMENT" json:"commentid"`
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
	DeletedAt time.Time `gorm:"column:deleted_at"`
	Reply     int       `gorm:"column:reply"`
	Content   string    `gorm:"column:content" json:"content"`
	Targetid  int       `gorm:"column:targetid;NOT NULL" json:"targetid"`
	Parentid  uint64    `gorm:"column:parentid;NOT NULL" json:"parentid"`
}

func newComment() *Comments {
	return &Comments{}
}

func BatchInsert(comments []*Comments) error {
	return GetDBConn().Create(comments).Error
}

func BatchUpdate(comments []*Comments) error {
	db := GetDBConn()
	for _, comment := range comments {
		if err := db.Model(comment).Select("reply").Updates(comment).Error; err != nil {
			return err
		}
	}
	return nil
}

func TXNBatchInsertAndUpdate(creates []*Comments, updates []*Comments) error {
	db := GetDBConn()

	return db.Transaction(func(tx *gorm.DB) error {
		if len(creates) > 0 {
			if err := BatchInsert(creates); err != nil {
				return err
			}
		}

		if len(updates) > 0 {
			if err := BatchUpdate(updates); err != nil {
				return err
			}
		}

		return nil
	})
}
