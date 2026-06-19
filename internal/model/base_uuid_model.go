package model

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type BaseUUIDModel struct {
	ID        uuid.UUID      `json:"id" gorm:"primaryKey;type:uuid"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `json:"deleted_at" gorm:"index"`
}

func (m *BaseUUIDModel) BeforeCreate(tx *gorm.DB) (err error) {
	if m.ID == uuid.Nil {
		m.ID, err = uuid.NewV7()
	}

	return err
}
