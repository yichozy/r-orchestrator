package task_orm

import (
	"context"
	"fmt"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func Create(ctx context.Context, db *gorm.DB, task model.Task) error {
	if err := db.WithContext(ctx).Create(&task).Error; err != nil {
		return fmt.Errorf("create task: %w", err)
	}

	return nil
}
