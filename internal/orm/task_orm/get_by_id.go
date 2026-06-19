package task_orm

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func GetByID(ctx context.Context, db *gorm.DB, taskID uuid.UUID) (model.Task, error) {
	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		return model.Task{}, fmt.Errorf("get task by id: %w", err)
	}
	return task, nil
}
