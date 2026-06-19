package task_orm

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func GetPendingTaskList(ctx context.Context, db *gorm.DB) ([]model.Task, error) {
	var tasks []model.Task
	err := db.WithContext(ctx).
		Where("status = ?", model.TaskStatusPending).
		Order("created_at asc").
		Find(&tasks).Error
	if err != nil {
		return nil, err
	}
	return tasks, nil
}
