package task_orm

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func MarkTasksStatus(ctx context.Context, db *gorm.DB, taskIDs []uuid.UUID, status string, lastError string) error {
	if len(taskIDs) == 0 {
		return nil
	}

	result := db.WithContext(ctx).
		Model(&model.Task{}).
		Where("id IN ?", taskIDs).
		Where("status = ?", model.TaskStatusPending).
		Updates(map[string]any{
			"status":     status,
			"last_error": lastError,
		})
	if result.Error != nil {
		return fmt.Errorf("mark tasks %s: %w", status, result.Error)
	}

	return nil
}
