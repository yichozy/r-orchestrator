package task_orm

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func CancelTaskById(ctx context.Context, db *gorm.DB, tenant_id uuid.UUID, task_id uuid.UUID) error {
	terminal_statuses := []string{model.TaskStatusSucceeded, model.TaskStatusFailed, model.TaskStatusCancelled}

	result := db.WithContext(ctx).
		Model(&model.Task{}).
		Where("tenant_id = ? AND id = ?", tenant_id, task_id).
		Where("status NOT IN ?", terminal_statuses).
		Updates(map[string]any{
			"status":      model.TaskStatusCancelled,
			"finished_at": time.Now(),
			"last_error":  "",
		})
	if result.Error != nil {
		return fmt.Errorf("cancel task: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		var task model.Task
		if err := db.WithContext(ctx).
			Select("id", "status").
			Where("tenant_id = ? AND id = ?", tenant_id, task_id).
			First(&task).Error; err != nil {
			return fmt.Errorf("cancel task: %w", err)
		}

		return fmt.Errorf("cancel task: task status %q is terminal", task.Status)
	}

	return nil
}
