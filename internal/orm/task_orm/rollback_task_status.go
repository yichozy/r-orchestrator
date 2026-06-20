package task_orm

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// RollbackTaskToWaiting attempts to move a task from QUEUED back to
// WAITING_FOR_AGENTS. This is used when a shard lease is rolled back and the
// task had not yet progressed beyond WAITING_FOR_AGENTS.
func RollbackTaskToWaiting(ctx context.Context, db *gorm.DB, taskID uuid.UUID) error {
	result := db.WithContext(ctx).Model(&model.Task{}).
		Where("id = ? AND status = ?", taskID, model.TaskStatusQueued).
		Update("status", model.TaskStatusWaitingForAgents)
	if result.Error != nil {
		return fmt.Errorf("rollback task to waiting: %w", result.Error)
	}
	if result.RowsAffected > 0 {
		return nil
	}
	return gorm.ErrRecordNotFound
}
