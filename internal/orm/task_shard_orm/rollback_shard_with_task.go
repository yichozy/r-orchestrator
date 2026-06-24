package task_shard_orm

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"gorm.io/gorm"
)

// RollbackShardWithTask rolls back a shard to QUEUED and, if the task is in
// WAITING_FOR_AGENTS status, rolls the task back to WAITING as well. This is the
// unified transaction used by timeout, orphan-cleanup, and lease-rollback paths.
func RollbackShardWithTask(ctx context.Context, tx *gorm.DB, shardID uuid.UUID, currentStatuses []string, task model.Task) error {
	if err := RollbackToQueued(ctx, tx, shardID, currentStatuses); err != nil {
		return err
	}
	if task.Status == model.TaskStatusWaitingForAgents {
		if err := task_orm.RollbackTaskToWaiting(ctx, tx, task.ID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}
	return nil
}
