package task_service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var cancelTaskBeforeShardCancelHook func(tx *gorm.DB, taskID uuid.UUID)
var cancelTaskAfterCommitHook func(taskID uuid.UUID)
var cancelTaskPostCommitTimeout = 5 * time.Second

func CancelTask(ctx context.Context, tenantName string, taskID uuid.UUID) error {
	db, err := orm.GetDB()
	if err != nil {
		return err
	}

	tenant, err := tenant_orm.GetByName(ctx, db, tenantName)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrTenantNotFound, tenantName)
	}

	task, err := task_orm.GetByID(ctx, db, taskID)
	if err != nil {
		return err
	}

	// Collect shards with assigned agents before cancelling.
	var runningShards []struct {
		AssignedAgentID string
		ID              uuid.UUID
	}
	if err := db.WithContext(ctx).
		Model(&model.TaskShard{}).
		Select("assigned_agent_id, id").
		Where("task_id = ?", taskID).
		Where("status IN ?", []string{model.ShardStatusLeased, model.ShardStatusRunning, model.ShardStatusResultReady}).
		Where("assigned_agent_id IS NOT NULL").
		Find(&runningShards).Error; err != nil {
		return err
	}

	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := task_orm.CancelTaskById(ctx, tx, tenant.ID, taskID); err != nil {
			return err
		}
		if cancelTaskBeforeShardCancelHook != nil {
			cancelTaskBeforeShardCancelHook(tx, taskID)
		}
		if err := task_shard_orm.CancelTaskShardsByTaskId(ctx, tx, taskID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	if cancelTaskAfterCommitHook != nil {
		cancelTaskAfterCommitHook(taskID)
	}

	postCommitCtx, cancel := context.WithTimeout(context.Background(), cancelTaskPostCommitTimeout)
	defer cancel()

	if payload := newCompletionHookPayload(task, model.TaskStatusCancelled, "", task.FinishedAt, false); payload != nil {
		refreshedTask, getErr := task_orm.GetByID(postCommitCtx, db, taskID)
		if getErr != nil {
			zap.L().Named("task_service").Warn("refresh cancelled task for completion hook failed",
				zap.Stringer("task_id", taskID),
				zap.Error(getErr),
			)
		} else {
			payload.FinishedAt = cloneTimePtr(refreshedTask.FinishedAt)
		}
		dispatchCompletionHookAsync(*payload)
	}

	// Notify agents to cancel running shards (best-effort).
	if notifyCancelShard != nil {
		for _, s := range runningShards {
			if err := notifyCancelShard(postCommitCtx, s.AssignedAgentID, s.ID); err != nil {
				zap.L().Named("task_service").Warn("notify shard cancellation failed",
					zap.Stringer("task_id", taskID),
					zap.String("agent_id", s.AssignedAgentID),
					zap.Stringer("shard_id", s.ID),
					zap.Error(err),
				)
			}
		}
	}

	return nil
}
