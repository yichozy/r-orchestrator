package task_service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var reportShardStatusAfterTaskLockHook func(tx *gorm.DB, taskID, shardID uuid.UUID)

type ReportShardStatusParams struct {
	ShardID       uuid.UUID
	ShardStatus   string
	ErrorMessage  *string
	OutputOSSKey  string
	OutputSHA256  string
}

func ReportShardStatus(ctx context.Context, params ReportShardStatusParams) error {
	db, err := orm.GetDB()
	if err != nil {
		return err
	}

	now := time.Now()
	var hookPayload *CompletionHookPayload

	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		shard, err := task_shard_orm.GetByID(ctx, tx, params.ShardID)
		if err != nil {
			return fmt.Errorf("load shard: %w", err)
		}

		var task model.Task
		if err := tx.WithContext(ctx).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id", "tenant_id", "status", "started_at").
			Where("id = ?", shard.TaskID).
			First(&task).Error; err != nil {
			return fmt.Errorf("load task: %w", err)
		}

		// Skip duplicate SUCCEEDED reports.
		if params.ShardStatus == model.ShardStatusSucceeded && shard.Status == model.ShardStatusSucceeded && shard.OutputOSSKey != "" {
			return nil
		}

		if task.Status == model.TaskStatusCancelled {
			return fmt.Errorf("report shard %s: task %q is cancelled", params.ShardStatus, shard.TaskID)
		}
		if reportShardStatusAfterTaskLockHook != nil {
			reportShardStatusAfterTaskLockHook(tx, task.ID, shard.ID)
		}

		updateParams := task_shard_orm.UpdateShardStatusParams{
			ShardID: params.ShardID,
			Status:  params.ShardStatus,
		}

		switch params.ShardStatus {
		case model.ShardStatusRunning:
			updateParams.CurrentStatuses = []string{model.ShardStatusLeased}
			updateParams.StartedAt = &now
		case model.ShardStatusResultReady:
			updateParams.CurrentStatuses = []string{model.ShardStatusRunning}
			updateParams.FinishedAt = &now
		case model.ShardStatusSucceeded:
			updateParams.CurrentStatuses = []string{model.ShardStatusResultReady}
		case model.ShardStatusFailed:
			updateParams.CurrentStatuses = []string{model.ShardStatusRunning, model.ShardStatusResultReady}
			updateParams.FinishedAt = &now
			updateParams.LastError = params.ErrorMessage
		}

		if err := task_shard_orm.UpdateShardStatus(ctx, tx, updateParams); err != nil {
			return fmt.Errorf("report shard %s: %w", params.ShardStatus, err)
		}

		if params.ShardStatus == model.ShardStatusSucceeded {
			// Update shard with OSS output details.
			if err := tx.WithContext(ctx).
				Model(&model.TaskShard{}).
				Where("id = ?", params.ShardID).
				Updates(map[string]any{
					"output_oss_key": params.OutputOSSKey,
					"output_sha256":  params.OutputSHA256,
				}).Error; err != nil {
				return fmt.Errorf("update shard oss output: %w", err)
			}
		}

		if params.ShardStatus == model.ShardStatusRunning {
			updates := map[string]any{"status": model.TaskStatusRunning}
			if task.StartedAt == nil {
				updates["started_at"] = now
			}
			result := tx.Model(&model.Task{}).
				Where("id = ?", shard.TaskID).
				Where("status <> ?", model.TaskStatusCancelled).
				Updates(updates)
			if result.Error != nil {
				return fmt.Errorf("mark task running: %w", result.Error)
			}
			if result.RowsAffected == 0 {
				return fmt.Errorf("mark task running: task %q is cancelled", shard.TaskID)
			}
		} else if params.ShardStatus == model.ShardStatusSucceeded || params.ShardStatus == model.ShardStatusFailed {
			lastError := ""
			if params.ErrorMessage != nil {
				lastError = *params.ErrorMessage
			}
			payload, err := RecomputeTaskStatus(ctx, tx, shard.TaskID, lastError)
			if err != nil {
				return fmt.Errorf("recompute task status after shard %s: %w", params.ShardStatus, err)
			}
			hookPayload = payload
		}

		return nil
	})
	if err != nil {
		return err
	}

	if hookPayload != nil {
		dispatchCompletionHookAsync(*hookPayload)
	}

	return nil
}
