package task_service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/artifact_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"github.com/yichozy/r-orchestrator/internal/util"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type StoreShardOutputFunc func(ctx context.Context, tx *gorm.DB, task model.Task, shard model.TaskShard, outputCSV []byte) error

var reportShardStatusAfterTaskLockHook func(tx *gorm.DB, taskID, shardID uuid.UUID)

type ReportShardStatusParams struct {
	ShardID            uuid.UUID
	ShardStatus        string
	ErrorMessage       *string
	OutputCSV          []byte
	StoreShardOutputFn StoreShardOutputFunc
}

func ReportShardStatus(ctx context.Context, params ReportShardStatusParams) error {
	db, err := orm.GetDB()
	if err != nil {
		return err
	}

	now := time.Now().UTC()
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
		if params.ShardStatus == model.ShardStatusSucceeded && shard.Status == model.ShardStatusSucceeded {
			exists, err := shardOutputArtifactExists(ctx, tx, shard.TaskID, shard.ShardIndex)
			if err != nil {
				return fmt.Errorf("check existing shard output: %w", err)
			}
			if exists {
				return nil
			}
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
			if params.ShardStatus == model.ShardStatusSucceeded {
				if err := storeShardOutput(ctx, tx, task, shard, params.OutputCSV, params.StoreShardOutputFn); err != nil {
					return fmt.Errorf("store shard output: %w", err)
				}
			}
			lastError := ""
			if params.ErrorMessage != nil {
				lastError = *params.ErrorMessage
			}
			payload, err := RecomputeTaskStatus(ctx, tx, shard.TaskID, lastError)
			if err != nil {
				return err
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

func storeShardOutput(
	ctx context.Context,
	tx *gorm.DB,
	task model.Task,
	shard model.TaskShard,
	outputCSV []byte,
	storeFn StoreShardOutputFunc,
) error {
	if storeFn != nil {
		return storeFn(ctx, tx, task, shard, outputCSV)
	}

	artifactID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generate artifact id: %w", err)
	}
	shardIndex := shard.ShardIndex
	contentBytes := append([]byte{}, outputCSV...)
	if err := artifact_orm.Create(ctx, tx, model.Artifact{
		BaseUUIDModel: model.BaseUUIDModel{ID: artifactID},
		TenantID:      task.TenantID,
		TaskID:        task.ID,
		ArtifactType:  model.ArtifactTypeShardOutput,
		ContentBytes:  contentBytes,
		ContentSize:   int64(len(outputCSV)),
		SHA256:        util.SumSHA256(outputCSV),
		ShardIndex:    &shardIndex,
	}); err != nil {
		return fmt.Errorf("create shard output artifact: %w", err)
	}

	return nil
}

func shardOutputArtifactExists(ctx context.Context, tx *gorm.DB, taskID uuid.UUID, shardIndex int) (bool, error) {
	var count int64
	if err := tx.WithContext(ctx).
		Model(&model.Artifact{}).
		Where("task_id = ?", taskID).
		Where("artifact_type = ?", model.ArtifactTypeShardOutput).
		Where("shard_index = ?", shardIndex).
		Count(&count).Error; err != nil {
		return false, err
	}

	return count > 0, nil
}
