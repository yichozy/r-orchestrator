package task_service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func RecomputeTaskStatus(ctx context.Context, db *gorm.DB, taskID uuid.UUID, lastError string) (*CompletionHookPayload, error) {
	terminalStatuses := []string{model.TaskStatusSucceeded, model.TaskStatusFailed, model.TaskStatusCancelled}

	var task model.Task
	if err := db.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "tenant_id", "status", "shard_count", "completion_hook_url").
		Where("id = ?", taskID).
		First(&task).Error; err != nil {
		return nil, fmt.Errorf("load task: %w", err)
	}
	if task.Status == model.TaskStatusCancelled {
		return nil, fmt.Errorf("refresh task status: task %q is cancelled", taskID)
	}
	if task.Status == model.TaskStatusSucceeded || task.Status == model.TaskStatusFailed {
		return nil, nil
	}

	var activeCount int64
	if err := db.WithContext(ctx).
		Model(&model.TaskShard{}).
		Where("task_id = ?", taskID).
		Where("status NOT IN ?", []string{model.ShardStatusSucceeded, model.ShardStatusFailed, model.ShardStatusCancelled}).
		Count(&activeCount).Error; err != nil {
		return nil, fmt.Errorf("count active shards: %w", err)
	}
	if activeCount > 0 {
		return nil, nil
	}

	var failedCount int64
	if err := db.WithContext(ctx).
		Model(&model.TaskShard{}).
		Where("task_id = ?", taskID).
		Where("status = ?", model.ShardStatusFailed).
		Count(&failedCount).Error; err != nil {
		return nil, fmt.Errorf("count failed shards: %w", err)
	}

	finishedAt := time.Now()
	updates := map[string]any{
		"finished_at": finishedAt,
	}

	if failedCount > 0 {
		resolvedLastError, err := resolveTaskFailureLastError(ctx, db, taskID, lastError)
		if err != nil {
			return nil, err
		}
		updates["status"] = model.TaskStatusFailed
		updates["last_error"] = resolvedLastError
		updated, err := updateTaskTerminalStatus(ctx, db, taskID, updates, terminalStatuses)
		if err != nil {
			return nil, err
		}
		if updated {
			return newCompletionHookPayload(task, model.TaskStatusFailed, resolvedLastError, &finishedAt, false), nil
		}
	} else {
		updates["status"] = model.TaskStatusSucceeded
		updates["last_error"] = ""
		updated, err := updateTaskTerminalStatus(ctx, db, taskID, updates, terminalStatuses)
		if err != nil {
			return nil, err
		}
		if updated {
			return newCompletionHookPayload(task, model.TaskStatusSucceeded, "", &finishedAt, true), nil
		}
	}

	return nil, nil
}

func resolveTaskFailureLastError(ctx context.Context, db *gorm.DB, taskID uuid.UUID, fallback string) (string, error) {
	var failedShard model.TaskShard
	err := db.WithContext(ctx).
		Select("id", "last_error", "finished_at", "updated_at").
		Where("task_id = ?", taskID).
		Where("status = ?", model.ShardStatusFailed).
		Where("last_error <> ''").
		Order("finished_at DESC").
		Order("updated_at DESC").
		First(&failedShard).Error
	if err == nil {
		return failedShard.LastError, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", fmt.Errorf("load failed shard last error: %w", err)
	}
	if fallback != "" {
		return fallback, nil
	}

	return "task failed because one or more shards failed", nil
}

func updateTaskTerminalStatus(
	ctx context.Context,
	db *gorm.DB,
	taskID uuid.UUID,
	updates map[string]any,
	terminalStatuses []string,
) (bool, error) {
	result := db.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ?", taskID).
		Where("status NOT IN ?", terminalStatuses).
		Updates(updates)
	if result.Error != nil {
		return false, fmt.Errorf("refresh task status: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		var current model.Task
		if err := db.WithContext(ctx).
			Select("id", "status").
			Where("id = ?", taskID).
			First(&current).Error; err != nil {
			return false, fmt.Errorf("refresh task status: %w", err)
		}
		switch current.Status {
		case model.TaskStatusCancelled:
			return false, fmt.Errorf("refresh task status: task %q is cancelled", taskID)
		case model.TaskStatusSucceeded, model.TaskStatusFailed:
			return false, nil
		default:
			return false, fmt.Errorf("refresh task status: task %q update conflicted; current status %q", taskID, current.Status)
		}
	}

	return true, nil
}
