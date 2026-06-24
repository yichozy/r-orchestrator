package task_shard_orm

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// MarkRunning transitions a shard from LEASED to RUNNING.
func MarkRunning(ctx context.Context, db *gorm.DB, shardID uuid.UUID) error {
	return transitionStatus(ctx, db, shardID, model.ShardStatusRunning,
		[]string{model.ShardStatusLeased},
		map[string]any{"started_at": time.Now()})
}

// MarkSucceeded transitions a shard from RUNNING to SUCCEEDED.
func MarkSucceeded(ctx context.Context, db *gorm.DB, shardID uuid.UUID) error {
	return transitionStatus(ctx, db, shardID, model.ShardStatusSucceeded,
		[]string{model.ShardStatusRunning},
		map[string]any{"finished_at": time.Now()})
}

// MarkFailed transitions a shard from RUNNING to FAILED.
func MarkFailed(ctx context.Context, db *gorm.DB, shardID uuid.UUID, lastError string) error {
	return transitionStatus(ctx, db, shardID, model.ShardStatusFailed,
		[]string{model.ShardStatusRunning},
		map[string]any{"finished_at": time.Now(), "last_error": lastError})
}

// RollbackToQueued transitions a shard to QUEUED and clears the agent
// assignment, guarded by currentStatuses.
func RollbackToQueued(ctx context.Context, db *gorm.DB, shardID uuid.UUID, currentStatuses []string) error {
	return transitionStatus(ctx, db, shardID, model.ShardStatusQueued,
		currentStatuses,
		map[string]any{"assigned_agent_id": nil})
}

// transitionStatus atomically sets a shard to targetStatus if its current
// status is in currentStatuses. The updates map carries additional column
// changes. Returns a descriptive error if the status guard fails.
func transitionStatus(ctx context.Context, db *gorm.DB, shardID uuid.UUID, targetStatus string, currentStatuses []string, updates map[string]any) error {
	updates["status"] = targetStatus
	result := db.WithContext(ctx).
		Model(&model.TaskShard{}).
		Where("id = ?", shardID).
		Where("status IN ?", currentStatuses).
		Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("transition shard to %s: %w", targetStatus, result.Error)
	}
	if result.RowsAffected == 0 {
		var shard model.TaskShard
		if err := db.WithContext(ctx).Select("status").Where("id = ?", shardID).First(&shard).Error; err != nil {
			return fmt.Errorf("transition shard to %s: %w", targetStatus, err)
		}
		return fmt.Errorf("transition shard to %s: shard %s is %s, expected one of %v", targetStatus, shardID, shard.Status, currentStatuses)
	}
	return nil
}
