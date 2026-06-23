package task_service

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// RollbackAssignedShard rolls back a leased shard to QUEUED within a transaction,
// including task rollback if the task is in WAITING_FOR_AGENTS status.
func RollbackAssignedShard(ctx context.Context, shardID uuid.UUID, task model.Task) error {
	db, err := orm.GetDB()
	if err != nil {
		return err
	}

	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return task_shard_orm.RollbackShardWithTask(ctx, tx, shardID, []string{model.ShardStatusLeased}, task)
	})
}

// RollbackOrphanShardsForAgent rolls back all active shards assigned to the given
// agent back to QUEUED. Individual shard rollback failures are logged but do not
// stop the loop. Returns the count of successfully rolled-back shards.
func RollbackOrphanShardsForAgent(ctx context.Context, agentID string) (int, error) {
	db, err := orm.GetDB()
	if err != nil {
		return 0, err
	}

	activeShards, err := task_shard_orm.GetActiveTaskShardByAgent(ctx, db, agentID)
	if err != nil {
		return 0, fmt.Errorf("load active shards for agent %s: %w", agentID, err)
	}

	rolledBack := 0
	for _, shard := range activeShards {
		zap.L().Named("task_service").Warn("rolling back orphaned shard on agent IDLE report",
			zap.String("agent_id", agentID),
			zap.Stringer("shard_id", shard.ID),
			zap.String("shard_status", shard.Status),
		)
		if rollbackErr := task_shard_orm.UpdateShardStatus(ctx, db, task_shard_orm.UpdateShardStatusParams{
			ShardID:         shard.ID,
			Status:          model.ShardStatusQueued,
			CurrentStatuses: []string{shard.Status},
			ClearAgent:      true,
		}); rollbackErr != nil {
			zap.L().Named("task_service").Error("failed to roll back orphaned shard",
				zap.Stringer("shard_id", shard.ID),
				zap.Error(rollbackErr),
			)
		} else {
			rolledBack++
		}
	}
	return rolledBack, nil
}

// RollbackTimedOutShard loads a validated shard and rolls it back to QUEUED
// within a transaction (including task rollback if applicable). Used when an
// agent heartbeat or grace timer fires.
func RollbackTimedOutShard(ctx context.Context, shardID uuid.UUID, agentID string, tenantID uuid.UUID, backendName string) error {
	shard, err := LoadValidatedShard(ctx, shardID, agentID, tenantID, backendName)
	if err != nil {
		return err
	}

	db, dbErr := orm.GetDB()
	if dbErr != nil {
		return dbErr
	}

	var task model.Task
	if err := db.WithContext(ctx).
		Model(&model.Task{}).
		Select("id", "status").
		Where("id = ?", shard.TaskID).
		First(&task).Error; err != nil {
		return fmt.Errorf("load task for timed-out shard rollback: %w", err)
	}

	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return task_shard_orm.RollbackShardWithTask(ctx, tx, shardID, []string{
			model.ShardStatusLeased, model.ShardStatusRunning,
		}, task)
	})
}
