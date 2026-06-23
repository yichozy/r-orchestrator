package task_shard_orm

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

type UpdateShardStatusParams struct {
	ShardID         uuid.UUID
	Status          string
	CurrentStatuses []string
	StartedAt       *time.Time
	FinishedAt      *time.Time
	LastError       *string
	AgentID         *uuid.UUID
	ClearAgent      bool
}

func UpdateShardStatus(ctx context.Context, db *gorm.DB, p UpdateShardStatusParams) error {
	if len(p.CurrentStatuses) == 0 {
		p.CurrentStatuses = defaultCurrentStatusesFor(p.Status)
	}

	updates := map[string]any{
		"status": p.Status,
	}

	if p.StartedAt != nil {
		updates["started_at"] = *p.StartedAt
	}
	if p.FinishedAt != nil {
		updates["finished_at"] = *p.FinishedAt
	}
	if p.LastError != nil {
		updates["last_error"] = *p.LastError
	}

	if p.ClearAgent {
		updates["assigned_agent_id"] = nil
	} else if p.AgentID != nil {
		updates["assigned_agent_id"] = *p.AgentID
	}

	query := db.WithContext(ctx).Model(&model.TaskShard{}).Where("id = ?", p.ShardID)
	if len(p.CurrentStatuses) > 0 {
		query = query.Where("status IN ?", p.CurrentStatuses)
	}

	result := query.Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("update shard status: %w", result.Error)
	}
	if len(p.CurrentStatuses) > 0 && result.RowsAffected == 0 {
		var shard model.TaskShard
		if err := db.WithContext(ctx).
			Select("id", "status").
			Where("id = ?", p.ShardID).
			First(&shard).Error; err != nil {
			return fmt.Errorf("update shard status: %w", err)
		}
		return fmt.Errorf("update shard status: shard %s is in status %s", p.ShardID, shard.Status)
	}
	return nil
}

func defaultCurrentStatusesFor(status string) []string {
	switch status {
	case model.ShardStatusRunning:
		return []string{model.ShardStatusLeased}
	case model.ShardStatusSucceeded, model.ShardStatusFailed:
		return []string{model.ShardStatusRunning}
	default:
		return nil
	}
}
