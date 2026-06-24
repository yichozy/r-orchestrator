package task_shard_orm

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// RollbackShardLease rolls back a leased shard to QUEUED, clearing the agent
// assignment. Returns gorm.ErrRecordNotFound if the shard was not in LEASED
// status assigned to the given agent.
func RollbackShardLease(ctx context.Context, db *gorm.DB, shardID uuid.UUID, agentID string) error {
	result := db.WithContext(ctx).Model(&model.TaskShard{}).
		Where("id = ? AND status = ? AND assigned_agent_id = ?", shardID, model.ShardStatusLeased, agentID).
		Updates(map[string]any{
			"status":            model.ShardStatusQueued,
			"assigned_agent_id": "",
		})
	if result.Error != nil {
		return fmt.Errorf("rollback shard lease: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}
