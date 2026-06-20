package task_shard_orm

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// GetActiveTaskShardByAgent returns all non-terminal shards assigned to the given agent.
func GetActiveTaskShardByAgent(ctx context.Context, db *gorm.DB, agentID string) ([]model.TaskShard, error) {
	var shards []model.TaskShard
	err := db.WithContext(ctx).
		Where("assigned_agent_id = ?", agentID).
		Where("status IN ?", []string{model.ShardStatusLeased, model.ShardStatusRunning, model.ShardStatusResultReady}).
		Find(&shards).Error
	return shards, err
}
