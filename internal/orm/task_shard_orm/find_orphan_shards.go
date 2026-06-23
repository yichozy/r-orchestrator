package task_shard_orm

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// FindOrphanShards returns shards in LEASED/RUNNING whose
// assigned_agent_id is not in the given set of registered agent IDs.
func FindOrphanShards(ctx context.Context, db *gorm.DB, registeredAgentIDs []string) ([]model.TaskShard, error) {
	var shards []model.TaskShard
	query := db.WithContext(ctx).
		Where("status IN ?", []string{
			model.ShardStatusLeased, model.ShardStatusRunning,
		}).
		Where("assigned_agent_id IS NOT NULL AND assigned_agent_id != ''")

	if len(registeredAgentIDs) > 0 {
		query = query.Where("assigned_agent_id NOT IN ?", registeredAgentIDs)
	}

	err := query.Find(&shards).Error
	return shards, err
}
