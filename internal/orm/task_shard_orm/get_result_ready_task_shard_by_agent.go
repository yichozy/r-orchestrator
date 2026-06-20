package task_shard_orm

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// GetResultReadyTaskShardByAgent returns the most recent RESULT_READY shard assigned to
// the given agent. Returns gorm.ErrRecordNotFound if none exists.
func GetResultReadyTaskShardByAgent(ctx context.Context, db *gorm.DB, agentID string) (model.TaskShard, error) {
	var shard model.TaskShard
	err := db.WithContext(ctx).
		Where("assigned_agent_id = ?", agentID).
		Where("status = ?", model.ShardStatusResultReady).
		Order("updated_at desc").
		Order("id asc").
		First(&shard).Error
	return shard, err
}
