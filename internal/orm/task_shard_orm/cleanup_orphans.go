package task_shard_orm

import (
	"context"
	"time"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// CleanupOrphanResult contains the count of shards rolled back.
type CleanupOrphanResult struct {
	RolledBack int
}

// CleanupOrphanShards finds shards in active states whose assigned agent is
// not in the registeredAgents list and whose started_at exceeds the threshold.
// Each matching shard is rolled back to QUEUED within a transaction that also
// rolls back the parent task if applicable.
func CleanupOrphanShards(ctx context.Context, db *gorm.DB, registeredAgentIDs []string, threshold time.Duration) (CleanupOrphanResult, error) {
	shards, err := FindOrphanShards(ctx, db, registeredAgentIDs)
	if err != nil {
		return CleanupOrphanResult{}, err
	}

	cutoff := time.Now().Add(-threshold)
	result := CleanupOrphanResult{}

	for _, shard := range shards {
		if shard.StartedAt != nil && shard.StartedAt.Before(cutoff) {
			var task model.Task
			if err := db.WithContext(ctx).
				Model(&model.Task{}).
				Select("id", "status").
				Where("id = ?", shard.TaskID).
				First(&task).Error; err != nil {
				continue
			}
			if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				return RollbackShardWithTask(ctx, tx, shard.ID, []string{
					model.ShardStatusLeased, model.ShardStatusRunning, model.ShardStatusResultReady,
				}, task)
			}); err != nil {
				continue
			}
			result.RolledBack++
		}
	}

	return result, nil
}
