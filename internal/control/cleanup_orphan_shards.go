package control

import (
	"context"
	"errors"
	"time"

	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// CleanupOrphanShards performs a one-time scan of DB shards that are in active
// states but whose assigned agent has no registered agent in memory. Shards
// whose started_at exceeds the orphan threshold are rolled back to QUEUED.
func (server *Server) CleanupOrphanShards(ctx context.Context, orphanThreshold time.Duration) error {
	if server.db == nil {
		return nil
	}

	registeredAgents := server.agentService.GetAllAgentIDs()
	shards, err := task_shard_orm.FindOrphanShards(ctx, server.db, registeredAgents)
	if err != nil {
		return err
	}

	threshold := time.Now().Add(-orphanThreshold)
	rolled := 0
	for _, shard := range shards {
		if shard.StartedAt != nil && shard.StartedAt.Before(threshold) {
			server.logger.Warn("cleaning up orphan shard on startup",
				zap.Stringer("shard_id", shard.ID),
				zap.String("agent_id", shard.AssignedAgentID),
				zap.Time("started_at", *shard.StartedAt),
			)
			var task model.Task
			if err := server.db.WithContext(ctx).
				Model(&model.Task{}).
				Select("id", "status").
				Where("id = ?", shard.TaskID).
				First(&task).Error; err != nil {
				server.logger.Error("failed to load orphan shard task",
					zap.Stringer("shard_id", shard.ID),
					zap.Error(err),
				)
				continue
			}
			if err := server.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				if err := task_shard_orm.UpdateShardStatus(ctx, tx, task_shard_orm.UpdateShardStatusParams{
					ShardID:         shard.ID,
					Status:          model.ShardStatusQueued,
					CurrentStatuses: []string{model.ShardStatusLeased, model.ShardStatusRunning, model.ShardStatusResultReady},
					ClearAgent:      true,
				}); err != nil {
					return err
				}
				if task.Status == model.TaskStatusWaitingForAgents {
					if err := task_orm.RollbackTaskToWaiting(ctx, tx, task.ID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
						return err
					}
				}
				return nil
			}); err != nil {
				server.logger.Error("failed to rollback orphan shard",
					zap.Stringer("shard_id", shard.ID),
					zap.Error(err),
				)
				continue
			}
			rolled++
		}
	}

	if rolled > 0 {
		server.logger.Info("startup orphan shard cleanup completed",
			zap.Int("rolled_back", rolled),
		)
	}
	return nil
}
