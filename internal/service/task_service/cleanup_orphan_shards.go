package task_service

import (
	"context"
	"time"

	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"go.uber.org/zap"
)

// CleanupOrphanShards performs a one-time scan of DB shards that are in active
// states but whose assigned agent has no registered agent in memory. Shards
// whose started_at exceeds the orphan threshold are rolled back to QUEUED.
func CleanupOrphanShards(ctx context.Context, threshold time.Duration) error {
	db, err := orm.GetDB()
	if err != nil {
		return err
	}

	registeredAgents := agent_service.GetAllAgentIDs()
	result, err := task_shard_orm.CleanupOrphanShards(ctx, db, registeredAgents, threshold)
	if err != nil {
		return err
	}

	if result.RolledBack > 0 {
		zap.L().Named("task_service").Info("startup orphan shard cleanup completed",
			zap.Int("rolled_back", result.RolledBack),
		)
	}
	return nil
}
