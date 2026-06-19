package control

import (
	"context"
	"time"

	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"go.uber.org/zap"
)

// ResetTimedOutShards periodically resets LEASED/RUNNING shards whose assigned
// agent has no active gRPC stream back to QUEUED.
func (server *Server) ResetTimedOutShards(ctx context.Context, interval, staleThreshold time.Duration) {
	if interval <= 0 || staleThreshold <= 0 {
		server.logger.Error("timed out shard reset disabled: invalid timing configuration",
			zap.Duration("interval", interval),
			zap.Duration("stale_threshold", staleThreshold),
		)
		return
	}

	server.logger.Info("timed out shard reset started",
		zap.Duration("interval", interval),
		zap.Duration("stale_threshold", staleThreshold),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			server.logger.Info("timed out shard reset stopped")
			return
		case <-ticker.C:
			activeAgentIDs := server.collectActiveAgentIDs()
			if server.db == nil {
				continue
			}
			threshold := time.Now().Add(-staleThreshold)
			rolled, err := task_shard_orm.ResetTimedOutShards(ctx, server.db, activeAgentIDs, threshold)
			if err != nil && ctx.Err() == nil {
				server.logger.Error("reset timed out shards failed", zap.Error(err))
			}
			if rolled > 0 {
				server.logger.Info("reset timed out shards", zap.Int("count", rolled))
			}
		}
	}
}
