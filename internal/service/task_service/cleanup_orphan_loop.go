package task_service

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// CleanupOrphanShardsLoop periodically scans for shards in active states whose
// assigned agent is no longer registered. Stale shards exceeding the threshold
// are rolled back to QUEUED so they can be re-leased.
func CleanupOrphanShardsLoop(ctx context.Context) {
	const interval = 5 * time.Minute
	const threshold = 10 * time.Minute

	logger := zap.L().Named("orphan-cleanup")
	logger.Info("orphan shard cleanup loop started",
		zap.Duration("interval", interval),
		zap.Duration("threshold", threshold),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("orphan shard cleanup loop stopped")
			return
		case <-ticker.C:
			if err := CleanupOrphanShards(ctx, threshold); err != nil {
				logger.Error("orphan shard cleanup failed", zap.Error(err))
			}
		}
	}
}
