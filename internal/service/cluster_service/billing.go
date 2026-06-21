package cluster_service

import (
	"context"
	"time"

	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// IsNearBoundary 判断当前时间距下次计费边界是否在阈值内。
func IsNearBoundary(cluster model.Cluster, threshold time.Duration) bool {
	return time.Until(cluster.NextBillingBoundaryAt) <= threshold
}

// IsExpired 判断计费边界是否已过。
func IsExpired(cluster model.Cluster) bool {
	return time.Now().After(cluster.NextBillingBoundaryAt)
}

// IsIdleExpired 判断 cluster 空闲时间是否超过阈值。
func IsIdleExpired(cluster model.Cluster, threshold time.Duration) bool {
	if cluster.IdleSince == nil {
		return false
	}
	return time.Since(*cluster.IdleSince) >= threshold
}

// ShouldTerminate 判断 cluster 是否应被销毁：
//   - 无活跃任务
//   - 空闲时间超过阈值
func ShouldTerminate(ctx context.Context, db *gorm.DB, cluster model.Cluster, idleThreshold time.Duration) (bool, error) {
	activeCount, err := task_orm.CountActiveTasks(ctx, db, cluster.TenantID)
	if err != nil {
		return false, err
	}
	if activeCount > 0 {
		zap.L().Named("billing").Debug("cluster has active tasks, skip terminate",
			zap.Stringer("cluster_id", cluster.ID),
			zap.Int("active_tasks", int(activeCount)),
		)
		return false, nil
	}

	if !IsIdleExpired(cluster, idleThreshold) {
		zap.L().Named("billing").Debug("cluster idle but threshold not reached, skip terminate",
			zap.Stringer("cluster_id", cluster.ID),
		)
		return false, nil
	}

	return true, nil
}
