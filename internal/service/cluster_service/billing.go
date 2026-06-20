package cluster_service

import (
	"context"
	"time"

	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"go.uber.org/zap"
	"gorm.io/gorm"

	agent_service "github.com/yichozy/r-orchestrator/internal/service/agent_service"
)

// IsNearBoundary 判断当前时间距下次计费边界是否在阈值内。
func IsNearBoundary(cluster model.Cluster, threshold time.Duration) bool {
	return time.Until(cluster.NextBillingBoundaryAt) <= threshold
}

// IsExpired 判断计费边界是否已过。
func IsExpired(cluster model.Cluster) bool {
	return time.Now().After(cluster.NextBillingBoundaryAt)
}

// ShouldTerminate 判断 cluster 是否应被销毁：
//   - 该 tenant 无活跃任务
//   - 无已连接的 IDLE/RUNNING agent
func ShouldTerminate(ctx context.Context, db *gorm.DB, cluster model.Cluster) (bool, error) {
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

	activeTenants := agent_service.GetActiveTenantIDs()
	if activeTenants[cluster.TenantID] {
		zap.L().Named("billing").Debug("cluster has connected agents, skip terminate",
			zap.Stringer("cluster_id", cluster.ID),
		)
		return false, nil
	}

	return true, nil
}
