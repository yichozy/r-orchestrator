package cluster_service

import (
	"context"
	"time"

	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/cluster_orm"
	"github.com/yichozy/r-orchestrator/internal/service/cluster_service/backend"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// RecycleClusters 定期扫描 ACTIVE cluster，空闲时销毁回收，临近计费边界时续费。
func RecycleClusters(
	ctx context.Context,
	db *gorm.DB,
	provider backend.Provider,
	interval time.Duration,
	billingAdvanceSeconds int,
) {
	if interval <= 0 {
		panic("recycler interval must be positive")
	}

	logger := zap.L().Named("cluster-recycler")
	threshold := time.Duration(billingAdvanceSeconds) * time.Second

	logger.Info("cluster recycler started",
		zap.Duration("interval", interval),
		zap.Duration("threshold", threshold),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("cluster recycler stopped")
			return
		case <-ticker.C:
			processExpiringClusters(ctx, db, provider, threshold)
		}
	}
}

func processExpiringClusters(
	ctx context.Context,
	db *gorm.DB,
	provider backend.Provider,
	threshold time.Duration,
) {
	clusters, err := cluster_orm.GetActiveClusterList(ctx, db)
	if err != nil {
		zap.L().Named("cluster-recycler").Error("list active clusters failed", zap.Error(err))
		return
	}

	for _, cluster := range clusters {
		if ctx.Err() != nil {
			return
		}
		evaluateCluster(ctx, db, provider, cluster, threshold)
	}
}

func evaluateCluster(
	ctx context.Context,
	db *gorm.DB,
	provider backend.Provider,
	cluster model.Cluster,
	threshold time.Duration,
) {
	logger := zap.L().Named("cluster-recycler").With(zap.Stringer("cluster_id", cluster.ID))

	// PROVISIONING cluster 尚未完成部署，跳过 idle 检查。
	if cluster.Status != string(model.ClusterStatusActive) {
		return
	}

	// 无论是否临近计费边界，空闲 cluster 应立即回收。
	shouldTerm, err := ShouldTerminate(ctx, db, cluster)
	if err != nil {
		logger.Error("evaluate cluster termination failed", zap.Error(err))
		return
	}
	if shouldTerm {
		terminateClusterAndResource(ctx, db, provider, cluster, logger)
		return
	}

	// cluster 有活跃任务或已连接 agent，检查是否需要续费。
	if IsExpired(cluster) {
		logger.Info("expired cluster has activity, force renewing")
		if err := RenewBilling(ctx, db, cluster.ID); err != nil {
			logger.Error("force renew billing failed", zap.Error(err))
		}
		return
	}

	// 未临近边界，跳过续费评估
	if !IsNearBoundary(cluster, threshold) {
		return
	}

	// 临近边界且有活跃任务，续费
	logger.Info("cluster near boundary but has activity, renewing billing",
		zap.Time("next_boundary", cluster.NextBillingBoundaryAt),
	)
	if err := RenewBilling(ctx, db, cluster.ID); err != nil {
		logger.Error("renew billing failed", zap.Error(err))
	}
}

func terminateClusterAndResource(
	ctx context.Context,
	db *gorm.DB,
	provider backend.Provider,
	cluster model.Cluster,
	logger *zap.Logger,
) {
	logger.Info("terminating idle cluster")

	// 先销毁 K8s 资源，成功后再更新 DB 状态。
	// 如果 K8s 销毁失败，DB 保留 ACTIVE，下次 recycler 会重试。
	tenant := model.Tenant{BaseUUIDModel: model.BaseUUIDModel{ID: cluster.TenantID}}
	if err := provider.DestroyCluster(ctx, tenant); err != nil {
		logger.Error("destroy cluster resource failed", zap.Error(err))
		return
	}

	if err := TerminateCluster(ctx, db, cluster.ID); err != nil {
		// K8s 已销毁但 DB 更新失败。DestroyCluster 是幂等的，下次 recycler 会重试销毁。
		logger.Error("terminate cluster record failed after resource destroy, will retry", zap.Error(err))
		return
	}

	logger.Info("cluster terminated successfully")
}
