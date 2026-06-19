package cluster_service

import (
	"context"
	"time"

	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/cluster_orm"
	agent_service "github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/cluster_service/backend"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ReapExpiringClusters 定期扫描 ACTIVE cluster，临近计费边界时决策续费或销毁。
func ReapExpiringClusters(
	ctx context.Context,
	db *gorm.DB,
	provider backend.Provider,
	agentSvc *agent_service.Service,
	interval time.Duration,
	billingAdvanceSeconds int,
) {
	if interval <= 0 {
		panic("reaper interval must be positive")
	}

	logger := zap.L().Named("cluster-reaper")
	threshold := time.Duration(billingAdvanceSeconds) * time.Second

	logger.Info("cluster reaper started",
		zap.Duration("interval", interval),
		zap.Duration("threshold", threshold),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("cluster reaper stopped")
			return
		case <-ticker.C:
			processExpiringClusters(ctx, db, provider, agentSvc, threshold)
		}
	}
}

func processExpiringClusters(
	ctx context.Context,
	db *gorm.DB,
	provider backend.Provider,
	agentSvc *agent_service.Service,
	threshold time.Duration,
) {
	clusters, err := cluster_orm.GetActiveClusterList(ctx, db)
	if err != nil {
		zap.L().Named("cluster-reaper").Error("list active clusters failed", zap.Error(err))
		return
	}

	for _, cluster := range clusters {
		if ctx.Err() != nil {
			return
		}
		evaluateCluster(ctx, db, provider, agentSvc, cluster, threshold)
	}
}

func evaluateCluster(
	ctx context.Context,
	db *gorm.DB,
	provider backend.Provider,
	agentSvc *agent_service.Service,
	cluster model.Cluster,
	threshold time.Duration,
) {
	logger := zap.L().Named("cluster-reaper").With(zap.Stringer("cluster_id", cluster.ID))

	// 已过期兜底评估
	if IsExpired(cluster) {
		logger.Warn("cluster billing boundary already passed, force evaluating")
		shouldTerm, err := ShouldTerminate(ctx, db, agentSvc, cluster)
		if err != nil {
			logger.Error("evaluate expired cluster failed", zap.Error(err))
			return
		}
		if shouldTerm {
			terminateClusterAndResource(ctx, db, provider, cluster, logger)
		} else {
			logger.Info("expired cluster has activity, force renewing")
			if err := RenewBilling(ctx, db, cluster.ID); err != nil {
				logger.Error("force renew billing failed", zap.Error(err))
			}
		}
		return
	}

	// 未临近边界，跳过
	if !IsNearBoundary(cluster, threshold) {
		return
	}

	// 临近边界，评估是否应终止
	shouldTerm, err := ShouldTerminate(ctx, db, agentSvc, cluster)
	if err != nil {
		logger.Error("evaluate cluster termination failed", zap.Error(err))
		return
	}

	if shouldTerm {
		terminateClusterAndResource(ctx, db, provider, cluster, logger)
	} else {
		logger.Info("cluster near boundary but has activity, renewing billing",
			zap.Time("next_boundary", cluster.NextBillingBoundaryAt),
		)
		if err := RenewBilling(ctx, db, cluster.ID); err != nil {
			logger.Error("renew billing failed", zap.Error(err))
		}
	}
}

func terminateClusterAndResource(
	ctx context.Context,
	db *gorm.DB,
	provider backend.Provider,
	cluster model.Cluster,
	logger *zap.Logger,
) {
	logger.Info("terminating cluster before billing boundary")

	if err := TerminateCluster(ctx, db, cluster.ID); err != nil {
		logger.Error("terminate cluster record failed", zap.Error(err))
		return
	}

	tenant := model.Tenant{BaseUUIDModel: model.BaseUUIDModel{ID: cluster.TenantID}}
	if err := provider.DestroyCluster(ctx, tenant); err != nil {
		logger.Error("destroy cluster resource failed", zap.Error(err))
		return
	}

	logger.Info("cluster terminated successfully")
}
