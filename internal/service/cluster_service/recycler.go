package cluster_service

import (
	"context"
	"time"

	"github.com/yichozy/r-orchestrator/internal/config"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/cluster_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/service/cluster_service/backend"
	"go.uber.org/zap"
)

// RecycleClusters 定期扫描 ACTIVE cluster，管理空闲计时、续费和回收。
func RecycleClusters(ctx context.Context, registry *backend.Registry) {
	const interval = 30 * time.Second
	cfg := config.GlobalConfig.Cluster

	advanceThreshold := time.Duration(cfg.BillingAdvanceSeconds) * time.Second
	idleThreshold := time.Duration(cfg.IdleThresholdSeconds) * time.Second

	logger := zap.L().Named("cluster-recycler")

	logger.Info("cluster recycler started",
		zap.Duration("interval", interval),
		zap.Duration("advance_threshold", advanceThreshold),
		zap.Duration("idle_threshold", idleThreshold),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("cluster recycler stopped")
			return
		case <-ticker.C:
			db, err := orm.GetDB()
			if err != nil {
				logger.Error("get db failed", zap.Error(err))
				continue
			}

			clusters, err := cluster_orm.GetActiveClusterList(ctx, db)
			if err != nil {
				logger.Error("list active clusters failed", zap.Error(err))
				continue
			}

			for _, cluster := range clusters {
				if ctx.Err() != nil {
					return
				}

				log := logger.With(zap.Stringer("cluster_id", cluster.ID))

				// PROVISIONING cluster 尚未完成部署，跳过。
				if cluster.Status != string(model.ClusterStatusActive) {
					continue
				}

				activeCount, err := task_orm.CountActiveTasks(ctx, db, cluster.TenantID)
				if err != nil {
					log.Error("count active tasks failed", zap.Error(err))
					continue
				}

				if activeCount > 0 {
					if cluster.IdleSince != nil {
						if err := cluster_orm.UpdateIdleSince(ctx, db, cluster.ID, nil); err != nil {
							log.Error("clear idle_since failed", zap.Error(err))
						}
					}
					if IsExpired(cluster) {
						log.Info("expired cluster has activity, force renewing")
						if err := RenewBilling(ctx, db, cluster.ID); err != nil {
							log.Error("force renew billing failed", zap.Error(err))
						}
						continue
					}
					if IsNearBoundary(cluster, advanceThreshold) {
						log.Info("cluster near boundary with activity, renewing billing")
						if err := RenewBilling(ctx, db, cluster.ID); err != nil {
							log.Error("renew billing failed", zap.Error(err))
						}
					}
					continue
				}

				// 无活跃任务
				if cluster.IdleSince == nil {
					now := time.Now()
					if err := cluster_orm.UpdateIdleSince(ctx, db, cluster.ID, &now); err != nil {
						log.Error("set idle_since failed", zap.Error(err))
					}
					cluster.IdleSince = &now
					log.Info("cluster idle timer started")
				}

				if IsIdleExpired(cluster, idleThreshold) && (IsNearBoundary(cluster, advanceThreshold) || IsExpired(cluster)) {
					log.Info("terminating idle cluster")
					tenant := model.Tenant{BaseUUIDModel: model.BaseUUIDModel{ID: cluster.TenantID}}
					provider, err := registry.Get(cluster.BackendName)
					if err != nil {
						log.Error("get provider failed", zap.Error(err))
						continue
					}
					if err := provider.DestroyCluster(ctx, tenant); err != nil {
						log.Error("destroy cluster resource failed", zap.Error(err))
						continue
					}
					if err := TerminateCluster(ctx, db, cluster.ID); err != nil {
						log.Error("terminate cluster record failed after resource destroy, will retry", zap.Error(err))
						continue
					}
					log.Info("cluster terminated successfully")
					continue
				}

				if IsExpired(cluster) {
					log.Info("expired cluster within idle threshold, force renewing")
					if err := RenewBilling(ctx, db, cluster.ID); err != nil {
						log.Error("force renew billing failed", zap.Error(err))
					}
					continue
				}
				if IsNearBoundary(cluster, advanceThreshold) {
					log.Info("cluster near boundary within idle threshold, renewing billing")
					if err := RenewBilling(ctx, db, cluster.ID); err != nil {
						log.Error("renew billing failed", zap.Error(err))
					}
				}
			}
		}
	}
}
