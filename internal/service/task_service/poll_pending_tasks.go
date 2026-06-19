package task_service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/config"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/cluster_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
	cluster_service "github.com/yichozy/r-orchestrator/internal/service/cluster_service"
	"github.com/yichozy/r-orchestrator/internal/service/cluster_service/backend"
	"go.uber.org/zap"
)

// PollPendingTasks 定期扫描 PENDING 任务，按 tenant 分组后确保 cluster 可用并调度执行。
func PollPendingTasks(ctx context.Context, registry *backend.Registry) {
	interval := 5 * time.Second
	billingCycleSeconds := config.GlobalConfig.Cluster.BillingCycleSeconds
	if interval <= 0 {
		panic("interval must be positive")
	}

	logger := zap.L().Named("task-poller")
	db, err := orm.GetDB()
	if err != nil {
		logger.Error("get db failed", zap.Error(err))
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("task poller stopped")
			return
		case <-ticker.C:
			pending_tasks, err := task_orm.GetPendingTaskList(ctx, db)
			if err != nil {
				if ctx.Err() == nil {
					logger.Error("get pending tasks failed", zap.Error(err))
				}
				continue
			}

			// 按 tenant 分组
			groups := make(map[uuid.UUID][]uuid.UUID)
			for _, task := range pending_tasks {
				groups[task.TenantID] = append(groups[task.TenantID], task.ID)
			}

			for tenant_id, task_ids := range groups {
				if ctx.Err() != nil {
					continue
				}

				tenant, err := tenant_orm.GetById(ctx, db, tenant_id)
				if err != nil {
					logger.Error("get tenant failed",
						zap.Stringer("tenant_id", tenant_id), zap.Error(err))
					continue
				}

				logger.Info("processing tenant tasks",
					zap.Stringer("tenant_id", tenant.ID),
					zap.Int("tasks", len(task_ids)),
					zap.Int("max_agents", tenant.MaxAgents),
					zap.String("backend", tenant.PrimaryBackendName),
				)

				if tenant.MaxAgents <= 0 {
					logger.Info("max_agents is 0, marking tasks failed",
						zap.Stringer("tenant_id", tenant.ID))
					if err := task_orm.MarkTasksStatus(ctx, db, task_ids, model.TaskStatusFailed, "tenant max_agents is 0"); err != nil {
						logger.Error("mark tasks failed failed", zap.Error(err))
					}
					continue
				}

				provider, err := registry.Get(tenant.PrimaryBackendName)
				if err != nil {
					logger.Error("get backend provider failed",
						zap.Stringer("tenant_id", tenant.ID),
						zap.String("backend", tenant.PrimaryBackendName),
						zap.Error(err),
					)
					if markErr := task_orm.MarkTasksStatus(ctx, db, task_ids, model.TaskStatusPending, err.Error()); markErr != nil {
						logger.Error("mark tasks scaling failed", zap.Error(markErr))
					}
					continue
				}

				_, err = cluster_orm.GetOrCreateByTenant(ctx, db, model.Cluster{
					TenantID:              tenant.ID,
					ProviderKind:          tenant.PrimaryBackendName,
					Status:                string(model.ClusterStatusProvisioning),
					BillingCycleSeconds:  billingCycleSeconds,
					NextBillingBoundaryAt: time.Now().Add(time.Duration(billingCycleSeconds) * time.Second),
				})
				if err != nil {
					logger.Error("create cluster failed",
						zap.Stringer("tenant_id", tenant.ID),
						zap.Error(err),
					)
					if markErr := task_orm.MarkTasksStatus(ctx, db, task_ids, model.TaskStatusPending, err.Error()); markErr != nil {
						logger.Error("mark tasks scaling failed", zap.Error(markErr))
					}
					continue
				}

				if err := provider.ProvisionCluster(ctx, tenant); err != nil {
					logger.Error("ensure cluster resources failed",
						zap.Stringer("tenant_id", tenant.ID),
						zap.Error(err),
					)
					if markErr := task_orm.MarkTasksStatus(ctx, db, task_ids, model.TaskStatusPending, fmt.Sprintf("ensure: %v", err)); markErr != nil {
						logger.Error("mark tasks scaling failed", zap.Error(markErr))
					}
					continue
				}

				if err := provider.ScaleCluster(ctx, tenant, tenant.MaxAgents); err != nil {
					logger.Error("scale cluster failed",
						zap.Stringer("tenant_id", tenant.ID),
						zap.Int("replicas", tenant.MaxAgents),
						zap.Error(err),
					)
					if markErr := task_orm.MarkTasksStatus(ctx, db, task_ids, model.TaskStatusPending, fmt.Sprintf("scale: %v", err)); markErr != nil {
						logger.Error("mark tasks scaling failed", zap.Error(markErr))
					}
					continue
				}

				existingCluster, getErr := cluster_service.GetByTenant(ctx, db, tenant.ID)
				if getErr == nil {
					if err := cluster_service.MarkActive(ctx, db, existingCluster.ID); err != nil {
						logger.Warn("mark cluster active failed", zap.Error(err))
					}
				}

				if err := task_orm.MarkTasksStatus(ctx, db, task_ids, model.TaskStatusWaitingForAgents, ""); err != nil {
					logger.Error("mark tasks waiting for agents failed", zap.Error(err))
					continue
				}

				logger.Info("tenant tasks provisioned",
					zap.Stringer("tenant_id", tenant.ID),
					zap.Int("tasks", len(task_ids)),
				)
			}
		}
	}
}
