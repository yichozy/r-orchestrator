package task_service

import (
	"context"
	"fmt"
	"time"

	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
	cluster_service "github.com/yichozy/r-orchestrator/internal/service/cluster_service"
	"github.com/yichozy/r-orchestrator/internal/service/cluster_service/backend"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// PollPendingTasks 定期扫描 PENDING 任务，按 tenant 分组后确保 cluster 可用并调度执行。
func PollPendingTasks(
	ctx context.Context,
	db *gorm.DB,
	registry *backend.Registry,
	billingCycleSeconds int,
	interval time.Duration,
) {
	if interval <= 0 {
		panic("interval must be positive")
	}

	logger := zap.L().Named("task-poller")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("task poller stopped")
			return
		case <-ticker.C:
			processPendingTasks(ctx, db, registry, billingCycleSeconds)
		}
	}
}

func processPendingTasks(
	ctx context.Context,
	db *gorm.DB,
	registry *backend.Registry,
	billingCycleSeconds int,
) {
	groups, err := GroupPendingTasks(ctx)
	if err != nil {
		if ctx.Err() == nil {
			zap.L().Named("task-poller").Error("group pending tasks failed", zap.Error(err))
		}
		return
	}

	for _, group := range groups {
		if ctx.Err() != nil {
			return
		}

		tenant, err := tenant_orm.GetById(ctx, db, group.TenantID)
		if err != nil {
			zap.L().Named("task-poller").Error("get tenant failed",
				zap.Stringer("tenant_id", group.TenantID), zap.Error(err))
			return
		}

		zap.L().Named("task-poller").Info("processing tenant tasks",
			zap.Stringer("tenant_id", tenant.ID),
			zap.Int("tasks", len(group.TaskIDs)),
			zap.Int("max_agents", tenant.MaxAgents),
			zap.String("backend", tenant.PrimaryBackendName),
		)

		if tenant.MaxAgents <= 0 {
			zap.L().Named("task-poller").Info("max_agents is 0, skipping cluster launch",
				zap.Stringer("tenant_id", tenant.ID))
			if err := MarkTasksStatus(ctx, group.TaskIDs, model.TaskStatusWaitingForAgents, ""); err != nil {
				zap.L().Named("task-poller").Error("mark tasks waiting for agents failed", zap.Error(err))
			}
			continue
		}

		// 按 PrimaryBackendName 路由到对应后端 Provider
		provider, err := registry.Get(tenant.PrimaryBackendName)
		if err != nil {
			zap.L().Named("task-poller").Error("get backend provider failed",
				zap.Stringer("tenant_id", tenant.ID),
				zap.String("backend", tenant.PrimaryBackendName),
				zap.Error(err),
			)
			if markErr := MarkTasksStatus(ctx, group.TaskIDs, model.TaskStatusPending, err.Error()); markErr != nil {
				zap.L().Named("task-poller").Error("mark tasks scaling failed failed", zap.Error(markErr))
			}
			return
		}

		// 确保 cluster 存在
		_, err = cluster_service.EnsureCluster(ctx, db, group.TenantID, tenant.PrimaryBackendName, billingCycleSeconds)
		if err != nil {
			zap.L().Named("task-poller").Error("ensure cluster failed",
				zap.Stringer("tenant_id", group.TenantID),
				zap.Error(err),
			)
			if markErr := MarkTasksStatus(ctx, group.TaskIDs, model.TaskStatusPending, err.Error()); markErr != nil {
				zap.L().Named("task-poller").Error("mark tasks scaling failed failed", zap.Error(markErr))
			}
			return
		}

		// 后端资源操作（EnsureCluster / ScaleCluster）
		if err := provider.EnsureCluster(ctx, tenant); err != nil {
			zap.L().Named("task-poller").Error("ensure cluster resources failed",
				zap.Stringer("tenant_id", tenant.ID),
				zap.Error(err),
			)
			if markErr := MarkTasksStatus(ctx, group.TaskIDs, model.TaskStatusPending, fmt.Sprintf("ensure: %v", err)); markErr != nil {
				zap.L().Named("task-poller").Error("mark tasks scaling failed failed", zap.Error(markErr))
			}
			return
		}

		if err := provider.ScaleCluster(ctx, tenant, tenant.MaxAgents); err != nil {
			zap.L().Named("task-poller").Error("scale cluster failed",
				zap.Stringer("tenant_id", tenant.ID),
				zap.Int("replicas", tenant.MaxAgents),
				zap.Error(err),
			)
			if markErr := MarkTasksStatus(ctx, group.TaskIDs, model.TaskStatusPending, fmt.Sprintf("scale: %v", err)); markErr != nil {
				zap.L().Named("task-poller").Error("mark tasks scaling failed failed", zap.Error(markErr))
			}
			return
		}

		// 标记 cluster 为 ACTIVE（后端操作成功后立即更新）
		existingCluster, getErr := cluster_service.GetByTenant(ctx, db, group.TenantID)
		if getErr == nil {
			if err := cluster_service.MarkActive(ctx, db, existingCluster.ID); err != nil {
				zap.L().Named("task-poller").Warn("mark cluster active failed", zap.Error(err))
			}
		}

		if err := MarkTasksStatus(ctx, group.TaskIDs, model.TaskStatusWaitingForAgents, ""); err != nil {
			zap.L().Named("task-poller").Error("mark tasks waiting for agents failed", zap.Error(err))
			return
		}

		zap.L().Named("task-poller").Info("tenant tasks provisioned",
			zap.Stringer("tenant_id", tenant.ID),
			zap.Int("tasks", len(group.TaskIDs)),
		)
	}
}
