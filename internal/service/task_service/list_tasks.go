package task_service

import (
	"context"
	"fmt"

	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
)

func ListTasks(ctx context.Context, tenantName string, status string) ([]TaskView, error) {
	db, err := orm.GetDB()
	if err != nil {
		return nil, err
	}

	tenant, err := tenant_orm.GetByName(ctx, db, tenantName)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrTenantNotFound, tenantName)
	}

	tasks, err := task_orm.ListTasksByTenant(ctx, db, tenant.ID, status)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return []TaskView{}, nil
	}

	result := make([]TaskView, 0, len(tasks))
	for _, task := range tasks {
		result = append(result, TaskView{
			ID:         task.ID,
			TenantName: tenant.Name,
			Status:     task.Status,
			CreatedAt:  task.CreatedAt,
			StartedAt:  task.StartedAt,
			FinishedAt: task.FinishedAt,
			ShardCount: task.ShardCount,
			LastError:  task.LastError,
		})
	}

	return result, nil
}
