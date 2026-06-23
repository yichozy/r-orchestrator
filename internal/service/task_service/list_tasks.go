package task_service

import (
	"context"
	"fmt"

	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
)

func ListTasks(ctx context.Context, tenantName string, status string) ([]model.Task, error) {
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

	return tasks, nil
}
