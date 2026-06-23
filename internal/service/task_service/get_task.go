package task_service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
	"gorm.io/gorm"
)

func GetTask(ctx context.Context, tenantName string, taskID uuid.UUID) (model.Task, error) {
	db, err := orm.GetDB()
	if err != nil {
		return model.Task{}, err
	}

	tenant, err := tenant_orm.GetByName(ctx, db, tenantName)
	if err != nil {
		return model.Task{}, fmt.Errorf("%w: %s", ErrTenantNotFound, tenantName)
	}

	task, err := task_orm.GetByID(ctx, db, taskID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.Task{}, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
		}
		return model.Task{}, err
	}
	if task.TenantID != tenant.ID {
		return model.Task{}, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}

	return task, nil
}
