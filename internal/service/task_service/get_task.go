package task_service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"gorm.io/gorm"
)

func GetTask(ctx context.Context, tenantName string, taskID uuid.UUID) (TaskView, error) {
	db, err := orm.GetDB()
	if err != nil {
		return TaskView{}, err
	}

	tenant, err := resolveTenantByName(ctx, db, tenantName)
	if err != nil {
		return TaskView{}, err
	}

	task, err := task_orm.GetByID(ctx, db, taskID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return TaskView{}, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
		}
		return TaskView{}, err
	}
	if task.TenantID != tenant.ID {
		return TaskView{}, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}

	return TaskView{
		ID:         task.ID,
		TenantName: tenant.Name,
		Status:     task.Status,
		CreatedAt:  task.CreatedAt,
		StartedAt:  task.StartedAt,
		FinishedAt: task.FinishedAt,
		ShardCount: task.ShardCount,
		LastError:  task.LastError,
	}, nil
}
