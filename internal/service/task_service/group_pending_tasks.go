package task_service

import (
	"context"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
)

// TenantPendingTasks represents a tenant's pending task IDs.
type TenantPendingTasks struct {
	TenantID uuid.UUID
	TaskIDs  []uuid.UUID
}

// GroupPendingTasks fetches all pending tasks and groups them by tenant.
func GroupPendingTasks(ctx context.Context) ([]TenantPendingTasks, error) {
	db, err := orm.GetDB()
	if err != nil {
		return nil, err
	}

	tasks, err := task_orm.GetPendingTaskList(ctx, db)
	if err != nil {
		return nil, err
	}

	groups := make(map[uuid.UUID][]uuid.UUID)
	for _, task := range tasks {
		groups[task.TenantID] = append(groups[task.TenantID], task.ID)
	}

	result := make([]TenantPendingTasks, 0, len(groups))
	for tenantID, taskIDs := range groups {
		result = append(result, TenantPendingTasks{TenantID: tenantID, TaskIDs: taskIDs})
	}
	return result, nil
}

// MarkTasksStatus batch-updates the given tasks to the target status.
func MarkTasksStatus(ctx context.Context, taskIDs []uuid.UUID, status string, lastError string) error {
	db, err := orm.GetDB()
	if err != nil {
		return err
	}
	return task_orm.MarkTasksStatus(ctx, db, taskIDs, status, lastError)
}
