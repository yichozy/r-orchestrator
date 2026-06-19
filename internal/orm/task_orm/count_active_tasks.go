package task_orm

import (
	"context"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func CountActiveTasks(ctx context.Context, db *gorm.DB, tenantID uuid.UUID) (int64, error) {
	activeStatuses := []string{
		model.TaskStatusPending,
		model.TaskStatusWaitingForAgents,
		model.TaskStatusQueued,
		model.TaskStatusRunning,
	}
	var count int64
	if err := db.WithContext(ctx).
		Table("tasks").
		Where("tenant_id = ? AND status IN ?", tenantID, activeStatuses).
		Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}
