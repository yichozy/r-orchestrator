package task_orm

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func ListTasksByTenant(ctx context.Context, db *gorm.DB, tenant_id uuid.UUID, status string) ([]model.Task, error) {
	query := db.WithContext(ctx).
		Preload("Shards", func(db *gorm.DB) *gorm.DB {
			return db.Order("script_name ASC")
		}).
		Where("tenant_id = ?", tenant_id).
		Order("created_at desc")
	if status != "" {
		query = query.Where("status = ?", status)
	}

	var tasks []model.Task
	if err := query.Find(&tasks).Error; err != nil {
		return nil, fmt.Errorf("list tasks by tenant: %w", err)
	}

	return tasks, nil
}
