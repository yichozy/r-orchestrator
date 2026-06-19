package tenant_orm

import (
	"context"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func GetById(ctx context.Context, db *gorm.DB, tenantID uuid.UUID) (model.Tenant, error) {
	var tenant model.Tenant
	if err := db.WithContext(ctx).Where("id = ?", tenantID).First(&tenant).Error; err != nil {
		return model.Tenant{}, err
	}
	return tenant, nil
}
