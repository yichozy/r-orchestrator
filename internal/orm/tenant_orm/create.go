package tenant_orm

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func Create(ctx context.Context, db *gorm.DB, tenant model.Tenant) (model.Tenant, error) {
	name, err := NormalizeName(tenant.Name)
	if err != nil {
		return model.Tenant{}, err
	}
	tenant.Name = name
	if err := db.WithContext(ctx).Create(&tenant).Error; err != nil {
		return model.Tenant{}, err
	}
	return tenant, nil
}
