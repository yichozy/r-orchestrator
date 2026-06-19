package tenant_orm

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func GetByName(ctx context.Context, db *gorm.DB, name string) (model.Tenant, error) {
	normalized, err := NormalizeName(name)
	if err != nil {
		return model.Tenant{}, err
	}

	var tenant model.Tenant
	if err := db.WithContext(ctx).Where("name = ?", normalized).First(&tenant).Error; err != nil {
		return model.Tenant{}, err
	}
	return tenant, nil
}
