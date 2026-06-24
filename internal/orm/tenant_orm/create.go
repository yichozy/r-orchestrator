package tenant_orm

import (
	"context"
	"fmt"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func Create(ctx context.Context, db *gorm.DB, tenant model.Tenant) (model.Tenant, error) {
	if tenant.Name == "" {
		return model.Tenant{}, fmt.Errorf("tenant name is required")
	}
	if err := db.WithContext(ctx).Create(&tenant).Error; err != nil {
		return model.Tenant{}, err
	}
	return tenant, nil
}
