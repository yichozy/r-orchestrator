package tenant_orm

import (
	"context"
	"fmt"
	"strings"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func GetByName(ctx context.Context, db *gorm.DB, name string) (model.Tenant, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return model.Tenant{}, fmt.Errorf("tenant name is required")
	}

	var tenant model.Tenant
	if err := db.WithContext(ctx).Where("name = ?", name).First(&tenant).Error; err != nil {
		return model.Tenant{}, err
	}
	return tenant, nil
}
