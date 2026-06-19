package task_service

import (
	"context"
	"errors"
	"fmt"

	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
	"gorm.io/gorm"
)

func resolveTenantByName(ctx context.Context, db *gorm.DB, tenantName string) (model.Tenant, error) {
	tenant, err := tenant_orm.GetByName(ctx, db, tenantName)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.Tenant{}, fmt.Errorf("%w: %s", ErrTenantNotFound, tenantName)
		}
		return model.Tenant{}, fmt.Errorf("query tenant: %w", err)
	}
	return tenant, nil
}
