package cluster_orm

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// ceilToHour 将时间向上取整到下一个整点。
func ceilToHour(t time.Time) time.Time {
	return t.Truncate(time.Hour).Add(time.Hour)
}

// Create 在数据库中创建一条 Cluster 记录。
func Create(ctx context.Context, db *gorm.DB, tenantID uuid.UUID, providerKind string, billingCycleSeconds int) (model.Cluster, error) {
	if billingCycleSeconds <= 0 {
		billingCycleSeconds = 3600
	}

	now := time.Now().UTC()
	cluster := model.Cluster{
		TenantID:              tenantID,
		Status:                string(model.ClusterStatusProvisioning),
		ProviderKind:          providerKind,
		BillingCycleSeconds:   billingCycleSeconds,
		NextBillingBoundaryAt: ceilToHour(now),
	}

	if err := db.WithContext(ctx).Create(&cluster).Error; err != nil {
		return model.Cluster{}, err
	}

	return cluster, nil
}
