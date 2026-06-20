package cluster_orm

import (
	"context"
	"strings"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// GetOrCreateByTenant 按 tenant_id 查询最新的非 TERMINATED Cluster 记录，
// 若不存在则创建一条新记录。若 terminated cluster 已存在（unique key 冲突），
// 则将其状态更新为 PROVISIONING 并复用。
func GetOrCreateActiveCluster(ctx context.Context, db *gorm.DB, cluster model.Cluster) (model.Cluster, error) {
	existing, err := GetByTenant(ctx, db, cluster.TenantID)
	if err == nil {
		return existing, nil
	}

	created, createErr := Create(ctx, db, cluster)
	if createErr == nil {
		return created, nil
	}

	// If Create failed with a duplicate key on tenant_id, a terminated
	// cluster may exist. Fetch it without the status filter.
	if strings.Contains(createErr.Error(), "duplicate key") {
		var found model.Cluster
		fetchErr := db.WithContext(ctx).
			Where("tenant_id = ?", cluster.TenantID).
			Order("created_at DESC").
			First(&found).Error
		if fetchErr != nil {
			return model.Cluster{}, createErr // return original create error
		}
		if found.Status == string(model.ClusterStatusTerminated) {
			if reactivateErr := db.WithContext(ctx).
				Model(&model.Cluster{}).
				Where("id = ?", found.ID).
				Updates(map[string]interface{}{
					"status":                   cluster.Status,
					"provider_kind":            cluster.ProviderKind,
					"billing_cycle_seconds":    cluster.BillingCycleSeconds,
					"next_billing_boundary_at": cluster.NextBillingBoundaryAt,
				}).Error; reactivateErr != nil {
				return model.Cluster{}, reactivateErr
			}
			found.Status = cluster.Status
			found.ProviderKind = cluster.ProviderKind
			found.BillingCycleSeconds = cluster.BillingCycleSeconds
			found.NextBillingBoundaryAt = cluster.NextBillingBoundaryAt
		}
		return found, nil
	}

	return model.Cluster{}, createErr
}

