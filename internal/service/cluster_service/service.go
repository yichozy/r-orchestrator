package cluster_service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/cluster_orm"
	"gorm.io/gorm"
)

// MarkActive 将 cluster 状态更新为 ACTIVE。
func MarkActive(ctx context.Context, db *gorm.DB, clusterID uuid.UUID) error {
	return cluster_orm.UpdateStatus(ctx, db, clusterID, string(model.ClusterStatusActive))
}

// TerminateCluster 终止 cluster：更新状态为 TERMINATED。
func TerminateCluster(ctx context.Context, db *gorm.DB, clusterID uuid.UUID) error {
	return cluster_orm.UpdateStatus(ctx, db, clusterID, string(model.ClusterStatusTerminated))
}

// RenewBilling 续费：将 NextBillingBoundaryAt 推迟一个计费周期。
func RenewBilling(ctx context.Context, db *gorm.DB, clusterID uuid.UUID) error {
	var cluster model.Cluster
	if err := db.WithContext(ctx).First(&cluster, "id = ?", clusterID).Error; err != nil {
		return err
	}

	newBoundary := cluster.NextBillingBoundaryAt.Add(
		time.Duration(cluster.BillingCycleSeconds) * time.Second,
	)

	return db.Model(&model.Cluster{}).
		Where("id = ?", clusterID).
		Update("next_billing_boundary_at", newBoundary).Error
}

// GetByTenant 按 tenant_id 查询 cluster。
func GetByTenant(ctx context.Context, db *gorm.DB, tenantID uuid.UUID) (model.Cluster, error) {
	return cluster_orm.GetByTenant(ctx, db, tenantID)
}
