package model

import (
	"time"

	"github.com/google/uuid"
)

type ClusterStatus string

const (
	ClusterStatusProvisioning ClusterStatus = "PROVISIONING"
	ClusterStatusActive       ClusterStatus = "ACTIVE"
	ClusterStatusTerminated   ClusterStatus = "TERMINATED"
)

type Cluster struct {
	BaseUUIDModel
	TenantID              uuid.UUID `gorm:"column:tenant_id;uniqueIndex;not null;type:uuid"`
	Status                string    `gorm:"column:status;not null"`
	ProviderKind          string    `gorm:"column:provider_kind;not null"`
	BillingCycleSeconds   int       `gorm:"column:billing_cycle_seconds;not null;default:3600"`
	NextBillingBoundaryAt time.Time `gorm:"column:next_billing_boundary_at;not null"`
}

func (Cluster) TableName() string { return "clusters" }
