package task_shard_orm

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ShardTaskOwner holds the tenant and backend ownership for a shard.
type ShardTaskOwner struct {
	TaskID             uuid.UUID
	TenantID           uuid.UUID
	PrimaryBackendName string
}

// GetShardTaskOwner loads the task→tenant ownership chain for a shard.
// Returns gorm.ErrRecordNotFound if the shard's task or tenant doesn't exist.
func GetShardTaskOwner(ctx context.Context, db *gorm.DB, shardTaskID uuid.UUID) (ShardTaskOwner, error) {
	var owner ShardTaskOwner
	err := db.WithContext(ctx).Raw(`
		SELECT tasks.id as task_id, tasks.tenant_id, tenants.primary_backend_name
		FROM tasks
		JOIN tenants ON tenants.id = tasks.tenant_id
		WHERE tasks.id = ?
	`, shardTaskID).Scan(&owner).Error
	if err != nil {
		return ShardTaskOwner{}, fmt.Errorf("get shard task owner: %w", err)
	}
	return owner, nil
}
