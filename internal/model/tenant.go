package model

type Tenant struct {
	BaseUUIDModel
	Name               string `gorm:"column:name;not null;uniqueIndex"`
	PrimaryBackendName string `gorm:"column:primary_backend_name;not null"`
	MaxAgents          int    `gorm:"column:max_agents;not null"`
}

func (Tenant) TableName() string { return "tenants" }
