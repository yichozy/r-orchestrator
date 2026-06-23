package model

type Tenant struct {
	BaseUUIDModel
	Name               string `json:"name" gorm:"column:name;not null;uniqueIndex"`
	PrimaryBackendName string `json:"primary_backend_name" gorm:"column:primary_backend_name;not null"`
	MaxAgents          int    `json:"max_agents" gorm:"column:max_agents;not null"`
}

func (Tenant) TableName() string { return "tenants" }
