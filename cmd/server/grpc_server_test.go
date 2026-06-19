package main

import (
	"fmt"
	"testing"

	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNewGRPCServerRegistersControlService(t *testing.T) {
	db := open_test_db(t)

	_, grpcServer := NewGRPCServer(db, agent_service.NewService(), "token-1")
	service_info := grpcServer.GetServiceInfo()
	if _, ok := service_info["rorchestrator.control.v1.ControlService"]; !ok {
		t.Fatalf("control service not registered: %#v", service_info)
	}
}

func open_test_db(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := db.AutoMigrate(&model.Artifact{}); err != nil {
		t.Fatalf("auto migrate artifact: %v", err)
	}

	return db
}
