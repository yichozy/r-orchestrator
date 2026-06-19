//go:build integration

package integration

import (
	"fmt"
	"log"
	"net"
	"os"
	"testing"

	"github.com/yichozy/r-orchestrator/internal/config"
	"github.com/yichozy/r-orchestrator/internal/control"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var (
	testDB            *gorm.DB
	testServer        *grpc.Server
	testControlServer *control.Server
	testGrpcAddr      string
	testGrpcLis       net.Listener
	agentToken        string
)

func TestMain(m *testing.M) {
	if os.Getenv("ENV") == "" {
		os.Setenv("ENV", "dev")
	}

	logger := config.InitLogger()
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", "integration-test")
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("open sqlite db: %v", err)
	}
	orm.SetTestDB(db)
	if err := orm.AutoMigrate(db); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	testDB = db

	agentToken = "test-agent-token"
	agentSvc := agent_service.NewService()

	// Start gRPC server on random port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	testGrpcLis = lis
	testGrpcAddr = lis.Addr().String()

	controlServer := control.NewServer(db, agentSvc, agentToken)
	testControlServer = controlServer

	testServer = grpc.NewServer()
	controlv1.RegisterControlServiceServer(testServer, controlServer)
	go testServer.Serve(lis)

	code := m.Run()

	testServer.GracefulStop()
	testGrpcLis.Close()
	sqlDB, _ := db.DB()
	if sqlDB != nil {
		sqlDB.Close()
	}

	os.Exit(code)
}
