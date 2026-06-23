//go:build e2e

package e2e

import (
	"context"
	"log"
	"net"
	"os"
	"testing"

	"github.com/yichozy/r-orchestrator/internal/config"
	"github.com/yichozy/r-orchestrator/internal/control"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"gorm.io/gorm"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/cluster_service"
	k8s_backend "github.com/yichozy/r-orchestrator/internal/service/cluster_service/backend/k8"
	"github.com/yichozy/r-orchestrator/internal/service/cluster_service/backend"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	testDB            *gorm.DB
	testServer        *grpc.Server
	testControlServer *control.Server
	testGrpcAddr      string
	testGrpcLis       net.Listener
	testCtx           context.Context
	testCancel        context.CancelFunc
	registry         *backend.Registry
)

func TestMain(m *testing.M) {
	if os.Getenv("ENV") == "" {
		os.Setenv("ENV", "dev")
	}

	logger := config.InitLogger()
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	if err := config.LoadEnvVariable(); err != nil {
		log.Fatalf("load env: %v", err)
	}
	if err := config.InitGlobalConfig(); err != nil {
		log.Fatalf("init global config: %v", err)
	}
	cfg := config.GlobalConfig

	logger.Info("e2e: connecting to database",
		zap.String("host", cfg.Database.Host),
		zap.String("db", cfg.Database.DBName),
	)

	db, err := orm.Open(cfg.Database.URL)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	if err := orm.AutoMigrate(db); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	orm.SetTestDB(db)
	testDB = db

	agent_service.Init()

	registry = backend.NewRegistry()
	k8sProvider, err := k8s_backend.NewK8sProvider(k8s_backend.Config{
		Namespace:        cfg.Cluster.Kubernetes.Namespace,
		AgentImage:       cfg.Cluster.AgentImage,
		ImagePullSecrets: cfg.Cluster.Kubernetes.ImagePullSecrets,
		ServerGRPCAddr:   cfg.Server.GRPCPublicAddr,
		AgentToken:       cfg.Cluster.AgentToken,
		ServerPublicURL:  cfg.Server.PublicURL,
		KubeConfigPath:   cfg.Cluster.Kubernetes.KubeConfigPath,
		AgentLogLevel: cfg.Cluster.AgentLogLevel,
	})
	if err != nil {
		log.Fatalf("create k8s provider: %v", err)
	}
	registry.Register(string(backend.BackendKindKubernetes), k8sProvider)

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	testGrpcLis = lis
	testGrpcAddr = lis.Addr().String()

	testControlServer = control.NewServer(db, cfg.Cluster.AgentToken)
	task_service.SetNotifyCancelShard(testControlServer.NotifyCancelShard)

	testServer = grpc.NewServer()
	controlv1.RegisterControlServiceServer(testServer, testControlServer)
	go testServer.Serve(lis)

	logger.Info("e2e: server started",
		zap.String("grpc_addr", testGrpcAddr),
	)

	testCtx, testCancel = context.WithCancel(context.Background())

	// Start background goroutines.
	go task_service.PollPendingTasks(testCtx, registry)
	go cluster_service.RecycleClusters(testCtx, registry)

	code := m.Run()

	testCancel()
	testServer.GracefulStop()
	testGrpcLis.Close()

	sqlDB, _ := db.DB()
	if sqlDB != nil {
		sqlDB.Close()
	}

	os.Exit(code)
}
