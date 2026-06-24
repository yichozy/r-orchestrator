package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/gin-gonic/gin"
	"github.com/yichozy/r-orchestrator/graph"
	"github.com/yichozy/r-orchestrator/graph/generated"
	"github.com/yichozy/r-orchestrator/internal/config"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/cluster_service"
	"github.com/yichozy/r-orchestrator/internal/service/cluster_service/backend"
	k8s_backend "github.com/yichozy/r-orchestrator/internal/service/cluster_service/backend/k8"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger := config.InitLogger()
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	if err := config.LoadEnvVariable(); err != nil {
		logger.Error("load env failed", zap.Error(err))
		return
	}
	if err := config.InitGlobalConfig(); err != nil {
		logger.Error("init global config failed", zap.Error(err))
		return
	}
	cfg := config.GlobalConfig

	logger.Info("starting r-orchestrator",
		zap.String("http_addr", cfg.Server.HTTPAddr),
		zap.String("grpc_addr", cfg.Server.GRPCAddr),
		zap.String("agent_namespace", cfg.Cluster.Kubernetes.Namespace),
	)

	db, err := orm.Open(cfg.Database.URL)
	if err != nil {
		logger.Error("open db connection failed", zap.Error(err))
		return
	}
	if err := orm.AutoMigrate(db); err != nil {
		logger.Error("orm migration failed", zap.Error(err))
		return
	}

	agent_service.SetTimeouts(cfg.Cluster.AgentHeartbeatTimeout, cfg.Cluster.AgentDisconnectGrace)

	registry := backend.NewRegistry()
	k8s_provider, err := k8s_backend.NewK8sProvider(k8s_backend.Config{
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
		logger.Error("create k8s backend provider", zap.Error(err))
		return
	}
	registry.Register(string(backend.BackendKindKubernetes), k8s_provider)

	grpc_control_server, grpc_server := NewGRPCServer(db, cfg.Cluster.AgentToken)
	task_service.SetNotifyCancelShard(grpc_control_server.NotifyCancelShard)
	grpc_listener, err := net.Listen("tcp", cfg.Server.GRPCAddr)
	if err != nil {
		logger.Error("grpc server listen failed",
			zap.String("addr", cfg.Server.GRPCAddr),
			zap.Error(err),
		)
		return
	}

	graphql_handler := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{
		Resolvers: &graph.Resolver{},
	}))

	r := gin.New()
	r.GET("/healthz", func(context *gin.Context) {
		context.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	r.Any("/graphql", gin.WrapH(graphql_handler))

	http_server := &http.Server{
		Addr:    cfg.Server.HTTPAddr,
		Handler: r,
	}

	server_errs := make(chan error, 2)

	// One-time orphan shard cleanup on startup.
	if err := task_service.CleanupOrphanShards(ctx, 10*time.Minute); err != nil {
		logger.Error("startup orphan shard cleanup failed", zap.Error(err))
	}

	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		task_service.PollPendingTasks(ctx, registry)
	}()
	go func() {
		defer wg.Done()
		task_service.CleanupOrphanShardsLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		cluster_service.RecycleClusters(ctx, registry)
	}()
	go func() {
		defer wg.Done()
		logger.Info("gRPC server listening", zap.String("addr", cfg.Server.GRPCAddr))
		if err := grpc_server.Serve(grpc_listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			server_errs <- fmt.Errorf("serve control grpc: %w", err)
		}
	}()
	go func() {
		defer wg.Done()
		logger.Info("HTTP server listening", zap.String("addr", cfg.Server.HTTPAddr))
		if err := http_server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			server_errs <- fmt.Errorf("serve http: %w", err)
		}
	}()

	var serve_err error
	select {
	case <-ctx.Done():
	case serve_err = <-server_errs:
		cancel()
	}

	logger.Info("shutting down...")
	wg.Add(2)
	shutdown_ctx, shutdown_cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdown_cancel()

	go func() {
		defer wg.Done()
		if err := http_server.Shutdown(shutdown_ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Warn("shutdown http server error", zap.Error(err))
		}
	}()
	go func() {
		defer wg.Done()
		grpc_server.GracefulStop()
	}()
	time.AfterFunc(10*time.Second, func() {
		logger.Warn("shutdown timed out, forcing stop")
		grpc_server.Stop()
	})
	if err := grpc_listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		logger.Warn("close grpc listener error", zap.Error(err))
	}
	wg.Wait()
	if serve_err != nil {
		logger.Error("server run with error", zap.Error(serve_err))
		return
	}
}
