package main

import (
	"github.com/yichozy/r-orchestrator/internal/control"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"google.golang.org/grpc"
	"gorm.io/gorm"
)

func NewGRPCServer(db *gorm.DB, agent_service *agent_service.Service, agent_token string) (*control.Server, *grpc.Server) {
	controlServer := control.NewServer(db, agent_service, agent_token)
	agent_service.SetTimeoutCallback(controlServer.HandleAgentTimeout)
	server := grpc.NewServer()
	controlv1.RegisterControlServiceServer(server, controlServer)
	return controlServer, server
}
