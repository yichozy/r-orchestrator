package control

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type Server struct {
	controlv1.UnimplementedControlServiceServer
	db            *gorm.DB
	expectedToken string
	logger        *zap.Logger
	streams       sync.Map // map[string]*agentStream
}

func NewServer(db *gorm.DB, expectedToken string) *Server {
	return &Server{
		db:            db,
		expectedToken: expectedToken,
		logger:        zap.L().Named("control"),
	}
}

func (server *Server) OpenControlStream(stream grpc.BidiStreamingServer[controlv1.AgentMessage, controlv1.ServerMessage]) error {
	first_message, err := stream.Recv()
	if err == io.EOF {
		return status.Error(codes.InvalidArgument, "register message is required")
	}
	if err != nil {
		return status.Errorf(codes.Internal, "recv first control message: %v", err)
	}

	register := first_message.GetRegister()
	if register == nil {
		return status.Error(codes.InvalidArgument, "first control message must be register")
	}

	agentID := register.GetAgentId()
	if agentID == "" {
		return status.Error(codes.InvalidArgument, "agent_id is required")
	}

	tenantID, err := uuid.Parse(register.GetTenantId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid tenant_id: %v", err)
	}

	if register.GetToken() != server.expectedToken {
		return status.Error(codes.Unauthenticated, fmt.Errorf("tenant %s token mismatch", tenantID).Error())
	}

	if err := agent_service.RegisterAgent(agentID, tenantID, register.GetBackendName()); err != nil {
		server.logger.Warn("agent registration rejected",
			zap.String("agent_id", agentID),
			zap.Stringer("tenant_id", tenantID),
			zap.Error(err),
		)
		if errors.Is(err, agent_service.ErrAgentIdentityConflict) {
			return status.Error(codes.PermissionDenied, err.Error())
		}
		return status.Errorf(codes.Internal, "register agent: %v", err)
	}

	// Stop any leftover grace timer from a previous session, start heartbeat timer.
	agent_service.CancelTimer(agentID)
	agent_service.ResetHeartbeat(agentID)

	streamRef := &agentStream{stream: stream}
	server.streams.Store(agentID, streamRef)
	defer func() {
		server.streams.Delete(agentID)
		agent_service.BeginGrace(agentID)
		agent_service.DisconnectAgent(agentID)
	}()

	server.logger.Info("agent connected",
		zap.String("agent_id", agentID),
		zap.Stringer("tenant_id", tenantID),
		zap.String("backend", register.GetBackendName()),
	)

	sess := &agentSession{
		stream:   streamRef,
		server:   server,
		agentID:  agentID,
		tenantID: tenantID,
		backend:  register.GetBackendName(),
	}

	registered_agent, err := agent_service.GetAgent(agentID)
	if err != nil {
		return status.Errorf(codes.Internal, "get registered agent: %v", err)
	}
	registered_agent, err = server.HandleReconnectedAgent(sess, registered_agent)
	if err != nil {
		return err
	}

	server.logger.Info("agent ready",
		zap.String("agent_id", agentID),
		zap.String("status", registered_agent.Status),
	)

	for {
		if err := stream.Context().Err(); err != nil {
			server.logger.Info("agent stream cancelled",
				zap.String("agent_id", agentID),
				zap.Error(err),
			)
			return err
		}

		message, err := stream.Recv()
		if err == io.EOF {
			server.logger.Info("agent disconnected",
				zap.String("agent_id", agentID),
				zap.Stringer("tenant_id", tenantID),
				zap.String("reason", "eof"),
			)
			return nil
		}
		if err != nil {
			server.logger.Warn("agent stream error, disconnecting",
				zap.String("agent_id", agentID),
				zap.Stringer("tenant_id", tenantID),
				zap.Error(err),
			)
			return status.Errorf(codes.Internal, "recv control message: %v", err)
		}

		if heartbeat := message.GetHeartbeat(); heartbeat != nil {
			if err := server.HandleHeartbeat(sess, heartbeat); err != nil {
				return err
			}
			continue
		}

		if shard_accepted := message.GetShardAccepted(); shard_accepted != nil {
			server.logger.Debug("shard accepted",
				zap.String("agent_id", agentID),
				zap.String("shard_id", shard_accepted.GetShardId()),
			)
			continue
		}

		if shard_started := message.GetShardStarted(); shard_started != nil {
			if err := server.HandleShardStarted(sess, shard_started); err != nil {
				return err
			}
			continue
		}

		if shard_ready := message.GetShardResultReady(); shard_ready != nil {
			if err := server.HandleShardResultReady(sess, shard_ready); err != nil {
				return err
			}
			continue
		}

		if shard_failed := message.GetShardFailed(); shard_failed != nil {
			if err := server.HandleShardFailed(sess, shard_failed); err != nil {
				return err
			}
			continue
		}

		return status.Error(codes.InvalidArgument, "unsupported control message")
	}
}
