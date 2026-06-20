package control

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/artifact_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
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
	agentService  *agent_service.Service
	expectedToken string
	logger        *zap.Logger
	streams       sync.Map // map[string]*agentStream
	storeOutputFn func(ctx context.Context, tx *gorm.DB, tenantID, shardID uuid.UUID, outputCSV []byte) error
}

func NewServer(db *gorm.DB, agent_service *agent_service.Service, expected_token string) *Server {
	return &Server{
		db:            db,
		agentService:  agent_service,
		expectedToken: expected_token,
		logger:        zap.L().Named("control"),
	}
}

func (server *Server) SetStoreShardOutputFunc(fn func(ctx context.Context, tx *gorm.DB, tenantID, shardID uuid.UUID, outputCSV []byte) error) {
	server.storeOutputFn = fn
}

func (server *Server) OpenControlStream(stream grpc.BidiStreamingServer[controlv1.AgentMessage, controlv1.ServerMessage]) error {
	var current_agent_id string
	var current_tenant_id uuid.UUID
	var current_backend_name string

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

	current_agent_id = register.GetAgentId()
	if current_agent_id == "" {
		return status.Error(codes.InvalidArgument, "agent_id is required")
	}

	tenantID, err := uuid.Parse(register.GetTenantId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid tenant_id: %v", err)
	}

	if register.GetToken() != server.expectedToken {
		return status.Error(codes.Unauthenticated, fmt.Errorf("tenant %s token mismatch", tenantID).Error())
	}

	if err := server.agentService.RegisterAgent(agent_service.RegisterAgentParams{
		AgentID:     current_agent_id,
		TenantID:    tenantID,
		BackendName: register.GetBackendName(),
	}); err != nil {
		server.logger.Warn("agent registration rejected",
			zap.String("agent_id", current_agent_id),
			zap.Stringer("tenant_id", tenantID),
			zap.Error(err),
		)
		if errors.Is(err, agent_service.ErrAgentIdentityConflict) {
			return status.Error(codes.PermissionDenied, err.Error())
		}
		return status.Errorf(codes.Internal, "register agent: %v", err)
	}
	current_tenant_id = tenantID
	current_backend_name = register.GetBackendName()

	// Stop any leftover grace timer from a previous session, start heartbeat timer.
	server.agentService.StopTimer(current_agent_id)
	server.agentService.ResetHeartbeatTimer(current_agent_id)

	streamRef := &agentStream{stream: stream}
	server.streams.Store(current_agent_id, streamRef)
	defer func() {
		server.streams.Delete(current_agent_id)
		server.agentService.StartGraceTimer(current_agent_id)
		server.agentService.DisconnectAgent(current_agent_id)
	}()

	server.logger.Info("agent connected",
		zap.String("agent_id", current_agent_id),
		zap.Stringer("tenant_id", current_tenant_id),
		zap.String("backend", current_backend_name),
	)

	registered_agent, err := server.agentService.GetAgent(current_agent_id)
	if err != nil {
		return status.Errorf(codes.Internal, "get registered agent: %v", err)
	}
	registered_agent, err = server.HandleReconnectedAgent(streamRef, registered_agent, current_agent_id, current_tenant_id, current_backend_name)
	if err != nil {
		return err
	}

	// If reconnection restored a RESULT_READY agent, fetch the result now.
	if registered_agent.Status == agent_service.AgentStatusResultReady && registered_agent.CurrentShardID != nil {
		if err := streamRef.Send(&controlv1.ServerMessage{Payload: &controlv1.ServerMessage_FetchShardResult{FetchShardResult: &controlv1.FetchShardResult{ShardId: *registered_agent.CurrentShardID}}}); err != nil {
			return err
		}
	}

	for {
		if err := stream.Context().Err(); err != nil {
			server.logger.Info("agent stream cancelled",
				zap.String("agent_id", current_agent_id),
				zap.Error(err),
			)
			return err
		}

		message, err := stream.Recv()
		if err == io.EOF {
			server.logger.Info("agent disconnected",
				zap.String("agent_id", current_agent_id),
				zap.Stringer("tenant_id", current_tenant_id),
				zap.String("reason", "eof"),
			)
			return nil
		}
		if err != nil {
			server.logger.Warn("agent stream error, disconnecting",
				zap.String("agent_id", current_agent_id),
				zap.Stringer("tenant_id", current_tenant_id),
				zap.Error(err),
			)
			return status.Errorf(codes.Internal, "recv control message: %v", err)
		}

		if heartbeat := message.GetHeartbeat(); heartbeat != nil {
			if err := server.HandleHeartbeat(streamRef, stream, heartbeat, current_agent_id, current_tenant_id, current_backend_name); err != nil {
				return err
			}
			continue
		}

		if shard_accepted := message.GetShardAccepted(); shard_accepted != nil {
			server.logger.Debug("shard accepted",
				zap.String("agent_id", current_agent_id),
				zap.String("shard_id", shard_accepted.GetShardId()),
			)
			continue
		}

		if shard_started := message.GetShardStarted(); shard_started != nil {
			if err := server.HandleShardStarted(streamRef, stream, shard_started, current_agent_id, current_tenant_id, current_backend_name); err != nil {
				return err
			}
			continue
		}

		if shard_ready := message.GetShardResultReady(); shard_ready != nil {
			if err := server.HandleShardResultReady(streamRef, stream, shard_ready, current_agent_id, current_tenant_id, current_backend_name); err != nil {
				return err
			}
			continue
		}

		if shard_data := message.GetShardResultData(); shard_data != nil {
			if err := server.HandleShardResultData(streamRef, stream, shard_data, current_agent_id, current_tenant_id, current_backend_name); err != nil {
				return err
			}
			continue
		}

		if shard_completed := message.GetShardCompleted(); shard_completed != nil {
			return status.Error(codes.InvalidArgument, "ShardCompleted is deprecated; use ShardResultReady and ShardResultData")
		}

		if shard_failed := message.GetShardFailed(); shard_failed != nil {
			if err := server.HandleShardFailed(streamRef, stream, shard_failed, current_agent_id, current_tenant_id, current_backend_name); err != nil {
				return err
			}
			continue
		}

		return status.Error(codes.InvalidArgument, "unsupported control message")
	}
}

// completeCurrentWorkAndReassign acks the current shard result, resets the agent
// to IDLE, and tries to assign the next shard.
func (server *Server) completeCurrentWorkAndReassign(
	stream *agentStream,
	agentID string,
	tenantID uuid.UUID,
	backendName string,
	shardID string,
) error {
	if err := stream.Send(&controlv1.ServerMessage{Payload: &controlv1.ServerMessage_ShardResultStored{ShardResultStored: &controlv1.ShardResultStored{ShardId: shardID}}}); err != nil {
		return err
	}
	if err := server.agentService.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        agentID,
		Status:         agent_service.AgentStatusIdle,
		CurrentShardID: nil,
	}); err != nil {
		return status.Errorf(codes.Internal, "update agent idle state: %v", err)
	}
	if err := server.tryAssignShard(stream, agentID, tenantID, backendName); err != nil {
		return err
	}
	return nil
}

func (server *Server) rollbackAssignedShard(ctx context.Context, task model.Task, shard model.TaskShard, agentID string) error {
	if err := server.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := task_shard_orm.RollbackShardLease(ctx, tx, shard.ID, agentID); err != nil {
			return err
		}
		if task.Status == model.TaskStatusWaitingForAgents {
			if err := task_orm.RollbackTaskToWaiting(ctx, tx, task.ID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := server.agentService.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        agentID,
		Status:         agent_service.AgentStatusIdle,
		CurrentShardID: nil,
	}); err != nil {
		return fmt.Errorf("rollback agent heartbeat: %w", err)
	}
	return nil
}
func (server *Server) loadValidatedShard(
	ctx context.Context,
	shardID uuid.UUID,
	agentID string,
	tenantID uuid.UUID,
	backendName string,
) (model.TaskShard, error) {
	if server.db == nil {
		return model.TaskShard{}, status.Error(codes.FailedPrecondition, "db is not configured")
	}
	if shardID == uuid.Nil {
		return model.TaskShard{}, status.Error(codes.InvalidArgument, "shard_id is required")
	}

	shard, err := task_shard_orm.GetByID(ctx, server.db, shardID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.TaskShard{}, status.Errorf(codes.NotFound, "shard %s not found", shardID)
		}
		return model.TaskShard{}, status.Errorf(codes.Internal, "load shard %s: %v", shardID, err)
	}
	if shard.AssignedAgentID != agentID {
		return model.TaskShard{}, status.Errorf(codes.PermissionDenied, "shard %s is assigned to agent %s, not %s", shardID, shard.AssignedAgentID, agentID)
	}

	owner, err := task_shard_orm.GetShardTaskOwner(ctx, server.db, shard.TaskID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.TaskShard{}, status.Errorf(codes.NotFound, "task %s not found", shard.TaskID)
		}
		return model.TaskShard{}, status.Errorf(codes.Internal, "load shard task %s: %v", shard.TaskID, err)
	}
	if owner.TenantID != tenantID {
		return model.TaskShard{}, status.Errorf(codes.PermissionDenied, "shard %s belongs to tenant %s, not %s", shardID, owner.TenantID, tenantID)
	}
	if owner.PrimaryBackendName != backendName {
		return model.TaskShard{}, status.Errorf(codes.PermissionDenied, "shard %s belongs to backend %s, not %s", shardID, owner.PrimaryBackendName, backendName)
	}

	return shard, nil
}

func (server *Server) validateShardReport(
	ctx context.Context,
	shard_id uuid.UUID, current_agent_id string, current_tenant_id uuid.UUID, current_backend_name, required_status, action string,
) error {
	shard, err := server.loadValidatedShard(ctx, shard_id, current_agent_id, current_tenant_id, current_backend_name)
	if err != nil {
		return err
	}
	if shard.Status != required_status {
		return status.Errorf(codes.FailedPrecondition, "%s requires shard %s to be %s, got %s", action, shard_id, required_status, shard.Status)
	}

	return nil
}

func (server *Server) validateShardResultDataReport(
	ctx context.Context,
	shard_id uuid.UUID,
	current_agent_id string,
	current_tenant_id uuid.UUID,
	current_backend_name string,
) (bool, error) {
	shard, err := server.loadValidatedShard(ctx, shard_id, current_agent_id, current_tenant_id, current_backend_name)
	if err != nil {
		return false, err
	}

	switch shard.Status {
	case model.ShardStatusResultReady:
		return false, nil
	case model.ShardStatusSucceeded:
		hasOutput, err := artifact_orm.ExistsShardOutput(ctx, server.db, shard.TaskID, shard.ShardIndex)
		if err != nil {
			return false, status.Errorf(codes.Internal, "check shard output artifact: %v", err)
		}
		if hasOutput {
			return true, nil
		}
	case model.ShardStatusCancelled, model.ShardStatusFailed:
		// Shard was cancelled or failed while the agent was executing.
		// Treat as duplicate ack so the agent receives ShardResultStored
		// and transitions to IDLE gracefully.
		return true, nil
	}

	return false, status.Errorf(codes.FailedPrecondition, "store shard result data requires shard %s to be %s, got %s", shard_id, model.ShardStatusResultReady, shard.Status)
}
