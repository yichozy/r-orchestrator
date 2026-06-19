package control

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/artifact_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	"github.com/yichozy/r-orchestrator/internal/util"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type Server struct {
	controlv1.UnimplementedControlServiceServer
	db             *gorm.DB
	agent_service  *agent_service.Service
	expected_token string
	logger         *zap.Logger
	streams        sync.Map // map[string]*agentStream
	storeOutputFn  func(ctx context.Context, tx *gorm.DB, tenantID, shardID uuid.UUID, outputCSV []byte) error
}

type agentStream struct {
	stream grpc.BidiStreamingServer[controlv1.AgentMessage, controlv1.ServerMessage]
	sendMu sync.Mutex
}

func (stream *agentStream) Send(message *controlv1.ServerMessage) error {
	stream.sendMu.Lock()
	defer stream.sendMu.Unlock()
	return stream.stream.Send(message)
}

func (stream *agentStream) Context() context.Context {
	return stream.stream.Context()
}

func (stream *agentStream) SendWithContext(ctx context.Context, message *controlv1.ServerMessage) error {
	result := make(chan error, 1)
	go func() {
		result <- stream.Send(message)
	}()

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func NewServer(db *gorm.DB, agent_service *agent_service.Service, expected_token string) *Server {
	return &Server{
		db:             db,
		agent_service:  agent_service,
		expected_token: expected_token,
		logger:         zap.L().Named("control"),
	}
}

func ValidateAgentToken(tenantID uuid.UUID, got, want string) error {
	if got != want {
		return fmt.Errorf("tenant %s token mismatch", tenantID)
	}
	return nil
}

func (server *Server) SetStoreShardOutputFunc(fn func(ctx context.Context, tx *gorm.DB, tenantID, shardID uuid.UUID, outputCSV []byte) error) {
	server.storeOutputFn = fn
}

func ValidateMetadataToken(ctx context.Context, want string) error {
	got, err := metadataToken(ctx)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("agent token mismatch")
	}
	return nil
}

func metadataToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("authorization metadata is required")
	}

	for _, value := range md.Get("authorization") {
		if token, ok := strings.CutPrefix(value, "Bearer "); ok && token != "" {
			return token, nil
		}
	}
	for _, key := range []string{"x-agent-token", "agent-token"} {
		if values := md.Get(key); len(values) > 0 && values[0] != "" {
			return values[0], nil
		}
	}
	return "", fmt.Errorf("authorization metadata is required")
}

func (server *Server) OpenControlStream(stream grpc.BidiStreamingServer[controlv1.AgentMessage, controlv1.ServerMessage]) error {
	if server.agent_service == nil {
		return status.Error(codes.FailedPrecondition, "agent service is not configured")
	}

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

	if err := ValidateAgentToken(tenantID, register.GetToken(), server.expected_token); err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}

	if err := server.agent_service.RegisterAgent(agent_service.RegisterAgentParams{
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

	streamRef := &agentStream{stream: stream}
	server.streams.Store(current_agent_id, streamRef)
	defer func() {
		server.streams.Delete(current_agent_id)
		server.agent_service.DisconnectAgent(current_agent_id)
	}()

	server.logger.Info("agent connected",
		zap.String("agent_id", current_agent_id),
		zap.Stringer("tenant_id", current_tenant_id),
		zap.String("backend", current_backend_name),
	)

	registered_agent, err := server.agent_service.GetAgent(current_agent_id)
	if err != nil {
		return status.Errorf(codes.Internal, "get registered agent: %v", err)
	}
	registered_agent, err = server.reconcileRegisteredAgentState(stream.Context(), registered_agent)
	if err != nil {
		return err
	}
	if shouldTryAssign(registered_agent) {
		restored, err := server.restoreResultReadyAgentFromDB(stream.Context(), current_agent_id, current_tenant_id, current_backend_name)
		if err != nil {
			return err
		}
		if restored {
			registered_agent, err = server.agent_service.GetAgent(current_agent_id)
			if err != nil {
				return status.Errorf(codes.Internal, "get restored agent: %v", err)
			}
		}
	}
	if registered_agent.Status == agent_service.AgentStatusResultReady && registered_agent.CurrentShardID != nil {
		if err := server.sendFetchShardResult(streamRef, *registered_agent.CurrentShardID); err != nil {
			return err
		}
	} else if shouldTryAssign(registered_agent) {
		if err := server.try_assign_shard(streamRef, current_agent_id, current_tenant_id, current_backend_name); err != nil {
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
			server.logger.Debug("received heartbeat from agent",
				zap.String("agent_id", current_agent_id),
				zap.String("status", heartbeat.GetStatus()),
			)
			if heartbeat.GetAgentId() != "" && heartbeat.GetAgentId() != current_agent_id {
				return status.Error(codes.InvalidArgument, "heartbeat agent_id does not match registered agent")
			}
			if heartbeat.GetStatus() == "IDLE" {
				if err := server.ensure_agent_can_become_idle(stream.Context(), current_agent_id); err != nil {
					return err
				}
			}

			var shardID *string
			if sid := heartbeat.GetCurrentShardId(); sid != "" {
				shardID = &sid
			}

			if err := server.agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
				AgentID:        current_agent_id,
				Status:         heartbeat.GetStatus(),
				CurrentShardID: shardID,
			}); err != nil {
				if errors.Is(err, agent_service.ErrAgentNotFound) {
					return status.Error(codes.NotFound, err.Error())
				}
				return status.Errorf(codes.Internal, "heartbeat agent: %v", err)
			}

			// Refresh shard updated_at so the stale reaper doesn't roll back
			// actively-executing shards.
			if shardID != nil && *shardID != "" && server.db != nil {
				parsedTouch, parseErr := uuid.Parse(*shardID)
				if parseErr == nil && (heartbeat.GetStatus() == agent_service.AgentStatusRunning ||
					heartbeat.GetStatus() == agent_service.AgentStatusResultReady) {
					if _, touchErr := task_shard_orm.TouchShardUpdatedAt(stream.Context(), server.db, parsedTouch, []string{
						model.ShardStatusRunning,
						model.ShardStatusResultReady,
					}); touchErr != nil {
						server.logger.Warn("failed to touch shard updated_at",
							zap.Stringer("shard_id", parsedTouch),
							zap.Error(touchErr),
						)
					}
				}
			}

			if heartbeat.GetStatus() == agent_service.AgentStatusResultReady {
				if shardID == nil || *shardID == "" {
					return status.Error(codes.InvalidArgument, "result-ready heartbeat requires current_shard_id")
				}
				parsedShardID, parseErr := uuid.Parse(*shardID)
				if parseErr != nil {
					return status.Errorf(codes.InvalidArgument, "invalid shard_id in result-ready heartbeat: %v", parseErr)
				}
				if server.db != nil {
					var shard model.TaskShard
					if dbErr := server.db.WithContext(stream.Context()).Where("id = ?", parsedShardID).First(&shard).Error; dbErr != nil {
						if !errors.Is(dbErr, gorm.ErrRecordNotFound) {
							return status.Errorf(codes.Internal, "lookup shard %s for result-ready heartbeat: %v", *shardID, dbErr)
						}
						continue
					} else {
						switch shard.Status {
						case model.ShardStatusResultReady:
							if err := server.sendFetchShardResult(streamRef, *shardID); err != nil {
								return err
							}
						case model.ShardStatusSucceeded:
							// Shard was already stored but the agent lost the
							// ShardResultStored ack. Re-send the ack so the
							// agent can transition to IDLE and accept new work.
							if err := server.sendShardResultStored(streamRef, *shardID); err != nil {
								return err
							}
							if err := server.agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
								AgentID:        current_agent_id,
								Status:         agent_service.AgentStatusIdle,
								CurrentShardID: nil,
							}); err != nil {
								return status.Errorf(codes.Internal, "reset agent to idle after result-ready ack on succeeded shard: %v", err)
							}
							if err := server.try_assign_shard(streamRef, current_agent_id, current_tenant_id, current_backend_name); err != nil {
							return err
							}
						}
					}
				}
				continue
			}
			if heartbeat.GetStatus() == agent_service.AgentStatusIdle {
				if err := server.try_assign_shard(streamRef, current_agent_id, current_tenant_id, current_backend_name); err != nil {
					return err
				}
			}
			continue
		}

		if shard_accepted := message.GetShardAccepted(); shard_accepted != nil {
			server.logger.Info("shard accepted",
				zap.String("agent_id", current_agent_id),
				zap.String("shard_id", shard_accepted.GetShardId()),
			)
			continue
		}

		if shard_started := message.GetShardStarted(); shard_started != nil {
			shardID, err := uuid.Parse(shard_started.GetShardId())
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid shard_id: %v", err)
			}
			shardIDStr := shardID.String()
			if err := server.validate_shard_report(stream.Context(), shardID, current_agent_id, current_tenant_id, current_backend_name, model.ShardStatusLeased, "mark shard started"); err != nil {
				return err
			}
			if err := task_service.ReportShardStatus(stream.Context(), task_service.ReportShardStatusParams{ShardID: shardID, ShardStatus: model.ShardStatusRunning}); err != nil {
				return status.Errorf(codes.Internal, "mark shard started: %v", err)
			}
			if err := server.agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
				AgentID:        current_agent_id,
				Status:         model.ShardStatusRunning,
				CurrentShardID: &shardIDStr,
			}); err != nil {
				return status.Errorf(codes.Internal, "update agent running state: %v", err)
			}
			server.logger.Info("shard started",
				zap.String("agent_id", current_agent_id),
				zap.Stringer("shard_id", shardID),
			)
			continue
		}

		if shard_ready := message.GetShardResultReady(); shard_ready != nil {
			shardID, err := uuid.Parse(shard_ready.GetShardId())
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid shard_id: %v", err)
			}
			shardIDStr := shardID.String()
			if err := server.validate_shard_report(stream.Context(), shardID, current_agent_id, current_tenant_id, current_backend_name, model.ShardStatusRunning, "mark shard result ready"); err != nil {
				return err
			}
			if err := task_service.ReportShardStatus(stream.Context(), task_service.ReportShardStatusParams{
				ShardID:     shardID,
				ShardStatus: model.ShardStatusResultReady,
			}); err != nil {
				return status.Errorf(codes.Internal, "mark shard result ready: %v", err)
			}
			if err := server.agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
				AgentID:        current_agent_id,
				Status:         agent_service.AgentStatusResultReady,
				CurrentShardID: &shardIDStr,
			}); err != nil {
				return status.Errorf(codes.Internal, "update agent result-ready state: %v", err)
			}
			server.logger.Info("shard result ready",
				zap.String("agent_id", current_agent_id),
				zap.Stringer("shard_id", shardID),
			)
			if err := server.sendFetchShardResult(streamRef, shardID.String()); err != nil {
				return err
			}
			continue
		}

		if shard_data := message.GetShardResultData(); shard_data != nil {
			shardID, err := uuid.Parse(shard_data.GetShardId())
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid shard_id: %v", err)
			}
			duplicateAck, err := server.validate_shard_result_data_report(stream.Context(), shardID, current_agent_id, current_tenant_id, current_backend_name)
			if err != nil {
				return err
			}
			if duplicateAck {
				if err := server.sendShardResultStored(streamRef, shardID.String()); err != nil {
					return err
				}
				if err := server.agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
					AgentID:        current_agent_id,
					Status:         agent_service.AgentStatusIdle,
					CurrentShardID: nil,
				}); err != nil {
					return status.Errorf(codes.Internal, "update agent idle state after duplicate shard result data: %v", err)
				}
				if err := server.try_assign_shard(streamRef, current_agent_id, current_tenant_id, current_backend_name); err != nil {
					return err
				}
				continue
			}
			var storeShardOutputFn task_service.StoreShardOutputFunc
			if server.storeOutputFn != nil {
				storeShardOutputFn = func(ctx context.Context, tx *gorm.DB, task model.Task, shard model.TaskShard, outputCSV []byte) error {
					return server.storeOutputFn(ctx, tx, task.TenantID, shard.ID, outputCSV)
				}
			}
			if err := task_service.ReportShardStatus(stream.Context(), task_service.ReportShardStatusParams{
				ShardID:            shardID,
				ShardStatus:        model.ShardStatusSucceeded,
				OutputCSV:          shard_data.GetOutputCsv(),
				StoreShardOutputFn: storeShardOutputFn,
			}); err != nil {
				return status.Errorf(codes.Internal, "store shard result data: %v", err)
			}
			server.logger.Info("shard result stored",
				zap.String("agent_id", current_agent_id),
				zap.Stringer("shard_id", shardID),
			)
			if err := server.sendShardResultStored(streamRef, shardID.String()); err != nil {
				return err
			}
			if err := server.agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
				AgentID:        current_agent_id,
				Status:         agent_service.AgentStatusIdle,
				CurrentShardID: nil,
			}); err != nil {
				return status.Errorf(codes.Internal, "update agent idle state: %v", err)
			}
			if err := server.try_assign_shard(streamRef, current_agent_id, current_tenant_id, current_backend_name); err != nil {
				return err
			}
			continue
		}

		if shard_completed := message.GetShardCompleted(); shard_completed != nil {
			return status.Error(codes.InvalidArgument, "ShardCompleted is deprecated; use ShardResultReady and ShardResultData")
		}

		if shard_failed := message.GetShardFailed(); shard_failed != nil {
			shardID, err := uuid.Parse(shard_failed.GetShardId())
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid shard_id: %v", err)
			}
			if err := server.validate_shard_report(stream.Context(), shardID, current_agent_id, current_tenant_id, current_backend_name, model.ShardStatusRunning, "mark shard failed"); err != nil {
				return err
			}
			errMsg := shard_failed.GetErrorMessage()
			if err := task_service.ReportShardStatus(stream.Context(), task_service.ReportShardStatusParams{
				ShardID:      shardID,
				ShardStatus:  model.ShardStatusFailed,
				ErrorMessage: &errMsg,
			}); err != nil {
				return status.Errorf(codes.Internal, "mark shard failed: %v", err)
			}
			if err := server.agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
				AgentID:        current_agent_id,
				Status:         agent_service.AgentStatusIdle,
				CurrentShardID: nil,
			}); err != nil {
				return status.Errorf(codes.Internal, "update agent idle state: %v", err)
			}
			server.logger.Warn("shard failed",
				zap.String("agent_id", current_agent_id),
				zap.Stringer("shard_id", shardID),
				zap.String("error", shard_failed.GetErrorMessage()),
			)
			if err := server.try_assign_shard(streamRef, current_agent_id, current_tenant_id, current_backend_name); err != nil {
				return err
			}
			continue
		}

		return status.Error(codes.InvalidArgument, "unsupported control message")
	}
}

func (server *Server) FetchArtifact(request *controlv1.FetchArtifactRequest, stream grpc.ServerStreamingServer[controlv1.FetchArtifactChunk]) error {
	if server.agent_service == nil {
		return status.Error(codes.FailedPrecondition, "agent service is not configured")
	}
	if server.db == nil {
		return status.Error(codes.FailedPrecondition, "db is not configured")
	}
	if err := ValidateMetadataToken(stream.Context(), server.expected_token); err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	if request.GetArtifactId() == "" {
		return status.Error(codes.InvalidArgument, "artifact_id is required")
	}
	artifactID, err := uuid.Parse(request.GetArtifactId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid artifact_id: %v", err)
	}
	agent_identity, err := server.resolve_registered_agent(stream.Context())
	if err != nil {
		return err
	}

	artifact, err := artifact_orm.GetById(stream.Context(), server.db, artifactID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return status.Error(codes.NotFound, err.Error())
		}
		return status.Errorf(codes.Internal, "get artifact: %v", err)
	}
	if artifact.TenantID != agent_identity.TenantID {
		return status.Errorf(codes.PermissionDenied, "artifact %s does not belong to tenant %s", request.GetArtifactId(), agent_identity.TenantID)
	}

	// If shard params are provided, slice the CSV and return only this shard's portion
	data := artifact.ContentBytes
	if shard_idx := request.GetShardIndex(); shard_idx >= 0 && request.GetTotalShards() > 0 {
		total := int(request.GetTotalShards())
		idx := int(shard_idx)
		shard_csvs, err := util.SplitCSVRows(data, total)
		if err != nil {
			return status.Errorf(codes.Internal, "split csv artifact: %v", err)
		}
		if idx >= len(shard_csvs) {
			return status.Errorf(codes.InvalidArgument, "shard_index %d out of range (total: %d)", idx, len(shard_csvs))
		}
		data = shard_csvs[idx]
	}

	for start := 0; start < len(data); start += 64 * 1024 {
		end := start + 64*1024
		if end > len(data) {
			end = len(data)
		}

		if err := stream.Send(&controlv1.FetchArtifactChunk{
			Data: data[start:end],
		}); err != nil {
			return status.Errorf(codes.Internal, "send artifact chunk: %v", err)
		}
	}

	return nil
}

func (server *Server) try_assign_shard(
	stream *agentStream,
	agent_id string, tenant_id uuid.UUID, backend_name string,
) (ret_err error) {
	if server.db == nil || agent_id == "" || tenant_id == uuid.Nil || backend_name == "" {
		return nil
	}

	task, shard, err := task_service.LeaseNextShard(stream.Context(), tenant_id, backend_name, agent_id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			server.logger.Debug("no queued shard available for assignment",
				zap.String("agent_id", agent_id),
				zap.Stringer("tenant_id", tenant_id),
				zap.String("backend_name", backend_name),
			)
			return nil
		}
		return status.Errorf(codes.Internal, "lease next shard: %v", err)
	}
	should_rollback := true
	defer func() {
		if !should_rollback {
			return
		}
		if rollback_err := server.rollback_assigned_shard(stream.Context(), task, shard, agent_id); rollback_err != nil {
			if ret_err == nil {
				ret_err = status.Errorf(codes.Internal, "rollback assigned shard: %v", rollback_err)
				return
			}
			ret_err = status.Errorf(codes.Internal, "%v (rollback failed: %v)", ret_err, rollback_err)
		}
	}()

	shardIDStr := shard.ID.String()
	if err := server.agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        agent_id,
		Status:         agent_service.AgentStatusRunning,
		CurrentShardID: &shardIDStr,
	}); err != nil {
		return status.Errorf(codes.Internal, "mark agent busy: %v", err)
	}

	if err := stream.Send(&controlv1.ServerMessage{
		Payload: &controlv1.ServerMessage_AssignShard{
			AssignShard: &controlv1.AssignShard{
				ShardId:            shard.ID.String(),
				TaskId:             task.ID.String(),
				BundleArtifactId:   task.BundleArtifactID.String(),
				InputCsvArtifactId: task.InputCSVArtifactID.String(),
				ShardIndex:         int32(shard.ShardIndex),
				TotalShards:        int32(task.ShardCount),
			},
		},
	}); err != nil {
		return status.Errorf(codes.Internal, "send assign shard: %v", err)
	}

	server.logger.Info("shard assigned",
		zap.String("agent_id", agent_id),
		zap.Stringer("task_id", task.ID),
		zap.Stringer("shard_id", shard.ID),
	)

	should_rollback = false
	return nil
}

func (server *Server) rollback_assigned_shard(ctx context.Context, task model.Task, shard model.TaskShard, agent_id string) error {
	if err := server.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&model.TaskShard{}).
			Where("id = ? AND status = ? AND assigned_agent_id = ?", shard.ID, model.ShardStatusLeased, agent_id).
			Updates(map[string]any{
				"status":            model.ShardStatusQueued,
				"assigned_agent_id": "",
			})
		if result.Error != nil {
			return fmt.Errorf("rollback shard lease: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf("rollback shard lease: shard %s was not leased to agent %s", shard.ID, agent_id)
		}

		if task.Status != model.TaskStatusWaitingForAgents {
			return nil
		}

		task_result := tx.Model(&model.Task{}).
			Where("id = ? AND status = ?", task.ID, model.TaskStatusQueued).
			Update("status", model.TaskStatusWaitingForAgents)
		if task_result.Error != nil {
			return fmt.Errorf("rollback task status: %w", task_result.Error)
		}
		if task_result.RowsAffected > 0 {
			return nil
		}

		var current_task model.Task
		if err := tx.Model(&model.Task{}).
			Select("id", "status").
			Where("id = ?", task.ID).
			First(&current_task).Error; err != nil {
			return fmt.Errorf("rollback task status: %w", err)
		}
		if current_task.Status == model.TaskStatusWaitingForAgents {
			return nil
		}
		return fmt.Errorf("rollback task status: task %s is in unexpected status %s", task.ID, current_task.Status)
	}); err != nil {
		return err
	}

	if err := server.agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        agent_id,
		Status:         agent_service.AgentStatusIdle,
		CurrentShardID: nil,
	}); err != nil {
		return fmt.Errorf("rollback agent heartbeat: %w", err)
	}

	return nil
}

func (server *Server) restoreResultReadyAgentFromDB(
	ctx context.Context,
	agentID string,
	tenantID uuid.UUID,
	backendName string,
) (bool, error) {
	if server.db == nil {
		return false, nil
	}

	var shard struct {
		ID uuid.UUID
	}
	err := server.db.WithContext(ctx).
		Table("task_shards").
		Select("task_shards.id").
		Joins("JOIN tasks ON tasks.id = task_shards.task_id").
		Joins("JOIN tenants ON tenants.id = tasks.tenant_id").
		Where("task_shards.assigned_agent_id = ?", agentID).
		Where("task_shards.status = ?", model.ShardStatusResultReady).
		Where("tasks.tenant_id = ?", tenantID).
		Where("tenants.primary_backend_name = ?", backendName).
		Order("task_shards.updated_at desc").
		Order("task_shards.id asc").
		First(&shard).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, status.Errorf(codes.Internal, "restore result-ready agent state: %v", err)
	}

	shardID := shard.ID.String()
	if err := server.agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        agentID,
		Status:         agent_service.AgentStatusResultReady,
		CurrentShardID: &shardID,
	}); err != nil {
		return false, status.Errorf(codes.Internal, "restore result-ready agent heartbeat: %v", err)
	}

	server.logger.Info("restored result-ready agent state from shard storage",
		zap.String("agent_id", agentID),
		zap.Stringer("tenant_id", tenantID),
		zap.String("backend", backendName),
		zap.Stringer("shard_id", shard.ID),
	)

	return true, nil
}

func (server *Server) reconcileRegisteredAgentState(ctx context.Context, agent agent_service.Agent) (agent_service.Agent, error) {
	switch agent.Status {
	case agent_service.AgentStatusRunning, agent_service.AgentStatusResultReady:
	default:
		return agent, nil
	}

	if agent.CurrentShardID == nil || *agent.CurrentShardID == "" {
		return server.resetRegisteredAgentToIdle(agent.ID)
	}

	shardID, err := uuid.Parse(*agent.CurrentShardID)
	if err != nil {
		return server.resetRegisteredAgentToIdle(agent.ID)
	}

	shard, err := server.load_validated_shard(ctx, shardID, agent.ID, agent.TenantID, agent.BackendName)
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound, codes.PermissionDenied:
			return server.resetRegisteredAgentToIdle(agent.ID)
		default:
			return agent_service.Agent{}, err
		}
	}

	switch agent.Status {
	case agent_service.AgentStatusResultReady:
		if shard.Status == model.ShardStatusResultReady {
			return agent, nil
		}
		if shard.Status == model.ShardStatusSucceeded {
			hasOutput, err := server.shard_output_exists(ctx, shard.TaskID, shard.ShardIndex)
			if err != nil {
				return agent_service.Agent{}, status.Errorf(codes.Internal, "check shard output artifact: %v", err)
			}
			if hasOutput {
				return agent, nil
			}
		}
		return server.resetRegisteredAgentToIdle(agent.ID)
	case agent_service.AgentStatusRunning:
		if shard.Status != model.ShardStatusLeased && shard.Status != model.ShardStatusRunning {
			return server.resetRegisteredAgentToIdle(agent.ID)
		}
	}

	return agent, nil
}

func (server *Server) resetRegisteredAgentToIdle(agentID string) (agent_service.Agent, error) {
	if err := server.agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        agentID,
		Status:         agent_service.AgentStatusIdle,
		CurrentShardID: nil,
	}); err != nil {
		return agent_service.Agent{}, status.Errorf(codes.Internal, "reset stale agent state: %v", err)
	}

	agent, err := server.agent_service.GetAgent(agentID)
	if err != nil {
		return agent_service.Agent{}, status.Errorf(codes.Internal, "get reset agent: %v", err)
	}

	return agent, nil
}

func (server *Server) ensure_agent_can_become_idle(ctx context.Context, agent_id string) error {
	if server.db == nil {
		return nil
	}
	if agent_id == "" {
		return status.Error(codes.InvalidArgument, "agent_id is required")
	}

	var active_shards []model.TaskShard
	err := server.db.WithContext(ctx).
		Where("assigned_agent_id = ?", agent_id).
		Where("status IN ?", []string{model.ShardStatusLeased, model.ShardStatusRunning, model.ShardStatusResultReady}).
		Find(&active_shards).Error
	if err != nil {
		return status.Errorf(codes.Internal, "load active shards for agent %s: %v", agent_id, err)
	}
	if len(active_shards) == 0 {
		return nil
	}

	// Agent reports IDLE but server has non-terminal shards assigned to it.
	// The agent likely restarted and lost its in-memory state (no PVC).
	// Roll back all such shards to QUEUED so they can be re-assigned.
	for _, shard := range active_shards {
		server.logger.Warn("rolling back orphaned shard on agent IDLE report",
			zap.String("agent_id", agent_id),
			zap.Stringer("shard_id", shard.ID),
			zap.String("shard_status", shard.Status),
		)
		if rollbackErr := task_shard_orm.UpdateShardStatus(ctx, server.db, task_shard_orm.UpdateShardStatusParams{
			ShardID:         shard.ID,
			Status:          model.ShardStatusQueued,
			CurrentStatuses: []string{shard.Status},
			ClearAgent:      true,
		}); rollbackErr != nil {
			server.logger.Error("failed to roll back orphaned shard",
				zap.Stringer("shard_id", shard.ID),
				zap.Error(rollbackErr),
			)
		}
	}

	return nil
}

func metadataAgentID(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("agent-id metadata is required")
	}

	for _, key := range []string{"agent-id", "x-agent-id"} {
		if values := md.Get(key); len(values) > 0 && values[0] != "" {
			return values[0], nil
		}
	}
	return "", fmt.Errorf("agent-id metadata is required")
}

func (server *Server) resolve_registered_agent(ctx context.Context) (agent_service.Agent, error) {
	agent_id, err := metadataAgentID(ctx)
	if err != nil {
		return agent_service.Agent{}, status.Error(codes.Unauthenticated, err.Error())
	}

	registered_agent, err := server.agent_service.GetAgent(agent_id)
	if err != nil {
		if errors.Is(err, agent_service.ErrAgentNotFound) {
			return agent_service.Agent{}, status.Errorf(codes.Unauthenticated, "agent %s is not registered", agent_id)
		}
		return agent_service.Agent{}, status.Errorf(codes.Internal, "get registered agent: %v", err)
	}

	return registered_agent, nil
}

func (server *Server) load_validated_shard(
	ctx context.Context,
	shard_id uuid.UUID,
	current_agent_id string,
	current_tenant_id uuid.UUID,
	current_backend_name string,
) (model.TaskShard, error) {
	if server.db == nil {
		return model.TaskShard{}, status.Error(codes.FailedPrecondition, "db is not configured")
	}
	if shard_id == uuid.Nil {
		return model.TaskShard{}, status.Error(codes.InvalidArgument, "shard_id is required")
	}

	var shard model.TaskShard
	if err := server.db.WithContext(ctx).Where("id = ?", shard_id).First(&shard).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.TaskShard{}, status.Errorf(codes.NotFound, "shard %s not found", shard_id)
		}
		return model.TaskShard{}, status.Errorf(codes.Internal, "load shard %s: %v", shard_id, err)
	}
	if shard.AssignedAgentID != current_agent_id {
		return model.TaskShard{}, status.Errorf(codes.PermissionDenied, "shard %s is assigned to agent %s, not %s", shard_id, shard.AssignedAgentID, current_agent_id)
	}

	var task struct {
		ID                 uuid.UUID
		TenantID           uuid.UUID
		PrimaryBackendName string
	}
	if err := server.db.WithContext(ctx).
		Table("tasks").
		Select("tasks.id, tasks.tenant_id, tenants.primary_backend_name").
		Joins("JOIN tenants ON tenants.id = tasks.tenant_id").
		Where("tasks.id = ?", shard.TaskID).
		First(&task).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.TaskShard{}, status.Errorf(codes.NotFound, "task %s not found", shard.TaskID)
		}
		return model.TaskShard{}, status.Errorf(codes.Internal, "load shard task %s: %v", shard.TaskID, err)
	}
	if task.TenantID != current_tenant_id {
		return model.TaskShard{}, status.Errorf(codes.PermissionDenied, "shard %s belongs to tenant %s, not %s", shard_id, task.TenantID, current_tenant_id)
	}
	if task.PrimaryBackendName != current_backend_name {
		return model.TaskShard{}, status.Errorf(codes.PermissionDenied, "shard %s belongs to backend %s, not %s", shard_id, task.PrimaryBackendName, current_backend_name)
	}

	return shard, nil
}

func (server *Server) validate_shard_report(
	ctx context.Context,
	shard_id uuid.UUID, current_agent_id string, current_tenant_id uuid.UUID, current_backend_name, required_status, action string,
) error {
	shard, err := server.load_validated_shard(ctx, shard_id, current_agent_id, current_tenant_id, current_backend_name)
	if err != nil {
		return err
	}
	if shard.Status != required_status {
		return status.Errorf(codes.FailedPrecondition, "%s requires shard %s to be %s, got %s", action, shard_id, required_status, shard.Status)
	}

	return nil
}

func (server *Server) validate_shard_result_data_report(
	ctx context.Context,
	shard_id uuid.UUID,
	current_agent_id string,
	current_tenant_id uuid.UUID,
	current_backend_name string,
) (bool, error) {
	shard, err := server.load_validated_shard(ctx, shard_id, current_agent_id, current_tenant_id, current_backend_name)
	if err != nil {
		return false, err
	}

	switch shard.Status {
	case model.ShardStatusResultReady:
		return false, nil
	case model.ShardStatusSucceeded:
		hasOutput, err := server.shard_output_exists(ctx, shard.TaskID, shard.ShardIndex)
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

func (server *Server) shard_output_exists(ctx context.Context, taskID uuid.UUID, shardIndex int) (bool, error) {
	var count int64
	if err := server.db.WithContext(ctx).
		Model(&model.Artifact{}).
		Where("task_id = ?", taskID).
		Where("artifact_type = ?", model.ArtifactTypeShardOutput).
		Where("shard_index = ?", shardIndex).
		Count(&count).Error; err != nil {
		return false, err
	}

	return count > 0, nil
}

func shouldTryAssign(agent agent_service.Agent) bool {
	return agent.Status == agent_service.AgentStatusIdle && agent.CurrentShardID == nil
}

func (server *Server) sendFetchShardResult(
	stream *agentStream,
	shardID string,
) error {
	return stream.Send(&controlv1.ServerMessage{
		Payload: &controlv1.ServerMessage_FetchShardResult{
			FetchShardResult: &controlv1.FetchShardResult{ShardId: shardID},
		},
	})
}

func (server *Server) sendShardResultStored(
	stream *agentStream,
	shardID string,
) error {
	return stream.Send(&controlv1.ServerMessage{
		Payload: &controlv1.ServerMessage_ShardResultStored{
			ShardResultStored: &controlv1.ShardResultStored{ShardId: shardID},
		},
	})
}

func (server *Server) NotifyCancelShard(ctx context.Context, agentID string, shardID uuid.UUID) error {
	value, ok := server.streams.Load(agentID)
	if !ok {
		return nil // agent not connected, skip
	}

	stream, ok := value.(*agentStream)
	if !ok || stream == nil {
		server.streams.Delete(agentID)
		return nil
	}
	if err := stream.SendWithContext(ctx, &controlv1.ServerMessage{
		Payload: &controlv1.ServerMessage_CancelShard{
			CancelShard: &controlv1.CancelShard{
				ShardId: shardID.String(),
			},
		},
	}); err != nil {
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			server.streams.Delete(agentID)
		}
		server.logger.Error("send cancel shard to agent failed",
			zap.String("agent_id", agentID),
			zap.Stringer("shard_id", shardID),
			zap.Error(err),
		)
		return err
	}

	server.logger.Info("cancel shard sent to agent",
		zap.String("agent_id", agentID),
		zap.Stringer("shard_id", shardID),
	)
	return nil
}

// RollbackStaleShards periodically rolls back LEASED/RUNNING shards whose assigned
// agent has no active gRPC stream back to QUEUED.
func (server *Server) RollbackStaleShards(ctx context.Context, interval, staleThreshold time.Duration) {
	if interval <= 0 || staleThreshold <= 0 {
		server.logger.Error("stale shard reaper disabled: invalid timing configuration",
			zap.Duration("interval", interval),
			zap.Duration("stale_threshold", staleThreshold),
		)
		return
	}

	server.logger.Info("stale shard reaper started",
		zap.Duration("interval", interval),
		zap.Duration("stale_threshold", staleThreshold),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			server.logger.Info("stale shard reaper stopped")
			return
		case <-ticker.C:
			activeAgentIDs := server.collectActiveAgentIDs()
			if server.db == nil {
				continue
			}
			threshold := time.Now().UTC().Add(-staleThreshold)
			rolled, err := task_shard_orm.RollbackStaleShards(ctx, server.db, activeAgentIDs, threshold)
			if err != nil && ctx.Err() == nil {
				server.logger.Error("rollback stale shards failed", zap.Error(err))
			}
			if rolled > 0 {
				server.logger.Info("reaped stale shards", zap.Int("count", rolled))
			}
		}
	}
}

func (server *Server) collectActiveAgentIDs() []string {
	var ids []string
	server.streams.Range(func(key, _ any) bool {
		if id, ok := key.(string); ok {
			ids = append(ids, id)
		}
		return true
	})
	return ids
}
