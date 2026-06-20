package control

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/auth"
	"github.com/yichozy/r-orchestrator/internal/orm/artifact_orm"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/util"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

func (server *Server) FetchArtifact(request *controlv1.FetchArtifactRequest, stream grpc.ServerStreamingServer[controlv1.FetchArtifactChunk]) error {
	if server.db == nil {
		return status.Error(codes.FailedPrecondition, "db is not configured")
	}
	if err := auth.ValidateToken(stream.Context(), server.expectedToken); err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	if request.GetArtifactId() == "" {
		return status.Error(codes.InvalidArgument, "artifact_id is required")
	}
	artifactID, err := uuid.Parse(request.GetArtifactId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid artifact_id: %v", err)
	}
	agent_identity, err := func() (agent_service.Agent, error) {
		md, ok := metadata.FromIncomingContext(stream.Context())
		if !ok {
			return agent_service.Agent{}, fmt.Errorf("agent-id metadata is required")
		}
		var agentID string
		for _, key := range []string{"agent-id", "x-agent-id"} {
			if values := md.Get(key); len(values) > 0 && values[0] != "" {
				agentID = values[0]
				break
			}
		}
		if agentID == "" {
			return agent_service.Agent{}, fmt.Errorf("agent-id metadata is required")
		}
		registered_agent, err := agent_service.GetAgent(agentID)
		if err != nil {
			if errors.Is(err, agent_service.ErrAgentNotFound) {
				return agent_service.Agent{}, status.Errorf(codes.Unauthenticated, "agent %s is not registered", agentID)
			}
			return agent_service.Agent{}, status.Errorf(codes.Internal, "get registered agent: %v", err)
		}
		return registered_agent, nil
	}()
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
