package task_service

import (
	"strings"
	"testing"

	"github.com/yichozy/r-orchestrator/internal/model"
)

func TestAggregateTaskResultCSV_MergesShardRows(t *testing.T) {
	artifacts := []model.Artifact{
		{ShardIndex: intPtr(0), ContentBytes: []byte("id,value\n1,a\n2,b\n")},
		{ShardIndex: intPtr(1), ContentBytes: []byte("id,value\n3,c\n4,d\n")},
	}

	got, err := aggregateTaskResultCSV(artifacts, 2)
	if err != nil {
		t.Fatalf("aggregateTaskResultCSV() error = %v", err)
	}

	const want = "id,value\n1,a\n2,b\n3,c\n4,d\n"
	if string(got) != want {
		t.Fatalf("aggregateTaskResultCSV() = %q, want %q", string(got), want)
	}
}

func TestAggregateTaskResultCSV_RejectsHeaderMismatch(t *testing.T) {
	artifacts := []model.Artifact{
		{ShardIndex: intPtr(0), ContentBytes: []byte("id,value\n1,a\n")},
		{ShardIndex: intPtr(1), ContentBytes: []byte("id,other\n2,b\n")},
	}

	_, err := aggregateTaskResultCSV(artifacts, 2)
	if err == nil || !strings.Contains(err.Error(), "header mismatch") {
		t.Fatalf("aggregateTaskResultCSV() error = %v, want header mismatch", err)
	}
}

func TestAggregateTaskResultCSV_RejectsInvalidCSV(t *testing.T) {
	artifacts := []model.Artifact{
		{ShardIndex: intPtr(0), ContentBytes: []byte("id,value\n1,a\n")},
		{ShardIndex: intPtr(1), ContentBytes: []byte("\"unterminated\n")},
	}

	_, err := aggregateTaskResultCSV(artifacts, 2)
	if err == nil || !strings.Contains(err.Error(), "invalid csv in shard output") {
		t.Fatalf("aggregateTaskResultCSV() error = %v, want invalid csv in shard output", err)
	}
}

func TestAggregateTaskResultCSV_RejectsDuplicateShardIndex(t *testing.T) {
	artifacts := []model.Artifact{
		{ShardIndex: intPtr(0), ContentBytes: []byte("id,value\n1,a\n")},
		{ShardIndex: intPtr(0), ContentBytes: []byte("id,value\n2,b\n")},
	}

	_, err := aggregateTaskResultCSV(artifacts, 2)
	if err == nil || !strings.Contains(err.Error(), "duplicate shard output artifact") {
		t.Fatalf("aggregateTaskResultCSV() error = %v, want duplicate shard output artifact", err)
	}
}

func TestAggregateTaskResultCSV_RejectsMissingShardIndexArtifact(t *testing.T) {
	artifacts := []model.Artifact{
		{ShardIndex: intPtr(0), ContentBytes: []byte("id,value\n1,a\n")},
	}

	_, err := aggregateTaskResultCSV(artifacts, 2)
	if err == nil || !strings.Contains(err.Error(), "missing shard output artifact") {
		t.Fatalf("aggregateTaskResultCSV() error = %v, want missing shard output artifact", err)
	}
}

func TestAggregateTaskResultCSV_RejectsAnyEmptyShardOutput(t *testing.T) {
	artifacts := []model.Artifact{
		{ShardIndex: intPtr(0), ContentBytes: []byte("id,value\n1,a\n")},
		{ShardIndex: intPtr(1), ContentBytes: nil},
	}

	_, err := aggregateTaskResultCSV(artifacts, 2)
	if err == nil || !strings.Contains(err.Error(), "empty shard output for shard_index 1") {
		t.Fatalf("aggregateTaskResultCSV() error = %v, want empty shard output for shard_index 1", err)
	}
}

func intPtr(v int) *int {
	return &v
}
