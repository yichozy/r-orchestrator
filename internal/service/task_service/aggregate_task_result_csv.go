package task_service

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"slices"

	"github.com/yichozy/r-orchestrator/internal/model"
)

func aggregateTaskResultCSV(artifacts []model.Artifact, expectedShardCount int) ([]byte, error) {
	orderedArtifacts, err := validateShardOutputArtifacts(artifacts, expectedShardCount)
	if err != nil {
		return nil, err
	}

	var (
		out    bytes.Buffer
		writer = csv.NewWriter(&out)
		header []string
	)

	for _, artifact := range orderedArtifacts {
		if len(artifact.ContentBytes) == 0 {
			return nil, fmt.Errorf("empty shard output for shard_index %d", *artifact.ShardIndex)
		}

		reader := csv.NewReader(bytes.NewReader(artifact.ContentBytes))
		rows, err := reader.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("invalid csv in shard output for shard_index %d: %w", *artifact.ShardIndex, err)
		}
		if len(rows) == 0 {
			return nil, fmt.Errorf("invalid csv in shard output for shard_index %d: empty csv", *artifact.ShardIndex)
		}

		if len(header) == 0 {
			header = append([]string(nil), rows[0]...)
			if err := writer.Write(header); err != nil {
				return nil, fmt.Errorf("write aggregated header: %w", err)
			}
		} else if !slices.Equal(rows[0], header) {
			return nil, fmt.Errorf("header mismatch at shard_index %d: %q != %q", *artifact.ShardIndex, rows[0], header)
		}

		for _, row := range rows[1:] {
			if err := writer.Write(row); err != nil {
				return nil, fmt.Errorf("write aggregated row: %w", err)
			}
		}
	}
	if len(header) == 0 {
		return nil, fmt.Errorf("no valid shard output csv with header/data")
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("flush aggregated csv: %w", err)
	}

	return out.Bytes(), nil
}

func validateShardOutputArtifacts(artifacts []model.Artifact, expectedShardCount int) ([]model.Artifact, error) {
	if expectedShardCount <= 0 {
		return nil, fmt.Errorf("invalid shard count: %d", expectedShardCount)
	}
	if len(artifacts) == 0 {
		return nil, fmt.Errorf("no shard output artifacts")
	}

	orderedArtifacts := make([]model.Artifact, expectedShardCount)
	seen := make([]bool, expectedShardCount)

	for _, artifact := range artifacts {
		if artifact.ShardIndex == nil {
			return nil, fmt.Errorf("missing shard_index for shard output artifact %s", artifact.ID)
		}

		shardIndex := *artifact.ShardIndex
		if shardIndex < 0 || shardIndex >= expectedShardCount {
			return nil, fmt.Errorf("invalid shard_index %d for shard output artifact %s", shardIndex, artifact.ID)
		}
		if seen[shardIndex] {
			return nil, fmt.Errorf("duplicate shard output artifact for shard_index %d", shardIndex)
		}

		orderedArtifacts[shardIndex] = artifact
		seen[shardIndex] = true
	}

	for shardIndex, ok := range seen {
		if !ok {
			return nil, fmt.Errorf("missing shard output artifact for shard_index %d", shardIndex)
		}
	}

	return orderedArtifacts, nil
}
