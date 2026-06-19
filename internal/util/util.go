package util

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
)

func SplitCSVRows(csvBytes []byte, count int) ([][]byte, error) {
	if count <= 0 {
		return nil, fmt.Errorf("shard count must be positive")
	}

	reader := csv.NewReader(bytes.NewReader(csvBytes))
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read csv: %w", err)
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("csv is empty")
	}

	dataRows := rows[1:]
	if len(dataRows) == 0 {
		return nil, fmt.Errorf("csv has no data rows")
	}

	header := rows[0]
	shardSlices := make([][]*[]string, count)
	for i := range dataRows {
		idx := i % count
		shardSlices[idx] = append(shardSlices[idx], &dataRows[i])
	}

	result := make([][]byte, 0, count)
	for _, slice := range shardSlices {
		if len(slice) == 0 {
			continue
		}
		buf := new(bytes.Buffer)
		writer := csv.NewWriter(buf)
		shardRows := make([][]string, 0, len(slice)+1)
		shardRows = append(shardRows, header)
		for _, row := range slice {
			shardRows = append(shardRows, *row)
		}
		if err := writer.WriteAll(shardRows); err != nil {
			return nil, err
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			return nil, err
		}
		result = append(result, buf.Bytes())
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no shards produced")
	}

	return result, nil
}

func SumSHA256(contentBytes []byte) string {
	sum := sha256.Sum256(contentBytes)
	return hex.EncodeToString(sum[:])
}
