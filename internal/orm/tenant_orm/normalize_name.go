package tenant_orm

import (
	"fmt"
	"strings"
)

func NormalizeName(raw string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		return "", fmt.Errorf("tenant name is required")
	}
	return normalized, nil
}
