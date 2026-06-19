package control

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
)

func ValidateMetadataToken(ctx context.Context, want string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return fmt.Errorf("authorization metadata is required")
	}

	for _, value := range md.Get("authorization") {
		if token, ok := strings.CutPrefix(value, "Bearer "); ok && token != "" {
			if token != want {
				return fmt.Errorf("agent token mismatch")
			}
			return nil
		}
	}
	for _, key := range []string{"x-agent-token", "agent-token"} {
		if values := md.Get(key); len(values) > 0 && values[0] != "" {
			if values[0] != want {
				return fmt.Errorf("agent token mismatch")
			}
			return nil
		}
	}
	return fmt.Errorf("authorization metadata is required")
}
