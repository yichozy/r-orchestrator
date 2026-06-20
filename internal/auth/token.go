package auth

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
)

// ValidateToken extracts a bearer token from the gRPC metadata and compares
// it against the expected value. Supports "Authorization: Bearer <token>",
// "x-agent-token", and "agent-token" headers.
func ValidateToken(ctx context.Context, want string) error {
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
