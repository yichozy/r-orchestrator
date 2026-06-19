package generated

import (
	"context"

	"github.com/99designs/gqlgen/graphql"
	"github.com/google/uuid"
	"github.com/vektah/gqlparser/v2/ast"
	graphmodel "github.com/yichozy/r-orchestrator/graph/model"
)

func (ec *executionContext) unmarshalInputUUID(ctx context.Context, v any) (uuid.UUID, error) {
	id, err := graphmodel.UnmarshalUUID(v)
	return id, graphql.ErrorOnPath(ctx, err)
}

func (ec *executionContext) _UUID(ctx context.Context, _ ast.SelectionSet, v *uuid.UUID) graphql.Marshaler {
	if v == nil {
		return graphql.Null
	}
	return graphmodel.MarshalUUID(*v)
}
