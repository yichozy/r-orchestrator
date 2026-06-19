package model

import (
	"fmt"
	"io"

	"github.com/99designs/gqlgen/graphql"
	"github.com/google/uuid"
)

func MarshalUUID(id uuid.UUID) graphql.Marshaler {
	return graphql.WriterFunc(func(w io.Writer) {
		graphql.MarshalString(id.String()).MarshalGQL(w)
	})
}

func UnmarshalUUID(v any) (uuid.UUID, error) {
	value, err := graphql.UnmarshalString(v)
	if err != nil {
		return uuid.Nil, err
	}

	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse uuid: %w", err)
	}

	return id, nil
}
