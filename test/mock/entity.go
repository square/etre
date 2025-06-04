// Copyright 2017-2020, Square, Inc.

package mock

import (
	"context"

	"github.com/square/etre"
	"github.com/square/etre/entity"
	"github.com/square/etre/query"
)

type EntityStore struct {
	ReadEntitiesFunc      func(context.Context, string, query.Query, etre.QueryFilter) ([]etre.Entity, error)
	DeleteEntityLabelFunc func(context.Context, entity.WriteOp, string) (etre.Entity, error)
	CreateEntitiesFunc    func(context.Context, entity.WriteOp, []etre.Entity) ([]string, error)
	UpdateEntitiesFunc    func(context.Context, entity.WriteOp, query.Query, etre.Entity) ([]etre.Entity, error)
	DeleteEntitiesFunc    func(context.Context, entity.WriteOp, query.Query) ([]etre.Entity, error)
	DeleteLabelFunc       func(context.Context, entity.WriteOp, string) (etre.Entity, error)
}

func (s EntityStore) DeleteEntityLabel(ctx context.Context, wo entity.WriteOp, label string) (etre.Entity, error) {
	if s.DeleteEntityLabelFunc != nil {
		return s.DeleteEntityLabelFunc(ctx, wo, label)
	}
	return nil, nil
}

func (s EntityStore) CreateEntities(ctx context.Context, wo entity.WriteOp, entities []etre.Entity) ([]string, error) {
	if s.CreateEntitiesFunc != nil {
		return s.CreateEntitiesFunc(ctx, wo, entities)
	}
	return nil, nil
}

func (s EntityStore) ReadEntities(ctx context.Context, entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
	if s.ReadEntitiesFunc != nil {
		return s.ReadEntitiesFunc(ctx, entityType, q, f)
	}
	return nil, nil
}

func (s EntityStore) UpdateEntities(ctx context.Context, wo entity.WriteOp, q query.Query, u etre.Entity) ([]etre.Entity, error) {
	if s.UpdateEntitiesFunc != nil {
		return s.UpdateEntitiesFunc(ctx, wo, q, u)
	}
	return nil, nil
}

func (s EntityStore) DeleteEntities(ctx context.Context, wo entity.WriteOp, q query.Query) ([]etre.Entity, error) {
	if s.DeleteEntitiesFunc != nil {
		return s.DeleteEntitiesFunc(ctx, wo, q)
	}
	return nil, nil
}

func (s EntityStore) DeleteLabel(ctx context.Context, wo entity.WriteOp, label string) (etre.Entity, error) {
	if s.DeleteLabelFunc != nil {
		return s.DeleteLabelFunc(ctx, wo, label)
	}
	return etre.Entity{}, nil
}
