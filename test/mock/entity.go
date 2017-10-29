// Copyright 2017, Square, Inc.

package mock

import (
	"github.com/square/etre"
	"github.com/square/etre/entity"
	"github.com/square/etre/query"
)

type EntityStore struct {
	ReadEntitiesFunc      func(string, query.Query, etre.QueryFilter) ([]etre.Entity, error)
	DeleteEntityLabelFunc func(entity.WriteOp, string) (etre.Entity, error)
	CreateEntitiesFunc    func(entity.WriteOp, []etre.Entity) ([]string, error)
	UpdateEntitiesFunc    func(entity.WriteOp, query.Query, etre.Entity) ([]etre.Entity, error)
	DeleteEntitiesFunc    func(entity.WriteOp, query.Query) ([]etre.Entity, error)
}

func (s *EntityStore) DeleteEntityLabel(wo entity.WriteOp, label string) (etre.Entity, error) {
	if s.DeleteEntityLabelFunc != nil {
		return s.DeleteEntityLabelFunc(wo, label)
	}
	return nil, nil
}

func (s *EntityStore) CreateEntities(wo entity.WriteOp, entities []etre.Entity) ([]string, error) {
	if s.CreateEntitiesFunc != nil {
		return s.CreateEntitiesFunc(wo, entities)
	}
	return nil, nil
}

func (s *EntityStore) ReadEntities(entityType string, q query.Query, f etre.QueryFilter) ([]etre.Entity, error) {
	if s.ReadEntitiesFunc != nil {
		return s.ReadEntitiesFunc(entityType, q, f)
	}
	return nil, nil
}

func (s *EntityStore) UpdateEntities(wo entity.WriteOp, q query.Query, u etre.Entity) ([]etre.Entity, error) {
	if s.UpdateEntitiesFunc != nil {
		return s.UpdateEntitiesFunc(wo, q, u)
	}
	return nil, nil
}

func (s *EntityStore) DeleteEntities(wo entity.WriteOp, q query.Query) ([]etre.Entity, error) {
	if s.DeleteEntitiesFunc != nil {
		return s.DeleteEntitiesFunc(wo, q)
	}
	return nil, nil
}
