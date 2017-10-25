// Copyright 2017, Square, Inc.

package mock

import (
	"github.com/square/etre"
	"github.com/square/etre/query"
)

type EntityStore struct {
	DeleteEntityLabelFunc func(string, string, string) (etre.Entity, error)
	CreateEntitiesFunc    func(string, []etre.Entity, string) ([]string, error)
	ReadEntitiesFunc      func(string, query.Query) ([]etre.Entity, error)
	UpdateEntitiesFunc    func(string, query.Query, etre.Entity, string) ([]etre.Entity, error)
	DeleteEntitiesFunc    func(string, query.Query, string) ([]etre.Entity, error)
}

func (s *EntityStore) DeleteEntityLabel(entityType string, id string, label string) (etre.Entity, error) {
	if s.DeleteEntityLabelFunc != nil {
		return s.DeleteEntityLabelFunc(entityType, id, label)
	}
	return nil, nil
}

func (s *EntityStore) CreateEntities(entityType string, entities []etre.Entity, user string) ([]string, error) {
	if s.CreateEntitiesFunc != nil {
		return s.CreateEntitiesFunc(entityType, entities, user)
	}
	return nil, nil
}

func (s *EntityStore) ReadEntities(entityType string, q query.Query) ([]etre.Entity, error) {
	if s.ReadEntitiesFunc != nil {
		return s.ReadEntitiesFunc(entityType, q)
	}
	return nil, nil
}

func (s *EntityStore) UpdateEntities(t string, q query.Query, u etre.Entity, user string) ([]etre.Entity, error) {
	if s.UpdateEntitiesFunc != nil {
		return s.UpdateEntitiesFunc(t, q, u, user)
	}
	return nil, nil
}

func (s *EntityStore) DeleteEntities(t string, q query.Query, user string) ([]etre.Entity, error) {
	if s.DeleteEntitiesFunc != nil {
		return s.DeleteEntitiesFunc(t, q, user)
	}
	return nil, nil
}
