// Copyright 2017, Square, Inc.

package mock

import (
	"github.com/square/etre"
	"github.com/square/etre/query"
)

type EntityManager struct {
	DeleteEntityLabelFunc func(string, string, string) (etre.Entity, error)
	CreateEntitiesFunc    func(string, []etre.Entity, string) ([]string, error)
	ReadEntitiesFunc      func(string, query.Query) ([]etre.Entity, error)
	UpdateEntitiesFunc    func(string, query.Query, etre.Entity, string) ([]etre.Entity, error)
	DeleteEntitiesFunc    func(string, query.Query, string) ([]etre.Entity, error)
}

func (e *EntityManager) DeleteEntityLabel(entityType string, id string, label string) (etre.Entity, error) {
	if e.DeleteEntityLabelFunc != nil {
		return e.DeleteEntityLabelFunc(entityType, id, label)
	}
	return nil, nil
}

func (e *EntityManager) CreateEntities(entityType string, entities []etre.Entity, user string) ([]string, error) {
	if e.CreateEntitiesFunc != nil {
		return e.CreateEntitiesFunc(entityType, entities, user)
	}
	return nil, nil
}

func (e *EntityManager) ReadEntities(entityType string, q query.Query) ([]etre.Entity, error) {
	if e.ReadEntitiesFunc != nil {
		return e.ReadEntitiesFunc(entityType, q)
	}
	return nil, nil
}

func (e *EntityManager) UpdateEntities(t string, q query.Query, u etre.Entity, user string) ([]etre.Entity, error) {
	if e.UpdateEntitiesFunc != nil {
		return e.UpdateEntitiesFunc(t, q, u, user)
	}
	return nil, nil
}

func (e *EntityManager) DeleteEntities(t string, q query.Query, user string) ([]etre.Entity, error) {
	if e.DeleteEntitiesFunc != nil {
		return e.DeleteEntitiesFunc(t, q, user)
	}
	return nil, nil
}
