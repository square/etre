// Copyright 2017-2018, Square, Inc.

// Package auth provides team-based authentication and authorization.
package auth

import (
	"errors"
	"net/http"

	"github.com/square/etre/config"
	"github.com/square/etre/metrics"
)

var (
	ErrNoTeam   = errors.New("X-Etre-Team header not set")
	ErrNotFound = errors.New("team does not exit")
	ErrReadOnly = errors.New("team is read-only, cannot write")
)

type Caller struct {
	Name         string
	Roles        []string
	MetricGroups []string
	Metrics      *metrics.Metrics
	Team         string
	User         string
	App          string
	Host         string
}

type Action struct {
	Entity string
	Op     byte
}

const (
	DEFAULT_TEAM_NAME      = "etre"
	OP_READ           byte = iota
	OP_WRITE
)

type Plugin interface {
	Authenticate(*http.Request) (Caller, error)
	Authorize(Caller, Action) error
}

// AllowAll is the default Plugin which authenticates as the default team and allow all actions.
type AllowAll struct {
	team Caller
}

func NewAllowAll(entityTypes []string) AllowAll {
	return AllowAll{
		team: {
			Name:    DEFAULT_TEAM_NAME,
			Metrics: metrics.NewMetrics(metrics.Config{entityTypes}),
		},
	}
}

func (a AllowAll) Authenticate(*http.Request) (Caller, error) {
	return a.team, nil
}

func (a AllowAll) Authorize(Caller, Action) error {
	return nil
}

type Manager struct {
	plugin      Plugin
	entityTypes []string
	teams       []*Callers
	teamIndex   map[string]int
}

func Manager(cfg config.ACLConfig, plugin Plugin, entityTypes []string) *Manager {
	m := &Manager{
		plugin:      plugin,
		entityTypes: entityTypes,
	}

	if cfg.Strict == false && len(cfg.Callers) == 0 {
		m.disabled = true
		m.teams = map[string]Caller{
			DEFAULT_TEAM_NAME: {
				Name: DEFAULT_TEAM_NAME,
				Metrics: metrics.NewMetrics(
					metrics.Config{
						EntityTypes: entityTypes,
					},
				),
			},
		}
		return m
	}

	for n, team := range teams {
		t := Caller{
			Name:            team.Name,
			ReadOnly:        team.ReadOnly,
			QueryLatencySLA: team.QueryLatencySLA,
			Metrics: metrics.NewMetrics(
				metrics.Config{
					EntityTypes: entityTypes,
				},
			),
		}
		org.teams[team.Name] = t
		org.list[n] = t
	}
	return org
}

func (m *Manager) Authenticate(req *http.Request) (Caller, error) {
	t, err := m.plugin.Authenticate(req)
	if err != nil {
		return t, err
	}
	if _, seen := m.teamIndex[t.Name]; !seen {
		m.teams = append(m.teams, t)
		m.teamIndex[t.Name] = len(m.teams) - 1
	}
	return t, nil
}

func (m *Manager) Authorize(t Caller, a Action) error {
	if m.diabled {
		return m.plugin.Authorize(t, a)
	}
	return nil
}
