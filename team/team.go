package team

import (
	"errors"

	"github.com/square/etre/config"
	"github.com/square/etre/metrics"
)

var (
	ErrNoTeam   = errors.New("X-Etre-Team header not set")
	ErrNotFound = errors.New("team does not exit")
	ErrReadOnly = errors.New("team is read-only, cannot write")
)

type Team struct {
	Name            string
	ReadOnly        bool
	QueryLatencySLA uint
	Metrics         *metrics.Metrics
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

type Authorizer interface {
	Team(name string) (Team, error)
	Allowed(t Team, op byte, entityType string) error
	List() []Team
}

type AllowAll struct {
	t Team
}

func NewAllowAll(entityTypes []string) *AllowAll {
	all := &AllowAll{
		t: Team{
			Name: DEFAULT_TEAM_NAME,
			Metrics: metrics.NewMetrics(
				metrics.Config{
					EntityTypes: entityTypes,
				},
			),
		},
	}
	return all
}

func (all *AllowAll) Team(name string) (Team, error) {
	return all.t, nil
}

func (all *AllowAll) Allowed(t Team, op byte, entityType string) error {
	return nil
}

func (all *AllowAll) List() []Team {
	return []Team{all.t}
}

type Org struct {
	teams map[string]Team
	list  []Team
}

func NewOrgAuthorizer(teams []config.TeamConfig, entityTypes []string) *Org {
	org := &Org{
		teams: map[string]Team{},
		list:  make([]Team, len(teams)),
	}
	for n, team := range teams {
		t := Team{
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

func (o *Org) Team(name string) (Team, error) {
	if name == "" {
		return Team{}, ErrNoTeam
	}
	team, ok := o.teams[name]
	if !ok {
		return Team{}, ErrNotFound
	}
	return team, nil
}

func (o *Org) Allowed(t Team, op byte, entityType string) error {
	if t.ReadOnly && op != OP_READ {
		return ErrReadOnly
	}
	return nil
}

func (o *Org) List() []Team {
	return o.list
}
