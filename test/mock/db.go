package mock

import (
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/square/etre/config"
	"github.com/square/etre/db"
)

var _ db.Plugin = DBPlugin{}

type DBPlugin struct {
	ConnectFunc func(config.DatasourceConfig) (*mongo.Client, error)
}

func (d DBPlugin) Connect(cfg config.DatasourceConfig) (*mongo.Client, error) {
	return d.ConnectFunc(cfg)
}
