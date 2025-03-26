// Copyright 2017-2020, Square, Inc.

package test

import (
	"log"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	url      = "localhost:27017/?replicaSet=rs0&directConnection=true"
	database = "etre_test"
	username = "root"
)

func DbCollections(entityTypes []string) (*mongo.Client, map[string]*mongo.Collection, error) {
	url := "mongodb://" + url
	log.Printf("Connecting to %s", url)
	client, err := mongo.Connect(options.Client().ApplyURI(url))
	if err != nil {
		return nil, nil, err
	}
	coll := map[string]*mongo.Collection{}
	for _, t := range entityTypes {
		coll[t] = client.Database(database).Collection(t)
	}
	return client, coll, nil
}
