// Copyright 2017-2020, Square, Inc.

package test

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	url      = "localhost:27017"
	database = "etre_test"
	username = "root"
)

func DbCollections(entityTypes []string) (*mongo.Client, map[string]*mongo.Collection, error) {
	url := "mongodb://" + url
	log.Printf("Connecting to %s", url)
	client, err := mongo.NewClient(options.Client().ApplyURI(url))
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		return nil, nil, err
	}
	coll := map[string]*mongo.Collection{}
	for _, t := range entityTypes {
		coll[t] = client.Database(database).Collection(t)
	}
	return client, coll, nil
}
