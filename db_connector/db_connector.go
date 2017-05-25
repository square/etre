// Copyright 2017, Square, Inc.

// Package db_connector is a connector to query a
// MongoDB instance with a Query object passed in.
package db_connector

import (
	"errors"
	"strconv"

	"github.com/square/etre/query"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Stores config for DB and original session. Create new
// sessions by copying the original session:
// https://godoc.org/labix.org/v2/mgo#Session.Copy.
type DBConnector struct {
	URL        string
	Database   string
	Collection string
	Session    *mgo.Session
}

// NewDBConnector create an instance of DBConnector,
// which includes storing the config needed to connect
// to DB and creating the first session.
func NewDBConnector(url string, database string, collection string) (*DBConnector, error) {
	session, err := mgo.Dial(url)
	if err != nil {
		return nil, err
	}

	return &DBConnector{
		URL:        url,
		Database:   database,
		Collection: collection,
		Session:    session,
	}, nil
}

// This should be called when finished with db connector instance.
func (dbc *DBConnector) CloseSession() {
	dbc.Session.Close()
}

// Query MongoDB instance with a Query object, which is
// tranlsated to bson.M object for mgo driver to
// understand.
func (dbc *DBConnector) Query(q query.Query) (bson.M, error) {
	var err error
	var mgoQuery bson.M
	var entity bson.M

	mgoQuery, err = translateQuery(q)
	if err != nil {
		return entity, err
	}

	session := dbc.Session.Copy()
	defer session.Close()

	c := session.DB(dbc.Database).C(dbc.Collection)
	err = c.Find(mgoQuery).One(&entity)
	if err != nil {
		return entity, err
	}

	return entity, nil
}

// Translates Query object to bson.M object, which will
// be used to query mongodb instance. Part of this
// translation includes mapping Kubernetes Selection
// Operator
// (https://github.com/kubernetes/apimachinery/blob/master/pkg/selection/operator.go)
// to a MongoDB Operator
// (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors).
// It also translates the Values in the query passed in
// as the Kubernetes Labels Selector (KLS) parsed it into a
// set of strings, but there are some assumptions we can
// make based off of these rules:
// https://github.com/kubernetes/apimachinery/blob/master/pkg/labels/selector.go#L104-L110.
//
// Note: because KLS stores its values as strings, we
// cannot query with a value other than a string. An
// exception is made when querying with "gt" or "lt"
// operator, because although they're stored as strings,
// they were ensured to be integers, so we convert
// string to int.
func translateQuery(q query.Query) (bson.M, error) {
	mgoQuery := make(bson.M)
	for _, p := range q.Predicates {
		switch p.Operator {
		case "in":
			mgoQuery[p.Label] = bson.M{"$in": p.Values}
		case "nin":
			mgoQuery[p.Label] = bson.M{"$nin": p.Values}
		case "=", "==":
			mgoQuery[p.Label] = p.Values[0]
		case "!=":
			mgoQuery[p.Label] = bson.M{"$ne": p.Values[0]}
		case "lt":
			i, err := strconv.Atoi(p.Values[0])
			if err != nil {
				return mgoQuery, err
			}
			mgoQuery[p.Label] = bson.M{"$lt": i}
		case "gt":
			i, err := strconv.Atoi(p.Values[0])
			if err != nil {
				return mgoQuery, err
			}
			mgoQuery[p.Label] = bson.M{"$gt": i}
		case "exists":
			mgoQuery[p.Label] = bson.M{"$exists": true}
		case "!":
			mgoQuery[p.Label] = bson.M{"$exists": false}
		default:
			return nil, errors.New("unknown operator")
		}
	}
	return mgoQuery, nil
}
