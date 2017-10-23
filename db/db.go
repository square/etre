// Copyright 2017, Square, Inc.

// Package db is a connector to execute CRUD commands for a single entity and
// many entities on a DB instance.
//
// A note on querying limitations:
//
//   This package uses the Kubernetes Labels Selector syntax (KLS) for its
//   query language. There are some querying limitations with using KLS which
//   is explained below:
//
//     Querying many entities: there are some limitations.
//
//       Because of limitations in KLS, a caller can only query by field names
//       that are alphanumeric characters, '-', '_' or '.', and that start and
//       end with an alphanumeric character. The limitations also extend to
//       operators and values. Less than and greater than operators will
//       interpret their values as an integer.  All other operators will
//       interpret their values as a string. For example, caller cannot query
//       for all documents that have field name "x" with value 2.
//
//     Creating entities: there are no limitations.
//
//       CreateEntities will allow you to create any entity that's a map of a
//       string to interface{}. However, given the query limitations explained
//       above, if you expect to be able to query for a field/value, ensure you
//       only create an entity that would then satisfy a query.
//
package db

import (
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/square/etre/query"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Entity is a representation of what we store in the database.
type Entity map[string]interface{}

// Connector interface has methods needed to do CRUD operations on entities.
type Connector interface {
	// Managing labels for a single entity
	DeleteEntityLabel(string, string, string) (Entity, error)

	// Managing Multiple entities
	CreateEntities(string, []Entity) ([]string, error)
	ReadEntities(string, query.Query) ([]Entity, error)
	UpdateEntities(string, query.Query, Entity) ([]Entity, error)
	DeleteEntities(string, query.Query) ([]Entity, error)

	// Managing connection to DB
	Connect() error
	Disconnect()
}

// mongo struct stores all info needed to connect and query a mongo instance.
type mongo struct {
	url         string
	database    string
	entityTypes []string
	timeout     int
	tlsConfig   *tls.Config
	session     *mgo.Session
	credentials map[string]string
}

// Map of Kubernetes Selection Operator to mongoDB Operator.
//
// Relevant documentation:
//
//     https://github.com/kubernetes/apimachinery/blob/master/pkg/selection/operator.go
//     https://docs.mongodb.com/manual/reference/operator/query/#query-selectors
//
var operatorMap = map[string]string{
	"in":    "$in",
	"notin": "$nin",
	"=":     "$eq",
	"==":    "$eq",
	"!=":    "$ne",
	"lt":    "$lt",
	"gt":    "$gt",
}

var reservedNames = []string{"entity", "entities"}

// NewConnector creates an instance of Connector.
func NewConnector(url string, database string, entityTypes []string, timeout int, tlsConfig *tls.Config, credentials map[string]string) (Connector, error) {

	// Ensure no entityType name is a reserved word
	for _, r := range reservedNames {
		for _, c := range entityTypes {
			if r == c {
				return nil, errors.New(fmt.Sprintf("Entity type (%s) cannot be a reserved word (%s).", c, r))
			}
		}
	}

	return &mongo{
		url:         url,
		database:    database,
		entityTypes: entityTypes,
		timeout:     timeout,
		tlsConfig:   tlsConfig,
		credentials: credentials,
	}, nil
}

// Connect creates a session to db. All queries to DB will copy this session to
// make a request. When you are finished, you must call Disconnect to close the session.
func (m *mongo) Connect() error {
	// Make custom dialer that can do TLS
	dialInfo, err := mgo.ParseURL(m.url)
	if err != nil {
		// Returning raw, low-level error since this method only does one thing.
		return err
	}

	timeoutSec := time.Duration(m.timeout) * time.Second

	dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		if m.tlsConfig != nil {
			dialer := &net.Dialer{
				Timeout: timeoutSec,
			}
			conn, err := tls.DialWithDialer(dialer, "tcp", addr.String(), m.tlsConfig)
			if err != nil {
				return nil, err
			}
			return conn, nil
		} else {
			conn, err := net.DialTimeout("tcp", addr.String(), timeoutSec)
			if err != nil {
				return nil, err
			}
			return conn, nil
		}
	}

	// Connect
	s, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return err
	}

	m.session = s

	// Login
	if m.credentials["username"] != "" && m.credentials["source"] != "" && m.credentials["mechanism"] != "" {
		cred := &mgo.Credential{
			Username:  m.credentials["username"],
			Source:    m.credentials["source"],
			Mechanism: m.credentials["mechanism"],
		}
		err = s.Login(cred)
		if err != nil {
			return err
		}
	}

	return nil
}

// Disconnect must be called when finished with db connector.
func (m *mongo) Disconnect() {
	m.session.Close()
}

// DeleteEntityLabel deletes a label from an entity.
//
// Relevant documentation:
//
//     https://docs.mongodb.com/manual/reference/operator/update/unset/#up._S_unset
//
func (m *mongo) DeleteEntityLabel(entityType string, id string, label string) (Entity, error) {
	if !validEntityType(m, entityType) {
		return nil, errors.New(fmt.Sprintf("Invalid entityType name: %s. Valid entityType names: %s.", entityType, m.entityTypes))
	}

	s := m.session.Copy()
	defer s.Close()

	c := s.DB(m.database).C(entityType)

	// ReturnNew: false is the default option. Will return the original document
	// before update was performed.
	change := mgo.Change{
		Update:    bson.M{"$unset": bson.M{label: ""}},
		ReturnNew: false,
	}

	var diff Entity
	// We call Select so that Apply will return the orginal deleted label (and
	// "_id" field, which is included by default) rather than returning the
	// entire original document
	_, err := c.Find(bson.M{"_id": id}).Select(bson.M{label: 1}).Apply(change, &diff)
	if err != nil {
		return nil, ErrDeleteLabel{DbError: err}
	}

	return diff, nil
}

// CreateEntities inserts many entities into DB. This method allows for partial
// success and failure which means the return value and error are _not_
// mutually exclusive. Caller should check and handle both.
//
// Mgo does not return a useful error when inserting many documents and a
// failure occurs (e.g. want to insert 4, 3 are inserted ok but the 4th fails).
// So, CreateEntities inserts entities one at a time in order to return known
// inserted entities to caller.
//
// If all entities are successfully inserted, a slice of their IDs are
// returned. If some inserts failed, a slice of the successfully inserted
// entities are returned alone with an error that contains the total number of
// entities inserted. Since the entities were inserted in order (guranteed by
// inserting one by one), caller should only return subset of entities that
// failed to be inserted.
func (m *mongo) CreateEntities(entityType string, entities []Entity) ([]string, error) {
	if !validEntityType(m, entityType) {
		return nil, errors.New(fmt.Sprintf("Invalid entityType name (%s). Valid entityType names: %s.", entityType, m.entityTypes))
	}

	for _, e := range entities {
		if _, ok := e["_id"]; ok {
			return nil, fmt.Errorf("_id set in entity[%d]; _id cannot be set on insert")
		}
		if _, ok := e["_type"]; ok {
			return nil, fmt.Errorf("_type set in entity[%d]; _type cannot be set on insert")
		}
		if _, ok := e["_rev"]; ok {
			return nil, fmt.Errorf("_rev set in entity[%d]; _rev cannot be set on insert")
		}
	}

	s := m.session.Copy()
	defer s.Close()
	c := m.session.DB(m.database).C(entityType)

	// A slice of IDs we generate to insert along with entities into DB
	insertedObjectIds := make([]string, 0, len(entities))

	for _, e := range entities {
		// Mgo driver does not return the ObjectId that Mongo creates, so create it ourself.
		id := bson.NewObjectId()
		e["_id"] = id
		e["_type"] = entityType
		e["_rev"] = uint(0)

		if err := c.Insert(e); err != nil {
			return insertedObjectIds, ErrCreate{DbError: err, N: len(insertedObjectIds)}
		}

		// bson.ObjectId.String() yields "ObjectId("abc")", but we need to report only "abc",
		// so re-encode the raw bytes to a hex string. This make GET /entity/{t}/abc work.
		insertedObjectIds = append(insertedObjectIds, hex.EncodeToString([]byte(id)))
	}

	return insertedObjectIds, nil
}

// ReadEntities queries the db and returns a slice of Entity objects if
// something is found, a nil slice if nothing is found, and an error if one
// occurs.
func (m *mongo) ReadEntities(entityType string, q query.Query) ([]Entity, error) {
	if !validEntityType(m, entityType) {
		return nil, errors.New(fmt.Sprintf("Invalid entityType name (%s). Valid entityType names: %s.", entityType, m.entityTypes))
	}

	s := m.session.Copy()
	defer s.Close()
	c := s.DB(m.database).C(entityType)

	mgoQuery := translateQuery(q)

	var entities []Entity
	err := c.Find(mgoQuery).All(&entities)
	if err != nil {
		return nil, ErrRead{DbError: err}
	}

	return entities, nil
}

// UpdateEntities queries the db and updates all Entity matching that query.
// This method allows for partial success and failure which means the return
// value and error are _not_ mutually exclusive. Caller should check and handle
// both.
//
// Returns a slice of partial entities ("_id" field and changed fields only)
// for the ones updated and an error if there is one. For example, if 4
// entities were supposed to be updated and 3 are ok and the 4th fails, a slice
// with 3 updated entities and an error will be returned.
//
//   q, _ := query.Translate("y=foo")
//   update := db.Entity{"y": "bar"}
//
//   diffs, err := c.UpdateEntities(q, update)
//
func (m *mongo) UpdateEntities(t string, q query.Query, u Entity) ([]Entity, error) {
	if !validEntityType(m, t) {
		return nil, errors.New(fmt.Sprintf("Invalid entityType name (%s). Valid entityType names: %s.", t, m.entityTypes))
	}

	mgoQuery := translateQuery(q)

	s := m.session.Copy()
	defer s.Close()

	entityType := s.DB(m.database).C(t)

	// We can only call update on one doc at a time, so we generate a slice of IDs
	// for docs to update.
	ids, err := idsForQuery(entityType, mgoQuery)
	if err != nil {
		return nil, ErrUpdate{DbError: err}
	}

	// Change to make
	change := mgo.Change{
		Update: bson.M{"$set": u},
		// Return the original doc before modifications. This is the default
		// option.
		ReturnNew: false,
	}

	// diffs is a slice made up of a diff for each doc updated
	diffs := make([]Entity, 0, len(ids))

	// Query for each document and apply update
	for _, id := range ids {
		var diff Entity
		// We call Select so that Apply will return only the fields we select
		// ("_id" field and changed fields) rather than it returning the original
		// document.
		_, err := entityType.Find(bson.M{"_id": id}).Select(selectMap(u)).Apply(change, &diff)
		if err != nil {
			return diffs, ErrUpdate{DbError: err}
		}

		diffs = append(diffs, diff)
	}

	return diffs, nil
}

// DeleteEntities queries the db and deletes all Entity matching that query.
// This method allows for partial success and failure which means the return
// value and error are _not_ mutually exclusive. Caller should check and handle
// both.
//
// Returns a slice of successfully deleted entities an error if there is one.
// For example, if 4 entities were supposed to be deleted and 3 are ok and the
// 4th fails, a slice with 3 deleted entities and an error will be returned.
func (m *mongo) DeleteEntities(t string, q query.Query) ([]Entity, error) {
	if !validEntityType(m, t) {
		return nil, errors.New(fmt.Sprintf("Invalid entityType name (%s). Valid entityType names: %s.", t, m.entityTypes))
	}

	mgoQuery := translateQuery(q)

	s := m.session.Copy()
	defer s.Close()

	entityType := s.DB(m.database).C(t)

	// List of IDs for docs to update
	ids, err := idsForQuery(entityType, mgoQuery)
	if err != nil {
		return nil, ErrDelete{DbError: err}
	}

	// Change to make
	change := mgo.Change{
		Remove: true,
		// Return the original doc before modifications. This is the default
		// option.
		ReturnNew: false,
	}

	// deletedEntities is a slice of entities that have been successfully deleted
	deletedEntities := make([]Entity, 0, len(ids))

	// Query for each document and delete it
	for _, id := range ids {
		var deletedEntity Entity
		_, err := entityType.Find(bson.M{"_id": id}).Apply(change, &deletedEntity)
		if err != nil {
			return deletedEntities, ErrDelete{DbError: err}
		}

		deletedEntities = append(deletedEntities, deletedEntity)
	}

	return deletedEntities, nil
}

// Translates Query object to bson.M object, which will
// be used to query DB.
func translateQuery(q query.Query) bson.M {
	mgoQuery := make(bson.M)
	for _, p := range q.Predicates {
		switch p.Operator {
		case "exists":
			mgoQuery[p.Label] = bson.M{"$exists": true}
		case "!":
			mgoQuery[p.Label] = bson.M{"$exists": false}
		default:
			mgoQuery[p.Label] = bson.M{operatorMap[p.Operator]: p.Value}
		}
	}
	return mgoQuery
}

// idsForQuery gets all ids of docs that satisfy query
func idsForQuery(c *mgo.Collection, q bson.M) ([]string, error) {
	var resultIds []map[string]string
	// Select only "_id" field in returned results
	err := c.Find(q).Select(bson.M{"_id": 1}).All(&resultIds)
	if err != nil {
		// Return raw, low-level error to allow caller wrap into higher level
		// error.
		return nil, err
	}

	ids := make([]string, len(resultIds))

	for i := range ids {
		ids[i] = resultIds[i]["_id"]
	}

	return ids, nil
}

// selectMap is a map of field name to int of value 1. It is used in Select
// query to tell DB which fields to return. "_id" field is included in return
// by default.
//
// Relevant documentation:
//
//     https://docs.mongodb.com/v3.0/tutorial/project-fields-from-query-results/
//
func selectMap(e Entity) map[string]int {
	selectMap := make(map[string]int)
	for k, _ := range e {
		selectMap[k] = 1
	}

	return selectMap
}

// validEntityType checks if the entityType passed in is in the entityType list of the connector
func validEntityType(m *mongo, c string) bool {
	for _, entityType := range m.entityTypes {
		if c == entityType {
			return true
		}
	}
	return false
}

// ErrCreate is a higher level error for caller to check against when calling
// CreateEntities. It holds the lower level error message from DB and the
// number of entities successfully inserted on create.
type ErrCreate struct {
	DbError error
	N       int // number of entities successfully created
}

func (e ErrCreate) Error() string {
	return e.DbError.Error()
}

// ErrRead is a higher level error for caller to check against when calling
// ReadEntities. It holds the lower level error message from DB.
type ErrRead struct {
	DbError error
}

func (e ErrRead) Error() string {
	return e.DbError.Error()
}

// ErrUpdate is a higher level error for caller to check against when calling
// UpdateEntities. It holds the lower level error message from DB.
type ErrUpdate struct {
	DbError error
}

func (e ErrUpdate) Error() string {
	return e.DbError.Error()
}

// ErrDelete is a higher level error for caller to check against when calling
// DeleteEntities. It holds the lower level error message from DB.
type ErrDelete struct {
	DbError error
}

func (e ErrDelete) Error() string {
	return e.DbError.Error()
}

// ErrDeleteLabel is a higher level error for caller to check against when calling
// DeleteEntityLabel. It holds the lower level error message from DB.
type ErrDeleteLabel struct {
	DbError error
}

func (e ErrDeleteLabel) Error() string {
	return e.DbError.Error()
}
