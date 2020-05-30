// Copyright 2017-2020, Square, Inc.

package entity

import (
	"fmt"

	"github.com/square/etre"
	"github.com/square/etre/query"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type DbError struct {
	Err      error
	Type     string
	EntityId string
}

func (e DbError) Error() string {
	return e.Err.Error()
}

// WriteOp represents common metadata for insert, update, and delete Store methods.
type WriteOp struct {
	Caller     string // required (auth.Caller.Name)
	EntityType string // required
	EntityId   string // optional

	// Delete ops do not support set ops like insert and update because no
	// entities are sent by the client on delete. However, the caller can do
	// DELETE /entity/node?setOp=foo&setId=bar&setSize=2 and the controller
	// will pass along the set op values. This could (but not currently) also
	// be used to impose/inject a set op on a write op that doesn't specify
	// a set op.
	SetOp   string // optional
	SetId   string // optional
	SetSize int    // optional
}

// Map of Kubernetes Selection Operator to mongoDB Operator.
var operatorMap = map[string]string{
	"in":    "$in",
	"notin": "$nin",
	"=":     "$eq",
	"==":    "$eq",
	"!=":    "$ne",
	"<":     "$lt",
	"<=":    "$lte",
	">":     "$gt",
	">=":    "$gte",
}

// Filter translates a query.Query into a mongo-driver filter paramter.
func Filter(q query.Query) bson.M {
	filter := bson.M{}
	for _, p := range q.Predicates {
		switch p.Operator {
		case "exists":
			filter[p.Label] = bson.M{"$exists": true}
		case "notexists":
			filter[p.Label] = bson.M{"$exists": false}
		default:
			if p.Label == etre.META_LABEL_ID {
				switch p.Value.(type) {
				case string:
					id, _ := primitive.ObjectIDFromHex(p.Value.(string))
					filter[p.Label] = bson.M{operatorMap[p.Operator]: id}
				case []string:
					vals := p.Value.([]string)
					oids := make([]primitive.ObjectID, len(vals))
					for i, v := range vals {
						oids[i], _ = primitive.ObjectIDFromHex(v)
					}
					filter[p.Label] = bson.M{operatorMap[p.Operator]: oids}
				case primitive.ObjectID:
					filter[p.Label] = bson.M{operatorMap[p.Operator]: p.Value}
				default:
					panic(fmt.Sprintf("invalid _id value type: %T", p.Value))
				}
			} else {
				filter[p.Label] = bson.M{operatorMap[p.Operator]: p.Value}
			}
		}
	}
	return filter
}

const dupeKeyCode = 11000

func IsDupeKeyError(err error) error {
	// mongo.WriteException{
	//	 WriteConcernError:(*mongo.WriteConcernError)(nil),
	//   WriteErrors:mongo.WriteErrors{
	//     mongo.WriteError{
	//       Index:0,
	//       Code:11000,
	//       Message:"E11000 duplicate key error collection: coll.nodes index: x_1 dup key: { : 6 }"
	//     }
	//   }
	// }
	if _, ok := err.(mongo.WriteException); ok {
		we := err.(mongo.WriteException)
		for _, e := range we.WriteErrors {
			if e.Code == dupeKeyCode {
				return e
			}
		}
	}
	if _, ok := err.(mongo.CommandError); ok {
		ce := err.(mongo.CommandError)
		if ce.Code == dupeKeyCode {
			return ce
		}
	}
	return nil
}
