// Copyright 2017-2020, Square, Inc.

package changestream_test

import (
	"github.com/square/etre"
)

var events1 = []etre.CDCEvent{
	{
		Id:         "1",
		EntityId:   "e1",
		EntityType: "node",
		EntityRev:  0,
		Ts:         100,
		Op:         "i",
		New: &etre.Entity{
			"_id":      "e1",
			"_type":    "node",
			"_rev":     int64(0),
			"hostname": "host1",
		},
	},
	{
		Id:         "2",
		EntityId:   "e1",
		EntityType: "node",
		EntityRev:  1,
		Ts:         200,
		Op:         "u",
		New: &etre.Entity{
			"_id":      "e1",
			"_type":    "node",
			"_rev":     int64(1),
			"hostname": "host2",
		},
	},
	{
		Id:         "3",
		EntityId:   "e1",
		EntityType: "node",
		EntityRev:  2,
		Ts:         300,
		Op:         "u",
		New: &etre.Entity{
			"_id":      "e1",
			"_type":    "node",
			"_rev":     int64(2),
			"hostname": "host3",
		},
	},
	{
		Id:         "4",
		EntityId:   "e1",
		EntityType: "node",
		EntityRev:  3,
		Ts:         400,
		Op:         "d",
		Old: &etre.Entity{
			"_id":      "e1",
			"_type":    "node",
			"_rev":     int64(2),
			"hostname": "host3",
		},
	},
}
