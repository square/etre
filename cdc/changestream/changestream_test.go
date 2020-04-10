package changestream_test

import (
	"github.com/square/etre"
)

var events1 = []etre.CDCEvent{
	{
		EventId:    "1",
		EntityId:   "e1",
		EntityType: "node",
		Rev:        0,
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
		EventId:    "2",
		EntityId:   "e1",
		EntityType: "node",
		Rev:        1,
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
		EventId:    "3",
		EntityId:   "e1",
		EntityType: "node",
		Rev:        2,
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
		EventId:    "4",
		EntityId:   "e1",
		EntityType: "node",
		Rev:        3,
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
