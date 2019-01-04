package metrics_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/square/etre"
	"github.com/square/etre/metrics"
)

func dump(v interface{}, t *testing.T) {
	bytes, _ := json.MarshalIndent(v, "", "  ")
	t.Log(string(bytes))
}

func TestEntityMetrics(t *testing.T) {
	// Basic, full metric test: a stroable metrics instance accessed via
	// an entity type metrics wrapper which binds the former to a specific
	// entity type to which the metric inc/val apply. Touch on all metrics
	// to ensure that Inc(ReadId) increments ReadId and not another metric,
	// which is verified by using a unique value (100, 101, 102, etc.) for
	// each metric.
	sm := metrics.NewMetrics()         // storable
	em := metrics.NewEntityMetrics(sm) // entity type

	em.EntityType("t1")

	em.Inc(metrics.Query, 100)
	em.Inc(metrics.SetOp, 101)
	em.Inc(metrics.MissSLA, 102)
	em.Inc(metrics.Read, 103)
	em.Inc(metrics.ReadQuery, 104)
	em.Inc(metrics.ReadId, 105)
	em.Inc(metrics.ReadLabels, 106)
	em.Inc(metrics.Write, 107)
	em.Inc(metrics.Insert, 108)
	em.Inc(metrics.InsertBulk, 109)
	em.Inc(metrics.Update, 110)
	em.Inc(metrics.UpdateBulk, 111)
	em.Inc(metrics.Delete, 112)
	em.Inc(metrics.DeleteBulk, 113)
	em.Inc(metrics.DeleteLabel, 114)

	em.IncLabel(metrics.LabelRead, "lr")
	em.IncLabel(metrics.LabelUpdate, "lu")
	em.IncLabel(metrics.LabelDelete, "ld")

	em.IncLabel(metrics.LabelRead, "la")
	em.IncLabel(metrics.LabelRead, "la")
	em.IncLabel(metrics.LabelUpdate, "la")
	em.IncLabel(metrics.LabelUpdate, "la")
	em.IncLabel(metrics.LabelDelete, "la")
	em.IncLabel(metrics.LabelDelete, "la")

	em.IncError(metrics.DbError)
	em.IncError(metrics.APIError)
	em.IncError(metrics.ClientError)

	em.Val(metrics.LatencyMs, 250) // ms
	em.Val(metrics.Labels, 5)
	em.Val(metrics.Labels, 10)
	em.Val(metrics.Labels, 20)

	em.Trace(map[string]string{
		"app":  "app1",
		"user": "user1",
	})
	em.Trace(map[string]string{
		"app":  "app1",
		"user": "user2",
	})

	expectReport := etre.MetricsReport{
		Ts:    time.Now().Unix(),
		Group: "",
		Global: &etre.MetricsGlobalReport{
			DbError:     1,
			APIError:    1,
			ClientError: 1,
		},
		Entity: map[string]*etre.MetricsEntityReport{
			"t1": &etre.MetricsEntityReport{
				EntityType: "t1",
				Query: &etre.MetricsQueryReport{
					Query:          100,
					SetOp:          101,
					MissSLA:        102,
					Read:           103,
					ReadQuery:      104,
					ReadId:         105,
					ReadLabels:     106,
					Write:          107,
					Insert:         108,
					InsertBulk:     109,
					Update:         110,
					UpdateBulk:     111,
					Delete:         112,
					DeleteBulk:     113,
					DeleteLabel:    114,
					Labels_min:     5,
					Labels_max:     20,
					Labels_avg:     11,
					Labels_med:     10,
					LatencyMs_max:  250,
					LatencyMs_p99:  250,
					LatencyMs_p999: 250,
				},
				Label: map[string]*etre.MetricsLabelReport{
					"lr": &etre.MetricsLabelReport{
						Read: 1,
					},
					"lu": &etre.MetricsLabelReport{
						Update: 1,
					},
					"ld": &etre.MetricsLabelReport{
						Delete: 1,
					},
					"la": &etre.MetricsLabelReport{
						Read:   2,
						Update: 2,
						Delete: 2,
					},
				},
				Trace: map[string]map[string]int64{
					"app": map[string]int64{
						"app1": 2,
					},
					"user": map[string]int64{
						"user1": 1,
						"user2": 1,
					},
				},
			},
		},
		CDC: &etre.MetricsCDCReport{},
	}
	gotReport := em.Report()
	if diff := deep.Equal(gotReport, expectReport); diff != nil {
		dump(gotReport, t)
		t.Error(diff)
	}
}

func TestMultipleEntityMetrics(t *testing.T) {
	// The same storable metrics should hold distrinct metrics for each
	// entity type used. Save a metric for entity type t1, then switch to
	// entity type t2, then switch back to t1--to ensure the pointer
	// inside em points where it should.
	sm := metrics.NewMetrics()         // storable
	em := metrics.NewEntityMetrics(sm) // entity type

	em.EntityType("t1")
	em.Inc(metrics.Query, 1)

	em.EntityType("t2")
	em.Inc(metrics.Query, 3)

	em.EntityType("t1")
	em.Inc(metrics.Query, 1)

	expectReport := etre.MetricsReport{
		Ts:     time.Now().Unix(),
		Group:  "",
		Global: &etre.MetricsGlobalReport{},
		Entity: map[string]*etre.MetricsEntityReport{
			"t1": &etre.MetricsEntityReport{
				EntityType: "t1",
				Query: &etre.MetricsQueryReport{
					Query: 2,
				},
				Label: map[string]*etre.MetricsLabelReport{},
				Trace: map[string]map[string]int64{},
			},
			"t2": &etre.MetricsEntityReport{
				EntityType: "t2",
				Query: &etre.MetricsQueryReport{
					Query: 3,
				},
				Label: map[string]*etre.MetricsLabelReport{},
				Trace: map[string]map[string]int64{},
			},
		},
		CDC: &etre.MetricsCDCReport{},
	}
	gotReport := em.Report()
	if diff := deep.Equal(gotReport, expectReport); diff != nil {
		dump(gotReport, t)
		t.Error(diff)
	}
}

func TestSharedEntityMetrics(t *testing.T) {
	// Like TestMultipleEntityMetrics above but this time we have 2 em
	// instances that concurrently read/write the same entity type (t1)
	// in the same storable metrcis (sm). With go test -race we ensure
	// that em is preventing race conditions.
	sm := metrics.NewMetrics()
	em1 := metrics.NewEntityMetrics(sm)
	em2 := metrics.NewEntityMetrics(sm)

	em1.EntityType("t1")
	em2.EntityType("t1")

	done1 := make(chan interface{})
	done2 := make(chan interface{})
	go func() {
		defer close(done1)
		for i := 0; i < 5; i++ {
			em1.Inc(metrics.Read, 1)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	go func() {
		defer close(done2)
		for i := 0; i < 5; i++ {
			em2.Inc(metrics.Write, 1)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	<-done1
	<-done2

	// Get report from both em1 and em2, and they should both report the
	// same thing:
	expectReport := etre.MetricsReport{
		Ts:     time.Now().Unix(),
		Group:  "",
		Global: &etre.MetricsGlobalReport{},
		Entity: map[string]*etre.MetricsEntityReport{
			"t1": &etre.MetricsEntityReport{
				EntityType: "t1",
				Query: &etre.MetricsQueryReport{
					Read:  5,
					Write: 5,
				},
				Label: map[string]*etre.MetricsLabelReport{},
				Trace: map[string]map[string]int64{},
			},
		},
		CDC: &etre.MetricsCDCReport{},
	}
	gotReport := em1.Report()
	if diff := deep.Equal(gotReport, expectReport); diff != nil {
		dump(gotReport, t)
		t.Error(diff)
	}
	gotReport = em2.Report()
	if diff := deep.Equal(gotReport, expectReport); diff != nil {
		dump(gotReport, t)
		t.Error(diff)
	}
}

func TestMemoryStore(t *testing.T) {
	sm := metrics.NewMetrics()         // storable
	em := metrics.NewEntityMetrics(sm) // entity type
	em.EntityType("t1")
	em.Inc(metrics.Read, 1)
	em.Inc(metrics.Write, 2)

	expectReport := etre.MetricsReport{
		Ts:     time.Now().Unix(),
		Group:  "",
		Global: &etre.MetricsGlobalReport{},
		Entity: map[string]*etre.MetricsEntityReport{
			"t1": &etre.MetricsEntityReport{
				EntityType: "t1",
				Query: &etre.MetricsQueryReport{
					Read:  1,
					Write: 2,
				},
				Label: map[string]*etre.MetricsLabelReport{},
				Trace: map[string]map[string]int64{},
			},
		},
		CDC: &etre.MetricsCDCReport{},
	}
	gotReport := em.Report()
	if diff := deep.Equal(gotReport, expectReport); diff != nil {
		dump(gotReport, t)
		t.Error(diff)
	}

	s := metrics.NewMemoryStore()

	// New store, shouldn't have the metrics yet
	if m := s.Get("test"); m != nil {
		t.Errorf("Get(test) returned non-nil, expected nil")
	}

	// Store, re-fetch, and ensure it has same values by checking reprot
	if err := s.Add(sm, "test"); err != nil {
		t.Error(err)
	}
	sm2 := s.Get("test")
	if sm2 == nil {
		t.Fatal("Get(test) returned nil, expected Metrics")
	}
	gotReport = sm2.Report()
	if diff := deep.Equal(gotReport, expectReport); diff != nil {
		dump(gotReport, t)
		t.Error(diff)
	}

	gotNames := s.Names()
	expectNames := []string{"test"}
	if diff := deep.Equal(gotNames, expectNames); diff != nil {
		t.Error(diff)
	}
}
