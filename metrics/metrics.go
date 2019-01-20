// Copyright 2018-2019, Square, Inc.

// Package metrics provides Etre metrics.
package metrics

import (
	"fmt"
	"sync"
	"time"

	gm "github.com/rcrowley/go-metrics"
	"github.com/square/etre"
)

var (
	defaultSampleSize int = 2000 // unique values, ~16 KiB per metric
)

// See ../metrics.go for docs of each metric. The number/order of these
// does not matter. They are const numbers only to avoid typos and enable
// compile-time checking, e.g. m.Inc(metrics.Reed, 1) will cause error
// "undefined: metrics.Reed". Implementations of Metrics must ensure that
// metric X = etre.MetricReport.X.
const (
	Query       byte = iota // counter
	SetOp                   // counter
	Labels                  // histogram
	LatencyMs               // histogram
	MissSLA                 // counter
	Read                    // counter
	ReadQuery               // counter
	ReadId                  // counter
	ReadMatch               // histogram
	ReadLabels              // counter
	Write                   // counter
	CreateOne               // counter
	CreateMany              // counter
	CreateBulk              // histogram
	UpdateId                // counter
	UpdateQuery             // counter
	UpdateBulk              // histogram
	DeleteId                // counter
	DeleteQuery             // counter
	DeleteBulk              // histogram
	DeleteLabel             // counter
	LabelRead               // counter (per-label)
	LabelUpdate             // counter (per-label)
	LabelDelete             // counter (per-label)
	DbError                 // counter (global)
	APIError                // counter (global)
	ClientError             // counter (global)
	CDCClients              // counter (global)
	Created                 // counter
	Updated                 // counter
	Deleted                 // counter
)

// Metrics abstracts how metrics are stored and sampled.
type Metrics interface {
	// EntityType binds the Metrics instance to an entity type before calling
	// other methods. This method must be called first.
	EntityType(string)

	// Inc increments the metric name (mn) by n. The metric must be a counter.
	Inc(mn byte, n int64)

	// IncLabel increments the metric name (mn) counter for the label by 1.
	// The metric must be LabelRead, LabelUpdate, or LabelDelete.
	IncLabel(mn byte, label string)

	// IncError increments the metric name (mn) by 1. The metric must be one
	// of the *Error metrics.
	IncError(mn byte)

	// Val records one measurement (n) for the metric name (mn). The metric
	// must be a gauge.
	Val(mn byte, n int64)

	// Trace increments trace value counters by 1.
	Trace(map[string]string)

	// Report returns a snapshot of all metrics, calculating stats like average
	// and percentiles.
	Report() etre.MetricsReport
}

var _ Metrics = &metrics{} // ensure metrics implements Metrics

type metrics struct {
	global *globalMetrics
	entity map[string]*entityMetrics
	cdc    *cdcMetrics
	*sync.Mutex
	report etre.MetricsReport
}

type globalMetrics struct {
	DbError     gm.Counter
	APIError    gm.Counter
	ClientError gm.Counter
}

type entityMetrics struct {
	query *queryMetrics
	label map[string]*labelMetrics
	trace map[string]map[string]gm.Counter
}

type queryMetrics struct {
	Query       gm.Counter
	Read        gm.Counter
	ReadQuery   gm.Counter
	ReadId      gm.Counter
	ReadMatch   gm.Histogram
	ReadLabels  gm.Counter
	Write       gm.Counter
	CreateOne   gm.Counter
	CreateMany  gm.Counter
	CreateBulk  gm.Histogram
	UpdateId    gm.Counter
	UpdateQuery gm.Counter
	UpdateBulk  gm.Histogram
	DeleteId    gm.Counter
	DeleteQuery gm.Counter
	DeleteBulk  gm.Histogram
	DeleteLabel gm.Counter
	SetOp       gm.Counter
	Labels      gm.Histogram
	Latency     gm.Histogram
	MissSLA     gm.Counter
	Created     gm.Counter
	Updated     gm.Counter
	Deleted     gm.Counter
}

type labelMetrics struct {
	Read   gm.Counter
	Update gm.Counter
	Delete gm.Counter
}

type cdcMetrics struct {
	Clients gm.Counter
}

func NewMetrics() *metrics {
	return &metrics{
		global: &globalMetrics{
			DbError:     gm.NewCounter(),
			APIError:    gm.NewCounter(),
			ClientError: gm.NewCounter(),
		},
		cdc: &cdcMetrics{
			Clients: gm.NewCounter(),
		},
		entity: map[string]*entityMetrics{},
		Mutex:  &sync.Mutex{},
		report: etre.MetricsReport{
			Global: &etre.MetricsGlobalReport{},
			Entity: map[string]*etre.MetricsEntityReport{},
			CDC:    &etre.MetricsCDCReport{},
		},
	}
}

func (m *metrics) EntityType(string) {
	panic("metrics.EntityType() called directly; need to call entityTypeMetrics.EntityType()")
}

func (m *metrics) Inc(mn byte, n int64) {
	panic("metrics.Inc() called directly; need to call entityTypeMetrics.Inc()")
}

func (m *metrics) IncLabel(mn byte, label string) {
	panic("metrics.IncLabel() called directly; need to call entityTypeMetrics.IncLabel()")
}

func (m *metrics) IncError(mn byte) {
	switch mn {
	case DbError:
		m.global.DbError.Inc(1)
	case APIError:
		m.global.APIError.Inc(1)
	case ClientError:
		m.global.ClientError.Inc(1)
	default:
		errMsg := fmt.Sprintf("non-counter metric number passed to IncError: %d", mn)
		panic(errMsg)
	}
}

func (m *metrics) Val(mn byte, n int64) {
	panic("metrics.Val() called directly; need to call entityTypeMetrics.Val()")
}

func (m *metrics) Trace(map[string]string) {
	panic("metrics.Trace() called directly; need to call entityTypeMetrics.Trace()")
}

func (m *metrics) Report() etre.MetricsReport {
	m.Lock()
	defer m.Unlock()

	m.report.Ts = time.Now().Unix()

	m.report.Global.DbError = m.global.DbError.Count()
	m.report.Global.APIError = m.global.APIError.Count()
	m.report.Global.ClientError = m.global.ClientError.Count()

	m.report.CDC.Clients = m.cdc.Clients.Count()

	for entityType := range m.entity {
		em := m.entity[entityType]
		er := m.report.Entity[entityType]
		qr := er.Query

		er.Query.Query = em.query.Query.Count()
		er.Query.Read = em.query.Read.Count()
		er.Query.ReadQuery = em.query.ReadQuery.Count()
		er.Query.ReadId = em.query.ReadId.Count()
		er.Query.ReadLabels = em.query.ReadLabels.Count()
		er.Query.Write = em.query.Write.Count()
		er.Query.CreateOne = em.query.CreateOne.Count()
		er.Query.CreateMany = em.query.CreateMany.Count()
		er.Query.UpdateId = em.query.UpdateId.Count()
		er.Query.UpdateQuery = em.query.UpdateQuery.Count()
		er.Query.DeleteId = em.query.DeleteId.Count()
		er.Query.DeleteQuery = em.query.DeleteQuery.Count()
		er.Query.DeleteLabel = em.query.DeleteLabel.Count()
		er.Query.SetOp = em.query.SetOp.Count()
		er.Query.MissSLA = em.query.MissSLA.Count()
		er.Query.Created = em.query.Created.Count()
		er.Query.Updated = em.query.Updated.Count()
		er.Query.Deleted = em.query.Deleted.Count()

		qr.ReadMatch_min, qr.ReadMatch_max, qr.ReadMatch_avg, qr.ReadMatch_med = minMaxAvgMed(em.query.ReadMatch)
		em.query.ReadMatch.Clear()

		qr.CreateBulk_min, qr.CreateBulk_max, qr.CreateBulk_avg, qr.CreateBulk_med = minMaxAvgMed(em.query.CreateBulk)
		em.query.CreateBulk.Clear()

		qr.UpdateBulk_min, qr.UpdateBulk_max, qr.UpdateBulk_avg, qr.UpdateBulk_med = minMaxAvgMed(em.query.UpdateBulk)
		em.query.UpdateBulk.Clear()

		qr.DeleteBulk_min, qr.DeleteBulk_max, qr.DeleteBulk_avg, qr.DeleteBulk_med = minMaxAvgMed(em.query.DeleteBulk)
		em.query.DeleteBulk.Clear()

		qr.Labels_min, qr.Labels_max, qr.Labels_avg, qr.Labels_med = minMaxAvgMed(em.query.Labels)
		em.query.Labels.Clear()

		p := em.query.Latency.Percentiles([]float64{0.99, 0.999})
		er.Query.LatencyMs_max = float64(em.query.Latency.Max())
		er.Query.LatencyMs_p99 = p[0]
		er.Query.LatencyMs_p999 = p[1]
		em.query.Latency.Clear()

		for label, lm := range em.label {
			lr, ok := er.Label[label]
			if !ok {
				lr = &etre.MetricsLabelReport{}
				er.Label[label] = lr
			}
			lr.Read = lm.Read.Count()
			lr.Update = lm.Update.Count()
			lr.Delete = lm.Delete.Count()
		}

		trace := map[string]map[string]int64{}
		for traceMetric, traceValues := range em.trace {
			trace[traceMetric] = map[string]int64{}
			for val, cnt := range traceValues {
				trace[traceMetric][val] = cnt.Count()
			}
		}
		er.Trace = trace
	}
	return m.report
}

func minMaxAvgMed(h gm.Histogram) (int64, int64, int64, int64) {
	return h.Min(), h.Max(), int64(h.Mean()), int64(h.Percentile(0.50))
}

// --------------------------------------------------------------------------

var _ Metrics = &entityTypeMetrics{} // ensure metrics implements Metrics

type entityTypeMetrics struct {
	*metrics                // embedded, provides implements Metrics except EntityType()
	em       *entityMetrics // points to *metrics.entity[EntityType()]
}

func NewEntityMetrics(metrics *metrics) *entityTypeMetrics {
	return &entityTypeMetrics{
		metrics: metrics,
		em:      nil, // call EntityType() to initialize
	}
}

func (m *entityTypeMetrics) EntityType(entityType string) {
	m.Lock()
	defer m.Unlock()

	var ok bool
	m.em, ok = m.entity[entityType] // m.*metrics.entity
	if ok {
		return
	}

	// First metrics for this entity type
	m.entity[entityType] = &entityMetrics{
		query: &queryMetrics{
			Query:       gm.NewCounter(),
			Read:        gm.NewCounter(),
			ReadQuery:   gm.NewCounter(),
			ReadId:      gm.NewCounter(),
			ReadMatch:   gm.NewHistogram(gm.NewUniformSample(defaultSampleSize)),
			ReadLabels:  gm.NewCounter(),
			Write:       gm.NewCounter(),
			CreateOne:   gm.NewCounter(),
			CreateMany:  gm.NewCounter(),
			CreateBulk:  gm.NewHistogram(gm.NewUniformSample(defaultSampleSize)),
			UpdateId:    gm.NewCounter(),
			UpdateQuery: gm.NewCounter(),
			UpdateBulk:  gm.NewHistogram(gm.NewUniformSample(defaultSampleSize)),
			DeleteId:    gm.NewCounter(),
			DeleteQuery: gm.NewCounter(),
			DeleteBulk:  gm.NewHistogram(gm.NewUniformSample(defaultSampleSize)),
			DeleteLabel: gm.NewCounter(),
			SetOp:       gm.NewCounter(),
			Created:     gm.NewCounter(),
			Updated:     gm.NewCounter(),
			Deleted:     gm.NewCounter(),
			Labels: gm.NewHistogram(
				// Possible values are [1, len(all labels)]. A reasonable
				// guess is that an entity has < 512 labels.
				gm.NewUniformSample(512),
			),
			Latency: gm.NewHistogram(
				// https://www.app-metrics.io/getting-started/reservoir-sampling/
				gm.NewExpDecaySample(1028, 0.015),
			),
			MissSLA: gm.NewCounter(),
		},
		label: map[string]*labelMetrics{},
		trace: map[string]map[string]gm.Counter{},
	}

	m.report.Entity[entityType] = &etre.MetricsEntityReport{
		EntityType: entityType,
		Query:      &etre.MetricsQueryReport{},
		Label:      map[string]*etre.MetricsLabelReport{},
	}

	m.em = m.entity[entityType]
}

func (m *entityTypeMetrics) Inc(mn byte, n int64) {
	switch mn {
	case Query:
		m.em.query.Query.Inc(n)
	case SetOp:
		m.em.query.SetOp.Inc(n)
	case MissSLA:
		m.em.query.MissSLA.Inc(n)
	case Read:
		m.em.query.Read.Inc(n)
	case ReadQuery:
		m.em.query.ReadQuery.Inc(n)
	case ReadId:
		m.em.query.ReadId.Inc(n)
	case ReadLabels:
		m.em.query.ReadLabels.Inc(n)
	case Write:
		m.em.query.Write.Inc(n)
	case CreateOne:
		m.em.query.CreateOne.Inc(n)
	case CreateMany:
		m.em.query.CreateMany.Inc(n)
	case UpdateId:
		m.em.query.UpdateId.Inc(n)
	case UpdateQuery:
		m.em.query.UpdateQuery.Inc(n)
	case DeleteId:
		m.em.query.DeleteId.Inc(n)
	case DeleteQuery:
		m.em.query.DeleteQuery.Inc(n)
	case DeleteLabel:
		m.em.query.DeleteLabel.Inc(n)
	case Created:
		m.em.query.Created.Inc(n)
	case Updated:
		m.em.query.Updated.Inc(n)
	case Deleted:
		m.em.query.Deleted.Inc(n)
	case CDCClients:
		if n > 0 {
			m.cdc.Clients.Inc(n)
		} else {
			m.cdc.Clients.Dec(n)
		}
	default:
		errMsg := fmt.Sprintf("non-counter metric number passed to Inc: %d", mn)
		panic(errMsg)
	}
}

func (m *entityTypeMetrics) IncLabel(mn byte, label string) {
	lm := m.em.label[label]
	if lm == nil {
		lm = &labelMetrics{
			Read:   gm.NewCounter(),
			Update: gm.NewCounter(),
			Delete: gm.NewCounter(),
		}
		m.em.label[label] = lm
	}
	switch mn {
	case LabelRead:
		lm.Read.Inc(1)
	case LabelUpdate:
		lm.Update.Inc(1)
	case LabelDelete:
		lm.Delete.Inc(1)
	default:
		errMsg := fmt.Sprintf("non-counter metric number passed to IncLabel: %d", mn)
		panic(errMsg)
	}
}

func (m *entityTypeMetrics) Val(mn byte, n int64) {
	switch mn {
	case LatencyMs:
		m.em.query.Latency.Update(n)
	case Labels:
		m.em.query.Labels.Update(n)
	case ReadMatch:
		m.em.query.ReadMatch.Update(n)
	case CreateBulk:
		m.em.query.CreateBulk.Update(n)
	case UpdateBulk:
		m.em.query.UpdateBulk.Update(n)
	case DeleteBulk:
		m.em.query.DeleteBulk.Update(n)
	default:
		errMsg := fmt.Sprintf("non-gauge metric number passed to Val: %d", mn)
		panic(errMsg)
	}
}

func (m *entityTypeMetrics) Trace(trace map[string]string) {
	for traceMetric, traceValue := range trace {
		traceValues, ok := m.em.trace[traceMetric]
		if !ok {
			traceValues = map[string]gm.Counter{}
			m.em.trace[traceMetric] = traceValues
		}
		cnt, ok := traceValues[traceValue]
		if !ok {
			cnt = gm.NewCounter()
			traceValues[traceValue] = cnt
		}
		cnt.Inc(1)
	}
}
