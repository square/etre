package metrics

import (
	"fmt"
	"sync"
	"time"

	gm "github.com/rcrowley/go-metrics"
	"github.com/square/etre"
)

const (
	// Query counter is the grand total number of queries to the server.
	// It can be used to calculate overall QPS.
	Query byte = iota

	// SetOp counter is the total number of queries that used a set op.
	SetOp

	// Labels gauge is the number of labels used by a query. The _min, _max,
	// _avg, and _med (median) stats are reported for Labels.
	Labels

	// LatencyMs gauge is response time (in milliseconds) of a query. The _max,
	// _p99 (99th percentile/top 1%), and _p999 (99.9th percentile/top 0.1%)
	// stats are reported for LatencyMs.
	LatencyMs

	// MissSLA counter is the total number of queries with LatencyMs
	// greater than the configured query latency SLA.
	MissSLA

	// Read counter is the grand total number of read queries to the server.
	Read

	// ReadQuery counter is the total number of read queries that selected
	// entities with a query string (e.g. "host=foo,env=production").
	// GET /entities and POST /query
	ReadQuery

	// ReadId counter is the total number of read queries that selected one
	// entity by its unique _id.
	// GET /entity/:id
	ReadId

	// ReadLabels counter is the total number of read label queries.
	// GET /entity/:id/:label
	ReadLabels

	// Write counter is the grand total number of write queries to the server.
	// PUT, POST, DELETE * (except ^)
	Write

	// POST /entity
	Insert

	// POST /entities
	InsertBulk

	// PUT /entity/:id
	Update

	// PUT /entities?query
	UpdateBulk

	// DELETE /entity/:id
	Delete

	// DELETE /entities?query
	DeleteBulk

	// DELETE /entity/:id/labels/:label
	DeleteLabel

	LabelRead
	LabelUpdate
	LabelDelete

	DbError
	APIError
	ClientError
)

type Metrics interface {
	// EntityType binds the Metrics instance to an entity type before calling
	// other methods. This method must be called first.
	EntityType(string)

	// Inc increments the metric name (mn) by n. The metric must be a counter.
	Inc(mn byte, n int64)

	// IncLabel increments the metric name (mn) counter for the label by 1.
	// The metric must be LabelRead, LabelUpdate, or LabelDelete.
	IncLabel(mn byte, label string)

	// IncError increments the metric name (mn) by 1. The metirc must be one
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

var _ = &metrics{} // ensure metrics implements Metrics

type metrics struct {
	global *globalMetrics
	entity map[string]*entityMetrics
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
	cdc   *cdcMetrics
}

type queryMetrics struct {
	Query       gm.Counter
	Read        gm.Counter
	ReadQuery   gm.Counter
	ReadId      gm.Counter
	ReadLabels  gm.Counter
	Write       gm.Counter
	Insert      gm.Counter
	InsertBulk  gm.Counter
	Update      gm.Counter
	UpdateBulk  gm.Counter
	Delete      gm.Counter
	DeleteBulk  gm.Counter
	DeleteLabel gm.Counter
	SetOp       gm.Counter
	Labels      gm.Histogram
	Latency     gm.Histogram
	MissSLA     gm.Counter
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
		entity: map[string]*entityMetrics{},
		Mutex:  &sync.Mutex{},
		report: etre.MetricsReport{
			Global: &etre.MetricsGlobalReport{},
			Entity: map[string]*etre.MetricsEntityReport{},
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

	for entityType := range m.entity {
		em := m.entity[entityType]
		er := m.report.Entity[entityType]

		er.Query.Query = em.query.Query.Count()
		er.Query.Read = em.query.Read.Count()
		er.Query.ReadQuery = em.query.ReadQuery.Count()
		er.Query.ReadId = em.query.ReadId.Count()
		er.Query.ReadLabels = em.query.ReadLabels.Count()
		er.Query.Write = em.query.Write.Count()
		er.Query.Insert = em.query.Insert.Count()
		er.Query.InsertBulk = em.query.InsertBulk.Count()
		er.Query.Update = em.query.Update.Count()
		er.Query.UpdateBulk = em.query.UpdateBulk.Count()
		er.Query.Delete = em.query.Delete.Count()
		er.Query.DeleteBulk = em.query.DeleteBulk.Count()
		er.Query.DeleteLabel = em.query.DeleteLabel.Count()
		er.Query.SetOp = em.query.SetOp.Count()
		er.Query.MissSLA = em.query.MissSLA.Count()

		er.Query.Labels_min = em.query.Labels.Min()
		er.Query.Labels_max = em.query.Labels.Max()
		er.Query.Labels_avg = int64(em.query.Labels.Mean())
		er.Query.Labels_med = int64(em.query.Labels.Percentile(0.50))
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

// --------------------------------------------------------------------------

var _ = entityTypeMetrics{} // ensure metrics implements Metrics

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
			ReadLabels:  gm.NewCounter(),
			Write:       gm.NewCounter(),
			Insert:      gm.NewCounter(),
			InsertBulk:  gm.NewCounter(),
			Update:      gm.NewCounter(),
			UpdateBulk:  gm.NewCounter(),
			Delete:      gm.NewCounter(),
			DeleteBulk:  gm.NewCounter(),
			DeleteLabel: gm.NewCounter(),
			SetOp:       gm.NewCounter(),
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
		cdc: &cdcMetrics{
			Clients: gm.NewCounter(),
		},
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
	case Insert:
		m.em.query.Insert.Inc(n)
	case InsertBulk:
		m.em.query.InsertBulk.Inc(n)
	case Update:
		m.em.query.Update.Inc(n)
	case UpdateBulk:
		m.em.query.UpdateBulk.Inc(n)
	case Delete:
		m.em.query.Delete.Inc(n)
	case DeleteBulk:
		m.em.query.DeleteBulk.Inc(n)
	case DeleteLabel:
		m.em.query.DeleteLabel.Inc(n)
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

// --------------------------------------------------------------------------

type Factory interface {
	Make() Metrics
}

type factory struct{}

func NewFactory() factory {
	return factory{}
}

func (f factory) Make() Metrics {
	return NewMetrics()
}

type Store interface {
	Add(m Metrics, name string) error
	Get(name string) Metrics
	Names() []string
}

type memoryStore struct {
	metrics map[string]Metrics
	*sync.RWMutex
}

func NewMemoryStore() Store {
	return &memoryStore{
		metrics: map[string]Metrics{},
		RWMutex: &sync.RWMutex{},
	}
}

func (s *memoryStore) Add(m Metrics, name string) error {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.metrics[name]; ok {
		return fmt.Errorf("metrics for %s already stored", name)
	}
	s.metrics[name] = m
	return nil
}

func (s *memoryStore) Get(name string) Metrics {
	s.RLock()
	defer s.RUnlock()
	return s.metrics[name]
}

func (s *memoryStore) Names() []string {
	s.RLock()
	names := make([]string, 0, len(s.metrics))
	for k := range s.metrics {
		names = append(names, k)
	}
	s.RUnlock()
	return names
}
