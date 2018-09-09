package metrics

import (
	"sync"
	"time"

	gm "github.com/rcrowley/go-metrics"
	"github.com/square/etre"
)

type Config struct {
	EntityTypes []string
}

type Metrics struct {
	Config Config
	Global *GlobalMetrics
	Entity map[string]*EntityMetrics

	*sync.Mutex
	report etre.MetricsReport
}

type GlobalMetrics struct {
	DbError     gm.Counter
	APIError    gm.Counter
	ClientError gm.Counter
}

type EntityMetrics struct {
	Query *QueryMetrics
	Label map[string]*LabelMetrics
	Trace *TraceMetrics
	CDC   *CDCMetrics
}

type QueryMetrics struct {
	All gm.Counter // Read + Write

	Read       gm.Counter // Read*
	ReadQuery  gm.Counter // GET /entities and POST /query
	ReadId     gm.Counter // GET /entity/:id
	ReadLabels gm.Counter // GET /entity/:id/:label

	Write       gm.Counter // PUT, POST, DELETE * (except ^)
	Insert      gm.Counter // POST /entity
	InsertBulk  gm.Counter // POST /entities
	Update      gm.Counter // PUT /entity/:id
	UpdateBulk  gm.Counter // PUT /entities?query
	Delete      gm.Counter // DELETE /entity/:id
	DeleteBulk  gm.Counter // DELETE /entities?query
	DeleteLabel gm.Counter // DELETE /entity/:id/labels/:label

	SetOp   gm.Counter   // query uses set op
	Labels  gm.Histogram // number of labels in query
	Latency gm.Histogram // query response time (ms)
	MissSLA gm.Counter   // Latency > config.team.QueryLatencySLA
}

type LabelMetrics struct {
	Read   gm.Counter
	Update gm.Counter
	Delete gm.Counter
}

type TraceMetrics struct {
	User map[string]gm.Counter
	App  map[string]gm.Counter
	Host map[string]gm.Counter
}

type CDCMetrics struct {
	Clients gm.Counter
}

func NewMetrics(c Config) *Metrics {
	m := &Metrics{
		Config: c,
		Global: &GlobalMetrics{
			DbError:     gm.NewCounter(),
			APIError:    gm.NewCounter(),
			ClientError: gm.NewCounter(),
		},
		Entity: map[string]*EntityMetrics{},
		// --
		Mutex: &sync.Mutex{},
	}
	report := etre.MetricsReport{
		Global: &etre.MetricsGlobalReport{},
		Entity: map[string]*etre.MetricsEntityReport{},
	}
	for _, entityType := range c.EntityTypes {
		m.Entity[entityType] = &EntityMetrics{
			Query: &QueryMetrics{
				All:         gm.NewCounter(),
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
			Label: map[string]*LabelMetrics{},
			Trace: &TraceMetrics{
				User: map[string]gm.Counter{},
				App:  map[string]gm.Counter{},
				Host: map[string]gm.Counter{},
			},
			CDC: &CDCMetrics{
				Clients: gm.NewCounter(),
			},
		}
		report.Entity[entityType] = &etre.MetricsEntityReport{
			EntityType: entityType,
			Query:      &etre.MetricsQueryReport{},
			Label:      map[string]*etre.MetricsLabelReport{},
			Trace: &etre.MetricsTraceReport{
				User: map[string]int64{},
				App:  map[string]int64{},
				Host: map[string]int64{},
			},
		}
	}
	m.report = report
	return m
}

const (
	read byte = iota
	update
	delete
)

func (em *EntityMetrics) ReadLabel(label string) {
	em.incLabel(label, read)
}

func (em *EntityMetrics) UpdateLabel(label string) {
	em.incLabel(label, update)
}

func (em *EntityMetrics) DeleteLabel(label string) {
	em.incLabel(label, delete)
}

func (em *EntityMetrics) incLabel(label string, op byte) {
	lm := em.Label[label]
	if lm == nil {
		lm = &LabelMetrics{
			Read:   gm.NewCounter(),
			Update: gm.NewCounter(),
			Delete: gm.NewCounter(),
		}
		em.Label[label] = lm
	}
	switch op {
	case read:
		lm.Read.Inc(1)
	case update:
		lm.Update.Inc(1)
	case delete:
		lm.Delete.Inc(1)
	}
}

func (em *EntityMetrics) User(name string) {
	user, ok := em.Trace.User[name]
	if !ok {
		user = gm.NewCounter()
		em.Trace.User[name] = user
	}
	user.Inc(1)
}

func (em *EntityMetrics) App(name string) {
	user, ok := em.Trace.App[name]
	if !ok {
		user = gm.NewCounter()
		em.Trace.App[name] = user
	}
	user.Inc(1)
}

func (em *EntityMetrics) Host(name string) {
	user, ok := em.Trace.Host[name]
	if !ok {
		user = gm.NewCounter()
		em.Trace.Host[name] = user
	}
	user.Inc(1)
}

func (m *Metrics) Report() etre.MetricsReport {
	m.Lock()

	m.report.Ts = time.Now().Unix()

	m.report.Global.DbError = m.Global.DbError.Count()
	m.report.Global.APIError = m.Global.APIError.Count()
	m.report.Global.ClientError = m.Global.ClientError.Count()

	for _, entityType := range m.Config.EntityTypes {
		em := m.Entity[entityType]
		er := m.report.Entity[entityType]

		er.Query.All = em.Query.All.Count()
		er.Query.Read = em.Query.Read.Count()
		er.Query.ReadQuery = em.Query.ReadQuery.Count()
		er.Query.ReadId = em.Query.ReadId.Count()
		er.Query.ReadLabels = em.Query.ReadLabels.Count()
		er.Query.Write = em.Query.Write.Count()
		er.Query.Insert = em.Query.Insert.Count()
		er.Query.InsertBulk = em.Query.InsertBulk.Count()
		er.Query.Update = em.Query.Update.Count()
		er.Query.UpdateBulk = em.Query.UpdateBulk.Count()
		er.Query.Delete = em.Query.Delete.Count()
		er.Query.DeleteBulk = em.Query.DeleteBulk.Count()
		er.Query.DeleteLabel = em.Query.DeleteLabel.Count()
		er.Query.SetOp = em.Query.SetOp.Count()
		er.Query.MissSLA = em.Query.MissSLA.Count()

		er.Query.LabelsMin = em.Query.Labels.Min()
		er.Query.LabelsMax = em.Query.Labels.Max()
		er.Query.LabelsAvg = int64(em.Query.Labels.Mean())
		er.Query.LabelsMed = int64(em.Query.Labels.Percentile(0.50))
		em.Query.Labels.Clear()

		er.Query.LatencyMax = float64(em.Query.Latency.Max())
		p := em.Query.Latency.Percentiles([]float64{0.99, 0.999})
		er.Query.LatencyP99 = p[0]
		er.Query.LatencyP999 = p[1]
		em.Query.Latency.Clear()

		for label, lm := range em.Label {
			lr, ok := er.Label[label]
			if !ok {
				lr = &etre.MetricsLabelReport{}
				er.Label[label] = lr
			}
			lr.Read = lm.Read.Count()
			lr.Update = lm.Update.Count()
			lr.Delete = lm.Delete.Count()
		}

		for user, c := range em.Trace.User {
			er.Trace.User[user] = c.Count()
		}

		for app, c := range em.Trace.App {
			er.Trace.App[app] = c.Count()
		}

		for host, c := range em.Trace.Host {
			er.Trace.Host[host] = c.Count()
		}
	}
	m.Unlock()
	return m.report
}
