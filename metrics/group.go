// Copyright 2018-2020, Square, Inc.

package metrics

import (
	"fmt"
	"sync"
	"time"

	gm "github.com/daniel-nichter/go-metrics"
	"github.com/square/etre"
)

// Group is a one-to-many proxy for setting metrics in multiple metric groups.
// Group implements the Metrics inteface. The API uses a single Group for all
// the auth.Caller.MetricGroups. The API can make a single call to Inc(), for
// example, and Group calls Inc() for every metric group.
type Group struct {
	groups []*groupEntityMetrics
}

var _ Metrics = &Group{} // ensure Group implements Metrics

func NewGroup(groupNames []string, store Store) Metrics {
	groups := make([]*groupEntityMetrics, len(groupNames))
	for i, groupName := range groupNames {
		gm := store.Get(groupName) // returns Metrics
		if gm == nil {
			gm = NewGroupMetrics() // returns *groupMetrics
			store.Add(gm, groupName)
		}
		groups[i] = NewGroupEntityMetrics(gm.(*groupMetrics))
	}
	return Group{
		groups: groups,
	}
}

func (gm Group) EntityType(entityType string) {
	for _, m := range gm.groups {
		m.EntityType(entityType)
	}
}

func (gm Group) Inc(mn byte, n int64) {
	for _, m := range gm.groups {
		m.Inc(mn, n)
	}
}

func (gm Group) IncLabel(mn byte, label string) {
	for _, m := range gm.groups {
		m.IncLabel(mn, label)
	}
}

func (gm Group) Val(mn byte, n int64) {
	for _, m := range gm.groups {
		m.Val(mn, n)
	}
}

func (gm Group) Trace(trace map[string]string) {
	for _, m := range gm.groups {
		m.Trace(trace)
	}
}

func (gm Group) Report(reset bool) etre.Metrics {
	panic("do not call groupMetrics.Group.Report() directly")
}

// --------------------------------------------------------------------------

// groupMetrics contains persistent metrics for one group. The group metrics are
// persisted between requests by a Store. Once metrics for a group are created,
// they remain for the lifetime of the Etre instance, i.e. there is currently no
// pruning or eviction of unused groups.
//
// Each group has its own entity metrics, per entity type (the entity map).
// Entity metrics are created on demand and concurrent: multiple requests in the
// same group can increment the same metrics. However, groupMetrics is not
// concurrent because the Metrics interface requires binding to an entity type.
// So multiple requests in the same group cannot increment metrics in different
// entity types because groupMetrics cannot bind to two different entity types
// at the same time. groupEntityMetrics solves this problem.
type groupMetrics struct {
	request *globalMetrics
	entity  map[string]*entityMetrics // keyed on entity type;
	cdc     *cdcMetrics
	*sync.Mutex
	report etre.MetricsGroupReport
}

var _ Metrics = &groupMetrics{} // ensure groupMetrics implements Metrics

type globalMetrics struct {
	AuthorizationFailed *gm.Counter
	InvalidEntityType   *gm.Counter
	DbError             *gm.Counter
	APIError            *gm.Counter
	ClientError         *gm.Counter
}

type entityMetrics struct {
	query *queryMetrics
	label map[string]*labelMetrics
	trace map[string]map[string]*gm.Counter
}

type queryMetrics struct {
	Query       *gm.Counter
	Read        *gm.Counter
	ReadQuery   *gm.Counter
	ReadId      *gm.Counter
	ReadMatch   *gm.Histogram
	ReadLabels  *gm.Counter
	Write       *gm.Counter
	CreateOne   *gm.Counter
	CreateMany  *gm.Counter
	CreateBulk  *gm.Histogram
	UpdateId    *gm.Counter
	UpdateQuery *gm.Counter
	UpdateBulk  *gm.Histogram
	DeleteId    *gm.Counter
	DeleteQuery *gm.Counter
	DeleteBulk  *gm.Histogram
	DeleteLabel *gm.Counter
	SetOp       *gm.Counter
	Labels      *gm.Histogram
	Latency     *gm.Histogram
	MissSLA     *gm.Counter
	Created     *gm.Counter
	Updated     *gm.Counter
	Deleted     *gm.Counter
}

type labelMetrics struct {
	Read   *gm.Counter
	Update *gm.Counter
	Delete *gm.Counter
}

type cdcMetrics struct {
	Clients *gm.Counter
}

func NewGroupMetrics() *groupMetrics {
	return &groupMetrics{
		request: &globalMetrics{
			AuthorizationFailed: gm.NewCounter(),
			InvalidEntityType:   gm.NewCounter(),
			DbError:             gm.NewCounter(),
			APIError:            gm.NewCounter(),
			ClientError:         gm.NewCounter(),
		},
		cdc: &cdcMetrics{
			Clients: gm.NewCounter(),
		},
		entity: map[string]*entityMetrics{},
		Mutex:  &sync.Mutex{},
		report: etre.MetricsGroupReport{
			Request: &etre.MetricsRequestReport{},
			Entity:  map[string]*etre.MetricsEntityReport{},
			CDC:     &etre.MetricsCDCReport{},
		},
	}
}

func (m *groupMetrics) EntityType(string) {
	panic("groupMetrics.EntityType() called directly; need to call groupMetrics.EntityType()")
}

func (m *groupMetrics) Inc(mn byte, n int64) {
	panic("groupMetrics.Inc() called directly; need to call groupMetrics.Inc()")
}

func (m *groupMetrics) IncLabel(mn byte, label string) {
	panic("groupMetrics.IncLabel() called directly; need to call groupMetrics.IncLabel()")
}

func (m *groupMetrics) Val(mn byte, n int64) {
	panic("groupMetrics.Val() called directly; need to call groupMetrics.Val()")
}

func (m *groupMetrics) Trace(map[string]string) {
	panic("groupMetrics.Trace() called directly; need to call groupMetrics.Trace()")
}

func (m *groupMetrics) Report(reset bool) etre.Metrics {
	m.Lock()
	defer m.Unlock()

	m.report.Ts = time.Now().Unix()

	m.report.Request.AuthorizationFailed = m.request.AuthorizationFailed.Count()
	m.report.Request.InvalidEntityType = m.request.InvalidEntityType.Count()
	m.report.Request.DbError = m.request.DbError.Count()
	m.report.Request.APIError = m.request.APIError.Count()
	m.report.Request.ClientError = m.request.ClientError.Count()

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

		// Histograms
		qr.ReadMatch_min, qr.ReadMatch_max, qr.ReadMatch_avg, qr.ReadMatch_med = minMaxAvgMed(em.query.ReadMatch, reset)
		qr.CreateBulk_min, qr.CreateBulk_max, qr.CreateBulk_avg, qr.CreateBulk_med = minMaxAvgMed(em.query.CreateBulk, reset)
		qr.UpdateBulk_min, qr.UpdateBulk_max, qr.UpdateBulk_avg, qr.UpdateBulk_med = minMaxAvgMed(em.query.UpdateBulk, reset)
		qr.DeleteBulk_min, qr.DeleteBulk_max, qr.DeleteBulk_avg, qr.DeleteBulk_med = minMaxAvgMed(em.query.DeleteBulk, reset)
		qr.Labels_min, qr.Labels_max, qr.Labels_avg, qr.Labels_med = minMaxAvgMed(em.query.Labels, reset)
		latencySnap := em.query.Latency.Snapshot(reset)
		er.Query.LatencyMs_max = latencySnap.Max
		er.Query.LatencyMs_p99 = latencySnap.Percentile[0.99]
		er.Query.LatencyMs_p999 = latencySnap.Percentile[0.999]

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

	return etre.Metrics{Groups: []etre.MetricsGroupReport{m.report}}
}

func minMaxAvgMed(h *gm.Histogram, reset bool) (int64, int64, int64, int64) {
	snap := h.Snapshot(reset)
	var avg int64
	if snap.N > 0 {
		avg = int64(snap.Sum / float64(snap.N))
	}
	return int64(snap.Min), int64(snap.Max), avg, int64(snap.Percentile[0.50])
}

// --------------------------------------------------------------------------

// groupEntityMetrics represents a groupMetrics bound to one entity type.
// See groupMetrics for the problem this solves. For a given group G and
// two or more requests R1 and R2, there is one and only one groupMetrics
// for G and two groupEntityMetrics: EM1 and EM1 for each request, respectively.
// If R1 binds to entity type X, then EM1 points to G.entity[X]. R2 can do
// the same because entity metrics are concurrent, or R2 can bind to entity
// type Y, then EM2 points to G.entity[Y].
//
// You can think of groupEntityMetrics as a concurrent groupMetrics bound to
// one entity type.
//
// Consequently, groupEntityMetrics provides all the Metrics interface methods
// except Report() which groupMetrics provides because the report is not
// entity type specific.
type groupEntityMetrics struct {
	*groupMetrics                // embedded, provides Report()
	em            *entityMetrics // points to groupMetrics.entity[EntityType()]
}

var _ Metrics = &groupEntityMetrics{} // ensure groupEntityMetrics implements Metrics

func NewGroupEntityMetrics(gm *groupMetrics) *groupEntityMetrics {
	return &groupEntityMetrics{
		groupMetrics: gm,  // embedded
		em:           nil, // call EntityType() to initialize
	}
}

func (m *groupEntityMetrics) EntityType(entityType string) {
	// Mutex in groupMetrics to ensure group has only one metrics per entity type.
	// Do NOT lock in any other groupEntityMetrics funcs--the underlying metrics
	// (e.g. what gm.NewCounter() returns) are all concurrent.
	m.Lock() // embedded m.groupMetrics.Mutex
	defer m.Unlock()

	var ok bool
	m.em, ok = m.entity[entityType] // m.groupMetrics.entity
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
			ReadMatch:   gm.NewHistogram(medConfig),
			ReadLabels:  gm.NewCounter(),
			Write:       gm.NewCounter(),
			CreateOne:   gm.NewCounter(),
			CreateMany:  gm.NewCounter(),
			CreateBulk:  gm.NewHistogram(medConfig),
			UpdateId:    gm.NewCounter(),
			UpdateQuery: gm.NewCounter(),
			UpdateBulk:  gm.NewHistogram(medConfig),
			DeleteId:    gm.NewCounter(),
			DeleteQuery: gm.NewCounter(),
			DeleteBulk:  gm.NewHistogram(medConfig),
			DeleteLabel: gm.NewCounter(),
			SetOp:       gm.NewCounter(),
			Created:     gm.NewCounter(),
			Updated:     gm.NewCounter(),
			Deleted:     gm.NewCounter(),
			Labels:      gm.NewHistogram(medConfig),
			Latency:     gm.NewHistogram(latencyConfig),
			MissSLA:     gm.NewCounter(),
		},
		label: map[string]*labelMetrics{},
		trace: map[string]map[string]*gm.Counter{},
	}

	m.report.Entity[entityType] = &etre.MetricsEntityReport{
		EntityType: entityType,
		Query:      &etre.MetricsQueryReport{},
		Label:      map[string]*etre.MetricsLabelReport{},
	}

	m.em = m.entity[entityType]
}

func (m *groupEntityMetrics) Inc(mn byte, n int64) {
	switch mn {
	// Query
	case Query:
		m.em.query.Query.Add(n)
	case SetOp:
		m.em.query.SetOp.Add(n)
	case MissSLA:
		m.em.query.MissSLA.Add(n)
	case Read:
		m.em.query.Read.Add(n)
	case ReadQuery:
		m.em.query.ReadQuery.Add(n)
	case ReadId:
		m.em.query.ReadId.Add(n)
	case ReadLabels:
		m.em.query.ReadLabels.Add(n)
	case Write:
		m.em.query.Write.Add(n)
	case CreateOne:
		m.em.query.CreateOne.Add(n)
	case CreateMany:
		m.em.query.CreateMany.Add(n)
	case UpdateId:
		m.em.query.UpdateId.Add(n)
	case UpdateQuery:
		m.em.query.UpdateQuery.Add(n)
	case DeleteId:
		m.em.query.DeleteId.Add(n)
	case DeleteQuery:
		m.em.query.DeleteQuery.Add(n)
	case DeleteLabel:
		m.em.query.DeleteLabel.Add(n)
	case Created:
		m.em.query.Created.Add(n)
	case Updated:
		m.em.query.Updated.Add(n)
	case Deleted:
		m.em.query.Deleted.Add(n)
	// CDC
	case CDCClients:
		m.cdc.Clients.Add(n)
	// Request
	case AuthorizationFailed:
		m.request.AuthorizationFailed.Add(n)
	case InvalidEntityType:
		m.request.InvalidEntityType.Add(n)
	case DbError:
		m.request.DbError.Add(n)
	case APIError:
		m.request.APIError.Add(n)
	case ClientError:
		m.request.ClientError.Add(n)

	default:
		errMsg := fmt.Sprintf("non-counter metric number passed to Inc: %d", mn)
		panic(errMsg)
	}
}

func (m *groupEntityMetrics) IncLabel(mn byte, label string) {
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
		lm.Read.Add(1)
	case LabelUpdate:
		lm.Update.Add(1)
	case LabelDelete:
		lm.Delete.Add(1)
	default:
		errMsg := fmt.Sprintf("non-counter metric number passed to IncLabel: %d", mn)
		panic(errMsg)
	}
}

func (m *groupEntityMetrics) Val(mn byte, n int64) {
	f := float64(n)
	switch mn {
	case LatencyMs:
		m.em.query.Latency.Record(f)
	case Labels:
		m.em.query.Labels.Record(f)
	case ReadMatch:
		m.em.query.ReadMatch.Record(f)
	case CreateBulk:
		m.em.query.CreateBulk.Record(f)
	case UpdateBulk:
		m.em.query.UpdateBulk.Record(f)
	case DeleteBulk:
		m.em.query.DeleteBulk.Record(f)
	default:
		errMsg := fmt.Sprintf("non-gauge metric number passed to Val: %d", mn)
		panic(errMsg)
	}
}

func (m *groupEntityMetrics) Trace(trace map[string]string) {
	for traceMetric, traceValue := range trace {
		traceValues, ok := m.em.trace[traceMetric]
		if !ok {
			traceValues = map[string]*gm.Counter{}
			m.em.trace[traceMetric] = traceValues
		}
		cnt, ok := traceValues[traceValue]
		if !ok {
			cnt = gm.NewCounter()
			traceValues[traceValue] = cnt
		}
		cnt.Add(1)
	}
}
