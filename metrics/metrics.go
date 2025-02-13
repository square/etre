// Copyright 2018-2019, Square, Inc.

// Package metrics provides Etre metrics.
package metrics

import (
	gm "github.com/daniel-nichter/go-metrics"
	"github.com/square/etre"
)

var (
	defaultSampleSize int = 2000  // unique values, ~16 KiB per metric
	latencySampleSize int = 10000 // unique values, ~80 KiB
	latencyConfig         = gm.Config{Percentiles: []float64{0.99, 0.999}}
	medConfig             = gm.Config{Percentiles: []float64{0.5}}
)

// See ../metrics.go for docs of each metric. The number/order of these
// does not matter. They are const numbers only to avoid typos and enable
// compile-time checking, e.g. m.Inc(metrics.Reed, 1) will cause error
// "undefined: metrics.Reed". Implementations of Metrics must ensure that
// metric X = etre.MetricReport.X.
const (
	Query                byte = iota // 00. counter (group and system)
	SetOp                            // 01. counter
	Labels                           // 02. histogram
	LatencyMs                        // 03. histogram
	MissSLA                          // 04. counter
	Read                             // 05. counter
	ReadQuery                        // 06. counter
	ReadId                           // 07. counter
	ReadMatch                        // 08. histogram
	ReadLabels                       // 09. counter
	Write                            // 10. counter
	CreateOne                        // 11. counter
	CreateMany                       // 12. counter
	CreateBulk                       // 13. histogram
	UpdateId                         // 14. counter
	UpdateQuery                      // 15. counter
	UpdateBulk                       // 16. histogram
	DeleteId                         // 17. counter
	DeleteQuery                      // 18. counter
	DeleteBulk                       // 19. histogram
	DeleteLabel                      // 20. counter
	LabelRead                        // 21. counter (per-label)
	LabelUpdate                      // 22. counter (per-label)
	LabelDelete                      // 23. counter (per-label)
	DbError                          // 24. counter (global)
	APIError                         // 25. counter (global)
	ClientError                      // 26. counter (global)
	CDCClients                       // 27. counter (global)
	Created                          // 28. counter
	Updated                          // 29. counter
	Deleted                          // 30. counter
	AuthenticationFailed             // 31. counter (system)
	AuthorizationFailed              // 32. counter
	InvalidEntityType                // 33. counter
	QueryTimeout                     // 34. counter
	Load                             // 35. gauge   (system)
	Error                            // 36. counter (system)
)

// Metrics abstracts how metrics are stored and sampled.
type Metrics interface {
	// EntityType binds the Metrics instance to an entity type before calling
	// other methods. This method must be called first.
	//
	// The caller is responsible for validating the entity type.
	//
	// This is only valid for group metrics.
	EntityType(string)

	// Inc increments the metric name (mn) by n. The metric must be a counter.
	Inc(mn byte, n int64)

	// IncLabel increments the metric name (mn) counter for the label by 1.
	// The metric must be LabelRead, LabelUpdate, or LabelDelete.
	//
	// This is only valid for group metrics.
	IncLabel(mn byte, label string)

	// Val records one measurement (n) for the metric name (mn). The metric
	// must be a gauge.
	Val(mn byte, n int64)

	// Trace increments trace value counters by 1.
	Trace(map[string]string)

	// Report returns a snapshot of all metrics, calculating stats like average
	// and percentiles.
	Report(reset bool) etre.Metrics
}

type Factory interface {
	Make(groupNames []string) Metrics
}

// GroupFactory implements Factory to make Group metrics in the API.
type GroupFactory struct {
	Store Store
}

func (f GroupFactory) Make(groupNames []string) Metrics {
	return NewGroup(groupNames, f.Store)
}
