// Copyright 2018-2019, Square, Inc.

package etre

type Metrics struct {
	Groups []MetricsReport `json:"groups"`
}

type MetricsReport struct {
	Ts     int64                           `json:"ts"`
	Group  string                          `json:"group"`
	Global *MetricsGlobalReport            `json:"global"`
	Entity map[string]*MetricsEntityReport `json:"entity"`
	CDC    *MetricsCDCReport               `json:"cdc"`
}

type MetricsGlobalReport struct {
	DbError     int64 `json:"db-error"`
	APIError    int64 `json:"api-error"`
	ClientError int64 `json:"client-error"`
}

type MetricsEntityReport struct {
	EntityType string                         `json:"entity-type"`
	Query      *MetricsQueryReport            `json:"query"`
	Label      map[string]*MetricsLabelReport `json:"label"`
	Trace      map[string]map[string]int64    `json:"trace,omitempty"`
}

type MetricsQueryReport struct {
	// Query counter is the grand total number of queries. Every authenticated
	// query increments Query by 1. Query = Read + Write.
	Query int64 `json:"query"`

	// Read counter is the total number of read queries. All read queries
	// increment Read by 1. Read = ReadQuery + ReadId + ReadLabels.
	Read int64 `json:"read"`

	// ReadQuery counter is the number of reads by query. It is a subset of Read.
	// These API endpoints increment ReadQuery by 1:
	//   GET  /api/v1/entities/:type
	//   POST /api/v1/query/:type
	// See Labels stats for the number of labels used in the query.
	ReadQuery int64 `json:"read-query"`

	// ReadId counter is the number of reads by entity ID. It is a subset of Read.
	// These API endpoints increment ReadId by 1:
	//   GET /api/v1/entity/:id
	ReadId int64 `json:"read-id"`

	// ReadLabels counter is the number of read label queries. It is a subset of Read.
	// These API endpoints increment ReadLabels by 1:
	//   GET /api/v1/entity/:type/:id/labels
	ReadLabels int64 `json:"read-labels"`

	// ReadMatch stats represent the number of entities that matched the read
	// query and were returned to the client. See Labels stats for the number
	// of labels used in the query.
	ReadMatch_min int64 `json:"read-match_min"`
	ReadMatch_max int64 `json:"read-match_max"`
	ReadMatch_avg int64 `json:"read-match_avg"`
	ReadMatch_med int64 `json:"read-match_med"`

	// Write counter is the grand total number of write queries. All write queries
	// increment Write by 1. Write = CreateOne + CreateMany + UpdateId +
	// UpdateQuery + DeleteId + DeleteQuery + DeleteLabel.
	//
	// Write queries are incremented before validating and writing entities, so
	// they do not measure entities successfully written. Successfully written
	// entities are measured by counters Created, Updated, and Deleted.
	Write int64 `json:"write"`

	// CreateOne and CreateMany counters are the number of create queries.
	// They are subsets of Write. These API endpoints increment the metrics:
	//   POST /api/v1/entity/:type   (one)
	//   POST /api/v1/entities/:type (many/bulk)
	CreateOne  int64 `json:"create-one"`
	CreateMany int64 `json:"create-many"`

	// CreateBulk stats represent the number of entities received for CreateMany
	// (API endpoing POST /api/v1/entities/:type). The Created counter measures
	// the number of entities successfully created. These stats measure the size
	// of bulk create requests.
	CreateBulk_min int64 `json:"create-bulk_min"`
	CreateBulk_max int64 `json:"create-bulk_max"`
	CreateBulk_avg int64 `json:"create-bulk_avg"`
	CreateBulk_med int64 `json:"create-bulk_med"`

	// UpdateId and UpdateQuery counters are the number of update (patch) queries.
	// They are a subset of Write. These API endpoints increment the metrics:
	//   PUT /api/v1/entity/:type/:id (id)
	//   PUT /api/v1/entities/:type   (query)
	// See Labels stats for the number of labels used in the UpdateQuery query.
	UpdateId    int64 `json:"update-id"`
	UpdateQuery int64 `json:"update-query"`

	// UpdateBulk stats represent the number of entities that matched the bulk
	// update query and were updated. The Updated counter measures the number
	// of entities successfully updated. These stats measure the size of bulk
	// update requests.
	UpdateBulk_min int64 `json:"update-bulk_min"`
	UpdateBulk_max int64 `json:"update-bulk_max"`
	UpdateBulk_avg int64 `json:"update-bulk_avg"`
	UpdateBulk_med int64 `json:"update-bulk_med"`

	// DeleteId and DeleteQuery counters are the number of delete queries.
	// They are a subset of Write. These API endpoints increment the metrics:
	//   DELETE /api/v1/entity/:type   (id)
	//   DELETE /api/v1/entities/:type (query)
	// See Labels stats for the number of labels used in the DeleteQuery query.
	DeleteId    int64 `json:"delete-id"`
	DeleteQuery int64 `json:"delete-query"`

	// DeleteBulk stats represent the number of entities that matched the bulk
	// delete query and were deleted. The Deleted counter measures the number
	// of entities successfully deleted. These stats measure the size of bulk
	// delete requests.
	DeleteBulk_min int64 `json:"delete-bulk_min"`
	DeleteBulk_max int64 `json:"delete-bulk_max"`
	DeleteBulk_avg int64 `json:"delete-bulk_avg"`
	DeleteBulk_med int64 `json:"delete-bulk_med"`

	// DeleteLabel counter is the number of delete label queries. It is a subset of Write.
	// These API endpoints increment DeleteLabel:
	//   DELETE /api/v1/entity/:type/:id/labels/:label
	DeleteLabel int64 `json:"delete-label"`

	// Created, Updated, and Deleted counters are the number of entities successfully
	// created, updated, and deleted. These metrics are incremented in their
	// corresponding metric API endpoints when entities are successfully created,
	// updated, or deleted.
	//
	// For example, a request to PUT /api/v1/entity/:type/:id always increments
	// UpdateId by 1, but it increments Updated by 1 only if successful.
	Created int64 `json:"created"`
	Updated int64 `json:"updated"`
	Deleted int64 `json:"deleted"`

	// SetOp counter is the number of queries that used a set op.
	SetOp int64 `json:"set-op"`

	// Labels stats represent the number of labels in read, update, and delete
	// queries. The metric is incremented in these API endpoints:
	//   GET    /api/v1/entities/:type (read)
	//   POST   /api/v1/query/:type    (read)
	//   PUT    /api/v1/entities/:type (update bulk)
	//   DELETE /api/v1/entities/:type (delete bulk)
	// The metric counts all labels in the query. See MetricsLabelReport for
	// label-specific counters.
	//
	// For example, with query "a=1,!b,c in (x,y)" the label count is 3.
	Labels_min int64 `json:"labels_min"`
	Labels_max int64 `json:"labels_max"`
	Labels_avg int64 `json:"labels_avg"`
	Labels_med int64 `json:"labels_med"`

	// LatencyMs stats represent query latency (response time) in milliseconds
	// for all queries (read and write). Low query latency is not a problem,
	// so stats only represent the worst case: high query latency. _p99 is the
	// 99th percentile (ignoring the top 1% as outliers). _p999 is the 99.9th
	// percentile (ignoring the top 0.1% as outliers).
	LatencyMs_max  float64 `json:"latency-ms_max"`
	LatencyMs_p99  float64 `json:"latency-ms_p99"`
	LatencyMs_p999 float64 `json:"latency-ms_p999"`

	// MissSLA counter is the number of queries with LatencyMs greater than
	// the configured query latency SLA (config.metrics.query_latency_sla).
	MissSLA int64 `json:"miss-sla"`
}

type MetricsLabelReport struct {
	Read   int64 `json:"read"`
	Update int64 `json:"update"`
	Delete int64 `json:"delete"`
}

type MetricsCDCReport struct {
	Clients int64 `json:"clients"`
}
