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
	Trace      map[string]map[string]int64    `json:"trace"`
}

type MetricsQueryReport struct {
	Query          int64   `json:"query"`
	Read           int64   `json:"read"`
	ReadQuery      int64   `json:"read-query"`
	ReadId         int64   `json:"read-id"`
	ReadLabels     int64   `json:"read-label"`
	Write          int64   `json:"write"`
	Insert         int64   `json:"insert"`
	InsertBulk     int64   `json:"insert-bulk"`
	Update         int64   `json:"update"`
	UpdateBulk     int64   `json:"update-bulk"`
	Delete         int64   `json:"delete"`
	DeleteBulk     int64   `json:"delete-bulk"`
	DeleteLabel    int64   `json:"delete-label"`
	SetOp          int64   `json:"set-op"`
	Labels_min     int64   `json:"labels_min"`
	Labels_max     int64   `json:"labels_max"`
	Labels_avg     int64   `json:"labels_avg"`
	Labels_med     int64   `json:"labels_med"`
	LatencyMs_max  float64 `json:"latency_max"`
	LatencyMs_p99  float64 `json:"latency_p99"`
	LatencyMs_p999 float64 `json:"latency_p999"`
	MissSLA        int64   `json:"miss-sla"`
}

type MetricsLabelReport struct {
	Read   int64 `json:"read"`
	Update int64 `json:"update"`
	Delete int64 `json:"delete"`
}

type MetricsCDCReport struct {
	Clients int64 `json:"clients"`
}
