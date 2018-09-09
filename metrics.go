package etre

type Metrics struct {
	Teams []MetricsReport `json:"teams"`
}

type MetricsReport struct {
	Ts     int64                           `json:"ts"`
	Team   string                          `json:"team"`
	Global *MetricsGlobalReport            `json:"global"`
	Entity map[string]*MetricsEntityReport `json:"entity"`
	CDC    *MetricsCDCReport               `json:"cdc,omitempty"`
}

type MetricsGlobalReport struct {
	DbError     int64 `json:"db-error"`
	APIError    int64 `json:"api-error"`
	ClientError int64 `json:"client-error"`
}

type MetricsEntityReport struct {
	EntityType string                         `json:"entityType"`
	Query      *MetricsQueryReport            `json:"query"`
	Label      map[string]*MetricsLabelReport `json:"label"`
	Trace      *MetricsTraceReport            `json:"trace"`
}

type MetricsQueryReport struct {
	All         int64   `json:"all"`
	Read        int64   `json:"read"`
	ReadQuery   int64   `json:"read-query"`
	ReadId      int64   `json:"read-id"`
	ReadLabels  int64   `json:"read-label"`
	Write       int64   `json:"write"`
	Insert      int64   `json:"insert"`
	InsertBulk  int64   `json:"insert-bulk"`
	Update      int64   `json:"update"`
	UpdateBulk  int64   `json:"update-bulk"`
	Delete      int64   `json:"delete"`
	DeleteBulk  int64   `json:"delete-bulk"`
	DeleteLabel int64   `json:"delete-label"`
	SetOp       int64   `json:"set-op"`
	LabelsMin   int64   `json:"labels_min"`
	LabelsMax   int64   `json:"labels_max"`
	LabelsAvg   int64   `json:"labels_avg"`
	LabelsMed   int64   `json:"labels_med"`
	LatencyMax  float64 `json:"latency_max"`
	LatencyP99  float64 `json:"latency_p99"`
	LatencyP999 float64 `json:"latency_p999"`
	MissSLA     int64   `json:"miss-sla"`
}

type MetricsLabelReport struct {
	Read   int64 `json:"read"`
	Update int64 `json:"update"`
	Delete int64 `json:"delete"`
}

type MetricsTraceReport struct {
	User map[string]int64 `json:"user"`
	App  map[string]int64 `json:"app"`
	Host map[string]int64 `json:"host"`
}

type MetricsCDCReport struct {
	Clients int64 `json:"clients"`
}
