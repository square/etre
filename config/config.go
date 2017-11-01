// Copyright 2017, Square, Inc.

package config

///////////////////////////////////////////////////////////////////////////////
// High-Level Config Structs
///////////////////////////////////////////////////////////////////////////////

type Config struct {
	Server     ServerConfig     `yaml:"server"`
	Datasource DatasourceConfig `yaml:"datasource"`
	Entity     EntityConfig     `yaml:"entity"`
	CDC        CDCConfig        `yaml:"cdc"`
	Delay      DelayConfig      `yaml:"delay"`
	Feed       FeedConfig       `yaml:"feed"`
}

///////////////////////////////////////////////////////////////////////////////
// Config Components
///////////////////////////////////////////////////////////////////////////////

type DatasourceConfig struct {
	URL      string `yaml:"url"`
	Database string `yaml:"database"`
	Timeout  int    `yaml:"timeout"`

	// Certs
	TLSCert string `yaml:"tls-cert"`
	TLSKey  string `yaml:"tls-key"`
	TLSCA   string `yaml:"tls-ca"`

	// Credentials
	Username  string `yaml:"username"`
	Source    string `yaml:"source"`
	Mechanism string `yaml:"mechanism"`
}

type EntityConfig struct {
	Types []string `yaml:"types"`
}

type CDCConfig struct {
	// The collection that CDC events are stored in.
	Collection string `yaml:"collection"`
	// If set, CDC events will attempt to be written to this file if they cannot
	// be written to mongo.
	FallbackFile string `yaml:"fallback_file"`
	// Number of times CDC events will retry writing to mongo in the event of an error.
	WriteRetryCount int `yaml:"write_retry_count"`
	// Wait time in milliseconds between write retry events.
	WriteRetryWait int `yaml:"write_retry_wait"` // milliseconds
}

type DelayConfig struct {
	// The collection that delays are stored in.
	Collection string `yaml:"collection"`
	// If this value is positive, the delayer will always return a max timestamp
	// that is time.Now() minus this config value. If this value is negative,
	// the delayer will return a max timestamp that dynamically changes
	// depending on the active API calls into Etre. Units in milliseconds.
	StaticDelay int `yaml:"static_delay"`
}

type FeedConfig struct {
	// The buffer size that a streamer has when consuming from the poller. If the
	// poller fills the buffer up then the streamer will error out since it won't
	// be able to catch up to the poller anymore.
	StreamerBufferSize int `yaml:"streamer_buffer_size"`
	// The amount of time that the poller will sleep between polls, in milliseconds.
	PollInterval int `yaml:"poll_interval"`
}

type ServerConfig struct {
	Addr string `yaml:"addr"`

	// Certs
	TLSCert string `yaml:"tls-cert"`
	TLSKey  string `yaml:"tls-key"`
	TLSCA   string `yaml:"tls-ca"`

	// Etre will look at this HTTP header to get the username of the requestor of
	// all API calls.
	UsernameHeader string `yaml:"username_header"`
}
