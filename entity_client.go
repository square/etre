// Copyright 2017-2020, Square, Inc.

package etre

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// EntityClient represents a entity type-specific client. No interface method has
// an entity type argument because a client is bound to only one entity type.
// Use a EntityClients map to pass multiple clients for different entity types.
type EntityClient interface {
	// Query returns entities that match the query and pass the filter.
	Query(query string, filter QueryFilter) ([]Entity, error)

	// Get returns a single entity by internal ID.
	Get(id string) (Entity, error)

	// Insert is a bulk operation that creates the given entities.
	Insert([]Entity) (WriteResult, error)

	// Update is a bulk operation that patches entities that match the query.
	Update(query string, patch Entity) (WriteResult, error)

	// UpdateOne patches the given entity by internal ID.
	UpdateOne(id string, patch Entity) (WriteResult, error)

	// Delete is a bulk operation that removes all entities that match the query.
	Delete(query string) (WriteResult, error)

	// DeleteOne removes the given entity by internal ID.
	DeleteOne(id string) (WriteResult, error)

	// Labels returns all labels on the given entity by internal ID.
	Labels(id string) ([]string, error)

	// DeleteLabel removes the given label from the given entity by internal ID.
	// Labels should be stable, long-lived. Consequently, there's no bulk label delete.
	DeleteLabel(id string, label string) (WriteResult, error)

	// EntityType returns the entity type of the client.
	EntityType() string

	// WithSet returns a new EntityClient that uses the given Set for all write operations.
	// The Set cannot be removed. Therefore, when the set is complete, discard the new
	// EntityClient (let its reference count become zero). On insert, the given Set is added
	// to entities that do not have explicit set labels (_setOp, _setId, and _setSize).
	// On update and delete, the given Set is passed as URL query parameteres (setOp, setId,
	// and setSize). Sets do not apply to queries. The Set is not checked or validated; the
	// caller must ensure that Set.Size is greater than zero and Set.Op and Set.Id are nonempty
	// strings.
	WithSet(Set) EntityClient

	// WithTrace returns a new EntityClient that sends the trace string with every request
	// for server-side metrics. The trace string is a comma-separated list of key=value
	// pairs like: app=foo,host=bar. Invalid trace values are silently ignored by the server.
	WithTrace(string) EntityClient

	// WithContext returns a new EntityClient that attaches the context to every request.
	WithContext(ctx context.Context) EntityClient

	// Context returns the EntityClient's context. To change the context, use
	// WithContext.
	//
	// The returned context is always non-nil; it defaults to the
	// background context.
	Context() context.Context
}

// EntityClientConfig represents required and optional configuration for an EntityClient.
// This is used to make an EntityClient by calling NewEntityClientWithConfig.
type EntityClientConfig struct {
	EntityType   string        // entity type name
	Addr         string        // Etre server address (e.g. https://localhost:3848)
	HTTPClient   *http.Client  // configured http.Client
	Retry        uint          // optional retry count on network or API error
	RetryWait    time.Duration // optional wait time between retries
	RetryLogging bool          // log error on retry to stderr
	QueryTimeout time.Duration // timeout passed to API via etre.QUERY_TIMEOUT_HEADER
	Debug        bool
}

// EntityClients represents type-specific entity clients keyed on user-defined const
// which define each entity type. For example:
//
//	const (
//	  ENTITY_TYPE_FOO string = "foo"
//	  ENTITY_TYPE_BAR        = "bar"
//	)
//
// Pass an etre.EntityClients to use like:
//
//	func CreateFoo(ec etre.EntityClients) {
//	  ec[ENTITY_TYPE_FOO].Insert(...)
//	}
//
// Using EntityClients and const entity types is optional but helps avoid typos.
type EntityClients map[string]EntityClient

// Internal implementation of EntityClient interface using http.Client. See NewEntityClient.
type entityClient struct {
	entityType       string
	addr             string
	httpClient       *http.Client
	set              Set
	traceHeaderValue string
	retry            uint
	retryWait        time.Duration
	retryLogging     bool
	queryTimeout     time.Duration
	ctx              context.Context
}

// NewEntityClient creates a new type-specific Etre API client that makes requests
// with the given http.Client. An Etre client is bound to the specified entity
// type. Use an etre.EntityClients map to pass multiple type-specific clients. Like
// the given http.Client, an Etre client is safe for use by multiple goroutines,
// so only one entity type-specific client should be created.
func NewEntityClient(entityType, addr string, httpClient *http.Client) EntityClient {
	c := entityClient{
		entityType: entityType,
		addr:       addr,
		httpClient: httpClient,
	}
	return c
}

func NewEntityClientWithConfig(c EntityClientConfig) EntityClient {
	DebugEnabled = c.Debug
	return entityClient{
		entityType:   c.EntityType,
		addr:         c.Addr,
		httpClient:   c.HTTPClient,
		retry:        c.Retry,
		retryWait:    c.RetryWait,
		retryLogging: c.RetryLogging,
		queryTimeout: c.QueryTimeout,
	}
}

func (c entityClient) WithSet(set Set) EntityClient {
	// This func makes use of copy on write:
	new := c      // new = c (same memory address)
	new.set = set // on write to new, new becomes its own var (different memory address)
	return new
}

func (c entityClient) WithTrace(trace string) EntityClient {
	new := c
	new.traceHeaderValue = trace
	return new
}

func (c entityClient) WithContext(ctx context.Context) EntityClient {
	new := c
	new.ctx = ctx
	return new
}

func (c entityClient) Query(query string, filter QueryFilter) ([]Entity, error) {
	if query == "" {
		return nil, ErrNoQuery
	}
	Debug("query='%s', filter=%+v", query, filter)

	path := "/entities/" + c.entityType + "?query=" + url.QueryEscape(query) // always escape the query
	if len(filter.ReturnLabels) > 0 {
		rl := strings.Join(filter.ReturnLabels, ",")
		path += "&labels=" + rl
	}
	if filter.Distinct {
		path += "&distinct"
	}

	var entities []Entity
	err := c.apiRetry(func() (bool, error) {
		resp, bytes, err := c.do("GET", path, nil)
		if err != nil {
			return false, err
		}
		if resp.StatusCode != http.StatusOK {
			return readError(resp, bytes)
		}
		if len(bytes) > 0 {
			if err := json.Unmarshal(bytes, &entities); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	return entities, err
}

func (c entityClient) Get(id string) (Entity, error) {
	if id == "" {
		return nil, ErrIdNotSet
	}
	var entity Entity
	err := c.apiRetry(func() (bool, error) {
		resp, bytes, err := c.do("GET", "/entity/"+c.entityType+"/"+url.PathEscape(id), nil)
		if err != nil {
			return false, err
		}
		if resp.StatusCode != http.StatusOK {
			return readError(resp, bytes)
		}
		if len(bytes) > 0 {
			if err := json.Unmarshal(bytes, &entity); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	return entity, err
}

func (c entityClient) Insert(entities []Entity) (WriteResult, error) {
	if len(entities) == 0 {
		return WriteResult{}, ErrNoEntity
	}
	// Let API validate the new entities. Currently, they cannot contain _id,
	// for example, but let the API be the single source of truth.
	return c.write(entities, 1, "POST", "/entities/"+c.entityType)
}

func (c entityClient) Update(query string, patch Entity) (WriteResult, error) {
	if query == "" {
		return WriteResult{}, ErrNoQuery
	}
	Debug("query='%s', patch=%+v", query, patch)
	query = url.QueryEscape(query) // always escape the query
	if len(patch) == 0 {
		return WriteResult{}, ErrNoEntity
	}
	// Let API return error if patch contains (meta)labels that cannot be updated,
	// e.g. _id. Currently, the API does not allow any metalabels in the patch.
	return c.write(patch, -1, "PUT", "/entities/"+c.entityType+"?query="+query)
}

func (c entityClient) UpdateOne(id string, patch Entity) (WriteResult, error) {
	if id == "" {
		return WriteResult{}, ErrIdNotSet
	}
	Debug("_id=%s, patch=%+v", id, patch)
	// Let API return error if patch contains (meta)labels that cannot be updated,
	// e.g. _id. Currently, the API does not allow any metalabels in the patch.
	wr, err := c.write(patch, 1, "PUT", "/entity/"+c.entityType+"/"+id)
	if err != nil {
		return WriteResult{}, err
	}
	return wr, nil
}

func (c entityClient) Delete(query string) (WriteResult, error) {
	if query == "" {
		return WriteResult{}, ErrNoQuery
	}
	Debug("query='%s'", query)
	query = url.QueryEscape(query) // always escape the query
	return c.write(nil, -1, "DELETE", "/entities/"+c.entityType+"?query="+query)
}

func (c entityClient) DeleteOne(id string) (WriteResult, error) {
	if id == "" {
		return WriteResult{}, ErrIdNotSet
	}
	Debug("_id=%s", id)
	wr, err := c.write(nil, 1, "DELETE", "/entity/"+c.entityType+"/"+id)
	if err != nil {
		return WriteResult{}, err
	}
	return wr, nil
}

func (c entityClient) Labels(id string) ([]string, error) {
	if id == "" {
		return nil, ErrIdNotSet
	}

	var labels []string
	err := c.apiRetry(func() (bool, error) {
		resp, bytes, err := c.do("GET", "/entity/"+c.entityType+"/"+id+"/labels", nil)
		if err != nil {
			return false, err
		}
		if resp.StatusCode != http.StatusOK {
			return readError(resp, bytes)
		}
		if err := json.Unmarshal(bytes, &labels); err != nil {
			return false, err
		}
		return true, nil
	})
	return labels, err
}

func (c entityClient) DeleteLabel(id string, label string) (WriteResult, error) {
	if id == "" {
		return WriteResult{}, ErrIdNotSet
	}
	if label == "" {
		return WriteResult{}, ErrNoLabel
	}
	Debug("_id=%s, label=%s", id, label)
	wr, err := c.write(nil, 1, "DELETE", "/entity/"+c.entityType+"/"+id+"/labels/"+label)
	if err != nil {
		return WriteResult{}, err
	}
	return wr, nil
}

func (c entityClient) EntityType() string {
	return c.entityType
}

// --------------------------------------------------------------------------

// write sends payload via method to endpoint, expecting n successful writes.
// If n is -1, the number of writes is variable (bulk update or delete).
func (c entityClient) write(payload interface{}, n int, method, endpoint string) (WriteResult, error) {
	var wr WriteResult

	// If entities (insert and update), marshal them. If not (delete), pass nil.
	var bytes []byte
	var err error
	if payload != nil {
		bytes, err = json.Marshal(payload)
		if err != nil {
			return wr, fmt.Errorf("json.Marshal: %s", err)
		}
	}

	// Add the set url query params, if set
	if c.set.Size > 0 {
		if strings.Contains(endpoint, "?") {
			// Add to existing query params
			endpoint += fmt.Sprintf("&setId=%s&setOp=%s&setSize=%d", c.set.Id, c.set.Op, c.set.Size)
		} else {
			// No query params yet
			endpoint += fmt.Sprintf("?setId=%s&setOp=%s&setSize=%d", c.set.Id, c.set.Op, c.set.Size)
		}
	}

	err = c.apiRetry(func() (bool, error) {
		// Do low-level HTTP request. An erorr here is probably network not API error.
		resp, bytes, err := c.do(method, endpoint, bytes)
		if err != nil {
			return false, err
		}

		done := resp.StatusCode >= 400 && resp.StatusCode < 500

		// On write, API should return an etre.WriteResult, but if API crashes
		// there won't be response data
		if len(bytes) == 0 {
			return done, fmt.Errorf("Server error: HTTP status %d, no response (check API logs)", resp.StatusCode)
		}
		wr = WriteResult{} // outer scope, reset on retry
		if err := json.Unmarshal(bytes, &wr); err != nil {
			return done, fmt.Errorf("json.Unmarshal: %s", err)
		}
		Debug("write result: %+v", wr)
		if resp.StatusCode == http.StatusNotFound {
			return done, ErrEntityNotFound
		}
		if wr.IsZero() && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			if resp.StatusCode >= 500 {
				return done, fmt.Errorf("Server error: HTTP status %d, response: '%s'", resp.StatusCode, string(bytes))
			}
			return done, fmt.Errorf("Client error: HTTP status %d, response: '%s'", resp.StatusCode, string(bytes))
		}
		return true, nil
	})
	return wr, err
}

func (c entityClient) do(method, endpoint string, payload []byte) (*http.Response, []byte, error) {
	// Make a complete URL: addr + API_ROOT + endpoint
	// _CALLER MUST url.QueryEscape(query)!_ We can't escape the whole endpoint
	// here because it'll escape /.
	url := c.url(endpoint)

	// Make request
	var req *http.Request
	var err error
	if payload != nil {
		buf := bytes.NewBuffer(payload)
		req, err = http.NewRequestWithContext(c.Context(), method, url, buf)
	} else {
		// Can't use a nil *bytes.Buffer because net/http/request.go looks at the type:
		//   switch v := body.(type) {
		//       case *bytes.Buffer:
		// So even though it's nil, request.go will attempt to read it, causing a panic.
		req, err = http.NewRequestWithContext(c.Context(), method, url, nil)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("http.NewRequest: %s: %s", url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(VERSION_HEADER, VERSION)
	if c.queryTimeout > 0 {
		req.Header.Set(QUERY_TIMEOUT_HEADER, c.queryTimeout.String())
	}
	if c.traceHeaderValue != "" {
		req.Header.Set(TRACE_HEADER, c.traceHeaderValue)
	}

	// Send request
	Debug("request: %+v", req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		Debug("httpClient.Do() error: %v", err)
		if err, ok := err.(net.Error); ok && err.Timeout() {
			return nil, nil, ErrClientTimeout
		}
		return nil, nil, fmt.Errorf("http.Client.Do: %s", err)
	}
	Debug("response: %+v", resp)

	// Read API response
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, fmt.Errorf("ioutil.ReadAll: %s", err)
	}

	return resp, body, nil
}

func (c entityClient) url(endpoint string) string {
	return c.addr + API_ROOT + endpoint
}

func (c entityClient) Context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}

func readError(resp *http.Response, bytes []byte) (bool, error) {
	done := resp.StatusCode >= 400 && resp.StatusCode < 500

	if resp.StatusCode == http.StatusNotFound {
		return done, ErrEntityNotFound
	}

	// No response data from API, it crashed or had unhandled error
	if len(bytes) == 0 {
		return done, fmt.Errorf("Server error: HTTP status %d, no response (check API logs)", resp.StatusCode)
	}

	// Response data should be an etre.Error
	var errResp Error
	if err := json.Unmarshal(bytes, &errResp); err != nil {
		return done, fmt.Errorf("Server error: HTTP status %d, cannot decode response (%s): %s", resp.StatusCode, err, string(bytes))
	}
	if errResp.Type == "" || errResp.Message == "" {
		return done, fmt.Errorf("Server error: HTTP status %d, unknown response: %s", resp.StatusCode, string(bytes))
	}
	if resp.StatusCode >= 500 {
		return done, fmt.Errorf("Server error: %s: %s (HTTP status %d)", errResp.Type, errResp.Message, resp.StatusCode)
	}
	return done, fmt.Errorf("Client error: %s: %s (HTTP status %d)", errResp.Type, errResp.Message, resp.StatusCode)
}

func (c entityClient) apiRetry(f func() (bool, error)) error {
	tries := 1 + c.retry
	var err error
	var done bool
	for tryNo := uint(1); tryNo <= tries; tryNo++ {
		done, err = f()
		if done {
			return err
		}
		if err == nil {
			return nil // success
		}
		if tryNo < tries { // don't log or sleep on last try
			if c.retryLogging {
				log.Printf("Error querying Etre: %s (try %d of %d, retry in %s)", err, tryNo, tries, c.retryWait)
			}
			time.Sleep(c.retryWait)
		}
	}
	return err // last error
}

// //////////////////////////////////////////////////////////////////////////
// Mock client
// //////////////////////////////////////////////////////////////////////////

// MockEntityClient implements EntityClient for testing. Defined callback funcs
// are called for the respective interface method, otherwise the default methods
// return empty slices and no error. Defining a callback function allows tests
// to intercept, save, and inspect Client calls and simulate Etre API returns.
type MockEntityClient struct {
	QueryFunc       func(string, QueryFilter) ([]Entity, error)
	GetFunc         func(string) (Entity, error)
	InsertFunc      func([]Entity) (WriteResult, error)
	UpdateFunc      func(query string, patch Entity) (WriteResult, error)
	UpdateOneFunc   func(id string, patch Entity) (WriteResult, error)
	DeleteFunc      func(query string) (WriteResult, error)
	DeleteOneFunc   func(id string) (WriteResult, error)
	LabelsFunc      func(id string) ([]string, error)
	DeleteLabelFunc func(id string, label string) (WriteResult, error)
	EntityTypeFunc  func() string
	WithSetFunc     func(Set) EntityClient
	WithTraceFunc   func(string) EntityClient
	WithContextFunc func(ctx context.Context) EntityClient
	ContextFunc     func() context.Context
}

func (c MockEntityClient) Query(query string, filter QueryFilter) ([]Entity, error) {
	if c.QueryFunc != nil {
		return c.QueryFunc(query, filter)
	}
	return nil, nil
}

func (c MockEntityClient) Get(id string) (Entity, error) {
	if c.GetFunc != nil {
		return c.GetFunc(id)
	}
	return nil, nil
}

func (c MockEntityClient) Insert(entities []Entity) (WriteResult, error) {
	if c.InsertFunc != nil {
		return c.InsertFunc(entities)
	}
	return WriteResult{}, nil
}

func (c MockEntityClient) Update(query string, patch Entity) (WriteResult, error) {
	if c.UpdateFunc != nil {
		return c.UpdateFunc(query, patch)
	}
	return WriteResult{}, nil
}

func (c MockEntityClient) UpdateOne(id string, patch Entity) (WriteResult, error) {
	if c.UpdateOneFunc != nil {
		return c.UpdateOneFunc(id, patch)
	}
	return WriteResult{}, nil
}

func (c MockEntityClient) Delete(query string) (WriteResult, error) {
	if c.DeleteFunc != nil {
		return c.DeleteFunc(query)
	}
	return WriteResult{}, nil
}

func (c MockEntityClient) DeleteOne(id string) (WriteResult, error) {
	if c.DeleteOneFunc != nil {
		return c.DeleteOneFunc(id)
	}
	return WriteResult{}, nil
}

func (c MockEntityClient) Labels(id string) ([]string, error) {
	if c.LabelsFunc != nil {
		return c.LabelsFunc(id)
	}
	return nil, nil
}

func (c MockEntityClient) DeleteLabel(id string, label string) (WriteResult, error) {
	if c.DeleteLabelFunc != nil {
		return c.DeleteLabelFunc(id, label)
	}
	return WriteResult{}, nil
}

func (c MockEntityClient) EntityType() string {
	if c.EntityTypeFunc != nil {
		return c.EntityTypeFunc()
	}
	return ""
}

func (c MockEntityClient) WithSet(set Set) EntityClient {
	if c.WithSetFunc != nil {
		return c.WithSetFunc(set)
	}
	return c
}

func (c MockEntityClient) WithTrace(trace string) EntityClient {
	if c.WithTraceFunc != nil {
		return c.WithTraceFunc(trace)
	}
	return c
}

func (c MockEntityClient) WithContext(ctx context.Context) EntityClient {
	if c.WithContextFunc != nil {
		return c.WithContextFunc(ctx)
	}
	return c
}

func (c MockEntityClient) Context() context.Context {
	if c.ContextFunc != nil {
		return c.ContextFunc()
	}
	return context.Background()
}
