// Package hyperbytedb is a client for HyperbyteDB's InfluxDB v1 API
// (/write, /query). It has no dependencies outside the standard library.
//
//	db := hyperbytedb.NewClient("http://hyperbytedb:8086", hyperbytedb.Options{Username: "writer", Password: "..."})
//	_, err := db.Write(ctx, "app", []hyperbytedb.Point{{
//		Measurement: "cpu",
//		Tags:        map[string]string{"host": "web1"},
//		Fields:      map[string]any{"usage": 12.5},
//	}}, hyperbytedb.WriteOptions{})
//	results, err := db.Query(ctx, `SELECT mean("usage") FROM "cpu" WHERE time > now() - 1h GROUP BY "host"`,
//		hyperbytedb.QueryOptions{Database: "app"})
package hyperbytedb

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Version is this client's version.
const Version = "0.9.3-beta"

// Options configures a Client. The zero value of each field means its default.
type Options struct {
	// Username and Password go in an Authorization: Basic header, never the URL.
	Username string
	Password string
	// Timeout is per request (default 30 s).
	Timeout time.Duration
	// Retries is how often a request is retried on connection errors, 429
	// and 5xx (default 5; -1 for none).
	Retries int
	// Backoff is the first retry delay, doubled each time up to MaxBackoff,
	// with jitter (default 0.5 s and 30 s). A Retry-After header wins.
	Backoff    time.Duration
	MaxBackoff time.Duration
	// DisableGzip sends writes uncompressed.
	DisableGzip bool
	// BatchPoints and BatchBytes bound each write request, whichever comes
	// first (default 5,000 points, 4 MiB).
	BatchPoints int
	BatchBytes  int
	// HTTPClient replaces the default client (for TLS settings, proxies, ...).
	HTTPClient *http.Client
}

// Client is a HyperbyteDB connection. URL is a node, or a load balancer or
// Kubernetes Service in front of the cluster: any node accepts any write or
// query. A Client is safe for concurrent use.
type Client struct {
	url  string
	opts Options
	http *http.Client
	auth string
}

// NewClient returns a client for the server at url.
func NewClient(url string, opts Options) *Client {
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.Retries == 0 {
		opts.Retries = 5
	} else if opts.Retries < 0 {
		opts.Retries = 0
	}
	if opts.Backoff <= 0 {
		opts.Backoff = 500 * time.Millisecond
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = 30 * time.Second
	}
	if opts.BatchPoints <= 0 {
		opts.BatchPoints = 5_000
	}
	if opts.BatchBytes <= 0 {
		opts.BatchBytes = 4 << 20
	}
	hc := opts.HTTPClient
	if hc == nil {
		hc = &http.Client{}
	}
	c := &Client{url: strings.TrimRight(url, "/"), opts: opts, http: hc}
	if opts.Username != "" {
		c.auth = "Basic " + basicAuth(opts.Username, opts.Password)
	}
	return c
}

// retryable statuses: rate limited, overloaded (HyperbyteDB answers 503 with
// Retry-After when write admission is full), or a server or gateway error
// that may pass.
func retryable(status int) bool {
	switch status {
	case 429, 500, 502, 503, 504:
		return true
	}
	return false
}

type response struct {
	status int
	header http.Header
	body   []byte
}

// do sends one request with retries and returns any final status for the
// caller to handle. Statuses in answers are returned at once, not retried.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body []byte, header http.Header, answers ...int) (*response, error) {
	u := c.url + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	for attempt := 0; ; attempt++ {
		resp, err := c.once(ctx, method, u, body, header)
		var wait time.Duration
		switch {
		case err != nil:
			if ctx.Err() != nil || attempt >= c.opts.Retries {
				return nil, fmt.Errorf("hyperbytedb: %s %s: %w", method, path, err)
			}
		case !retryable(resp.status) || contains(answers, resp.status) || attempt >= c.opts.Retries:
			return resp, nil
		default:
			wait = retryAfter(resp.header)
		}
		if wait == 0 {
			wait = c.backoff(attempt + 1)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (c *Client) once(ctx context.Context, method, u string, body []byte, header http.Header) (*response, error) {
	ctx, cancel := context.WithTimeout(ctx, c.opts.Timeout)
	defer cancel()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, r)
	if err != nil {
		return nil, err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	req.Header.Set("User-Agent", "hyperbytedb-go/"+Version)
	if c.auth != "" {
		req.Header.Set("Authorization", c.auth)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	// The transport decompresses gzip responses itself.
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &response{status: resp.StatusCode, header: resp.Header, body: data}, nil
}

func (c *Client) backoff(attempt int) time.Duration {
	d := c.opts.Backoff << (attempt - 1)
	if d <= 0 || d > c.opts.MaxBackoff {
		d = c.opts.MaxBackoff
	}
	return time.Duration(float64(d) * (0.5 + rand.Float64()/2))
}

// Ping returns the server's version, e.g. "0.9.3-beta", or an error if it is
// down. Servers before 0.9.3 report it as X-Influxdb-Version.
func (c *Client) Ping(ctx context.Context) (string, error) {
	resp, err := c.do(ctx, http.MethodGet, "/ping", nil, nil, nil)
	if err != nil {
		return "", err
	}
	if resp.status >= 300 {
		return "", &ServerError{Status: resp.status, Message: errorText(resp.body)}
	}
	if v := resp.header.Get("X-Hyperbytedb-Version"); v != "" {
		return v, nil
	}
	return strings.TrimPrefix(resp.header.Get("X-Influxdb-Version"), "HyperbyteDB-"), nil
}

// Health returns /health: the node's state. A 503 (draining, syncing) is the
// node's answer, returned with the body, not retried.
func (c *Client) Health(ctx context.Context) (map[string]any, error) {
	resp, err := c.do(ctx, http.MethodGet, "/health", nil, nil, nil, 503)
	if err != nil {
		return nil, err
	}
	if resp.status >= 300 && resp.status != 503 {
		return nil, &ServerError{Status: resp.status, Message: errorText(resp.body)}
	}
	out := map[string]any{}
	if len(resp.body) > 0 {
		if err := json.Unmarshal(resp.body, &out); err != nil {
			return nil, fmt.Errorf("hyperbytedb: /health: %w", err)
		}
	}
	return out, nil
}

// WriteOptions are a write's retention policy (default: the database's) and
// timestamp precision (default ns).
type WriteOptions struct {
	RetentionPolicy string
	Precision       Precision
}

// Write writes points and returns how many lines were sent. Points with no
// field left are skipped. On a refused batch it returns a *WriteError (or
// *AuthError, *ServerError); batches before it have been written.
func (c *Client) Write(ctx context.Context, database string, points []Point, opts WriteOptions) (int, error) {
	lines := make([]string, 0, len(points))
	for _, p := range points {
		line, ok, err := EncodePoint(p, opts.Precision)
		if err != nil {
			return 0, err
		}
		if ok {
			lines = append(lines, line)
		}
	}
	return c.WriteLines(ctx, database, lines, opts)
}

// WriteLines writes ready-made line protocol, one line per element (blank
// ones skipped), batched like Write. On an error the count is the lines
// written before it, including half of a batch split after a 413.
func (c *Client) WriteLines(ctx context.Context, database string, lines []string, opts WriteOptions) (int, error) {
	if _, err := opts.Precision.divisor(); err != nil {
		return 0, err
	}
	q := url.Values{"db": {database}, "precision": {string(opts.Precision)}}
	if opts.Precision == "" {
		q.Set("precision", string(Nanosecond))
	}
	if opts.RetentionPolicy != "" {
		q.Set("rp", opts.RetentionPolicy)
	}
	sent := 0
	var batch []string
	size := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := c.writeBatch(ctx, batch, q)
		sent += n
		if err != nil {
			return err
		}
		batch, size = nil, 0
		return nil
	}
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if len(batch) > 0 && (len(batch) >= c.opts.BatchPoints || size+len(line)+1 > c.opts.BatchBytes) {
			if err := flush(); err != nil {
				return sent, err
			}
		}
		batch = append(batch, line)
		size += len(line) + 1
	}
	return sent, flush()
}

// writeBatch returns how many of batch's lines were written, also on an
// error: after a 413 one half may land and the other fail.
func (c *Client) writeBatch(ctx context.Context, batch []string, q url.Values) (int, error) {
	body := []byte(strings.Join(batch, "\n"))
	header := http.Header{"Content-Type": {"text/plain; charset=utf-8"}}
	if !c.opts.DisableGzip {
		var buf bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
		_, _ = zw.Write(body)
		if err := zw.Close(); err != nil {
			return 0, err
		}
		body = buf.Bytes()
		header.Set("Content-Encoding", "gzip")
	}
	resp, err := c.do(ctx, http.MethodPost, "/write", q, body, header)
	if err != nil {
		return 0, err
	}
	if resp.status == 200 || resp.status == 204 {
		return len(batch), nil
	}
	if resp.status == 413 && len(batch) > 1 {
		half := len(batch) / 2
		first, err := c.writeBatch(ctx, batch[:half], q)
		if err != nil {
			return first, err
		}
		second, err := c.writeBatch(ctx, batch[half:], q)
		return first + second, err
	}
	return 0, statusError(resp.status, errorText(resp.body), true)
}

func statusError(status int, message string, write bool) error {
	se := ServerError{Status: status, Message: message}
	switch {
	case status == 401 || status == 403:
		return &AuthError{se}
	case write && status < 500 && status != 429:
		return &WriteError{se}
	}
	return &se
}

// Epoch is the unit query times come back in.
type Epoch string

const (
	EpochNanosecond  Epoch = "ns"
	EpochMicrosecond Epoch = "u"
	EpochMillisecond Epoch = "ms" // the default
	EpochSecond      Epoch = "s"
	// EpochRFC3339 returns times as RFC 3339 strings.
	EpochRFC3339 Epoch = "rfc3339"
)

// QueryOptions are a query's database, time unit (default ms) and bound
// parameters ($name). By default a statement's error is returned as a
// *QueryError; with KeepErrors it is left in Result.Error instead.
type QueryOptions struct {
	Database   string
	Epoch      Epoch
	Params     map[string]any
	KeepErrors bool
}

// Series is one series of a result. Integer values are int64, other numbers
// float64.
type Series struct {
	Name    string            `json:"name"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns"`
	Values  [][]any           `json:"values"`
}

// Rows returns each row as a map of column to value, with the series' tags.
func (s Series) Rows() []map[string]any {
	rows := make([]map[string]any, 0, len(s.Values))
	for _, v := range s.Values {
		row := make(map[string]any, len(s.Tags)+len(s.Columns))
		for k, t := range s.Tags {
			row[k] = t
		}
		for i, col := range s.Columns {
			if i < len(v) {
				row[col] = v[i]
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// Result is one statement's result.
type Result struct {
	StatementID int      `json:"statement_id"`
	Series      []Series `json:"series"`
	Error       string   `json:"error,omitempty"`
}

// Rows returns the rows of every series.
func (r Result) Rows() []map[string]any {
	var rows []map[string]any
	for _, s := range r.Series {
		rows = append(rows, s.Rows()...)
	}
	return rows
}

// Query runs InfluxQL and returns one Result per statement.
func (c *Client) Query(ctx context.Context, q string, opts QueryOptions) ([]Result, error) {
	form := url.Values{"q": {q}}
	if opts.Database != "" {
		form.Set("db", opts.Database)
	}
	switch opts.Epoch {
	case "":
		form.Set("epoch", string(EpochMillisecond))
	case EpochRFC3339:
	default:
		form.Set("epoch", string(opts.Epoch))
	}
	if len(opts.Params) > 0 {
		p, err := json.Marshal(opts.Params)
		if err != nil {
			return nil, fmt.Errorf("hyperbytedb: params: %w", err)
		}
		form.Set("params", string(p))
	}
	header := http.Header{"Content-Type": {"application/x-www-form-urlencoded"}}
	resp, err := c.do(ctx, http.MethodPost, "/query", nil, []byte(form.Encode()), header)
	if err != nil {
		return nil, err
	}
	if resp.status == 401 || resp.status == 403 {
		return nil, &AuthError{ServerError{Status: resp.status, Message: errorText(resp.body)}}
	}
	if resp.status >= 300 && len(resp.body) == 0 {
		return nil, &ServerError{Status: resp.status}
	}
	var doc struct {
		Results []Result `json:"results"`
		Error   *string  `json:"error"`
	}
	dec := json.NewDecoder(bytes.NewReader(resp.body))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		if resp.status >= 300 {
			return nil, &ServerError{Status: resp.status, Message: strings.TrimSpace(string(resp.body))}
		}
		return nil, fmt.Errorf("hyperbytedb: query response: %w", err)
	}
	if doc.Error != nil && doc.Results == nil {
		if resp.status >= 500 {
			return nil, &ServerError{Status: resp.status, Message: *doc.Error}
		}
		return nil, &QueryError{Message: *doc.Error}
	}
	for i := range doc.Results {
		for j := range doc.Results[i].Series {
			for _, row := range doc.Results[i].Series[j].Values {
				for k, v := range row {
					row[k] = number(v)
				}
			}
		}
		if doc.Results[i].Error != "" && !opts.KeepErrors {
			return nil, &QueryError{Message: doc.Results[i].Error}
		}
	}
	return doc.Results, nil
}

// number turns a json.Number into int64 when it is an integer (nanosecond
// times do not fit a float64), else float64.
func number(v any) any {
	n, ok := v.(json.Number)
	if !ok {
		return v
	}
	if i, err := n.Int64(); err == nil {
		return i
	}
	if f, err := n.Float64(); err == nil {
		return f
	}
	return n.String()
}

// CreateDatabase creates a database (no error if it exists).
func (c *Client) CreateDatabase(ctx context.Context, name string) error {
	_, err := c.Query(ctx, `CREATE DATABASE "`+quoteIdent(name)+`"`, QueryOptions{Epoch: EpochRFC3339})
	return err
}

// DropDatabase drops a database and its data.
func (c *Client) DropDatabase(ctx context.Context, name string) error {
	_, err := c.Query(ctx, `DROP DATABASE "`+quoteIdent(name)+`"`, QueryOptions{Epoch: EpochRFC3339})
	return err
}

// Databases lists the databases.
func (c *Client) Databases(ctx context.Context) ([]string, error) {
	results, err := c.Query(ctx, "SHOW DATABASES", QueryOptions{Epoch: EpochRFC3339})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, r := range results {
		for _, row := range r.Rows() {
			if n, ok := row["name"].(string); ok {
				names = append(names, n)
			}
		}
	}
	return names, nil
}

func quoteIdent(name string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(name)
}

func retryAfter(h http.Header) time.Duration {
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	s, err := strconv.ParseFloat(v, 64)
	if err != nil || s < 0 {
		return 0
	}
	if s == 0 {
		return time.Millisecond
	}
	return time.Duration(s * float64(time.Second))
}

func errorText(body []byte) string {
	var doc struct {
		Error *string `json:"error"`
	}
	if json.Unmarshal(body, &doc) == nil && doc.Error != nil {
		return *doc.Error
	}
	return strings.TrimSpace(string(body))
}

func contains(xs []int, x int) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// IsRetryable reports whether err is worth retrying later: the server could
// not be reached, or answered 429 or 5xx after every retry.
func IsRetryable(err error) bool {
	var se *ServerError
	if errors.As(err, &se) {
		return retryable(se.Status)
	}
	var we *WriteError
	var ae *AuthError
	var qe *QueryError
	return err != nil && !errors.As(err, &we) && !errors.As(err, &ae) && !errors.As(err, &qe)
}
