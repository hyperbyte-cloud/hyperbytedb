package hyperbytedb

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

type reply struct {
	status int
	header map[string]string
	body   string
}

type request struct {
	method string
	path   string
	query  url.Values
	header http.Header
	body   string
}

// fake answers with the scripted replies in turn (then 204) and records
// every request.
type fake struct {
	mu       sync.Mutex
	script   []reply
	requests []request
}

func newFake(t *testing.T, script ...reply) (*fake, string) {
	f := &fake{script: script}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Errorf("gzip: %v", err)
				return
			}
			body = zr
		}
		b, _ := io.ReadAll(body)
		f.mu.Lock()
		f.requests = append(f.requests, request{r.Method, r.URL.Path, r.URL.Query(), r.Header.Clone(), string(b)})
		rep := reply{status: 204}
		if len(f.script) > 0 {
			rep, f.script = f.script[0], f.script[1:]
		}
		f.mu.Unlock()
		for k, v := range rep.header {
			w.Header().Set(k, v)
		}
		w.WriteHeader(rep.status)
		_, _ = io.WriteString(w, rep.body)
	}))
	t.Cleanup(srv.Close)
	return f, srv.URL
}

func testClient(u string, opts Options) *Client {
	if opts.Backoff == 0 {
		opts.Backoff = 10 * time.Millisecond
	}
	return NewClient(u, opts)
}

var ctx = context.Background()

func TestWriteBatchesGzipsAndAuthenticates(t *testing.T) {
	f, u := newFake(t)
	var points []Point
	for i := 0; i < 5; i++ {
		points = append(points, Point{Measurement: "m", Tags: map[string]string{"h": "a"},
			Fields: map[string]any{"v": float64(i) + 0.5}, Time: time.Unix(int64(i), 0)})
	}
	n, err := testClient(u, Options{Username: "u", Password: "p", BatchPoints: 2}).
		Write(ctx, "mydb", points, WriteOptions{Precision: Second})
	if err != nil || n != 5 {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if len(f.requests) != 3 {
		t.Fatalf("5 points in batches of 2: %d requests", len(f.requests))
	}
	r := f.requests[0]
	if r.method != "POST" || r.path != "/write" || r.query.Get("db") != "mydb" || r.query.Get("precision") != "s" {
		t.Fatalf("request %+v", r)
	}
	if r.header.Get("Authorization") != "Basic dTpw" || r.header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("headers %v", r.header)
	}
	if r.body != "m,h=a v=0.5 0\nm,h=a v=1.5 1" {
		t.Fatalf("body %q", r.body)
	}
}

func TestRetries503HonouringRetryAfter(t *testing.T) {
	f, u := newFake(t, reply{503, map[string]string{"Retry-After": "0"}, "busy"}, reply{503, nil, "busy"})
	if n, err := testClient(u, Options{}).WriteLines(ctx, "db", []string{"m v=1"}, WriteOptions{}); err != nil || n != 1 {
		t.Fatalf("WriteLines = %d, %v", n, err)
	}
	if len(f.requests) != 3 {
		t.Fatalf("%d requests", len(f.requests))
	}
}

func TestGivesUpAfterTheLastRetry(t *testing.T) {
	busy := reply{503, nil, `{"error":"write capacity exhausted"}`}
	_, u := newFake(t, busy, busy, busy)
	_, err := testClient(u, Options{Retries: 2}).WriteLines(ctx, "db", []string{"m v=1"}, WriteOptions{})
	var se *ServerError
	if !errors.As(err, &se) || se.Status != 503 || !strings.Contains(err.Error(), "capacity") {
		t.Fatalf("err = %v", err)
	}
	if !IsRetryable(err) {
		t.Fatal("a 503 is retryable later")
	}
}

func Test413SplitsTheBatch(t *testing.T) {
	f, u := newFake(t, reply{413, nil, "too big"})
	n, err := testClient(u, Options{}).WriteLines(ctx, "db", []string{"m v=1", "m v=2", "m v=3", "m v=4"}, WriteOptions{})
	if err != nil || n != 4 {
		t.Fatalf("WriteLines = %d, %v", n, err)
	}
	var bodies []string
	for _, r := range f.requests {
		bodies = append(bodies, r.body)
	}
	want := []string{"m v=1\nm v=2\nm v=3\nm v=4", "m v=1\nm v=2", "m v=3\nm v=4"}
	if strings.Join(bodies, "|") != strings.Join(want, "|") {
		t.Fatalf("bodies %q", bodies)
	}
}

func TestA413SplitThatFailsHalfwayCountsWhatLanded(t *testing.T) {
	_, u := newFake(t, reply{413, nil, "too big"}, reply{204, nil, ""}, reply{400, nil, `{"error":"bad"}`})
	n, err := testClient(u, Options{}).WriteLines(ctx, "db", []string{"m v=1", "m v=2", "m v=3", "m v=4"}, WriteOptions{})
	var we *WriteError
	if !errors.As(err, &we) || n != 2 {
		t.Fatalf("WriteLines = %d, %v; want 2 written and a WriteError", n, err)
	}
}

func TestBadWritesAndAuthAreNotRetried(t *testing.T) {
	f, u := newFake(t, reply{400, nil, `{"error":"field type conflict"}`}, reply{401, nil, "authorization failed"})
	c := testClient(u, Options{})
	_, err := c.WriteLines(ctx, "db", []string{"m v=1"}, WriteOptions{})
	var we *WriteError
	if !errors.As(err, &we) || !strings.Contains(err.Error(), "field type conflict") || IsRetryable(err) {
		t.Fatalf("err = %v", err)
	}
	_, err = c.WriteLines(ctx, "db", []string{"m v=1"}, WriteOptions{})
	var ae *AuthError
	if !errors.As(err, &ae) {
		t.Fatalf("err = %v", err)
	}
	if len(f.requests) != 2 {
		t.Fatalf("%d requests", len(f.requests))
	}
}

func TestQueryResultsAndErrors(t *testing.T) {
	doc := `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"a"},"columns":["time","mean"],"values":[[1791806400500000001,2.5]]}]}]}`
	f, u := newFake(t,
		reply{200, nil, doc},
		reply{200, nil, `{"results":[{"statement_id":0,"error":"measurement not found"}]}`},
		reply{400, nil, `{"error":"error parsing query: bad"}`},
	)
	c := testClient(u, Options{})
	res, err := c.Query(ctx, "SELECT mean(v) FROM cpu GROUP BY host", QueryOptions{Database: "db", Params: map[string]any{"h": "a"}})
	if err != nil {
		t.Fatal(err)
	}
	rows := res[0].Rows()
	// A nanosecond time survives (it does not fit a float64).
	if len(rows) != 1 || rows[0]["host"] != "a" || rows[0]["time"] != int64(1791806400500000001) || rows[0]["mean"] != 2.5 {
		t.Fatalf("rows %v", rows)
	}
	form, _ := url.ParseQuery(f.requests[0].body)
	if f.requests[0].method != "POST" || form.Get("db") != "db" || form.Get("epoch") != "ms" || form.Get("params") != `{"h":"a"}` {
		t.Fatalf("form %v", form)
	}
	var qe *QueryError
	if _, err := c.Query(ctx, "SELECT * FROM nope", QueryOptions{Database: "db"}); !errors.As(err, &qe) || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err = %v", err)
	}
	if _, err := c.Query(ctx, "SELEC", QueryOptions{Database: "db"}); !errors.As(err, &qe) || !strings.Contains(err.Error(), "parsing") {
		t.Fatalf("err = %v", err)
	}
}

func TestQueryKeepErrorsAndRFC3339(t *testing.T) {
	f, u := newFake(t, reply{200, nil, `{"results":[{"statement_id":0,"error":"boom"}]}`})
	res, err := testClient(u, Options{}).Query(ctx, "SELECT 1", QueryOptions{Epoch: EpochRFC3339, KeepErrors: true})
	if err != nil || res[0].Error != "boom" {
		t.Fatalf("res %v err %v", res, err)
	}
	if form, _ := url.ParseQuery(f.requests[0].body); form.Has("epoch") {
		t.Fatal("RFC 3339 sends no epoch")
	}
}

func TestHealthAnswers503WithoutRetrying(t *testing.T) {
	f, u := newFake(t, reply{503, map[string]string{"Content-Type": "application/json"}, `{"status":"draining"}`})
	h, err := testClient(u, Options{Backoff: 5 * time.Second}).Health(ctx)
	if err != nil || h["status"] != "draining" || len(f.requests) != 1 {
		t.Fatalf("health %v %v (%d requests)", h, err, len(f.requests))
	}
}

func TestPingPrefersHyperbytedbVersionHeader(t *testing.T) {
	_, u := newFake(t,
		reply{204, map[string]string{"X-Hyperbytedb-Version": "0.9.3-beta", "X-Influxdb-Version": "HyperbyteDB-0.9.3-beta"}, ""},
		reply{204, map[string]string{"X-Influxdb-Version": "HyperbyteDB-0.9.2-beta"}, ""},
	)
	c := testClient(u, Options{})
	if v, err := c.Ping(ctx); err != nil || v != "0.9.3-beta" {
		t.Fatalf("Ping = %q, %v", v, err)
	}
	if v, err := c.Ping(ctx); err != nil || v != "0.9.2-beta" {
		t.Fatalf("a server before 0.9.3: Ping = %q, %v", v, err)
	}
}

func TestUnreachableServerFailsAfterRetries(t *testing.T) {
	_, err := testClient("http://127.0.0.1:1", Options{Retries: 1}).Ping(ctx)
	if err == nil || !IsRetryable(err) {
		t.Fatalf("err = %v", err)
	}
}

func TestContextCancelStopsRetrying(t *testing.T) {
	busy := reply{503, nil, "busy"}
	_, u := newFake(t, busy, busy, busy, busy, busy, busy)
	c, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := testClient(u, Options{Backoff: time.Second}).WriteLines(c, "db", []string{"m v=1"}, WriteOptions{})
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("err = %v after %v", err, time.Since(start))
	}
}
