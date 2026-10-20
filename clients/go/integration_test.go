package hyperbytedb

// Against a real HyperbyteDB: HYPERBYTEDB_URL (and, with auth on,
// HYPERBYTEDB_USER / HYPERBYTEDB_PASSWORD for an admin).

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func realClient(t *testing.T) (*Client, string) {
	u := os.Getenv("HYPERBYTEDB_URL")
	if u == "" {
		t.Skip("HYPERBYTEDB_URL not set")
	}
	c := NewClient(u, Options{Username: os.Getenv("HYPERBYTEDB_USER"), Password: os.Getenv("HYPERBYTEDB_PASSWORD")})
	name := fmt.Sprintf("goclient_%08x", rand.Uint32())
	if err := c.CreateDatabase(ctx, name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.DropDatabase(ctx, name) })
	return c, name
}

// waitRows waits for rows: they reach queries at the next flush.
func waitRows(t *testing.T, c *Client, q, db string, n int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		res, err := c.Query(ctx, q, QueryOptions{Database: db})
		if err != nil {
			t.Fatal(err)
		}
		rows := res[0].Rows()
		if len(rows) >= n || time.Now().After(deadline) {
			return rows
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func TestIntegrationPingAndHealth(t *testing.T) {
	c, _ := realClient(t)
	v, err := c.Ping(ctx)
	if err != nil || v == "" || v[0] < '0' || v[0] > '9' {
		t.Fatalf("Ping = %q, %v", v, err)
	}
	if _, err := c.Health(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationRoundTripWithAwkwardNamesAndEveryType(t *testing.T) {
	c, db := realClient(t)
	base := time.UnixMilli(1_700_000_000_000)
	var points []Point
	for i := 0; i < 3; i++ {
		points = append(points, Point{
			Measurement: "disk usage",
			Tags:        map[string]string{"host": "web 1,a=b", "service": "Filesystem /"},
			Fields:      map[string]any{"used": 12.5 + float64(i), "inodes": 100 + i, "mount": `C:\ "data"`},
			Time:        base.Add(time.Duration(i) * time.Second),
		})
	}
	if n, err := c.Write(ctx, db, points, WriteOptions{Precision: Millisecond}); err != nil || n != 3 {
		t.Fatalf("Write = %d, %v", n, err)
	}
	rows := waitRows(t, c, `SELECT * FROM "disk usage" ORDER BY time`, db, 3)
	if len(rows) != 3 {
		t.Fatalf("rows %v", rows)
	}
	r := rows[0]
	if r["time"] != int64(1_700_000_000_000) || r["host"] != "web 1,a=b" || r["service"] != "Filesystem /" ||
		r["used"] != 12.5 || rows[2]["inodes"] != int64(102) || r["mount"] != `C:\ "data"` {
		t.Fatalf("row %v", r)
	}
}

func TestIntegrationQueriesByTagWithEpochAndBoundParams(t *testing.T) {
	c, db := realClient(t)
	var lines []string
	for h := 0; h < 3; h++ {
		for i := 0; i < 4; i++ {
			lines = append(lines, fmt.Sprintf("cpu,host=h%d usage=%d.0 %d", h, h*10+i, 1_700_000_000+i))
		}
	}
	if n, err := c.WriteLines(ctx, db, lines, WriteOptions{Precision: Second}); err != nil || n != 12 {
		t.Fatalf("WriteLines = %d, %v", n, err)
	}
	waitRows(t, c, "SELECT * FROM cpu", db, 12)
	res, err := c.Query(ctx, "SELECT max(usage) FROM cpu GROUP BY host", QueryOptions{Database: db, Epoch: EpochSecond})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]any{}
	for _, s := range res[0].Series {
		got[s.Tags["host"]] = s.Values[0][1]
	}
	if fmt.Sprint(got) != "map[h0:3 h1:13 h2:23]" {
		t.Fatalf("max by host %v", got)
	}
	res, err = c.Query(ctx, "SELECT count(usage) FROM cpu WHERE host = $h", QueryOptions{Database: db, Params: map[string]any{"h": "h1"}})
	if err != nil || res[0].Series[0].Values[0][1] != int64(4) {
		t.Fatalf("count %v %v", res, err)
	}
}

func TestIntegrationErrors(t *testing.T) {
	c, db := realClient(t)
	var qe *QueryError
	if _, err := c.Query(ctx, "SELEKT nonsense", QueryOptions{Database: db}); !errors.As(err, &qe) {
		t.Fatalf("err = %v", err)
	}
	var we *WriteError
	if _, err := c.WriteLines(ctx, db, []string{"m v=1", "m v=\"text\""}, WriteOptions{}); !errors.As(err, &we) {
		t.Fatalf("a field type conflict: err = %v", err)
	}
	if user := os.Getenv("HYPERBYTEDB_USER"); user != "" {
		bad := NewClient(os.Getenv("HYPERBYTEDB_URL"), Options{Username: user, Password: "wrong", Retries: -1})
		var ae *AuthError
		if _, err := bad.Databases(ctx); !errors.As(err, &ae) {
			t.Fatalf("wrong password: err = %v", err)
		}
	}
}
