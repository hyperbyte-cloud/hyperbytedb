package hyperbytedb

import (
	"math"
	"testing"
	"time"
)

func mustEncode(t *testing.T, p Point, precision Precision) string {
	t.Helper()
	line, ok, err := EncodePoint(p, precision)
	if err != nil || !ok {
		t.Fatalf("EncodePoint(%+v) = %q, %v, %v", p, line, ok, err)
	}
	return line
}

func TestEscapesLikeTheServer(t *testing.T) {
	got := mustEncode(t, Point{
		Measurement: "cpu load,x",
		Tags:        map[string]string{"host name": "a,b=c", "empty": ""},
		Fields:      map[string]any{"usage idle": 1.5, `msg"`: `say "hi" \ bye`},
		Time:        time.Unix(0, 123),
	}, Nanosecond)
	want := `cpu\ load\,x,host\ name=a\,b\=c msg"="say \"hi\" \\ bye",usage\ idle=1.5 123`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestFieldTypes(t *testing.T) {
	got := mustEncode(t, Point{Measurement: "m", Fields: map[string]any{
		"f": 1.0, "g": float32(0.5), "i": 2, "u": uint8(3), "b": true, "s": "x", "big": 1e21,
	}}, "")
	if want := `m b=true,big=1e+21,f=1,g=0.5,i=2i,s="x",u=3i`; got != want {
		t.Fatalf("got %s want %s", got, want)
	}
	if _, _, err := EncodePoint(Point{Measurement: "m", Fields: map[string]any{"u": uint64(math.MaxUint64)}}, ""); err == nil {
		t.Fatal("a uint64 above int64 must be refused")
	}
	if _, _, err := EncodePoint(Point{Measurement: "m", Fields: map[string]any{"x": []int{1}}}, ""); err == nil {
		t.Fatal("an unsupported type must be refused")
	}
}

func TestNonFiniteAndNilFieldsAreDropped(t *testing.T) {
	got := mustEncode(t, Point{Measurement: "m", Fields: map[string]any{
		"a": math.NaN(), "b": math.Inf(1), "c": nil, "d": 1.5,
	}}, "")
	if got != "m d=1.5" {
		t.Fatalf("got %s", got)
	}
	if _, ok, _ := EncodePoint(Point{Measurement: "m", Fields: map[string]any{"a": math.NaN()}}, ""); ok {
		t.Fatal("a point with no field left is skipped")
	}
	lp, _ := Encode([]Point{{Measurement: "m", Fields: map[string]any{"a": math.NaN()}}, {Measurement: "m", Fields: map[string]any{"v": 1.5}}}, "")
	if lp != "m v=1.5" {
		t.Fatalf("got %q", lp)
	}
}

func TestTimesFollowThePrecision(t *testing.T) {
	ts := time.Date(2026, 10, 12, 12, 0, 0, 500_000_000, time.UTC)
	p := Point{Measurement: "m", Fields: map[string]any{"v": 1.5}, Time: ts}
	if got := mustEncode(t, p, Nanosecond); got != "m v=1.5 1791806400500000000" {
		t.Fatal(got)
	}
	if got := mustEncode(t, p, Millisecond); got != "m v=1.5 1791806400500" {
		t.Fatal(got)
	}
	if got := mustEncode(t, p, Second); got != "m v=1.5 1791806400" {
		t.Fatal(got)
	}
	if _, _, err := EncodePoint(p, "minutes"); err == nil {
		t.Fatal("a bad precision must be refused")
	}
}

func TestNewlinesAreRefused(t *testing.T) {
	for _, p := range []Point{
		{Measurement: "m", Tags: map[string]string{"host": "a\nb"}, Fields: map[string]any{"v": 1.0}},
		{Measurement: "m", Fields: map[string]any{"msg": "two\nlines"}},
	} {
		if _, _, err := EncodePoint(p, ""); err == nil {
			t.Fatalf("%+v: a newline must be refused", p)
		}
	}
}
