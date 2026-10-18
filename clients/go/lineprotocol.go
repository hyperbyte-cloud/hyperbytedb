package hyperbytedb

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Point is one point: a measurement, its tags, its fields and an optional
// time. A zero Time leaves the time to the server.
//
// Field values may be float32/float64, any signed or unsigned integer (sent
// as an integer, "1i"), bool or string. A field keeps the type it was first
// written with, so write 1.0, not 1, for a float field. Nil, NaN and ±Inf
// fields are left out, and so are tags with an empty value: line protocol
// cannot express them.
type Point struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]any
	Time        time.Time
}

// Precision is the unit of written timestamps.
type Precision string

const (
	Nanosecond  Precision = "ns"
	Microsecond Precision = "u"
	Millisecond Precision = "ms"
	Second      Precision = "s"
)

func (p Precision) divisor() (int64, error) {
	switch p {
	case "", Nanosecond:
		return 1, nil
	case Microsecond:
		return 1_000, nil
	case Millisecond:
		return 1_000_000, nil
	case Second:
		return 1_000_000_000, nil
	}
	return 0, fmt.Errorf("hyperbytedb: precision must be ns, u, ms or s, not %q", string(p))
}

func escape(b *strings.Builder, s, special string) error {
	for _, r := range s {
		if r == '\n' {
			// A newline ends the line; it has no escape.
			return fmt.Errorf("hyperbytedb: newline in line protocol name or tag: %q", s)
		}
		if r == '\\' || strings.ContainsRune(special, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return nil
}

func fieldValue(v any) (string, bool, error) {
	switch x := v.(type) {
	case nil:
		return "", false, nil
	case bool:
		if x {
			return "true", true, nil
		}
		return "false", true, nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return "", false, nil
		}
		return strconv.FormatFloat(x, 'g', -1, 64), true, nil
	case float32:
		return fieldValue(float64(x))
	case int:
		return strconv.FormatInt(int64(x), 10) + "i", true, nil
	case int8:
		return strconv.FormatInt(int64(x), 10) + "i", true, nil
	case int16:
		return strconv.FormatInt(int64(x), 10) + "i", true, nil
	case int32:
		return strconv.FormatInt(int64(x), 10) + "i", true, nil
	case int64:
		return strconv.FormatInt(x, 10) + "i", true, nil
	case uint:
		return uintValue(uint64(x))
	case uint8:
		return uintValue(uint64(x))
	case uint16:
		return uintValue(uint64(x))
	case uint32:
		return uintValue(uint64(x))
	case uint64:
		return uintValue(x)
	case string:
		if strings.ContainsRune(x, '\n') {
			// A newline ends the line even inside quotes; there is no escape.
			return "", false, fmt.Errorf("hyperbytedb: newline in line protocol string field: %q", x)
		}
		r := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
		return `"` + r.Replace(x) + `"`, true, nil
	}
	return "", false, fmt.Errorf("hyperbytedb: unsupported field value type %T", v)
}

func uintValue(x uint64) (string, bool, error) {
	if x > math.MaxInt64 {
		return "", false, fmt.Errorf("hyperbytedb: integer field %d overflows int64", x)
	}
	return strconv.FormatUint(x, 10) + "i", true, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// EncodePoint returns the point as one line of line protocol, and false when
// it has no field left to write.
func EncodePoint(p Point, precision Precision) (string, bool, error) {
	div, err := precision.divisor()
	if err != nil {
		return "", false, err
	}
	var fields strings.Builder
	for _, k := range sortedKeys(p.Fields) {
		v, ok, err := fieldValue(p.Fields[k])
		if err != nil {
			return "", false, err
		}
		if !ok {
			continue
		}
		if fields.Len() > 0 {
			fields.WriteByte(',')
		}
		if err := escape(&fields, k, ",= "); err != nil {
			return "", false, err
		}
		fields.WriteByte('=')
		fields.WriteString(v)
	}
	if fields.Len() == 0 {
		return "", false, nil
	}
	var b strings.Builder
	if err := escape(&b, p.Measurement, ", "); err != nil {
		return "", false, err
	}
	for _, k := range sortedKeys(p.Tags) {
		v := p.Tags[k]
		if v == "" {
			continue
		}
		b.WriteByte(',')
		if err := escape(&b, k, ",= "); err != nil {
			return "", false, err
		}
		b.WriteByte('=')
		if err := escape(&b, v, ",= "); err != nil {
			return "", false, err
		}
	}
	b.WriteByte(' ')
	b.WriteString(fields.String())
	if !p.Time.IsZero() {
		b.WriteByte(' ')
		b.WriteString(strconv.FormatInt(p.Time.UnixNano()/div, 10))
	}
	return b.String(), true, nil
}

// Encode returns the points as line protocol, one per line. Points with no
// fields left are dropped.
func Encode(points []Point, precision Precision) (string, error) {
	lines := make([]string, 0, len(points))
	for _, p := range points {
		line, ok, err := EncodePoint(p, precision)
		if err != nil {
			return "", err
		}
		if ok {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n"), nil
}
