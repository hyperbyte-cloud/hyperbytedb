/**
 * InfluxDB line protocol, as HyperbyteDB parses it.
 *
 * A `number` field is written as a float and a `bigint` as an integer
 * (`12n` → `12i`): a field keeps the type it was first written with. `null`,
 * `undefined`, NaN and ±Infinity fields are left out, and so are tags with an
 * empty value: line protocol cannot express them.
 */

export type FieldValue = number | bigint | boolean | string | null | undefined;

export interface Point {
  measurement: string;
  fields: Record<string, FieldValue>;
  tags?: Record<string, string | null | undefined>;
  /**
   * A `Date`, or a `number`/`bigint` in the write's precision units. Without
   * one the server stamps the point.
   */
  time?: Date | number | bigint;
}

export type Precision = "ns" | "u" | "ms" | "s";

const PER_MS: Record<Precision, bigint> = { ns: 1_000_000n, u: 1_000n, ms: 1n, s: 0n };

function escape(s: string, special: string): string {
  let out = "";
  for (const ch of s) {
    if (ch === "\n") {
      // A newline ends the line; it has no escape.
      throw new TypeError(`newline in line protocol name or tag: ${JSON.stringify(s)}`);
    }
    if (ch === "\\" || special.includes(ch)) out += "\\";
    out += ch;
  }
  return out;
}

function fieldValue(v: FieldValue): string | undefined {
  switch (typeof v) {
    case "boolean":
      return v ? "true" : "false";
    case "bigint":
      return `${v}i`;
    case "number":
      return Number.isFinite(v) ? String(v) : undefined;
    case "string":
      if (v.includes("\n")) {
        // A newline ends the line even inside quotes; there is no escape.
        throw new TypeError(`newline in line protocol string field: ${JSON.stringify(v)}`);
      }
      return `"${v.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
    case "undefined":
      return undefined;
    default:
      if (v === null) return undefined;
      throw new TypeError(`unsupported field value type: ${typeof v}`);
  }
}

function timestamp(t: Date | number | bigint, precision: Precision): string {
  if (t instanceof Date) {
    const ms = BigInt(t.getTime());
    return String(precision === "s" ? ms / 1000n : ms * PER_MS[precision]);
  }
  if (typeof t === "number") {
    if (!Number.isInteger(t)) throw new TypeError(`timestamp must be an integer: ${t}`);
    return BigInt(t).toString();
  }
  return t.toString();
}

export function checkPrecision(precision: string): asserts precision is Precision {
  if (!(precision in PER_MS)) {
    throw new TypeError(`precision must be ns, u, ms or s, not ${JSON.stringify(precision)}`);
  }
}

/** One line, or `undefined` when the point has no field left to write. */
export function encodePoint(p: Point, precision: Precision = "ns"): string | undefined {
  checkPrecision(precision);
  const fields: string[] = [];
  for (const k of Object.keys(p.fields).sort()) {
    const v = fieldValue(p.fields[k]);
    if (v !== undefined) fields.push(`${escape(k, ",= ")}=${v}`);
  }
  if (fields.length === 0) return undefined;
  let line = escape(p.measurement, ", ");
  for (const k of Object.keys(p.tags ?? {}).sort()) {
    const v = p.tags?.[k];
    if (v === undefined || v === null || v === "") continue;
    line += `,${escape(k, ",= ")}=${escape(String(v), ",= ")}`;
  }
  line += ` ${fields.join(",")}`;
  if (p.time !== undefined) line += ` ${timestamp(p.time, precision)}`;
  return line;
}

/** Points as line protocol, one per line; points with no fields dropped. */
export function encode(points: Iterable<Point>, precision: Precision = "ns"): string {
  const lines: string[] = [];
  for (const p of points) {
    const line = encodePoint(p, precision);
    if (line !== undefined) lines.push(line);
  }
  return lines.join("\n");
}
