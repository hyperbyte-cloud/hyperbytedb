import { AuthError, HyperbyteDBError, QueryError, ServerError, WriteError } from "./errors.js";
import { checkPrecision, encodePoint, type Point, type Precision } from "./lineProtocol.js";

export const VERSION = "0.9.3-beta";

export interface ClientOptions {
  /** Sent as `Authorization: Basic`, never in the URL. */
  username?: string;
  password?: string;
  /** Per request, in ms (default 30,000). */
  timeout?: number;
  /** Retries on connection errors, 429 and 5xx (default 5). */
  retries?: number;
  /** First retry delay in ms, doubled up to `maxBackoff`, with jitter (500 / 30,000). */
  backoff?: number;
  maxBackoff?: number;
  /** Gzip write bodies (default true). */
  gzipWrites?: boolean;
  /** Each write request holds at most this many points / bytes (5,000 / 4 MiB). */
  batchPoints?: number;
  batchBytes?: number;
  /** A `fetch` to use instead of the global one. */
  fetch?: typeof fetch;
}

export interface WriteOptions {
  retentionPolicy?: string;
  /** Unit of numeric point times (default `ns`). */
  precision?: Precision;
  signal?: AbortSignal;
}

export interface QueryOptions {
  database?: string;
  /**
   * Unit of returned times (default `ms`), or `"rfc3339"` for strings.
   * Nanosecond times do not fit a JS number exactly; prefer `ms` or `u`.
   */
  epoch?: "ns" | "u" | "ms" | "s" | "rfc3339";
  /** Binds `$name` placeholders. */
  params?: Record<string, unknown>;
  /** Leave statement errors in `Result.error` instead of throwing. */
  keepErrors?: boolean;
  signal?: AbortSignal;
}

export type Row = Record<string, unknown>;

export class Series {
  constructor(
    readonly name: string,
    readonly columns: string[],
    readonly values: unknown[][],
    readonly tags: Record<string, string> = {},
  ) {}

  /** Each row as an object of column to value, with the series' tags. */
  rows(): Row[] {
    return this.values.map((v) => {
      const row: Row = { ...this.tags };
      this.columns.forEach((c, i) => (row[c] = v[i]));
      return row;
    });
  }
}

/** One statement's result. */
export class Result {
  constructor(
    readonly statementId: number,
    readonly series: Series[],
    readonly error?: string,
  ) {}

  rows(): Row[] {
    return this.series.flatMap((s) => s.rows());
  }
}

/**
 * Statuses worth another attempt: rate limited, overloaded (HyperbyteDB
 * answers 503 with Retry-After when write admission is full), or a server or
 * gateway error that may pass.
 */
const RETRYABLE = new Set([429, 500, 502, 503, 504]);

interface Reply {
  status: number;
  headers: Headers;
  body: string;
}

/**
 * A HyperbyteDB connection. `url` is a node, or a load balancer or
 * Kubernetes Service in front of the cluster: any node accepts any write or
 * query.
 *
 * Writes are batched, gzipped and retried with backoff on connection errors,
 * 429 and 5xx, honouring `Retry-After`. A batch refused as too large (413) is
 * split in two and retried.
 */
export class HyperbyteDBClient {
  readonly url: string;
  private readonly opts: Required<Omit<ClientOptions, "username" | "password" | "fetch">>;
  private readonly fetch: typeof fetch;
  private readonly auth?: string;

  constructor(url = "http://localhost:8086", options: ClientOptions = {}) {
    this.url = url.replace(/\/+$/, "");
    this.opts = {
      timeout: options.timeout ?? 30_000,
      retries: options.retries ?? 5,
      backoff: options.backoff ?? 500,
      maxBackoff: options.maxBackoff ?? 30_000,
      gzipWrites: options.gzipWrites ?? true,
      batchPoints: Math.max(1, options.batchPoints ?? 5_000),
      batchBytes: Math.max(1, options.batchBytes ?? 4 * 1024 * 1024),
    };
    const f = options.fetch ?? globalThis.fetch;
    if (!f) throw new HyperbyteDBError("no fetch available: pass options.fetch");
    this.fetch = f.bind(globalThis);
    if (options.username !== undefined) {
      this.auth = `Basic ${base64(`${options.username}:${options.password ?? ""}`)}`;
    }
  }

  private async request(
    method: string,
    path: string,
    init: { query?: Record<string, string>; body?: BodyInit; headers?: Record<string, string>; answers?: number[]; signal?: AbortSignal } = {},
  ): Promise<Reply> {
    let url = this.url + path;
    if (init.query) url += `?${new URLSearchParams(init.query)}`;
    const headers: Record<string, string> = { ...init.headers };
    if (this.auth) headers.Authorization = this.auth;
    // Browsers refuse to set User-Agent; Node sends it.
    if (typeof window === "undefined") headers["User-Agent"] = `hyperbytedb-js/${VERSION}`;
    for (let attempt = 0; ; attempt++) {
      let wait: number | undefined;
      const deadline = withTimeout(init.signal, this.opts.timeout);
      try {
        const resp = await this.fetch(url, { method, headers, body: init.body, signal: deadline.signal });
        const reply = { status: resp.status, headers: resp.headers, body: await resp.text() };
        if (!RETRYABLE.has(reply.status) || init.answers?.includes(reply.status) || attempt >= this.opts.retries) {
          return reply;
        }
        wait = retryAfter(reply.headers);
      } catch (e) {
        if (init.signal?.aborted) throw init.signal.reason ?? e;
        if (attempt >= this.opts.retries) {
          throw new HyperbyteDBError(`${method} ${path}: ${(e as Error).message}`, { cause: e });
        }
      } finally {
        deadline.clear();
      }
      await sleep(wait ?? this.backoffDelay(attempt + 1), init.signal);
    }
  }

  private backoffDelay(attempt: number): number {
    const base = Math.min(this.opts.maxBackoff, this.opts.backoff * 2 ** (attempt - 1));
    return base * (0.5 + Math.random() / 2);
  }

  /** The server's version, e.g. `0.9.3-beta`; throws if it is down. */
  async ping(signal?: AbortSignal): Promise<string> {
    const r = await this.request("GET", "/ping", { signal });
    if (r.status >= 300) throw new ServerError(r.status, errorText(r.body));
    // Servers before 0.9.3 send only X-Influxdb-Version: HyperbyteDB-<version>.
    return r.headers.get("X-Hyperbytedb-Version") ?? (r.headers.get("X-Influxdb-Version") ?? "").replace(/^HyperbyteDB-/, "");
  }

  /** `/health`: the node's state. A 503 (draining, syncing) is its answer, not retried. */
  async health(signal?: AbortSignal): Promise<Record<string, unknown>> {
    const r = await this.request("GET", "/health", { answers: [503], signal });
    if (r.status >= 300 && r.status !== 503) throw new ServerError(r.status, errorText(r.body));
    return r.body ? JSON.parse(r.body) : {};
  }

  /**
   * Writes points, or ready-made line protocol (a string, or one line per
   * element), and returns how many lines were sent. Throws `WriteError` on a
   * refused batch; batches before it have been written.
   */
  async write(data: Point | Iterable<Point | string> | string, database: string, options: WriteOptions = {}): Promise<number> {
    const precision = options.precision ?? "ns";
    checkPrecision(precision);
    const lines = [...asLines(data, precision)];
    const query: Record<string, string> = { db: database, precision };
    if (options.retentionPolicy) query.rp = options.retentionPolicy;
    let sent = 0;
    for (const batch of this.batches(lines)) {
      await this.writeBatch(batch, query, options.signal);
      sent += batch.length;
    }
    return sent;
  }

  private *batches(lines: string[]): Generator<string[]> {
    let batch: string[] = [];
    let size = 0;
    for (const line of lines) {
      const n = utf8Length(line) + 1;
      if (batch.length && (batch.length >= this.opts.batchPoints || size + n > this.opts.batchBytes)) {
        yield batch;
        batch = [];
        size = 0;
      }
      batch.push(line);
      size += n;
    }
    if (batch.length) yield batch;
  }

  private async writeBatch(batch: string[], query: Record<string, string>, signal?: AbortSignal): Promise<void> {
    let body: BodyInit = batch.join("\n");
    const headers: Record<string, string> = { "Content-Type": "text/plain; charset=utf-8" };
    if (this.opts.gzipWrites && typeof CompressionStream !== "undefined") {
      body = await gzip(body);
      headers["Content-Encoding"] = "gzip";
    }
    const r = await this.request("POST", "/write", { query, body, headers, signal });
    if (r.status === 200 || r.status === 204) return;
    if (r.status === 413 && batch.length > 1) {
      const half = batch.length >> 1;
      await this.writeBatch(batch.slice(0, half), query, signal);
      await this.writeBatch(batch.slice(half), query, signal);
      return;
    }
    const message = errorText(r.body);
    if (r.status === 401 || r.status === 403) throw new AuthError(r.status, message);
    if (r.status >= 500 || r.status === 429) throw new ServerError(r.status, message);
    throw new WriteError(r.status, message);
  }

  /** Runs InfluxQL; one `Result` per statement. */
  async query(q: string, options: QueryOptions = {}): Promise<Result[]> {
    const form = new URLSearchParams({ q });
    if (options.database) form.set("db", options.database);
    const epoch = options.epoch ?? "ms";
    if (epoch !== "rfc3339") form.set("epoch", epoch);
    if (options.params) form.set("params", JSON.stringify(options.params));
    const r = await this.request("POST", "/query", {
      body: form.toString(),
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      signal: options.signal,
    });
    if (r.status === 401 || r.status === 403) throw new AuthError(r.status, errorText(r.body));
    if (r.status >= 300 && !r.body) throw new ServerError(r.status, "");
    let doc: { results?: RawResult[]; error?: string };
    try {
      doc = JSON.parse(r.body || "{}");
    } catch {
      throw new ServerError(r.status, r.body.trim());
    }
    if (doc.error !== undefined && doc.results === undefined) {
      if (r.status >= 500) throw new ServerError(r.status, doc.error);
      throw new QueryError(doc.error);
    }
    const results = (doc.results ?? []).map(
      (res, i) =>
        new Result(
          res.statement_id ?? i,
          (res.series ?? []).map((s) => new Series(s.name ?? "", s.columns ?? [], s.values ?? [], s.tags ?? {})),
          res.error,
        ),
    );
    if (!options.keepErrors) {
      const failed = results.find((res) => res.error);
      if (failed) throw new QueryError(failed.error!);
    }
    return results;
  }

  async createDatabase(name: string): Promise<void> {
    await this.query(`CREATE DATABASE "${quoteIdent(name)}"`, { epoch: "rfc3339" });
  }

  async dropDatabase(name: string): Promise<void> {
    await this.query(`DROP DATABASE "${quoteIdent(name)}"`, { epoch: "rfc3339" });
  }

  async databases(): Promise<string[]> {
    const [res] = await this.query("SHOW DATABASES", { epoch: "rfc3339" });
    return (res?.rows() ?? []).map((row) => String(row.name));
  }
}

interface RawResult {
  statement_id?: number;
  series?: { name?: string; columns?: string[]; values?: unknown[][]; tags?: Record<string, string> }[];
  error?: string;
}

function* asLines(data: Point | Iterable<Point | string> | string, precision: Precision): Generator<string> {
  if (typeof data === "string") {
    for (const line of data.split("\n")) if (line.trim()) yield line;
    return;
  }
  const items: Iterable<Point | string> = isPoint(data) ? [data] : data;
  for (const item of items) {
    if (typeof item === "string") {
      if (item.trim()) yield item;
    } else if (isPoint(item)) {
      const line = encodePoint(item, precision);
      if (line !== undefined) yield line;
    } else {
      throw new TypeError(`expected a Point or a string, not ${typeof item}`);
    }
  }
}

function isPoint(x: unknown): x is Point {
  return typeof x === "object" && x !== null && "measurement" in x && "fields" in x;
}

function quoteIdent(name: string): string {
  return name.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function retryAfter(headers: Headers): number | undefined {
  const v = headers.get("Retry-After");
  if (!v) return undefined;
  const s = Number(v);
  return Number.isFinite(s) && s >= 0 ? s * 1000 : undefined;
}

function errorText(body: string): string {
  try {
    const doc = JSON.parse(body);
    if (doc && typeof doc === "object" && "error" in doc) return String(doc.error);
  } catch {
    /* not JSON */
  }
  return body.trim();
}

function base64(s: string): string {
  let bin = "";
  for (const b of new TextEncoder().encode(s)) bin += String.fromCharCode(b);
  return btoa(bin);
}

function utf8Length(s: string): number {
  return new TextEncoder().encode(s).length;
}

async function gzip(text: string): Promise<Uint8Array> {
  const stream = new Blob([text]).stream().pipeThrough(new CompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

/**
 * A signal that aborts on `parent` or after `ms` (AbortSignal.any is Node
 * 20+). `clear()` once the request is done: the timer otherwise keeps a
 * Node process alive until it fires.
 */
function withTimeout(parent: AbortSignal | undefined, ms: number): { signal: AbortSignal; clear: () => void } {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(new HyperbyteDBError(`request timed out after ${ms} ms`)), ms);
  const onParentAbort = () => ctl.abort(parent?.reason);
  if (parent) {
    if (parent.aborted) ctl.abort(parent.reason);
    else parent.addEventListener("abort", onParentAbort, { once: true });
  }
  return {
    signal: ctl.signal,
    clear: () => {
      clearTimeout(timer);
      parent?.removeEventListener("abort", onParentAbort);
    },
  };
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
