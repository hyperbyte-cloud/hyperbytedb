#!/usr/bin/env python3
"""In-cluster ingest benchmark for linear write scaling.

Runs as a Job INSIDE the cluster. Two earlier harnesses could not measure
scaling at all:

  * a serial curl loop driven through `kubectl port-forward` from a laptop --
    one synchronous round-trip per batch, so throughput was bounded by RTT
    (19ms to the test cluster) and by the port-forward tunnel, not by the
    database;
  * a single entry point -- `port-forward svc/...` pins every connection to
    one backing pod, so as N rose that node did more forwarding and measured
    throughput FELL.

This sends concurrently from inside the cluster network, spread across every
node, which is the only shape in which "does write throughput grow with N?"
is a meaningful question.
"""
import argparse, os, queue, random, sys, threading, time, urllib.error, urllib.parse, urllib.request


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--endpoints", required=True, help="comma-separated base URLs")
    p.add_argument("--db", default="linearity")
    p.add_argument("--measurement", default="cpu")
    p.add_argument("--series", type=int, required=True, help="series to seed")
    p.add_argument("--measure-points", type=int, default=2_000_000)
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--concurrency", type=int, default=32)
    # Seed and measure run as separate Jobs so the sweep can wait, between
    # them, for the space to split. Splits are driven by heartbeat and
    # scheduler ticks (tens of seconds); a run that seeds and measures back
    # to back times one region, i.e. one primary, however many nodes exist.
    p.add_argument("--phase", choices=["seed", "measure", "all"], default="all")
    # One Python process tops out near 900k lines/s (GIL-bound string
    # building), which is about what N=3 measured -- the client, not the
    # cluster. The sweep runs several of these as an indexed Job; each takes
    # every `workers`-th batch. The index defaults to the Job's completion
    # index.
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--worker-index", type=int,
                   default=int(os.environ.get("JOB_COMPLETION_INDEX", "0")))
    return p.parse_args()


# Why writes failed: "<status> <first 120 chars of body>" -> count. A bare
# count of non-204s said nothing about the cause.
ERROR_KINDS = {}
ERROR_LOCK = threading.Lock()

# A write the server turned away before reading its body. Admission control
# answers 503 at once and closes the connection; a body larger than the
# socket buffers is still uploading, so the client sees a broken pipe or a
# reset instead of the 503. It is the same "not now" and is retried the same
# way.
REFUSED_UNREAD = 599
RETRYABLE = (429, 503, REFUSED_UNREAD)


def _refused_unread(e):
    reason = getattr(e, "reason", e)
    return isinstance(reason, (BrokenPipeError, ConnectionResetError))


def post(url, body, timeout=60, content_type="text/plain"):
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", content_type)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        if e.code not in (429, 503):
            try:
                detail = e.read(120).decode("utf-8", "replace").replace("\n", " ")
            except Exception:
                detail = ""
            with ERROR_LOCK:
                k = f"{e.code} {detail}"
                ERROR_KINDS[k] = ERROR_KINDS.get(k, 0) + 1
        return e.code
    except Exception as e:
        if _refused_unread(e):
            return REFUSED_UNREAD
        with ERROR_LOCK:
            k = f"0 {type(e).__name__}: {str(e)[:100]}"
            ERROR_KINDS[k] = ERROR_KINDS.get(k, 0) + 1
        return 0


def run_phase(endpoints, db, meas, start, count, batch, concurrency, ts_base, series=None,
              workers=1, worker_index=0):
    """Send `count` points as `batch`-sized writes, `concurrency` in flight.

    With `series`, point i goes to series `i % series`, so a measure phase
    rewrites the seeded series instead of creating new ones.
    """
    work = queue.Queue()
    for n, b in enumerate(range(0, count, batch)):
        if n % workers == worker_index:
            work.put(b)
    errors = [0]
    retries = [0]
    lock = threading.Lock()

    def worker(idx):
        ep = endpoints[(idx + worker_index) % len(endpoints)]
        url = f"{ep}/write?db={db}&rp=autogen&precision=ns"
        local_err = 0
        local_retry = 0
        while True:
            try:
                base = work.get_nowait()
            except queue.Empty:
                break
            lines = []
            for i in range(base, min(base + batch, count)):
                sid = start + (i % series if series else i)
                # One second per full pass over the series, so the data spans
                # seconds, like a live workload. `ts_base + i` spread 20M
                # points over 231 days; every flush block then touched more
                # than chDB's 100-partition INSERT limit, flushes failed, and
                # the run measured the resulting backlog instead of ingest.
                ts = ts_base + (i // series if series else i)
                lines.append(f"{meas},host=s{sid} value=1 {ts}{sid % 1_000_000_000:09d}")
            body = ("\n".join(lines)).encode()
            code = post(url, body)
            # 503/429 is admission control saying "not now": back off and
            # retry like a real client. Only a batch that never lands, or a
            # different failure, is an error.
            attempt = 0
            while code in RETRYABLE and attempt < 12:
                time.sleep(min(0.05 * 2 ** min(attempt, 5), 1.6) * (0.5 + random.random()))
                attempt += 1
                local_retry += 1
                code = post(url, body)
            if code in RETRYABLE:
                with ERROR_LOCK:
                    k = f"{code} still rejected after {attempt} retries"
                    ERROR_KINDS[k] = ERROR_KINDS.get(k, 0) + 1
            if code != 204:
                local_err += 1
        with lock:
            errors[0] += local_err
            retries[0] += local_retry

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(concurrency)]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.monotonic() - t0, errors[0], retries[0]


def main():
    a = parse_args()
    endpoints = [e.strip().rstrip("/") for e in a.endpoints.split(",") if e.strip()]
    print(f"[bench] endpoints={len(endpoints)} concurrency={a.concurrency} "
          f"series={a.series} measure_points={a.measure_points}", flush=True)

    if a.phase in ("seed", "all") and a.worker_index == 0:
        # Form-encoded: the /query endpoint expects `q` as a form field, not a
        # raw body. Sending text/plain silently created nothing, so every
        # subsequent write failed and the run still reported a throughput number.
        created = post(
            f"{endpoints[0]}/query",
            urllib.parse.urlencode({"q": f"CREATE DATABASE {a.db}"}).encode(),
            content_type="application/x-www-form-urlencoded",
        )
        print(f"[bench] create database -> HTTP {created}", flush=True)
        if created not in (200, 204):
            print("[bench] FATAL: could not create database; aborting", flush=True)
            return 2

    # Prove one write lands before timing millions of them. Retried: with
    # several workers, only worker 0 creates the database.
    deadline = time.monotonic() + 60
    while True:
        probe = post(
            f"{endpoints[0]}/write?db={a.db}&rp=autogen&precision=ns",
            f"{a.measurement},host=probe value=1 1700000000000000000".encode(),
        )
        if probe == 204 or time.monotonic() > deadline:
            break
        time.sleep(1)
    print(f"[bench] probe write -> HTTP {probe}", flush=True)
    if probe != 204:
        print("[bench] FATAL: probe write rejected; aborting", flush=True)
        return 3

    if a.phase in ("seed", "all"):
        # Seed: establish cardinality. Untimed.
        seed_s, seed_err, _ = run_phase(endpoints, a.db, a.measurement, 0, a.series,
                                     a.batch, a.concurrency, 1_700_000_000,
                                     workers=a.workers, worker_index=a.worker_index)
        print(f"[bench] seed complete in {seed_s:.1f}s (errors={seed_err})", flush=True)

    if a.phase in ("measure", "all"):
        # Measure: rewrite the seeded series, so this times steady-state
        # ingest against the split layout rather than first-touch series
        # creation. (It used to write series 0..measure_points, i.e. mostly
        # NEW series whenever measure_points > series.)
        dur, errs, retries = run_phase(endpoints, a.db, a.measurement, 0, a.measure_points,
                              a.batch, a.concurrency, 1_800_000_000, series=a.series,
                              workers=a.workers, worker_index=a.worker_index)
        mine = len(range(a.worker_index, -(-a.measure_points // a.batch), a.workers)) * a.batch
        pps = int(mine / dur) if dur > 0 else 0
        # Per-worker line; the sweep sums points and takes the slowest
        # duration across workers.
        print(f"[bench] worker={a.worker_index}/{a.workers} points={mine} "
              f"duration={dur:.3f}s errors={errs} retries={retries}", flush=True)
        print(f"[bench] nodes={len(endpoints)} points={mine} "
              f"duration={dur:.1f}s errors={errs}", flush=True)
        print(f"[bench] points_per_sec={pps}", flush=True)
        for k, n in sorted(ERROR_KINDS.items(), key=lambda kv: -kv[1])[:3]:
            print(f"[bench] error x{n}: {k}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
