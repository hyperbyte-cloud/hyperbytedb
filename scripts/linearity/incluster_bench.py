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
import argparse, queue, sys, threading, time, urllib.error, urllib.parse, urllib.request


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--endpoints", required=True, help="comma-separated base URLs")
    p.add_argument("--db", default="linearity")
    p.add_argument("--measurement", default="cpu")
    p.add_argument("--series", type=int, required=True, help="series to seed")
    p.add_argument("--measure-points", type=int, default=2_000_000)
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--concurrency", type=int, default=32)
    return p.parse_args()


def post(url, body, timeout=60, content_type="text/plain"):
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", content_type)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


def run_phase(endpoints, db, meas, start, count, batch, concurrency, ts_base):
    """Send `count` points as `batch`-sized writes, `concurrency` in flight."""
    work = queue.Queue()
    for b in range(0, count, batch):
        work.put(b)
    errors = [0]
    lock = threading.Lock()

    def worker(idx):
        ep = endpoints[idx % len(endpoints)]
        url = f"{ep}/write?db={db}&rp=autogen&precision=ns"
        local_err = 0
        while True:
            try:
                base = work.get_nowait()
            except queue.Empty:
                break
            lines = []
            for i in range(base, min(base + batch, count)):
                sid = start + i
                lines.append(f"{meas},host=s{sid} value=1 {ts_base + sid}000000000")
            code = post(url, ("\n".join(lines)).encode())
            if code != 204:
                local_err += 1
        with lock:
            errors[0] += local_err

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(concurrency)]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.monotonic() - t0, errors[0]


def main():
    a = parse_args()
    endpoints = [e.strip().rstrip("/") for e in a.endpoints.split(",") if e.strip()]
    print(f"[bench] endpoints={len(endpoints)} concurrency={a.concurrency} "
          f"series={a.series} measure_points={a.measure_points}", flush=True)

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

    # Prove one write lands before timing two million of them.
    probe = post(
        f"{endpoints[0]}/write?db={a.db}&rp=autogen&precision=ns",
        f"{a.measurement},host=probe value=1 1700000000000000000".encode(),
    )
    print(f"[bench] probe write -> HTTP {probe}", flush=True)
    if probe != 204:
        print("[bench] FATAL: probe write rejected; aborting", flush=True)
        return 3

    # Seed: establish cardinality. Untimed -- splitting is driven by heartbeat
    # and scheduler ticks, so nothing meaningful can be measured until the
    # regions it creates have settled.
    seed_s, seed_err = run_phase(endpoints, a.db, a.measurement, 0, a.series,
                                 a.batch, a.concurrency, 1_700_000_000)
    print(f"[bench] seed complete in {seed_s:.1f}s (errors={seed_err})", flush=True)

    # Measure: rewrite existing series, so this times steady-state ingest
    # against the converged layout rather than first-touch series creation.
    dur, errs = run_phase(endpoints, a.db, a.measurement, 0, a.measure_points,
                          a.batch, a.concurrency, 1_800_000_000)
    pps = int(a.measure_points / dur) if dur > 0 else 0
    print(f"[bench] nodes={len(endpoints)} points={a.measure_points} "
          f"duration={dur:.1f}s errors={errs}", flush=True)
    print(f"[bench] points_per_sec={pps}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
