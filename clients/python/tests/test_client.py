"""Client behaviour against a small fake server."""

import gzip
import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from hyperbytedb import (
    AuthError,
    HyperbyteDBClient,
    Point,
    QueryError,
    ServerError,
    WriteError,
)


class Fake:
    """Scripted responses for the next requests, and a record of them."""

    def __init__(self):
        self.script = []  # (status, headers, body) per request; default 204
        self.requests = []

    def start(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _handle(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                if self.headers.get("Content-Encoding") == "gzip":
                    body = gzip.decompress(body)
                fake.requests.append((self.command, self.path, dict(self.headers), body))
                status, headers, payload = fake.script.pop(0) if fake.script else (204, {}, b"")
                self.send_response(status)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = do_POST = _handle

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        return f"http://127.0.0.1:{self.server.server_port}"


@pytest.fixture
def fake():
    f = Fake()
    url = f.start()
    yield f, url
    f.server.shutdown()


def client(url, **kw):
    kw.setdefault("backoff", 0.01)
    return HyperbyteDBClient(url, **kw)


def test_write_batches_gzips_and_authenticates(fake):
    f, url = fake
    db = client(url, username="u", password="p", batch_points=2)
    n = db.write([Point("m", {"v": float(i)}, {"h": "a"}, time=i) for i in range(5)], "mydb", precision="s")
    assert n == 5
    assert len(f.requests) == 3, "5 points in batches of 2"
    method, path, headers, body = f.requests[0]
    assert method == "POST"
    q = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
    assert q == {"db": ["mydb"], "precision": ["s"]}
    assert headers["Authorization"] == "Basic dTpw"
    assert headers["Content-Encoding"] == "gzip"
    assert body.decode() == "m,h=a v=0.0 0\nm,h=a v=1.0 1"


def test_retries_503_honouring_retry_after(fake):
    f, url = fake
    f.script = [(503, {"Retry-After": "0"}, b"busy"), (503, {}, b"busy"), (204, {}, b"")]
    assert client(url).write("m v=1", "db") == 1
    assert len(f.requests) == 3


def test_gives_up_after_the_last_retry(fake):
    f, url = fake
    f.script = [(503, {}, b'{"error":"write capacity exhausted"}')] * 3
    with pytest.raises(ServerError) as e:
        client(url, retries=2).write("m v=1", "db")
    assert e.value.status == 503 and "capacity" in str(e.value)


def test_413_splits_the_batch(fake):
    f, url = fake
    f.script = [(413, {}, b"too big"), (204, {}, b""), (204, {}, b"")]
    assert client(url).write(["m v=1", "m v=2", "m v=3", "m v=4"], "db") == 4
    bodies = [r[3].decode() for r in f.requests]
    assert bodies == ["m v=1\nm v=2\nm v=3\nm v=4", "m v=1\nm v=2", "m v=3\nm v=4"]


def test_bad_writes_and_auth_are_not_retried(fake):
    f, url = fake
    f.script = [(400, {}, b'{"error":"field type conflict"}')]
    with pytest.raises(WriteError, match="field type conflict"):
        client(url).write("m v=1", "db")
    f.script = [(401, {}, b"authorization failed")]
    with pytest.raises(AuthError):
        client(url).write("m v=1", "db")
    assert len(f.requests) == 2


def test_query_results_and_errors(fake):
    f, url = fake
    doc = {
        "results": [
            {
                "statement_id": 0,
                "series": [
                    {"name": "cpu", "tags": {"host": "a"}, "columns": ["time", "mean"], "values": [[1, 2.5]]}
                ],
            }
        ]
    }
    f.script = [(200, {"Content-Encoding": "gzip"}, gzip.compress(json.dumps(doc).encode()))]
    res = client(url).query("SELECT mean(v) FROM cpu GROUP BY host", "db")
    assert list(res[0].rows()) == [{"host": "a", "time": 1, "mean": 2.5}]
    method, _, headers, body = f.requests[0]
    form = urllib.parse.parse_qs(body.decode())
    assert method == "POST" and form["db"] == ["db"] and form["epoch"] == ["ms"]

    f.script = [(200, {}, json.dumps({"results": [{"statement_id": 0, "error": "measurement not found"}]}).encode())]
    with pytest.raises(QueryError, match="not found"):
        client(url).query("SELECT * FROM nope", "db")
    f.script = [(400, {}, b'{"error":"error parsing query: bad"}')]
    with pytest.raises(QueryError, match="parsing"):
        client(url).query("SELEC", "db")


def test_health_answers_503_without_retrying(fake):
    f, url = fake
    f.script = [(503, {"Content-Type": "application/json"}, b'{"status":"draining"}')]
    assert client(url, backoff=5).health() == {"status": "draining"}
    assert len(f.requests) == 1, "a 503 health answer is not retried"


def test_unreachable_server_raises_after_retries():
    from hyperbytedb import HyperbyteDBError

    with pytest.raises(HyperbyteDBError):
        client("http://127.0.0.1:1", retries=1).ping()
