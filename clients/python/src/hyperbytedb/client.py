"""HTTP client for HyperbyteDB's InfluxDB v1 API (`/write`, `/query`)."""

from __future__ import annotations

import base64
import gzip
import json
import random
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Union

from . import __version__
from .errors import AuthError, HyperbyteDBError, QueryError, ServerError, WriteError
from .line_protocol import Point, encode, encode_point

# Statuses worth another attempt: rate limited, overloaded (HyperbyteDB
# answers 503 with Retry-After when write admission is full), or a server or
# gateway error that may pass.
RETRYABLE = {429, 500, 502, 503, 504}


@dataclass
class Series:
    name: str
    columns: List[str]
    values: List[List[Any]]
    tags: Dict[str, str] = field(default_factory=dict)

    def rows(self) -> Iterator[Dict[str, Any]]:
        """Each row as a dict of column to value, with the series' tags."""
        for v in self.values:
            row = dict(self.tags)
            row.update(zip(self.columns, v))
            yield row


@dataclass
class Result:
    """One statement's result."""

    statement_id: int
    series: List[Series]
    error: Optional[str] = None

    def rows(self) -> Iterator[Dict[str, Any]]:
        for s in self.series:
            yield from s.rows()


class HyperbyteDBClient:
    """A HyperbyteDB connection.

    ``url`` is a node, or a load balancer / Kubernetes Service in front of
    the cluster (any node accepts any write or query). Credentials go in an
    ``Authorization: Basic`` header, never the URL.

    Writes are batched (``batch_points`` points or ``batch_bytes`` bytes per
    request, whichever comes first), gzipped, and retried with backoff on
    connection errors, 429 and 5xx, honouring ``Retry-After``. A batch the
    server refuses as too large (413) is split in two and retried.
    """

    def __init__(
        self,
        url: str = "http://localhost:8086",
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 30.0,
        retries: int = 5,
        backoff: float = 0.5,
        max_backoff: float = 30.0,
        gzip_writes: bool = True,
        batch_points: int = 5_000,
        batch_bytes: int = 4 * 1024 * 1024,
        verify_tls: bool = True,
        ca_file: Optional[str] = None,
    ) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.max_backoff = max_backoff
        self.gzip_writes = gzip_writes
        self.batch_points = max(1, batch_points)
        self.batch_bytes = max(1, batch_bytes)
        self._headers = {"User-Agent": f"hyperbytedb-python/{__version__}"}
        if username is not None:
            token = base64.b64encode(f"{username}:{password or ''}".encode()).decode()
            self._headers["Authorization"] = f"Basic {token}"
        self._ssl: Optional[ssl.SSLContext] = None
        if self.url.startswith("https://"):
            self._ssl = ssl.create_default_context(cafile=ca_file)
            if not verify_tls:
                self._ssl.check_hostname = False
                self._ssl.verify_mode = ssl.CERT_NONE

    # -- context manager (no pooled state to close; for symmetry) ----------
    def __enter__(self) -> "HyperbyteDBClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        pass

    # -- HTTP --------------------------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Mapping[str, str]] = None,
        body: Optional[bytes] = None,
        headers: Optional[Mapping[str, str]] = None,
        answers: "frozenset[int]" = frozenset(),
    ) -> "tuple[int, Mapping[str, str], bytes]":
        """One request with retries. Returns (status, headers, body) for any
        status the caller should handle; raises after the last retry.
        Statuses in ``answers`` are returned at once, not retried."""
        url = self.url + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        all_headers = dict(self._headers)
        all_headers["Accept-Encoding"] = "gzip"
        if headers:
            all_headers.update(headers)
        attempt = 0
        while True:
            req = urllib.request.Request(url, data=body, method=method, headers=all_headers)
            retry_after: Optional[float] = None
            try:
                with urllib.request.urlopen(req, timeout=self.timeout, context=self._ssl) as resp:
                    return resp.status, resp.headers, _read(resp)
            except urllib.error.HTTPError as e:
                payload = _read(e)
                if e.code not in RETRYABLE or e.code in answers or attempt >= self.retries:
                    return e.code, e.headers, payload
                retry_after = _retry_after(e.headers)
            except (urllib.error.URLError, OSError) as e:
                if attempt >= self.retries:
                    raise HyperbyteDBError(f"{method} {path}: {getattr(e, 'reason', e)}") from e
            attempt += 1
            delay = retry_after if retry_after is not None else self._backoff_delay(attempt)
            time.sleep(delay)

    def _backoff_delay(self, attempt: int) -> float:
        base = min(self.max_backoff, self.backoff * (2 ** (attempt - 1)))
        return base * (0.5 + random.random() / 2)

    # -- health ------------------------------------------------------------
    def ping(self) -> str:
        """The server's version, e.g. ``0.9.3-beta``; raises if down.

        From ``X-Hyperbytedb-Version``, else (servers before 0.9.3) from
        ``X-Influxdb-Version`` without its ``HyperbyteDB-`` prefix."""
        status, headers, payload = self._request("GET", "/ping")
        if status >= 300:
            raise ServerError(status, _error_text(payload))
        version = headers.get("X-Hyperbytedb-Version")
        if version:
            return version
        legacy = headers.get("X-Influxdb-Version", "")
        return legacy[len("HyperbyteDB-"):] if legacy.startswith("HyperbyteDB-") else legacy

    def health(self) -> Dict[str, Any]:
        """``/health``: the node's state, as JSON."""
        # 503 is the node's answer (draining, syncing), not a failure to retry.
        status, _, payload = self._request("GET", "/health", answers=frozenset({503}))
        if status >= 300 and status != 503:
            raise ServerError(status, _error_text(payload))
        return json.loads(payload or b"{}")

    # -- writes ------------------------------------------------------------
    def write(
        self,
        data: Union[Point, Iterable[Point], str, bytes, Iterable[str]],
        database: str,
        *,
        retention_policy: Optional[str] = None,
        precision: str = "ns",
    ) -> int:
        """Write points (or ready-made line protocol) and return how many
        lines were sent. Raises :class:`WriteError` on a refused batch;
        batches before it have been written."""
        lines = list(_as_lines(data, precision))
        params = {"db": database, "precision": precision}
        if retention_policy:
            params["rp"] = retention_policy
        sent = 0
        for batch in self._batches(lines):
            self._write_batch(batch, params)
            sent += len(batch)
        return sent

    def _batches(self, lines: Sequence[str]) -> Iterator[List[str]]:
        batch: List[str] = []
        size = 0
        for line in lines:
            if batch and (len(batch) >= self.batch_points or size + len(line) + 1 > self.batch_bytes):
                yield batch
                batch, size = [], 0
            batch.append(line)
            size += len(line) + 1
        if batch:
            yield batch

    def _write_batch(self, batch: List[str], params: Mapping[str, str]) -> None:
        body = "\n".join(batch).encode()
        headers = {"Content-Type": "text/plain; charset=utf-8"}
        if self.gzip_writes:
            body = gzip.compress(body, compresslevel=1)
            headers["Content-Encoding"] = "gzip"
        status, _, payload = self._request("POST", "/write", params, body, headers)
        if status in (200, 204):
            return
        message = _error_text(payload)
        if status == 413 and len(batch) > 1:
            half = len(batch) // 2
            self._write_batch(batch[:half], params)
            self._write_batch(batch[half:], params)
            return
        if status in (401, 403):
            raise AuthError(status, message)
        if status >= 500 or status == 429:
            raise ServerError(status, message)
        raise WriteError(status, message)

    # -- queries -----------------------------------------------------------
    def query(
        self,
        q: str,
        database: Optional[str] = None,
        *,
        epoch: Optional[str] = "ms",
        params: Optional[Mapping[str, Any]] = None,
        raise_errors: bool = True,
    ) -> List[Result]:
        """Run InfluxQL; one :class:`Result` per statement.

        Times come back as integers in ``epoch`` units (``ns``, ``u``,
        ``ms``, ``s``), or RFC 3339 strings with ``epoch=None``. ``params``
        binds ``$name`` placeholders. A statement's error raises
        :class:`QueryError` unless ``raise_errors`` is false.
        """
        form: Dict[str, str] = {"q": q}
        if database:
            form["db"] = database
        if epoch:
            form["epoch"] = epoch
        if params:
            form["params"] = json.dumps(params)
        status, _, payload = self._request(
            "POST",
            "/query",
            body=urllib.parse.urlencode(form).encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if status in (401, 403):
            raise AuthError(status, _error_text(payload))
        if status >= 300 and not payload:
            raise ServerError(status, "")
        doc = json.loads(payload or b"{}")
        if "error" in doc and "results" not in doc:
            if status >= 500:
                raise ServerError(status, doc["error"])
            raise QueryError(doc["error"])
        results = [
            Result(
                statement_id=r.get("statement_id", i),
                series=[
                    Series(
                        name=s.get("name", ""),
                        columns=s.get("columns", []),
                        values=s.get("values", []),
                        tags=s.get("tags") or {},
                    )
                    for s in r.get("series", []) or []
                ],
                error=r.get("error"),
            )
            for i, r in enumerate(doc.get("results", []))
        ]
        if raise_errors:
            for r in results:
                if r.error:
                    raise QueryError(r.error)
        return results

    def create_database(self, name: str) -> None:
        self.query(f'CREATE DATABASE "{_quote_ident(name)}"', epoch=None)

    def drop_database(self, name: str) -> None:
        self.query(f'DROP DATABASE "{_quote_ident(name)}"', epoch=None)

    def databases(self) -> List[str]:
        return [row["name"] for row in self.query("SHOW DATABASES", epoch=None)[0].rows()]


def _as_lines(
    data: Union[Point, Iterable[Point], str, bytes, Iterable[str]], precision: str
) -> Iterator[str]:
    if isinstance(data, bytes):
        data = data.decode()
    if isinstance(data, str):
        yield from (line for line in data.splitlines() if line.strip())
        return
    if isinstance(data, Point):
        data = [data]
    for item in data:
        if isinstance(item, Point):
            line = encode_point(item, precision)
            if line is not None:
                yield line
        elif isinstance(item, str):
            if item.strip():
                yield item
        else:
            raise TypeError(f"expected Point or str, not {type(item).__name__}")


def _quote_ident(name: str) -> str:
    return name.replace("\\", "\\\\").replace('"', '\\"')


def _read(resp: Any) -> bytes:
    data = resp.read()
    if resp.headers.get("Content-Encoding", "").lower() == "gzip":
        data = gzip.decompress(data)
    return data


def _retry_after(headers: Mapping[str, str]) -> Optional[float]:
    value = headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _error_text(payload: bytes) -> str:
    if not payload:
        return ""
    try:
        doc = json.loads(payload)
    except ValueError:
        return payload.decode(errors="replace").strip()
    if isinstance(doc, dict) and "error" in doc:
        return str(doc["error"])
    return payload.decode(errors="replace").strip()


__all__ = ["HyperbyteDBClient", "Result", "Series", "Point", "encode"]
