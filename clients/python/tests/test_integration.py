"""Against a real HyperbyteDB: ``HYPERBYTEDB_URL`` (and, with auth on,
``HYPERBYTEDB_USER`` / ``HYPERBYTEDB_PASSWORD`` for an admin)."""

import os
import time
import uuid

import pytest

from hyperbytedb import AuthError, HyperbyteDBClient, Point, QueryError

URL = os.environ.get("HYPERBYTEDB_URL")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not URL, reason="HYPERBYTEDB_URL not set"),
]


@pytest.fixture
def db():
    client = HyperbyteDBClient(
        URL,
        username=os.environ.get("HYPERBYTEDB_USER"),
        password=os.environ.get("HYPERBYTEDB_PASSWORD"),
    )
    name = f"pyclient_{uuid.uuid4().hex[:8]}"
    client.create_database(name)
    yield client, name
    client.drop_database(name)


def wait_rows(client, q, database, n, timeout=15.0):
    """Rows reach queries at the next flush; wait for them."""
    deadline = time.time() + timeout
    while True:
        rows = list(client.query(q, database)[0].rows())
        if len(rows) >= n or time.time() > deadline:
            return rows
        time.sleep(0.5)


def test_ping_and_health():
    client = HyperbyteDBClient(URL)
    assert client.ping().startswith("HyperbyteDB")
    assert "status" in client.health() or client.health()


def test_round_trip_with_awkward_names_and_every_type(db):
    client, name = db
    base = 1_700_000_000_000
    points = [
        Point(
            "disk usage",
            {"used": 12.5 + i, "inodes": 100 + i, "ok": True, "mount": 'C:\\ "data"'},
            {"host": "web 1,a=b", "service": "Filesystem /"},
            time=base + i * 1000,
        )
        for i in range(3)
    ]
    assert client.write(points, name, precision="ms") == 3
    rows = wait_rows(client, 'SELECT * FROM "disk usage" ORDER BY time', name, 3)
    assert len(rows) == 3
    assert rows[0]["time"] == base
    assert rows[0]["host"] == "web 1,a=b"
    assert rows[0]["service"] == "Filesystem /"
    assert rows[0]["used"] == 12.5 and rows[2]["inodes"] == 102
    assert rows[0]["mount"] == 'C:\\ "data"'


@pytest.mark.xfail(strict=True, reason="server returns boolean fields as 1/0 (stored as UInt8); InfluxDB returns true/false")
def test_boolean_fields_come_back_as_booleans(db):
    client, name = db
    client.write([Point("flags", {"ok": True}, time=1_700_000_000)], name, precision="s")
    rows = wait_rows(client, "SELECT ok FROM flags", name, 1)
    assert rows[0]["ok"] is True


def test_queries_by_tag_with_epoch_and_bound_params(db):
    client, name = db
    lines = [f"cpu,host=h{h} usage={h * 10 + i}.0 {1_700_000_000 + i}" for h in range(3) for i in range(4)]
    assert client.write(lines, name, precision="s") == 12
    wait_rows(client, "SELECT * FROM cpu", name, 12)
    res = client.query("SELECT max(usage) FROM cpu GROUP BY host", name, epoch="s")
    got = {s.tags["host"]: s.values[0][1] for s in res[0].series}
    assert got == {"h0": 3.0, "h1": 13.0, "h2": 23.0}
    res = client.query("SELECT count(usage) FROM cpu WHERE host = $h", name, params={"h": "h1"})
    assert res[0].series[0].values[0][1] == 4


def test_errors_are_raised(db):
    client, name = db
    with pytest.raises(QueryError):
        client.query("SELEKT nonsense", name)


@pytest.mark.skipif(not os.environ.get("HYPERBYTEDB_USER"), reason="auth not on")
def test_wrong_password_is_refused():
    with pytest.raises(AuthError):
        HyperbyteDBClient(URL, username=os.environ["HYPERBYTEDB_USER"], password="wrong").databases()
