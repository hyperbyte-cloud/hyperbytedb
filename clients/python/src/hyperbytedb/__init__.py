"""Python client for HyperbyteDB (InfluxDB v1 API).

    from hyperbytedb import HyperbyteDBClient, Point

    with HyperbyteDBClient("http://localhost:8086", username="u", password="p") as db:
        db.write([Point("cpu", {"usage": 12.5}, {"host": "a"})], "telegraf")
        for row in db.query('SELECT last("usage") FROM "cpu" GROUP BY "host"', "telegraf")[0].rows():
            print(row)
"""

__version__ = "0.9.3b0"

from .client import HyperbyteDBClient, Result, Series  # noqa: E402
from .errors import (  # noqa: E402
    AuthError,
    HyperbyteDBError,
    QueryError,
    ServerError,
    WriteError,
)
from .line_protocol import Point, encode, encode_point  # noqa: E402

__all__ = [
    "HyperbyteDBClient",
    "Point",
    "Result",
    "Series",
    "encode",
    "encode_point",
    "HyperbyteDBError",
    "ServerError",
    "AuthError",
    "WriteError",
    "QueryError",
    "__version__",
]
