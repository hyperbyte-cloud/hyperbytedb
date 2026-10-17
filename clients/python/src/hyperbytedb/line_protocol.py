"""InfluxDB line protocol, as HyperbyteDB parses it.

Escaping follows the server's own encoder: commas and spaces in measurements;
commas, equals signs and spaces in tag keys, tag values and field keys;
backslashes everywhere; quotes and backslashes in string fields. A tag with an
empty value is left out (line protocol cannot express it, and the server
rejects the whole request). Floats that are not finite are left out too:
line protocol has no NaN or infinity.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Mapping, Optional, Union

FieldValue = Union[float, int, bool, str]
Timestamp = Union[int, float, datetime]

PRECISIONS = {"ns": 1, "u": 1_000, "ms": 1_000_000, "s": 1_000_000_000}


@dataclass
class Point:
    """One point: a measurement, its tags, its fields and an optional time.

    ``time`` is a ``datetime`` (naive means UTC), or a number in the unit of
    the write's ``precision``. Without one the server stamps the point.
    Integers are written as integers (``1i``); use a float (``1.0``) for a
    float field, as a field keeps the type it was first written with.
    """

    measurement: str
    fields: Mapping[str, FieldValue]
    tags: Mapping[str, str] = field(default_factory=dict)
    time: Optional[Timestamp] = None


def _escape(s: str, chars: str) -> str:
    out = []
    for ch in s:
        if ch == "\\" or ch in chars:
            out.append("\\")
        if ch == "\n":
            # A newline ends the line; it has no escape.
            raise ValueError(f"newline in line protocol name or tag: {s!r}")
        out.append(ch)
    return "".join(out)


def _field_value(v: FieldValue) -> Optional[str]:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return f"{v}i"
    if isinstance(v, float):
        if not math.isfinite(v):
            return None
        return repr(v)
    if isinstance(v, str):
        if "\n" in v:
            # A newline ends the line even inside quotes; there is no escape.
            raise ValueError(f"newline in line protocol string field: {v!r}")
        return '"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"'
    raise TypeError(f"unsupported field value type: {type(v).__name__}")


def _timestamp(t: Timestamp, precision: str) -> int:
    if isinstance(t, datetime):
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        delta = t - datetime(1970, 1, 1, tzinfo=timezone.utc)
        nanos = (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000
        return nanos // PRECISIONS[precision]
    return int(t)


def encode_point(p: Point, precision: str = "ns") -> Optional[str]:
    """One line, or ``None`` when the point has no field left to write."""
    fields = []
    for k, v in sorted(p.fields.items()):
        if v is None:
            continue
        encoded = _field_value(v)
        if encoded is not None:
            fields.append(f"{_escape(k, ',= ')}={encoded}")
    if not fields:
        return None
    line = _escape(p.measurement, ", ")
    for k, v in sorted(p.tags.items()):
        if v is None or v == "":
            continue
        line += f",{_escape(k, ',= ')}={_escape(str(v), ',= ')}"
    line += " " + ",".join(fields)
    if p.time is not None:
        line += f" {_timestamp(p.time, precision)}"
    return line


def encode(points: Iterable[Point], precision: str = "ns") -> str:
    """Points as line protocol, one per line; points with no fields dropped."""
    if precision not in PRECISIONS:
        raise ValueError(f"precision must be one of {sorted(PRECISIONS)}, not {precision!r}")
    lines = (encode_point(p, precision) for p in points)
    return "\n".join(line for line in lines if line is not None)
