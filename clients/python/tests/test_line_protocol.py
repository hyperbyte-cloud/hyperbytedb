from datetime import datetime, timezone

import pytest

from hyperbytedb import Point, encode, encode_point


def test_escapes_like_the_server():
    p = Point(
        "cpu load,x",
        {"usage idle": 1.5, 'msg"': 'say "hi" \\ bye'},
        {"host name": "a,b=c", "empty": ""},
        time=123,
    )
    assert encode_point(p) == (
        r"cpu\ load\,x,host\ name=a\,b\=c "
        r'msg"="say \"hi\" \\ bye",usage\ idle=1.5 123'
    )


def test_field_types():
    line = encode_point(Point("m", {"f": 1.0, "i": 2, "b": True, "s": "x"}))
    assert line == 'm b=true,f=1.0,i=2i,s="x"'


def test_non_finite_floats_and_none_are_dropped():
    assert encode_point(Point("m", {"a": float("nan"), "b": float("inf"), "c": None, "d": 1.0})) == "m d=1.0"
    assert encode_point(Point("m", {"a": float("nan")})) is None
    assert encode([Point("m", {"a": float("nan")}), Point("m", {"v": 1.0})]) == "m v=1.0"


def test_datetime_times_follow_the_precision():
    t = datetime(2026, 10, 12, 12, 0, 0, 500_000, tzinfo=timezone.utc)
    ns = encode_point(Point("m", {"v": 1.0}, time=t)).split()[-1]
    ms = encode_point(Point("m", {"v": 1.0}, time=t), "ms").split()[-1]
    assert int(ns) == 1_791_806_400_500_000_000
    assert int(ms) == 1_791_806_400_500
    naive = encode_point(Point("m", {"v": 1.0}, time=t.replace(tzinfo=None)))
    assert naive.split()[-1] == ns


def test_newlines_are_refused():
    with pytest.raises(ValueError):
        encode_point(Point("m", {"v": 1.0}, {"host": "a\nb"}))


def test_bad_precision_is_refused():
    with pytest.raises(ValueError):
        encode([Point("m", {"v": 1.0})], "minutes")
