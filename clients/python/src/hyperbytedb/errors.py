"""Errors raised by the client."""

from __future__ import annotations


class HyperbyteDBError(Exception):
    """Base class; also raised when the server cannot be reached."""


class ServerError(HyperbyteDBError):
    """An HTTP error status after the last retry (5xx, 429, ...)."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"HTTP {status}: {message}" if message else f"HTTP {status}")
        self.status = status
        self.message = message


class AuthError(ServerError):
    """401 or 403: missing or wrong credentials, or no privilege."""


class WriteError(ServerError):
    """A write the server refused (400: bad line protocol, a field type
    conflict, a cardinality limit, ...). Not retried."""


class QueryError(HyperbyteDBError):
    """A statement's error, as returned by the server."""
