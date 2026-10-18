/** Base class; also thrown when the server cannot be reached. */
export class HyperbyteDBError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** An HTTP error status after the last retry (5xx, 429, ...). */
export class ServerError extends HyperbyteDBError {
  constructor(
    readonly status: number,
    readonly serverMessage: string,
  ) {
    super(serverMessage ? `HTTP ${status}: ${serverMessage}` : `HTTP ${status}`);
  }
}

/** 401 or 403: missing or wrong credentials, or no privilege. */
export class AuthError extends ServerError {}

/**
 * A write the server refused (400: bad line protocol, a field type conflict,
 * a cardinality limit, ...). Not retried. Batches before it have been written.
 */
export class WriteError extends ServerError {}

/** A statement's error, as the server returned it. */
export class QueryError extends HyperbyteDBError {}
