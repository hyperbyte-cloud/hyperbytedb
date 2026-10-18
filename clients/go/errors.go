package hyperbytedb

import "fmt"

// ServerError is an HTTP error status after the last retry (5xx, 429, ...).
// AuthError and WriteError are the kinds a caller usually handles apart; use
// errors.As to tell them apart.
type ServerError struct {
	Status  int
	Message string
}

func (e *ServerError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("hyperbytedb: HTTP %d", e.Status)
	}
	return fmt.Sprintf("hyperbytedb: HTTP %d: %s", e.Status, e.Message)
}

// AuthError is a 401 or 403: missing or wrong credentials, or no privilege.
type AuthError struct{ ServerError }

// WriteError is a write the server refused (400: bad line protocol, a field
// type conflict, a cardinality limit, ...). It is not retried. Batches before
// the refused one have been written.
type WriteError struct{ ServerError }

// QueryError is a statement's error, as the server returned it.
type QueryError struct{ Message string }

func (e *QueryError) Error() string { return "hyperbytedb: query: " + e.Message }
