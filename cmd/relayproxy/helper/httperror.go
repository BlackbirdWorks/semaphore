package helper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	"github.com/danielgtaylor/huma/v2"

	reqlog "github.com/blackbirdworks/semaphore/cmdhelpers/api/middleware"
	"github.com/blackbirdworks/semaphore/internal/protocol"
)

// HTTPError is an error that carries the status code of the response and the message sent to the client. The
// body sent for it is {"message": ...}, plus {"error": ...} when the relay proxy runs with the debug level.
type HTTPError struct {
	err     error
	cause   error
	Message string
	Code    int
}

// NewHTTPError returns an HTTPError with the given status code and message.
func NewHTTPError(code int, message string) *HTTPError {
	return &HTTPError{Code: code, Message: message}
}

// WrapHTTPError returns an HTTPError with the status code, the text of the status as message and err as the
// wrapped error (exposed in the body at the debug level only).
func WrapHTTPError(code int, err error) *HTTPError {
	return &HTTPError{Code: code, Message: http.StatusText(code), err: err}
}

// WithCause records the error that made the response, for [errors.Is] and [errors.As]: unlike the wrapped error of
// [WrapHTTPError], it is never sent in the body.
func (e *HTTPError) WithCause(cause error) *HTTPError {
	e.cause = cause

	return e
}

// StatusCode returns the status code of the response.
func (e *HTTPError) StatusCode() int { return e.Code }

// GetStatus makes HTTPError a huma.StatusError.
func (e *HTTPError) GetStatus() int { return e.Code }

// ContentType keeps the content type of the negotiation (huma defaults an error to application/problem+json).
func (*HTTPError) ContentType(ct string) string { return ct }

// Schema documents the error as the body it is sent as.
func (*HTTPError) Schema(r huma.Registry) *huma.Schema {
	return r.Schema(reflect.TypeFor[ErrorBody](), true, "ErrorBody")
}

// Unwrap returns the wrapped error.
func (e *HTTPError) Unwrap() error {
	if e.err != nil {
		return e.err
	}

	return e.cause
}

// Error implements the error interface.
func (e *HTTPError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = http.StatusText(e.Code)
	}

	if e.err == nil {
		return fmt.Sprintf("code=%d, message=%v", e.Code, msg)
	}

	return fmt.Sprintf("code=%d, message=%v, err=%v", e.Code, msg, e.err.Error())
}

// ErrorBody is the JSON body sent for an error.
type ErrorBody struct {
	Message string `example:"Not Found"                                  json:"message"`
	Error   string `example:"the wrapped error, only at the debug level" json:"error,omitempty"`
}

// StatusError is the error of a status that has no HTTPError: the body is the text of the status.
type StatusError int

// Error returns the text of the status.
func (s StatusError) Error() string { return http.StatusText(int(s)) }

// StatusCode returns the status code.
func (s StatusError) StatusCode() int { return int(s) }

// Predefined errors of the statuses used without a message of their own. Their message is the text of the status.
var (
	ErrUnauthorized = StatusError(http.StatusUnauthorized)
	ErrNotFound     = StatusError(http.StatusNotFound)
)

// ErrorResponse returns the status code and the body to send to the client for err: the status of the error
// when it has one (500 otherwise) and its message. exposeError adds the wrapped error to the body.
func ErrorResponse(err error, exposeError bool) (int, ErrorBody) {
	code := http.StatusInternalServerError

	var sc interface{ StatusCode() int }
	if errors.As(err, &sc) {
		if c := sc.StatusCode(); c != 0 {
			code = c
		}
	}

	if httpErr, ok := errors.AsType[*HTTPError](err); ok {
		body := ErrorBody{Message: httpErr.Message}
		if body.Message == "" {
			body.Message = http.StatusText(code)
		}

		if exposeError && httpErr.err != nil {
			body.Error = httpErr.err.Error()
		}

		return code, body
	}

	body := ErrorBody{Message: http.StatusText(code)}
	if exposeError {
		body.Error = err.Error()
	}

	return code, body
}

// WriteJSON sends v as the JSON body of the response, with the status code.
func WriteJSON(w http.ResponseWriter, code int, v any) error {
	w.Header().Set(protocol.HeaderContentType, protocol.MIMEApplicationJSON)
	w.WriteHeader(code)

	err := json.NewEncoder(w).Encode(v)
	if err != nil {
		return fmt.Errorf("encoding the JSON response: %w", err)
	}

	return nil
}

// WriteError sends the error as the response. It sends nothing for a HEAD request but the status.
func WriteError(w http.ResponseWriter, r *http.Request, err error, exposeError bool) {
	code, body := ErrorResponse(err, exposeError)
	h := w.Header()
	// the representation headers of the response that failed do not describe the error.
	h.Del("Content-Encoding")
	h.Del("Content-Length")
	h.Del("Content-Disposition")

	if r.Method == http.MethodHead {
		w.WriteHeader(code)
		// commit the response now: the router must not add a body of its own.
		_ = http.NewResponseController(w).Flush()

		return
	}

	_ = WriteJSON(w, code, body)
}

// Handle makes a handler that returns the error to send a [http.Handler]: the error is sent as the response, and
// kept for the access log.
func Handle(h func(http.ResponseWriter, *http.Request) error, exposeError bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := h(w, r)
		if err != nil {
			reqlog.RecordError(r.Context(), err)

			// a client that is gone (its request was cancelled) receives no error: writing one would only make the
			// access log of the request an error line for a failure that nobody saw
			if errors.Is(r.Context().Err(), context.Canceled) {
				return
			}

			WriteError(w, r, err, exposeError)
		}
	})
}
