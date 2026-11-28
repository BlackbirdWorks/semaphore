package middleware

import (
	"bytes"
	"context"
	"crypto/sha1" //nolint:gosec // the ETag is a content fingerprint, not a security feature
	"encoding/hex"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/danielgtaylor/huma/v2"
)

// etagBuffer keeps the status and the body of a response until its ETag is known. It is the core of the two entry
// points: [ETag] for a [http.Handler], [ETagHuma] for a huma operation.
type etagBuffer struct {
	hash   hash.Hash
	buf    bytes.Buffer
	len    int
	status int
}

//nolint:gosec // G401: SHA-1 is used as an HTTP ETag content fingerprint, not for security
func newETagBuffer() *etagBuffer { return &etagBuffer{hash: sha1.New()} }

func (b *etagBuffer) Write(data []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}

	n, _ := b.buf.Write(data)
	_, _ = b.hash.Write(data)
	b.len += n

	return n, nil
}

func (b *etagBuffer) setStatus(status int) { b.status = status }

// answer returns the ETag of the response, and whether it has to be sent: a 2xx response with a body gets one,
// and a request that carries it in If-None-Match gets a 304 instead of the body.
func (b *etagBuffer) answer(ifNoneMatch []string) (string, bool) {
	if b.status/100 != 2 || b.status == http.StatusNoContent || b.buf.Len() == 0 {
		return "", false
	}

	etag := `"` + strconv.Itoa(b.len) + "-" + hex.EncodeToString(b.hash.Sum(nil)) + `"`

	return etag, ETagMatchesAny(ifNoneMatch, etag)
}

// ETagMatchesAny is [ETagMatches] for the lines of the If-None-Match header of a request: several lines are one
// comma separated list (RFC 9110 section 5.3).
func ETagMatchesAny(lines []string, etag string) bool {
	for _, line := range lines {
		if ETagMatches(line, etag) {
			return true
		}
	}

	return false
}

// ETagMatches reports whether an If-None-Match header value matches the entity tag, as RFC 9110 section 13.1.2
// defines it: the value is `*`, or a comma separated list of entity tags that is compared to the tag with the weak
// comparison (the `W/` prefix is ignored on both sides).
func ETagMatches(ifNoneMatch, etag string) bool {
	etag = strings.TrimPrefix(etag, "W/")

	for rest := ifNoneMatch; rest != ""; {
		var item string

		item, rest, _ = strings.Cut(rest, ",")
		item = strings.TrimPrefix(strings.TrimSpace(item), "W/")

		if item == "*" || item == etag {
			return true
		}
	}

	return false
}

// ETag adds a strong ETag to the 2xx responses with a body, and answers 304 when the request carries it in
// If-None-Match. The body is buffered to be hashed: only the routes that serve a small document use it.
func ETag() Func {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			bufWriter := &etagWriter{ResponseWriter: w, etagBuffer: newETagBuffer()}
			next.ServeHTTP(bufWriter, r)

			h := w.Header()
			if h.Get("ETag") == "" {
				if etag, notModified := bufWriter.answer(r.Header.Values("If-None-Match")); etag != "" {
					h.Set("ETag", etag)

					if notModified {
						w.WriteHeader(http.StatusNotModified)

						return
					}
				}
			}

			if bufWriter.status != 0 {
				w.WriteHeader(bufWriter.status)
				_, _ = w.Write(bufWriter.buf.Bytes())
			}
		})
	}
}

// etagWriter hands the status and the body of the response to the buffer.
type etagWriter struct {
	http.ResponseWriter
	*etagBuffer
}

func (w *etagWriter) WriteHeader(status int) { w.setStatus(status) }

func (w *etagWriter) Write(p []byte) (int, error) { return w.etagBuffer.Write(p) }

// Flush does nothing: the response is sent when the handler returns.
func (*etagWriter) Flush() {}

// Unwrap lets [http.ResponseController] reach the underlying writer.
func (w *etagWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// etagContext is the huma context that downstream handlers see: its status and its body go to the buffer.
// hctx lets etagContext embed the huma context without the field shadowing its Context method.
type hctx = huma.Context

type etagContext struct {
	hctx
	*etagBuffer
}

func (c *etagContext) SetStatus(code int) { c.setStatus(code) }

func (c *etagContext) Status() int { return c.status }

func (c *etagContext) BodyWriter() io.Writer { return c.etagBuffer }

// WithContext keeps the buffering for the contexts derived from this one.
//
//nolint:ireturn // the signature is fixed by huma.Context
func (c *etagContext) WithContext(ctx context.Context) huma.Context {
	return &etagContext{hctx: huma.WithContext(c.hctx, ctx), etagBuffer: c.etagBuffer}
}

// ETagHuma is [ETag] for a huma operation, register it in the middleware of the operation.
func ETagHuma() func(huma.Context, func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		etagCtx := &etagContext{hctx: ctx, etagBuffer: newETagBuffer()}
		next(etagCtx)

		if etag, notModified := etagCtx.answer(requestLines(ctx, "If-None-Match")); etag != "" {
			ctx.SetHeader("ETag", etag)

			if notModified {
				ctx.SetStatus(http.StatusNotModified)

				return
			}
		}

		if etagCtx.status != 0 {
			ctx.SetStatus(etagCtx.status)
			_, _ = ctx.BodyWriter().Write(etagCtx.buf.Bytes())
		}
	}
}

// requestLines returns every line of a header of the request.
func requestLines(ctx huma.Context, name string) []string {
	var lines []string

	ctx.EachHeader(func(key, value string) {
		if strings.EqualFold(key, name) {
			lines = append(lines, value)
		}
	})

	return lines
}
