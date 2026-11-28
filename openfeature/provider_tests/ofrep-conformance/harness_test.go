package ofrepconformance_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ofrepconformance "github.com/blackbirdworks/semaphore/openfeature/provider_tests/ofrep-conformance"
)

const (
	httpClientTimeout = 5 * time.Second
	crlfPairLen       = 4
	singleFlagPrefix  = "/ofrep/v1/evaluate/flags/"
)

// recorder keeps the requirement identifiers that the cases exercised.
type recorder struct {
	seen map[string]struct{}
	mu   sync.Mutex
}

func (r *recorder) add(ids []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, id := range ids {
		r.seen[id] = struct{}{}
	}
}

func (r *recorder) has(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, ok := r.seen[id]

	return ok
}

// harness is what a conformance case needs: the pinned document, the relays under test and the recorder.
type harness struct {
	spec *ofrepconformance.Spec
	rec  *recorder
	// base requires the API key authorized_token, public requires none, auth is base with other settings.
	base, auth, public, admin, stream string
}

type request struct {
	headers map[string]string
	// repeated are header fields sent on several lines.
	repeated    map[string][]string
	method      string
	url         string
	body        string
	contentType string
}

// response is what the relay answered. It records a requirement of the pinned document only when a case asserts on it:
// the want methods assert and then claim what they asserted (a response that was merely returned claims nothing).
type response struct {
	header http.Header
	rec    *recorder
	body   []byte
	// observed are the requirements the exchange exercised, claimed one kind at a time by the assertions.
	observed []string
	status   int
}

// claim records the observed requirements of the kinds.
func (r *response) claim(kinds ...string) {
	var ids []string

	for _, id := range r.observed {
		if slices.Contains(kinds, ofrepconformance.KindOf(id)) {
			ids = append(ids, id)
		}
	}

	r.rec.add(ids)
}

// wantStatus asserts the status; the response, the request parameters and the security scheme of the exchange are then
// claimed.
func (r *response) wantStatus(t *testing.T, want int) {
	t.Helper()

	if assert.Equal(t, want, r.status, "body: %s", string(r.body)) {
		r.claim(ofrepconformance.KindResponse, ofrepconformance.KindParam, ofrepconformance.KindSecurity)
	}
}

// wantETag asserts the ETag of the response and claims the header.
func (r *response) wantETag(t *testing.T, want string) {
	t.Helper()

	if assert.Equal(t, want, r.header.Get("ETag"), "the ETag") {
		r.claimETag()
	}
}

// wantStrongETag asserts that the response has an ETag that is a quoted strong entity tag, and claims the header.
func (r *response) wantStrongETag(t *testing.T) {
	t.Helper()

	if assert.Regexp(t, `^"[^"]+"$`, r.header.Get("ETag"), "the ETag") {
		r.claimETag()
	}
}

func (r *response) claimETag() {
	var ids []string

	suffix := " " + http.CanonicalHeaderKey("ETag")

	for _, id := range r.observed {
		if ofrepconformance.KindOf(id) == ofrepconformance.KindHeader && strings.HasSuffix(id, suffix) {
			ids = append(ids, id)
		}
	}

	r.rec.add(ids)
}

// bodyAsserted claims the schema and the value branches of the body: call it after asserting on the content of the
// body.
func (r *response) bodyAsserted() {
	r.claim(ofrepconformance.KindSchema, ofrepconformance.KindBranch)
}

// post sends a JSON POST request and returns what the relay answered.
func (h *harness) post(t *testing.T, rawURL string, headers map[string]string, body string) *response {
	t.Helper()

	return h.send(t, &request{method: http.MethodPost, url: rawURL, headers: headers, body: body})
}

// newHTTPRequest builds the HTTP request of a call.
func newHTTPRequest(ctx context.Context, call *request) (*http.Request, error) {
	var reader io.Reader = http.NoBody
	if call.body != "" {
		reader = strings.NewReader(call.body)
	}

	req, err := http.NewRequestWithContext(ctx, call.method, call.url, reader)
	if err != nil {
		return nil, fmt.Errorf("building the request: %w", err)
	}

	switch {
	case call.contentType != "":
		req.Header.Set("Content-Type", call.contentType)
	case call.body != "":
		req.Header.Set("Content-Type", contentTypeJSON)
	}

	for key, val := range call.headers {
		req.Header.Set(key, val)
	}

	for key, lines := range call.repeated {
		for _, line := range lines {
			req.Header.Add(key, line)
		}
	}

	return req, nil
}

// send sends the request and checks the response against the pinned document when the document defines it.
func (h *harness) send(t *testing.T, call *request) *response {
	t.Helper()

	req, err := newHTTPRequest(t.Context(), call)
	require.NoError(t, err)

	resp, err := (&http.Client{Timeout: httpClientTimeout}).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	observed := h.observe(t, req, resp.StatusCode, resp.Header, respBody)

	return &response{status: resp.StatusCode, header: resp.Header, body: respBody, observed: observed, rec: h.rec}
}

// postRaw reads the raw HTTP/1.1 wire bytes after the header block before net/http strips 304 bodies.
func (h *harness) postRaw(
	t *testing.T,
	rawURL string,
	headers map[string]string,
	body string,
) *response {
	t.Helper()

	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), httpClientTimeout)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", parsed.Host)
	require.NoError(t, err)

	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	defer func() { _ = conn.Close() }()

	var reqBuf bytes.Buffer
	fmt.Fprintf(&reqBuf, "POST %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n", parsed.RequestURI(), parsed.Host)

	for key, val := range headers {
		fmt.Fprintf(&reqBuf, "%s: %s\r\n", key, val)
	}

	if body != "" {
		fmt.Fprintf(&reqBuf, "Content-Type: %s\r\nContent-Length: %d\r\n", contentTypeJSON, len(body))
	}

	reqBuf.WriteString("\r\n")
	reqBuf.WriteString(body)

	_, err = conn.Write(reqBuf.Bytes())
	require.NoError(t, err)

	raw, err := io.ReadAll(conn)
	require.NoError(t, err)

	headerEnd := bytes.Index(raw, []byte("\r\n\r\n"))
	require.NotEqual(t, -1, headerEnd, "malformed HTTP/1.1 response: %q", string(raw))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, http.NoBody)
	require.NoError(t, err)

	for key, val := range headers {
		req.Header.Set(key, val)
	}

	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(raw[:headerEnd+crlfPairLen])), req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	wireBody := raw[headerEnd+crlfPairLen:]
	observed := h.observe(t, req, resp.StatusCode, resp.Header, wireBody)

	return &response{
		status: resp.StatusCode, header: resp.Header, body: wireBody, observed: observed, rec: h.rec,
	}
}

// observe validates the response against the pinned document, when the document defines it, and returns the
// requirements it exercised: they are recorded when a case asserts on them.
func (h *harness) observe(t *testing.T, req *http.Request, status int, hdr http.Header, body []byte) []string {
	t.Helper()

	path := req.URL.Path

	var template string

	switch {
	case path == bulkFlagsPath:
		template = bulkFlagsPath
	case strings.HasPrefix(path, singleFlagPrefix):
		template = singleFlagPath
	default:
		return nil
	}

	// the document defines the POST operations only: another method is no operation of the protocol
	if req.Method != http.MethodPost {
		return nil
	}

	require.True(t, h.spec.HasResponse(template, req.Method, status),
		"%s %s answered %d, a status the pinned document does not define for it: %s", req.Method, req.URL.Path,
		status, string(body))

	require.NoError(t, h.spec.ValidateResponse(template, req.Method, status, hdr, body),
		"%s %s -> %d: %s", req.Method, req.URL.Path, status, string(body))

	return h.spec.Observed(template, req.Method, &ofrepconformance.Exchange{
		ReqHeaders:    req.Header,
		RespHeaders:   hdr,
		Query:         req.URL.Query(),
		Body:          body,
		Status:        status,
		Authenticated: strings.HasPrefix(req.URL.String(), h.auth) || strings.HasPrefix(req.URL.String(), h.base),
	})
}
