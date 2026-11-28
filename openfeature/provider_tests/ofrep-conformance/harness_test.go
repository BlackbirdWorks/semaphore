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
	"strings"
	"sync"
	"testing"
	"time"

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

type response struct {
	header http.Header
	body   []byte
	status int
}

// post sends a JSON POST request and returns what the relay answered.
func (h *harness) post(t *testing.T, rawURL string, headers map[string]string, body string) (int, http.Header, []byte) {
	t.Helper()

	res := h.send(t, &request{method: http.MethodPost, url: rawURL, headers: headers, body: body})

	return res.status, res.header, res.body
}

// send sends the request, checks the response against the pinned document when the document defines it, and
// records what it exercised.
func (h *harness) send(t *testing.T, call *request) response {
	t.Helper()

	var reader io.Reader = http.NoBody
	if call.body != "" {
		reader = strings.NewReader(call.body)
	}

	req, err := http.NewRequestWithContext(t.Context(), call.method, call.url, reader)
	require.NoError(t, err)

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

	resp, err := (&http.Client{Timeout: httpClientTimeout}).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	h.observe(t, req, resp.StatusCode, resp.Header, respBody)

	return response{status: resp.StatusCode, header: resp.Header, body: respBody}
}

// postRaw reads the raw HTTP/1.1 wire bytes after the header block before net/http strips 304 bodies.
func (h *harness) postRaw(
	t *testing.T,
	rawURL string,
	headers map[string]string,
	body string,
) (int, http.Header, []byte) {
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
	h.observe(t, req, resp.StatusCode, resp.Header, wireBody)

	return resp.StatusCode, resp.Header, wireBody
}

// observe validates the response against the pinned document, when the document defines it, and records the
// requirements it covers.
func (h *harness) observe(t *testing.T, req *http.Request, status int, hdr http.Header, body []byte) {
	t.Helper()

	path := req.URL.Path

	var template string

	switch {
	case path == bulkFlagsPath:
		template = bulkFlagsPath
	case strings.HasPrefix(path, singleFlagPrefix):
		template = singleFlagPath
	default:
		return
	}

	// the document defines the POST operations only: another method is no operation of the protocol
	if req.Method != http.MethodPost {
		return
	}

	require.True(t, h.spec.HasResponse(template, req.Method, status),
		"%s %s answered %d, a status the pinned document does not define for it: %s", req.Method, req.URL.Path,
		status, string(body))

	require.NoError(t, h.spec.ValidateResponse(template, req.Method, status, hdr, body),
		"%s %s -> %d: %s", req.Method, req.URL.Path, status, string(body))

	h.rec.add(h.spec.Observed(template, req.Method, &ofrepconformance.Exchange{
		ReqHeaders:    req.Header,
		RespHeaders:   hdr,
		Query:         req.URL.Query(),
		Body:          body,
		Status:        status,
		Authenticated: strings.HasPrefix(req.URL.String(), h.auth) || strings.HasPrefix(req.URL.String(), h.base),
	}))
}
