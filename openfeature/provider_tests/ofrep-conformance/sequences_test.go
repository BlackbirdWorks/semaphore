package ofrepconformance_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sequenceWait   = 30 * time.Second
	sequenceTick   = 100 * time.Millisecond
	eventBuffer    = 16
	sseDataPrefix  = "data: "
	sseStreamPath  = "/stream/v1/sse/flag/change"
	stepRequest    = "request"
	stepPublish    = "publish"
	stepConnect    = "connect"
	stepEvent      = "event"
	varETag        = "etag"
	etagPresent    = "present"
	etagSamePrefix = "same:"
	etagDiffPrefix = "differs:"
)

// sequenceFile is ../ofrep-scenarios/sequences.json: the language neutral sequences that need a store that changes.
type sequenceFile struct {
	Documents   map[string]string         `json:"documents"`
	Contexts    map[string]map[string]any `json:"contexts"`
	Description string                    `json:"description"`
	Sequences   []sequence                `json:"sequences"`
}

type sequence struct {
	Relays map[string]relayOptions `json:"relays"`
	Name   string                  `json:"name"`
	Source string                  `json:"source"`
	Basis  string                  `json:"basis"`
	Store  []flagSetSpec           `json:"store"`
	Steps  []step                  `json:"steps"`
}

type step struct {
	Query       map[string]string `json:"query"`
	Do          string            `json:"do"`
	Relay       string            `json:"relay"`
	Key         string            `json:"key"`
	Single      string            `json:"single"`
	Context     string            `json:"context"`
	IfNoneMatch string            `json:"ifNoneMatch"`
	Save        string            `json:"save"`
	Stream      string            `json:"stream"`
	Store       []flagSetSpec     `json:"store"`
	Expect      expectation       `json:"expect"`
	Eventually  bool              `json:"eventually"`
}

type expectation struct {
	Values       map[string]any            `json:"values"`
	Reasons      map[string]string         `json:"reasons"`
	FlagMetadata map[string]map[string]any `json:"flagMetadata"`
	ETag         string                    `json:"etag"`
	ErrorCode    string                    `json:"errorCode"`
	EventStreams string                    `json:"eventStreams"`
	Metadata     string                    `json:"metadata"`
	Absent       []string                  `json:"absent"`
	NoValue      []string                  `json:"noValue"`
	Status       int                       `json:"status"`
	EmptyBody    bool                      `json:"emptyBody"`
}

func loadSequences(t *testing.T) *sequenceFile {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("..", "ofrep-scenarios", "sequences.json"))
	require.NoError(t, err)

	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()

	var file sequenceFile

	require.NoError(t, dec.Decode(&file))
	require.NotEmpty(t, file.Sequences)

	for idx := range file.Sequences {
		assert.Contains(t, []string{"spec", "provider", "relay"}, file.Sequences[idx].Source, file.Sequences[idx].Name)
		assert.NotEmpty(t, file.Sequences[idx].Basis, file.Sequences[idx].Name)
	}

	return &file
}

// sequenceRun is the state of one running sequence: its store, its relays, the variables the steps saved and the
// streams they opened.
type sequenceRun struct {
	hns      *harness
	file     *sequenceFile
	store    *liveStore
	relays   map[string]string
	vars     map[string]map[string]string
	streams  map[string]*sseClient
	sequence *sequence
}

func conformanceSequences(t *testing.T, hns *harness) {
	t.Helper()

	file := loadSequences(t)

	for idx := range file.Sequences {
		seq := &file.Sequences[idx]

		t.Run(seq.Name, func(t *testing.T) {
			t.Parallel()

			run := &sequenceRun{
				hns: &harness{spec: hns.spec, rec: hns.rec}, file: file, sequence: seq,
				store: newLiveStore(t, file.Documents), relays: map[string]string{},
				vars: map[string]map[string]string{}, streams: map[string]*sseClient{},
			}
			run.store.publish(t, seq.Store)

			relays := seq.Relays
			if len(relays) == 0 {
				relays = map[string]relayOptions{"a": {}}
			}

			for _, name := range slices.Sorted(maps.Keys(relays)) {
				run.relays[name] = startLiveRelay(t, run.store, relays[name])
			}

			for idx := range seq.Steps {
				run.step(t, idx, &seq.Steps[idx])
			}
		})
	}
}

func (r *sequenceRun) step(t *testing.T, idx int, stp *step) {
	t.Helper()

	switch stp.Do {
	case stepPublish:
		r.store.publish(t, stp.Store)
	case stepRequest:
		r.request(t, idx, stp)
	case stepConnect:
		r.connect(t, stp)
	case stepEvent:
		r.event(t, stp)
	default:
		require.FailNowf(t, "unknown step", "step %d: %q", idx, stp.Do)
	}
}

// resolve replaces $name and $name.field by the saved variable.
func (r *sequenceRun) resolve(t *testing.T, text string) string {
	t.Helper()

	name, ok := strings.CutPrefix(text, "$")
	if !ok {
		return text
	}

	name, field, hasField := strings.Cut(name, ".")
	if !hasField {
		field = varETag
	}

	value, found := r.vars[name][field]
	require.True(t, found, "variable %s.%s was not saved", name, field)

	return value
}

func (r *sequenceRun) call(t *testing.T, stp *step) *request {
	t.Helper()

	base, ok := r.relays[stp.Relay]
	require.True(t, ok, "relay %q", stp.Relay)

	body, err := json.Marshal(map[string]any{"context": r.file.Contexts[stp.Context]})
	require.NoError(t, err)

	target := base + bulkFlagsPath
	if stp.Single != "" {
		target = base + singleFlagPrefix + url.PathEscape(stp.Single)
	}

	query := url.Values{}
	for key, value := range stp.Query {
		query.Set(key, r.resolve(t, value))
	}

	if len(query) > 0 {
		target += "?" + query.Encode()
	}

	headers := map[string]string{}
	if stp.Key != "" {
		headers[headerXAPIKey] = stp.Key
	}

	if stp.IfNoneMatch != "" {
		headers[headerIfNoneMatch] = r.resolve(t, stp.IfNoneMatch)
	}

	return &request{method: http.MethodPost, url: target, headers: headers, body: string(body)}
}

func (r *sequenceRun) request(t *testing.T, idx int, stp *step) {
	t.Helper()

	call := r.call(t, stp)

	if stp.Eventually {
		require.EventuallyWithT(t, func(coll *assert.CollectT) {
			polled, err := rawSend(t.Context(), call)
			if !assert.NoError(coll, err) {
				return
			}

			assert.Empty(coll, r.failures(polled, stp))
		}, sequenceWait, sequenceTick, "step %d: the relay does not serve what the step expects", idx)
	}

	res := r.hns.send(t, call)
	assert.Empty(t, r.failures(res, stp), "step %d of %q", idx, r.sequence.Name)
	r.claim(t, res, &stp.Expect)

	if stp.Save != "" {
		r.vars[stp.Save] = map[string]string{varETag: res.header.Get("ETag")}
	}
}

// claim records the requirements of the document that the expectation asserted.
func (r *sequenceRun) claim(t *testing.T, res *response, want *expectation) {
	t.Helper()

	res.wantStatus(t, want.Status)

	switch {
	case want.ETag == etagPresent:
		res.wantStrongETag(t)
	case strings.HasPrefix(want.ETag, etagSamePrefix):
		res.wantETag(t, r.vars[strings.TrimPrefix(want.ETag, etagSamePrefix)][varETag])
	}

	checksBody := len(want.Values)+len(want.Absent)+len(want.NoValue)+len(want.Reasons)+len(want.FlagMetadata) > 0

	if checksBody || want.ErrorCode != "" {
		res.bodyAsserted()
	}
}

// failures lists what the response does not have of what the step expects, empty when it has all of it.
func (r *sequenceRun) failures(res *response, stp *step) []string {
	want := &stp.Expect

	var out []string

	fail := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }

	if res.status != want.Status {
		fail("status %d, want %d: %s", res.status, want.Status, string(res.body))

		return out
	}

	if want.EmptyBody && len(res.body) > 0 {
		fail("the body is not empty: %s", string(res.body))
	}

	out = append(out, r.etagFailures(res, want)...)

	if len(res.body) == 0 {
		return out
	}

	var parsed map[string]any

	err := json.Unmarshal(res.body, &parsed)
	if err != nil {
		return append(out, "the body is not a JSON object: "+string(res.body))
	}

	items := map[string]map[string]any{}
	if stp.Single != "" {
		items[stp.Single] = parsed
	} else if flags, ok := parsed["flags"].([]any); ok {
		for _, raw := range flags {
			if item, isItem := raw.(map[string]any); isItem {
				if key, isKey := item["key"].(string); isKey {
					items[key] = item
				}
			}
		}
	}

	return append(out, itemFailures(parsed, items, want)...)
}

func (r *sequenceRun) etagFailures(res *response, want *expectation) []string {
	got := res.header.Get("ETag")

	switch {
	case want.ETag == "":
		return nil
	case want.ETag == etagPresent && got == "":
		return []string{"no ETag"}
	case strings.HasPrefix(want.ETag, etagSamePrefix):
		if want := r.vars[strings.TrimPrefix(want.ETag, etagSamePrefix)][varETag]; got != want {
			return []string{fmt.Sprintf("ETag %s, want %s", got, want)}
		}
	case strings.HasPrefix(want.ETag, etagDiffPrefix):
		if other := r.vars[strings.TrimPrefix(want.ETag, etagDiffPrefix)][varETag]; got == other || got == "" {
			return []string{fmt.Sprintf("ETag %q, want one that differs from %q", got, other)}
		}
	}

	return nil
}

func itemFailures(parsed map[string]any, items map[string]map[string]any, want *expectation) []string {
	var out []string

	fail := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }

	for _, key := range slices.Sorted(maps.Keys(want.Values)) {
		item, found := items[key]

		switch {
		case !found:
			fail("no flag %s", key)
		case !assert.ObjectsAreEqual(want.Values[key], item["value"]):
			fail("flag %s has the value %v, want %v", key, item["value"], want.Values[key])
		}
	}

	for _, key := range want.Absent {
		if _, found := items[key]; found {
			fail("the flag %s is in the response", key)
		}
	}

	for _, key := range want.NoValue {
		if item, found := items[key]; !found || item["value"] != nil {
			fail("the flag %s is missing or has a value", key)
		}
	}

	for key, reason := range want.Reasons {
		if got := items[key]["reason"]; got != reason {
			fail("flag %s has the reason %v, want %s", key, got, reason)
		}
	}

	if got := parsed["errorCode"]; want.ErrorCode != "" && got != want.ErrorCode {
		fail("errorCode %v, want %s", got, want.ErrorCode)
	}

	out = append(out, metadataFailures(parsed, items, want)...)

	return append(out, streamFailures(parsed, want)...)
}

// metadataFailures checks the metadata of the response and of its flags.
func metadataFailures(parsed map[string]any, items map[string]map[string]any, want *expectation) []string {
	var out []string

	if _, has := parsed["metadata"]; want.Metadata == "absent" && has {
		out = append(out, fmt.Sprintf("the response has metadata: %v", parsed["metadata"]))
	}

	for key, entries := range want.FlagMetadata {
		got, _ := items[key]["metadata"].(map[string]any)

		for name, value := range entries {
			if !assert.ObjectsAreEqual(value, got[name]) {
				out = append(out, fmt.Sprintf("flag %s metadata %s is %v, want %v", key, name, got[name], value))
			}
		}
	}

	return out
}

// streamFailures checks the eventStreams of the bulk response against what a provider can use: one entry of type sse
// that has a url and no endpoint (the document allows exactly one of them).
func streamFailures(parsed map[string]any, want *expectation) []string {
	streams, has := parsed["eventStreams"].([]any)

	switch want.EventStreams {
	case "":
		return nil
	case "absent":
		if has {
			return []string{"the response advertises a stream"}
		}

		return nil
	}

	if len(streams) != 1 {
		return []string{fmt.Sprintf("eventStreams has %d entries, want 1", len(streams))}
	}

	entry, _ := streams[0].(map[string]any)

	var out []string

	if entry["type"] != "sse" {
		out = append(out, fmt.Sprintf("the type of the stream is %v", entry["type"]))
	}

	if address, _ := entry["url"].(string); address == "" {
		out = append(out, "the stream has no url")
	}

	if _, hasEndpoint := entry["endpoint"]; hasEndpoint {
		out = append(out, "the stream has both a url and an endpoint")
	}

	return out
}

// rawSend sends the request and returns what came back, without asserting: for the polls of a wait.
func rawSend(ctx context.Context, call *request) (*response, error) {
	req, err := newHTTPRequest(ctx, call)
	if err != nil {
		return nil, err
	}

	resp, err := (&http.Client{Timeout: httpClientTimeout}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending the request: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading the response: %w", err)
	}

	return &response{status: resp.StatusCode, header: resp.Header, body: body}, nil
}

// sseClient is a client of the flag change stream a bulk response advertises.
type sseClient struct {
	events chan sseEvent
}

type sseEvent struct {
	Type         string      `json:"type"`
	ETag         string      `json:"etag"`
	LastModified json.Number `json:"lastModified"`
}

// connect does what a provider does: it reads the stream the bulk response advertises and connects to its url.
func (r *sequenceRun) connect(t *testing.T, stp *step) {
	t.Helper()

	probe := *stp
	probe.Context = "free"
	probe.Expect = expectation{Status: http.StatusOK, EventStreams: "url"}

	res := r.hns.send(t, r.call(t, &probe))
	require.Empty(t, r.failures(res, &probe))

	var advertised struct {
		EventStreams []struct {
			URL string `json:"url"`
		} `json:"eventStreams"`
	}

	require.NoError(t, json.Unmarshal(res.body, &advertised))

	address, err := url.Parse(advertised.EventStreams[0].URL)
	require.NoError(t, err)
	assert.Equal(t, sseStreamPath, address.Path)
	assert.Equal(t, stp.Key, address.Query().Get("apiKey"), "the url carries the key of the request")

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address.String(), http.NoBody)
	require.NoError(t, err)

	// the response comes back once the client is subscribed: a change published after this is delivered to it
	resp, err := (&http.Client{}).Do(req)
	require.NoError(t, err)

	t.Cleanup(func() { _ = resp.Body.Close() })

	require.Equal(t, http.StatusOK, resp.StatusCode)

	client, done := newSSEClient(resp)
	r.streams[stp.Stream] = client

	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// newSSEClient reads the events of the body in the background, until the body ends; done closes then. It closes the
// body.
func newSSEClient(resp *http.Response) (*sseClient, <-chan struct{}) {
	client := &sseClient{events: make(chan sseEvent, eventBuffer)}
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer func() { _ = resp.Body.Close() }()

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			data, isData := strings.CutPrefix(scanner.Text(), sseDataPrefix)
			if !isData {
				continue
			}

			var event sseEvent
			if json.Unmarshal([]byte(data), &event) == nil {
				client.events <- event
			}
		}
	}()

	return client, done
}

// event waits for the next refetchEvaluation event of a stream and saves its etag and lastModified.
func (r *sequenceRun) event(t *testing.T, stp *step) {
	t.Helper()

	client, ok := r.streams[stp.Stream]
	require.True(t, ok, "stream %q", stp.Stream)

	ctx, cancel := context.WithTimeout(t.Context(), sequenceWait)
	defer cancel()

	for {
		select {
		case event := <-client.events:
			if event.Type != "refetchEvaluation" {
				continue
			}

			assert.NotEmpty(t, event.ETag, "the event names the configuration")

			modified, err := strconv.ParseInt(event.LastModified.String(), 10, 64)
			require.NoError(t, err, "lastModified is a unix time in seconds")
			assert.Positive(t, modified)

			r.vars[stp.Save] = map[string]string{varETag: event.ETag, "lastModified": event.LastModified.String()}

			return
		case <-ctx.Done():
			require.FailNow(t, "no refetchEvaluation event arrived on stream "+stp.Stream)
		}
	}
}
