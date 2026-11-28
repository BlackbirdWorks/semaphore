package corehttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/api"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/corehttp"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/events"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/flagruntime"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/relaycore"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/service"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/service/stream"
	"github.com/blackbirdworks/semaphore/modules/core/flagdef"
	"github.com/blackbirdworks/semaphore/notifier"
)

const labelKey = "corehttp_streams"

// delivery is what a stream service was asked to send.
type delivery struct {
	diff notifier.DiffCache
	name string
}

// fakeWebsocket is a websocket service whose deliveries wait for the gate: a client that stopped reading.
type fakeWebsocket struct {
	stream.WebsocketService

	gate    chan struct{}
	entered chan string
	got     chan delivery
	closed  chan struct{}
	once    sync.Once
}

func newFakeWebsocket(t *testing.T, gated bool) *fakeWebsocket {
	t.Helper()

	fake := &fakeWebsocket{
		gate: make(chan struct{}), entered: make(chan string, 64), got: make(chan delivery, 64),
		closed: make(chan struct{}),
	}
	if !gated {
		fake.open()
	}

	// the gate opens before the test ends, whatever failed
	t.Cleanup(fake.open)

	return fake
}

func (f *fakeWebsocket) BroadcastFlagChanges(name string, diff notifier.DiffCache) {
	f.entered <- name

	<-f.gate

	f.got <- delivery{name: name, diff: diff}
}

func (f *fakeWebsocket) Close() { close(f.closed) }

func (f *fakeWebsocket) open() { f.once.Do(func() { close(f.gate) }) }

// fakeSSE records the deliveries at once.
type fakeSSE struct {
	stream.SSEService

	gate    chan struct{}
	entered chan string
	got     chan delivery
	closed  chan struct{}
	once    sync.Once
}

func newFakeSSE(t *testing.T, gated bool) *fakeSSE {
	t.Helper()

	fake := &fakeSSE{
		gate: make(chan struct{}), entered: make(chan string, 64), got: make(chan delivery, 64),
		closed: make(chan struct{}),
	}
	if !gated {
		fake.open()
	}

	t.Cleanup(fake.open)

	return fake
}

func (f *fakeSSE) BroadcastConfigChange(name string, _ stream.ConfigIdentity) error {
	f.entered <- name

	<-f.gate

	f.got <- delivery{name: name}

	return nil
}

func (f *fakeSSE) Close() { close(f.closed) }

func (f *fakeSSE) open() { f.once.Do(func() { close(f.gate) }) }

func newStreams(t *testing.T, label string, socket *fakeWebsocket, sse *fakeSSE, limit int) *corehttp.Streams {
	t.Helper()

	return corehttp.NewStreamsWith(&corehttp.StreamsConfig{
		Websocket: socket, SSE: sse, Label: label, MaxPending: limit,
	})
}

// closeStreams closes the streams in a cleanup, where the context of the test is over; the wait is bounded.
func closeStreams(t *testing.T, streams *corehttp.Streams) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), waitBound)
	defer cancel()

	require.NoError(t, streams.Close(ctx))
}

func added(set, key string, flag flagdef.Flag) flagruntime.Change {
	return flagruntime.Change{FlagSet: set, Diff: notifier.DiffCache{Added: map[string]flagdef.Flag{key: flag}}}
}

//nolint:ireturn // a generic receive helper of the tests
func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(waitBound):
		require.FailNow(t, "timed out waiting for "+what)
	}

	var zero T

	return zero
}

// labelled returns the goroutines that carry the label value, settled: a worker that ended may need a few yields.
func labelled(value string) []string {
	var left []string

	for range 500 {
		left = left[:0]

		var profile bytes.Buffer

		_ = pprof.Lookup("goroutine").WriteTo(&profile, 1)

		for group := range strings.SplitSeq(profile.String(), "\n\n") {
			for line := range strings.SplitSeq(group, "\n") {
				raw, found := strings.CutPrefix(line, "# labels: ")
				if !found {
					continue
				}

				var labels map[string]string
				if json.Unmarshal([]byte(raw), &labels) == nil && strings.HasPrefix(labels[labelKey], value) {
					left = append(left, group)
				}
			}
		}

		if len(left) == 0 {
			return nil
		}

		for range 200 {
			runtime.Gosched()
		}
	}

	return left
}

func TestStreams_SlowWebsocketDoesNotBlock(t *testing.T) {
	t.Parallel()

	const label = "slow-websocket"

	socket, sse := newFakeWebsocket(t, true), newFakeSSE(t, false)
	streams := newStreams(t, label, socket, sse, 0)

	flag := &flagdef.InternalFlag{}

	// the websocket worker is inside a delivery that does not return
	streams.Broadcast(added("a", "f", flag))
	assert.Equal(t, "a", recv(t, socket.entered, "the websocket delivery"))

	// the core is not held: Broadcast returns, and the SSE clients still hear of every change
	streams.Broadcast(added("b", "f", flag))
	streams.Broadcast(added("c", "f", flag))

	for _, want := range []string{"a", "b", "c"} {
		assert.Equal(t, want, recv(t, sse.got, "the SSE delivery").name)
	}

	// Close is bounded by its context although a socket write is stuck, and says so
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := streams.Close(ctx)
	require.ErrorIs(t, err, corehttp.ErrNotStopped)
	require.ErrorIs(t, err, context.Canceled)

	// the stuck write returns: the worker ends, closes its service, and no goroutine is left
	socket.open()
	recv(t, socket.closed, "the websocket service to be closed")
	recv(t, sse.closed, "the SSE service to be closed")
	assert.Empty(t, labelled(label), "no worker goroutine is left")
}

func TestStreams_CloseWhenIdle(t *testing.T) {
	t.Parallel()

	const label = "idle"

	socket, sse := newFakeWebsocket(t, false), newFakeSSE(t, false)
	streams := newStreams(t, label, socket, sse, 0)

	streams.Broadcast(added("a", "f", &flagdef.InternalFlag{}))
	assert.Equal(t, "a", recv(t, socket.got, "the websocket delivery").name)
	assert.Equal(t, "a", recv(t, sse.got, "the SSE delivery").name)

	require.NoError(t, streams.Close(t.Context()))
	recv(t, socket.closed, "the websocket service to be closed")
	recv(t, sse.closed, "the SSE service to be closed")
	assert.Empty(t, labelled(label))

	// a change after Close is dropped without a worker
	streams.Broadcast(added("b", "f", &flagdef.InternalFlag{}))
	assert.Empty(t, sse.got)
	require.NoError(t, streams.Close(t.Context()), "closing twice is fine")

	// a change without a diff is not queued at all
	empty := corehttp.NewStreamsWith(
		&corehttp.StreamsConfig{Websocket: newFakeWebsocket(t, false), SSE: newFakeSSE(t, false)},
	)
	empty.Broadcast(flagruntime.Change{FlagSet: "none"})
	require.NoError(t, empty.Close(t.Context()))
}

func TestStreams_OverflowKeepsTheNewest(t *testing.T) {
	t.Parallel()

	const label = "overflow"

	socket, sse := newFakeWebsocket(t, true), newFakeSSE(t, false)
	streams := newStreams(t, label, socket, sse, 2)

	flag := &flagdef.InternalFlag{}

	streams.Broadcast(added("a", "f", flag))
	recv(t, socket.entered, "the websocket delivery")

	// two flag sets may wait: b and c are the oldest, d and e are kept
	for _, name := range []string{"b", "c", "d", "e"} {
		streams.Broadcast(added(name, "f", flag))
	}

	assert.EqualValues(t, 2, streams.Dropped().Websocket)

	socket.open()

	order := make([]string, 0, 3)
	for range 3 {
		order = append(order, recv(t, socket.got, "a websocket delivery").name)
	}

	assert.Equal(t, []string{"a", "d", "e"}, order)

	require.NoError(t, streams.Close(t.Context()))
	assert.Empty(t, labelled(label))
}

// TestStreams_MergeKeepsTheNetEffect: changes of one flag set that wait together reach the websocket as one diff
// that has the effect of all of them.
func TestStreams_MergeKeepsTheNetEffect(t *testing.T) {
	t.Parallel()

	one, two, three := &flagdef.InternalFlag{}, &flagdef.InternalFlag{}, &flagdef.InternalFlag{}
	names := map[flagdef.Flag]string{one: "one", two: "two", three: "three"}

	addOf := func(f flagdef.Flag) notifier.DiffCache {
		return notifier.DiffCache{Added: map[string]flagdef.Flag{"k": f}}
	}
	delOf := func(f flagdef.Flag) notifier.DiffCache {
		return notifier.DiffCache{Deleted: map[string]flagdef.Flag{"k": f}}
	}
	updOf := func(before, after flagdef.Flag) notifier.DiffCache {
		return notifier.DiffCache{Updated: map[string]notifier.DiffUpdated{"k": {Before: before, After: after}}}
	}

	tests := []struct {
		// want is the net effect on k: "" none, "added:two", "deleted:one", "updated:one>three".
		want  string
		name  string
		steps []notifier.DiffCache
	}{
		{name: "added then updated", steps: []notifier.DiffCache{addOf(one), updOf(one, two)}, want: "added:two"},
		{name: "updated twice", steps: []notifier.DiffCache{updOf(one, two), updOf(two, three)},
			want: "updated:one>three"},
		{name: "updated then deleted", steps: []notifier.DiffCache{updOf(one, two), delOf(two)}, want: "deleted:one"},
		{name: "deleted then added", steps: []notifier.DiffCache{delOf(one), addOf(two)}, want: "updated:one>two"},
		{name: "added then deleted", steps: []notifier.DiffCache{addOf(one), delOf(one)}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			socket, sse := newFakeWebsocket(t, true), newFakeSSE(t, false)
			streams := newStreams(t, "merge-"+tt.name, socket, sse, 0)

			t.Cleanup(func() { closeStreams(t, streams) })

			streams.Broadcast(added("blocker", "b", one))
			recv(t, socket.entered, "the websocket delivery")

			for _, diff := range tt.steps {
				streams.Broadcast(flagruntime.Change{FlagSet: "x", Diff: diff})
			}

			streams.Broadcast(added("marker", "m", one))
			socket.open()

			assert.Equal(t, "blocker", recv(t, socket.got, "the blocker").name)

			got := ""

			for next := recv(t, socket.got, "a delivery"); next.name != "marker"; next = recv(t, socket.got, "a delivery") {
				require.Equal(t, "x", next.name)

				switch {
				case len(next.diff.Added) == 1:
					got = "added:" + names[next.diff.Added["k"]]
				case len(next.diff.Deleted) == 1:
					got = "deleted:" + names[next.diff.Deleted["k"]]
				case len(next.diff.Updated) == 1:
					upd := next.diff.Updated["k"]
					got = "updated:" + names[upd.Before] + ">" + names[upd.After]
				}
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

// capped is a source whose exporters accept a number of events and then refuse: a full queue.
type capped struct {
	*relaycore.Core

	mu     sync.Mutex
	budget int
}

func (c *capped) Collect(events.Event) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.budget == 0 {
		return false
	}

	c.budget--

	return true
}

func (c *capped) remaining() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.budget
}

// TestServer_CollectorCountsOnlyAcceptedEvents: the collector answers 200 with the number of events that the exporters
// took, the schema has that field; the v1 answer (every event) is the one the equivalence cases pin.
func TestServer_CollectorCountsOnlyAcceptedEvents(t *testing.T) {
	t.Parallel()

	rig := newV2(t)

	source := &capped{Core: rig.core, budget: 2}
	srv, err := corehttp.New(source, &corehttp.Config{Settings: &api.Settings{}, Streams: rig.streams})
	require.NoError(t, err)

	event := `{"kind":"feature","contextKind":"user","userKey":"u","creationDate":1680246000,"key":"k","variation":"v",` +
		`"value":true,"default":false,"version":"1","source":"PROVIDER_CACHE"}`
	body := `{"events":[` + event + `,` + event + `,` + event + `]}`

	status, _, out := do(t, http.MethodPost, serve(t, &srv)+pathCollector, body, keyHeader(tenantKey))
	assert.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, `{"ingestedContentCount":2}`, string(out))
	assert.Zero(t, source.remaining())
}

func TestNew_RequiresStreams(t *testing.T) {
	t.Parallel()

	rig := newV2(t)

	_, err := corehttp.New(rig.core, &corehttp.Config{Settings: &api.Settings{}})
	require.ErrorIs(t, err, corehttp.ErrNoStreams)
}

// TestAdapter_FlagChangeIsComputedOnce: the hashes of a flag set are computed by its first call, the next calls
// return the same table and do not allocate (no flag is encoded again).
//
//nolint:paralleltest // AllocsPerRun counts allocations from concurrent tests across the process
func TestAdapter_FlagChangeIsComputedOnce(t *testing.T) {
	rig := newV2(t)

	flagSet, err := corehttp.NewAdapter(rig.core).FlagSet(tenantKey)
	require.NoError(t, err)

	changer, ok := flagSet.(service.FlagChanger)
	require.True(t, ok, "the flag set of a core keeps the hashes")

	first, hash, err := changer.FlagChange()
	require.NoError(t, err)
	assert.Len(t, first, len(rig.core.Holder().Load().FlagSets[setName].Flags))

	var sink uint32

	allocs := testing.AllocsPerRun(100, func() {
		again, againHash, againErr := changer.FlagChange()
		if againErr != nil || againHash != hash || len(again) != len(first) {
			sink++
		}
	})

	assert.Zero(t, sink, "the same hashes every time")
	assert.Zero(t, allocs, "the warm path encodes and allocates nothing")
}

// TestStreams_UndoneChangeStillTellsSSE: a flag added and removed while the change waits sends the SSE client one
// event (it may have fetched the flag meanwhile) and the websocket client none (the net diff is empty).
func TestStreams_UndoneChangeStillTellsSSE(t *testing.T) {
	t.Parallel()

	socket, sse := newFakeWebsocket(t, true), newFakeSSE(t, true)
	streams := newStreams(t, "undone", socket, sse, 0)

	t.Cleanup(func() { closeStreams(t, streams) })

	flag := &flagdef.InternalFlag{}

	streams.Broadcast(added("blocker", "b", flag))
	recv(t, socket.entered, "the websocket delivery")
	recv(t, sse.entered, "the SSE delivery")

	streams.Broadcast(added("x", "k", flag))
	streams.Broadcast(flagruntime.Change{
		FlagSet: "x", Diff: notifier.DiffCache{Deleted: map[string]flagdef.Flag{"k": flag}},
	})
	streams.Broadcast(added("marker", "m", flag))

	socket.open()
	sse.open()

	collect := func(ch <-chan delivery) []string {
		var names []string

		for next := recv(t, ch, "a delivery"); next.name != "marker"; next = recv(t, ch, "a delivery") {
			names = append(names, next.name)
		}

		return names
	}

	assert.Equal(t, []string{"blocker", "x"}, collect(sse.got), "SSE: the blocker and exactly one event for x")
	assert.Equal(t, []string{"blocker"}, collect(socket.got), "websocket: no message for an undone change")
}

// TestServer_CollectorCountsOnlyIngestedFeatureEvents: on a core an event that fails conversion is not ingested, and an
// event that the full queue refuses is not either.
func TestServer_CollectorCountsOnlyIngestedFeatureEvents(t *testing.T) {
	t.Parallel()

	rig := newV2(t)

	source := &capped{Core: rig.core, budget: 1}
	srv, err := corehttp.New(source, &corehttp.Config{Settings: &api.Settings{}, Streams: rig.streams})
	require.NoError(t, err)

	valid := `{"kind":"feature","contextKind":"user","userKey":"u","creationDate":1680246000,"key":"k","variation":"v",` +
		`"value":true,"default":false,"version":"1","source":"PROVIDER_CACHE"}`
	broken := `{"kind":"feature","contextKind":"user","userKey":"u","creationDate":1680246000,"key":{"a":1}}`
	body := `{"events":[` + valid + `,` + broken + `,` + valid + `]}`

	status, _, out := do(t, http.MethodPost, serve(t, &srv)+pathCollector, body, keyHeader(tenantKey))
	assert.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, `{"ingestedContentCount":1}`, string(out), "one accepted, one broken, one refused")
	assert.Zero(t, source.remaining())
}
