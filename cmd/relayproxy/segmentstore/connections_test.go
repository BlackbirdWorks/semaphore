package segmentstore_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/bootstrap"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/flagruntime"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/segmentstore"
	"github.com/blackbirdworks/semaphore/modules/core/segment"
)

const (
	connFast       = "fast"
	kindRedis      = "redis"
	segAudience    = "audience"
	connCache      = "cache"
	kindFile       = "file"
	memberAlice    = "alice"
	stateError     = "error"
	stateConnected = "connected"
	fileDoc        = `version: 2
connections:
  control: {type: file, path: /tmp/control}
  cache: {type: redis, addresses: ["127.0.0.1:1"], password: {fromEnv: SEGPASS}, tls: {enable: true}}
  fast: {type: valkey, addresses: ["127.0.0.1:1"], forceSingleClient: true, atomic: false,
    timeouts: {connect: 200ms, io: 2s}}
  tuned: {type: redis, addresses: ["127.0.0.1:1"], db: 2, maxRetries: -1, poolSize: 4, lookupTimeout: 50ms,
    keyPrefix: "p:", atomic: true, timeouts: {connect: 100ms, read: 100ms, write: 100ms}}
  nopass: {type: redis, addresses: ["127.0.0.1:1"], password: {fromEnv: MISSING}}
  goodca:
    type: valkey
    addresses: ["127.0.0.1:1"]
    forceSingleClient: true
    tls: {enable: true, serverName: x, caBundleFile: /ca.pem}
    timeouts: {connect: 200ms}
  notpem: {type: redis, addresses: ["127.0.0.1:1"], tls: {enable: true, caBundleFile: /notpem}}
  badca: {type: redis, addresses: ["127.0.0.1:1"], tls: {enable: true, caBundleFile: /no/such/ca.pem}}
`
	waitFor = 10 * time.Second
	tick    = 5 * time.Millisecond
)

var errDial = errors.New("dial refused")

// fakeHandle is a connected store over an in-memory source.
type fakeHandle struct {
	source      *segment.MemorySource
	bindErr     error
	closeErr    error
	closed      atomic.Bool
	binds       atomic.Int32
	noVersioner bool
	noSource    bool
}

func (h *fakeHandle) Bind(segment.MissingKey, []flagruntime.BoundDefinition) (*flagruntime.Binding, error) {
	h.binds.Add(1)

	if h.bindErr != nil {
		return nil, h.bindErr
	}

	if h.noVersioner {
		return &flagruntime.Binding{Provider: h.source}, nil
	}

	var source segment.SnapshotSource
	if !h.noSource {
		source = h.source
	}

	return &flagruntime.Binding{Provider: h.source, Versioner: h.source, Source: source}, nil
}

func (h *fakeHandle) Close() error {
	h.closed.Store(true)

	return h.closeErr
}

// secretEnv has the password of the connections, and nothing else.
func secretEnv(name string) (string, bool) {
	if name == "SEGPASS" {
		return "secret", true
	}

	return "", false
}

// readFile serves the CA bundles of the test: a real certificate, and a file that is not PEM.
func readFile(t *testing.T) func(string) ([]byte, error) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"}, IsCA: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	return func(name string) ([]byte, error) {
		switch name {
		case "/ca.pem":
			return pemBytes, nil
		case "/notpem":
			return []byte("not a certificate"), nil
		}

		return nil, errDial
	}
}

func parseDoc(tb testing.TB) *bootstrap.Config {
	tb.Helper()

	cfg, err := bootstrap.Parse([]byte(fileDoc), nil, bootstrap.LoadOptions{LookupEnv: secretEnv})
	require.NoError(tb, err)

	return cfg
}

func stateOf(conns *segmentstore.Connections, name string) string {
	for _, state := range conns.States() {
		if state.Name == name {
			return state.State
		}
	}

	return ""
}

// Bind marks a connection as needed and refuses a name that cannot serve segments.
func TestConnectionsBind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		conn      string
		wantErr   error
		wantState string
	}{
		{name: kindRedis, conn: connCache, wantState: "connecting"},
		{name: "unknown", conn: "nowhere", wantErr: segmentstore.ErrUnknownConnection, wantState: "idle"},
		{name: kindFile, conn: "control", wantErr: segmentstore.ErrCannotServe, wantState: "idle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conns := segmentstore.NewConnections(parseDoc(t), segmentstore.Options{})
			binding, err := conns.Bind(tt.conn, "", nil)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, binding)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, binding.Provider)
			}

			assert.Equal(t, tt.wantState, stateOf(conns, connCache), "only a bound connection is needed")
		})
	}
}

// A connection that is needed is dialled by the loop, never by Bind: the lookups are Unknown with ErrNotConnected
// until it is up, a failing dial is retried and logged once, the lookups work when it succeeds, and Shutdown closes
// the client and stops the loop with the context.
func TestConnectionsRun(t *testing.T) {
	t.Parallel()

	var (
		allow  atomic.Bool
		handle = &fakeHandle{source: segment.NewMemorySource(map[string][]string{segAudience: {memberAlice}})}
	)

	cfg := parseDoc(t)
	conns := segmentstore.NewConnections(cfg, segmentstore.Options{
		RetryInterval: tick,
		Dialers: map[string]segmentstore.Dialer{
			connCache: func(context.Context) (segmentstore.Handle, error) {
				if !allow.Load() {
					return nil, errDial
				}

				return handle, nil
			},
		},
	})

	binding, err := conns.Bind(connCache, segment.MissingNotMember, []flagruntime.BoundDefinition{{Name: segAudience}})
	require.NoError(t, err)

	lookups := []segment.Lookup{{Segment: segAudience, ID: memberAlice}}
	require.ErrorIs(t, binding.Provider.Contains(t.Context(), lookups), segmentstore.ErrNotConnected)
	assert.Equal(t, segment.Unknown, lookups[0].State)

	_, err = binding.Versioner.Version(t.Context(), segAudience)
	require.ErrorIs(t, err, segmentstore.ErrNotConnected)

	reporter, ok := binding.Provider.(segment.HealthReporter)
	require.True(t, ok)
	assert.Equal(t, "unavailable", reporter.Health().Failure)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		conns.Run(ctx)
	}()

	require.Eventually(t, func() bool { return stateOf(conns, connCache) == stateError }, waitFor, tick,
		"a failing dial is an error state, retried")

	allow.Store(true)

	require.Eventually(t, func() bool { return stateOf(conns, connCache) == stateConnected }, waitFor, tick,
		"the dial is retried until it works")

	require.NoError(t, binding.Provider.Contains(t.Context(), lookups))
	assert.Equal(t, segment.Member, lookups[0].State)
	assert.Equal(t, int32(1), handle.binds.Load(), "the client is bound once for the generation")

	version, err := binding.Versioner.Version(t.Context(), segAudience)
	require.NoError(t, err)
	assert.NotEmpty(t, version)

	cancel()

	select {
	case <-done:
	case <-time.After(waitFor):
		assert.Fail(t, "the loop did not stop when its context ended")
	}

	require.NoError(t, conns.Shutdown(t.Context()))
	assert.True(t, handle.closed.Load(), "the client is closed by Shutdown")
	assert.Equal(t, "closed", stateOf(conns, connCache), "the client is gone after Shutdown")
	assert.Equal(t, "closed", stateOf(conns, connFast))
}

// A connection that becomes needed after the loop started is dialled at once, not at the next tick: a retry interval
// of an hour never delays the first dial (the first requests of a healthy store would answer STORE_ERROR).
func TestConnectionsRunDialsAtOnceWhenBound(t *testing.T) {
	t.Parallel()

	handle := &fakeHandle{source: segment.NewMemorySource(map[string][]string{segAudience: {memberAlice}})}
	conns := segmentstore.NewConnections(parseDoc(t), segmentstore.Options{
		RetryInterval: time.Hour,
		Dialers: map[string]segmentstore.Dialer{
			connCache: func(context.Context) (segmentstore.Handle, error) { return handle, nil },
			connFast:  func(context.Context) (segmentstore.Handle, error) { return nil, errDial },
		},
	})

	// a connection that fails shows that the first pass of the loop ran
	_, err := conns.Bind(connFast, segment.MissingNotMember, []flagruntime.BoundDefinition{{Name: segAudience}})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		conns.Run(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	require.Eventually(t, func() bool { return stateOf(conns, connFast) == stateError }, waitFor, tick,
		"the first pass ran, and the other connection was idle")
	require.Equal(t, "idle", stateOf(conns, connCache))

	_, err = conns.Bind(connCache, segment.MissingNotMember, []flagruntime.BoundDefinition{{Name: segAudience}})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return stateOf(conns, connCache) == stateConnected }, waitFor, tick,
		"the bound connection is dialled without waiting for the retry interval")
}

// The real dialers of the kinds: a redis dial probes the store once (go-redis connects on the first command), and
// valkey-go connects when the client is created, so an unreachable store of either kind is an error state, retried; a
// CA bundle that cannot be read is an error state too.
func TestConnectionsDialKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		conn      string
		wantState string
	}{
		{name: "redis unreachable", conn: connCache, wantState: stateError},
		{name: "valkey unreachable", conn: connFast, wantState: stateError},
		{name: "unreadable CA bundle", conn: "badca", wantState: stateError},
		{name: "redis with settings, unreachable", conn: "tuned", wantState: stateError},
		{name: "unset password", conn: "nopass", wantState: stateError},
		{name: "CA bundle that is not PEM", conn: "notpem", wantState: stateError},
		{name: "valkey with a CA bundle", conn: "goodca", wantState: stateError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conns := segmentstore.NewConnections(parseDoc(t), segmentstore.Options{
				RetryInterval: time.Hour,
				Secrets:       bootstrap.LoadOptions{LookupEnv: secretEnv, ReadFile: readFile(t)},
			})

			binding, err := conns.Bind(
				tt.conn,
				segment.MissingNotMember,
				[]flagruntime.BoundDefinition{{Name: segAudience}},
			)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})

			go func() {
				defer close(done)

				conns.Run(ctx)
			}()

			require.Eventually(t, func() bool { return stateOf(conns, tt.conn) == tt.wantState }, waitFor, tick)

			assert.NotNil(t, binding.Provider)

			cancel()
			<-done
			require.NoError(t, conns.Shutdown(t.Context()))
		})
	}
}

// A dial that ignores its context does not hold the loop: Run returns when its context ends, and the client that
// arrives later is closed, never left open. The same when Shutdown comes while the dial runs.
func TestConnectionsDialThatBlocks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stop      func(cancel context.CancelFunc, conns *segmentstore.Connections)
		name      string
		wantState string
	}{
		{
			name: "context ends", wantState: "connecting",
			stop: func(cancel context.CancelFunc, _ *segmentstore.Connections) { cancel() },
		},
		{
			name:      "shutdown",
			wantState: "closed",
			stop:      func(_ context.CancelFunc, conns *segmentstore.Connections) { _ = conns.Shutdown(context.Background()) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				handle  = &fakeHandle{source: segment.NewMemorySource(nil)}
				entered = make(chan struct{})
				release = make(chan struct{})
			)

			conns := segmentstore.NewConnections(parseDoc(t), segmentstore.Options{
				RetryInterval: tick,
				Dialers: map[string]segmentstore.Dialer{
					connCache: func(context.Context) (segmentstore.Handle, error) {
						close(entered)
						<-release // ignores the context, as a client that dials while it is created

						return handle, nil
					},
				},
			})

			_, err := conns.Bind(connCache, segment.MissingNotMember, nil)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			done := make(chan struct{})

			go func() {
				defer close(done)

				conns.Run(ctx)
			}()

			select {
			case <-entered:
			case <-time.After(waitFor):
				require.FailNow(t, "the dial did not start")
			}

			tt.stop(cancel, conns)
			cancel()

			select {
			case <-done:
			case <-time.After(waitFor):
				require.FailNow(t, "Run did not return while the dial was blocked")
			}

			close(release)

			require.Eventually(t, handle.closed.Load, waitFor, tick, "the client that arrived late is closed")
			assert.Equal(t, tt.wantState, stateOf(conns, connCache))
		})
	}
}

// What a connected store can do wrong: no version source, a provider that reports no health, a bind that fails, a
// client that does not close.
func TestConnectionsBindingEdges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		handle          *fakeHandle
		check           func(t *testing.T, binding *flagruntime.Binding)
		wantShutdownErr error
		name            string
	}{
		{
			name: "no version source", handle: &fakeHandle{noVersioner: true},
			check: func(t *testing.T, binding *flagruntime.Binding) {
				t.Helper()

				_, err := binding.Versioner.Version(t.Context(), segAudience)
				require.ErrorIs(t, err, segment.ErrNoVersion)
			},
		},
		{
			name: "members of a set", handle: &fakeHandle{},
			check: func(t *testing.T, binding *flagruntime.Binding) {
				t.Helper()

				var got []string

				for id, err := range binding.Source.Members(t.Context(), segAudience) {
					require.NoError(t, err)

					got = append(got, id)
				}

				assert.Equal(t, []string{memberAlice}, got)
			},
		},
		{
			name: "members of a store that cannot list them", handle: &fakeHandle{noSource: true},
			check: func(t *testing.T, binding *flagruntime.Binding) {
				t.Helper()

				var errs []error

				for _, err := range binding.Source.Members(t.Context(), segAudience) {
					errs = append(errs, err)
				}

				require.Len(t, errs, 1)
				require.ErrorIs(t, errs[0], segmentstore.ErrCannotServe)
			},
		},
		{
			name: "provider without health", handle: &fakeHandle{},
			check: func(t *testing.T, binding *flagruntime.Binding) {
				t.Helper()

				lookups := []segment.Lookup{{Segment: segAudience, ID: memberAlice}}
				require.NoError(t, binding.Provider.Contains(t.Context(), lookups))

				reporter, ok := binding.Provider.(segment.HealthReporter)
				require.True(t, ok)
				assert.Equal(t, segment.Health{Ready: true}, reporter.Health())
			},
		},
		{
			name: "no traffic yet", handle: &fakeHandle{},
			check: func(t *testing.T, binding *flagruntime.Binding) {
				t.Helper()

				reporter, ok := binding.Provider.(segment.HealthReporter)
				require.True(t, ok)
				assert.Equal(t, segment.Health{Ready: true}, reporter.Health(), "a connected store is not failing")
			},
		},
		{
			name: "bind fails", handle: &fakeHandle{bindErr: errDial},
			check: func(t *testing.T, binding *flagruntime.Binding) {
				t.Helper()

				lookups := []segment.Lookup{{Segment: segAudience, ID: memberAlice}}
				require.ErrorIs(t, binding.Provider.Contains(t.Context(), lookups), errDial)
				require.ErrorIs(t, binding.Provider.Contains(t.Context(), lookups), errDial, "the bind is tried again")
			},
		},
		{
			name: "close fails", handle: &fakeHandle{closeErr: errDial}, wantShutdownErr: errDial,
			check: func(*testing.T, *flagruntime.Binding) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.handle.source = segment.NewMemorySource(map[string][]string{segAudience: {memberAlice}})
			conns := segmentstore.NewConnections(parseDoc(t), segmentstore.Options{
				RetryInterval: tick,
				Dialers: map[string]segmentstore.Dialer{
					connCache: func(context.Context) (segmentstore.Handle, error) { return tt.handle, nil },
				},
			})

			binding, err := conns.Bind(connCache, segment.MissingNotMember, nil)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			done := make(chan struct{})

			go func() {
				defer close(done)

				conns.Run(ctx)
			}()

			require.Eventually(t, func() bool { return stateOf(conns, connCache) == stateConnected }, waitFor, tick)
			tt.check(t, binding)
			cancel()

			select {
			case <-done:
			case <-time.After(waitFor):
				require.FailNow(t, "the loop did not stop")
			}

			err = conns.Shutdown(t.Context())
			if tt.wantShutdownErr != nil {
				require.ErrorIs(t, err, tt.wantShutdownErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// The connections are dialled apart: a store that does not answer does not delay the others.
func TestConnectionsDialInParallel(t *testing.T) {
	t.Parallel()

	var (
		release = make(chan struct{})
		entered = make(chan struct{})
		fast    = &fakeHandle{source: segment.NewMemorySource(nil)}
		slow    = &fakeHandle{source: segment.NewMemorySource(nil)}
	)

	conns := segmentstore.NewConnections(parseDoc(t), segmentstore.Options{
		RetryInterval: tick,
		Dialers: map[string]segmentstore.Dialer{
			connCache: func(ctx context.Context) (segmentstore.Handle, error) {
				close(entered)

				select {
				case <-release:
					return slow, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
			connFast: func(context.Context) (segmentstore.Handle, error) { return fast, nil },
		},
	})

	for _, name := range []string{connCache, connFast} {
		_, err := conns.Bind(name, segment.MissingNotMember, nil)
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		conns.Run(ctx)
	}()

	select {
	case <-entered:
	case <-time.After(waitFor):
		require.FailNow(t, "the slow dial did not start")
	}

	require.Eventually(t, func() bool { return stateOf(conns, connFast) == stateConnected }, waitFor, tick,
		"the healthy connection connects while the other dial is blocked")
	assert.Equal(t, "connecting", stateOf(conns, connCache))

	close(release)

	require.Eventually(t, func() bool { return stateOf(conns, connCache) == stateConnected }, waitFor, tick)
	cancel()

	select {
	case <-done:
	case <-time.After(waitFor):
		require.FailNow(t, "the loop did not stop")
	}
}

// The members of a set cannot be read before the connection is up: the stream yields the error once and ends.
func TestConnectionsMembersBeforeTheConnectionIsUp(t *testing.T) {
	t.Parallel()

	conns := segmentstore.NewConnections(parseDoc(t), segmentstore.Options{})

	binding, err := conns.Bind(connCache, segment.MissingNotMember, nil)
	require.NoError(t, err)

	var errs []error

	for _, err := range binding.Source.Members(t.Context(), segAudience) {
		errs = append(errs, err)
	}

	require.Len(t, errs, 1)
	require.ErrorIs(t, errs[0], segmentstore.ErrNotConnected)
}

// respStub is a store that answers every command with reply, or closes every connection while reply is empty.
type respStub struct {
	reply   atomic.Pointer[string]
	address string
}

func startRespStub(t *testing.T) *respStub {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	stub := &respStub{address: listener.Addr().String()}

	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			go stub.serve(conn)
		}
	}()

	return stub
}

func (r *respStub) serve(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 4096)

	for {
		read, err := conn.Read(buf)
		if err != nil {
			return
		}

		reply := r.reply.Load()
		if reply == nil {
			return
		}

		// one reply for each command of the chunk: a command is an array, which starts a line with "*"
		for line := range strings.SplitSeq(string(buf[:read]), "\r\n") {
			if strings.HasPrefix(line, "*") {
				_, _ = conn.Write([]byte(*reply))
			}
		}
	}
}

// A redis store is connected when it answers the probe, with any reply (the documented read-only ACL has no PING), and
// an error, retried, while it cannot be reached; it connects without a restart when the store appears.
func TestConnectionsRedisProbe(t *testing.T) {
	t.Parallel()

	const noPerm = "-NOPERM this user has no permissions to run the 'ping' command\r\n"

	stub := startRespStub(t)

	doc := "version: 2\nconnections:\n  control: {type: file, path: /tmp/control}\n  probe: {type: redis, addresses: [\"" +
		stub.address + "\"], timeouts: {connect: 200ms, read: 200ms, write: 200ms}}\n"

	cfg, err := bootstrap.Parse([]byte(doc), nil, bootstrap.LoadOptions{})
	require.NoError(t, err)

	conns := segmentstore.NewConnections(cfg, segmentstore.Options{RetryInterval: tick})

	binding, err := conns.Bind("probe", segment.MissingNotMember, []flagruntime.BoundDefinition{{Name: segAudience}})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		conns.Run(ctx)
	}()

	require.Eventually(t, func() bool { return stateOf(conns, "probe") == stateError }, waitFor, tick,
		"a store that closes the connection is an error, retried")

	reply := noPerm
	stub.reply.Store(&reply)

	require.Eventually(t, func() bool { return stateOf(conns, "probe") == stateConnected }, waitFor, tick,
		"a permission error is a reply of the store: it is reachable")

	// the lookups reach the store, which refuses them: not answered, and the provider says why
	lookups := []segment.Lookup{{Segment: segAudience, ID: memberAlice}}
	require.Error(t, binding.Provider.Contains(t.Context(), lookups))
	assert.Equal(t, segment.Unknown, lookups[0].State)

	reporter, ok := binding.Provider.(segment.HealthReporter)
	require.True(t, ok)
	assert.NotEmpty(t, reporter.Health().Failure)

	cancel()
	<-done
	require.NoError(t, conns.Shutdown(t.Context()))
}

// gatedDial is a dialer that blocks on a channel: every call announces itself on entered, then waits for the test to
// give it its answer on release.
type gatedDial struct {
	entered chan struct{}
	release chan error
	handle  *fakeHandle
	dial    segmentstore.Dialer
}

func newGatedDial() *gatedDial {
	gated := &gatedDial{
		entered: make(chan struct{}, 8), release: make(chan error, 8),
		handle: &fakeHandle{source: segment.NewMemorySource(map[string][]string{segAudience: {memberAlice}})},
	}

	gated.dial = func(ctx context.Context) (segmentstore.Handle, error) {
		gated.entered <- struct{}{}

		select {
		case err := <-gated.release:
			if err != nil {
				return nil, err
			}

			return gated.handle, nil
		case <-ctx.Done():
			return nil, fmt.Errorf("gated dial: %w", ctx.Err())
		}
	}

	return gated
}

// runGated starts the loop with a gated dialer for the connection and binds a generation to it.
func runGated(t *testing.T, gated *gatedDial) *flagruntime.Binding {
	t.Helper()

	// a probe connection shows that the loop runs: it is dialled by the first pass
	probed := make(chan struct{}, 1)

	conns := segmentstore.NewConnections(parseDoc(t), segmentstore.Options{
		RetryInterval: tick,
		Dialers: map[string]segmentstore.Dialer{
			connCache: gated.dial,
			connFast: func(context.Context) (segmentstore.Handle, error) {
				select {
				case probed <- struct{}{}:
				default:
				}

				return nil, errDial
			},
		},
	})

	_, err := conns.Bind(connFast, segment.MissingNotMember, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		conns.Run(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	// the loop runs before the generation binds, as in the relay
	awaitSignal(t, probed)

	binding, err := conns.Bind(connCache, segment.MissingNotMember, []flagruntime.BoundDefinition{{Name: segAudience}})
	require.NoError(t, err)

	return binding
}

type lookupResult struct {
	err   error
	state segment.State
}

// lookupAsync runs one lookup and reports its result.
func lookupAsync(ctx context.Context, binding *flagruntime.Binding) <-chan lookupResult {
	out := make(chan lookupResult, 1)

	go func() {
		lookups := []segment.Lookup{{Segment: segAudience, ID: memberAlice}}
		err := binding.Provider.Contains(ctx, lookups)

		out <- lookupResult{err: err, state: lookups[0].State}
	}()

	return out
}

func awaitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(waitFor):
		require.FailNow(t, "the signal did not arrive")
	}
}

func awaitLookup(t *testing.T, ch <-chan lookupResult) lookupResult {
	t.Helper()

	select {
	case got := <-ch:
		return got
	case <-time.After(waitFor):
		require.FailNow(t, "the lookup did not end")
	}

	return lookupResult{}
}

// A lookup that arrives while the first dial of its connection is under way waits for the dial, and gets the answer of
// the store, not an error.
func TestLookupDuringTheFirstDialWaitsForIt(t *testing.T) {
	t.Parallel()

	gated := newGatedDial()
	binding := runGated(t, gated)

	awaitSignal(t, gated.entered)

	result := lookupAsync(t.Context(), binding)

	gated.release <- nil

	got := awaitLookup(t, result)
	require.NoError(t, got.err)
	assert.Equal(t, segment.Member, got.state)
}

// When the first dial fails, the lookup that waited gets the store error: not connected.
func TestLookupDuringTheFirstDialGetsTheStoreErrorWhenItFails(t *testing.T) {
	t.Parallel()

	gated := newGatedDial()
	binding := runGated(t, gated)

	awaitSignal(t, gated.entered)

	result := lookupAsync(t.Context(), binding)

	gated.release <- errDial

	got := awaitLookup(t, result)
	require.ErrorIs(t, got.err, segmentstore.ErrNotConnected)
	assert.Equal(t, segment.Unknown, got.state)
}

// A caller whose context ends while it waits leaves at once, with the cause of the context.
func TestLookupLeavesWhenItsContextEndsDuringTheFirstDial(t *testing.T) {
	t.Parallel()

	gated := newGatedDial()
	binding := runGated(t, gated)

	awaitSignal(t, gated.entered)

	ctx, cancel := context.WithCancel(t.Context())
	result := lookupAsync(ctx, binding)

	cancel()

	got := awaitLookup(t, result)
	require.ErrorIs(t, got.err, context.Canceled)
	require.ErrorIs(t, got.err, segmentstore.ErrNotConnected)
}

// After the first outcome nobody waits: a dial that failed once and is being tried again does not hold the lookups.
func TestLookupsDoNotWaitAfterTheFirstDialEnded(t *testing.T) {
	t.Parallel()

	gated := newGatedDial()
	binding := runGated(t, gated)

	awaitSignal(t, gated.entered)

	gated.release <- errDial

	// the retry is under way and blocked: the lookup answers at once
	awaitSignal(t, gated.entered)

	got := awaitLookup(t, lookupAsync(t.Context(), binding))
	require.ErrorIs(t, got.err, segmentstore.ErrNotConnected)

	gated.release <- nil

	require.Eventually(t, func() bool {
		return (<-lookupAsync(t.Context(), binding)).err == nil
	}, waitFor, tick, "the retry connects")
}

// BenchmarkLookupOnAConnectedStore pins the connected path: a bound lookup waits for nothing and allocates nothing
// that the wait for a first dial could add.
func BenchmarkLookupOnAConnectedStore(b *testing.B) {
	gated := newGatedDial()
	conns := segmentstore.NewConnections(parseDoc(b), segmentstore.Options{
		RetryInterval: tick, Dialers: map[string]segmentstore.Dialer{connCache: gated.dial},
	})

	ctx, cancel := context.WithCancel(b.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		conns.Run(ctx)
	}()

	b.Cleanup(func() {
		cancel()
		<-done
	})

	binding, err := conns.Bind(connCache, segment.MissingNotMember, []flagruntime.BoundDefinition{{Name: segAudience}})
	require.NoError(b, err)

	gated.release <- nil

	require.Eventually(b, func() bool { return stateOf(conns, connCache) == stateConnected }, waitFor, tick)

	lookups := []segment.Lookup{{Segment: segAudience, ID: memberAlice}}

	b.ReportAllocs()

	for b.Loop() {
		_ = binding.Provider.Contains(ctx, lookups)
	}
}
