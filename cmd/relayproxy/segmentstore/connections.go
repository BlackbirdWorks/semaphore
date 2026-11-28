package segmentstore

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/bootstrap"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/flagruntime"
	"github.com/blackbirdworks/semaphore/modules/core/segment"
)

// DefaultRetryInterval is how often a connection that is needed and not connected is dialled again.
const DefaultRetryInterval = time.Second

var (
	// ErrNotConnected is the error of a lookup on a connection that is not connected yet: the lookups stay Unknown
	// and the segments answer STORE_ERROR until the connection is up.
	ErrNotConnected = errors.New("the segment store is not connected")
	// ErrUnknownConnection is returned by Bind for a name that is not a connection of the bootstrap file.
	ErrUnknownConnection = errors.New("not a connection of the bootstrap file")
	// ErrCannotServe is returned by Bind for a connection whose kind cannot serve segments.
	ErrCannotServe = errors.New("the connection cannot serve segments")
)

// Handle is a connected store: the client of one connection, shared by every generation. It binds the definitions of
// a generation to a provider that reads through the client; closing the provider of a generation never closes it.
type Handle interface {
	// Bind returns the binding of the definitions, with a missing key read as missing says.
	Bind(missing segment.MissingKey, defs []flagruntime.BoundDefinition) (*flagruntime.Binding, error)
	// Close closes the client.
	Close() error
}

// Dialer connects one connection. It must honour the context.
type Dialer func(ctx context.Context) (Handle, error)

// Options configure [Connections].
type Options struct {
	// Logger receives the connect and the failure of a connection. Nil discards.
	Logger *slog.Logger
	// Dialers replaces the dialer of a connection by name (a test, an embedder); a connection without one is
	// dialled by the kind it names.
	Dialers map[string]Dialer
	// Secrets reads the secrets and the files of the connections.
	Secrets bootstrap.LoadOptions
	// RetryInterval is how often the connections that are needed and down are dialled. Zero means
	// [DefaultRetryInterval].
	RetryInterval time.Duration
}

// ConnectionState is what /info and the metrics tell of one connection.
type ConnectionState struct {
	// Name is the name in the bootstrap file.
	Name string `json:"name"`
	// Kind is the type of the connection.
	Kind string `json:"kind"`
	// State is "idle" (no segment uses it), "connecting", "connected", "error" (the last dial failed) or "closed"
	// (after Shutdown).
	State string `json:"state"`
}

// Connections owns the stores of the process: one client for each connection of the bootstrap file that a valid
// dynamic definition uses, dialled when it is first needed, by the loop of [Connections.Run], and closed by
// [Connections.Shutdown]. A store that is down never stops the relay: its segments answer not a member, STORE_ERROR,
// until it is back. It is a [flagruntime.SegmentStores] and a [flagruntime.SegmentBinder].
type Connections struct {
	flagruntime.StoreTable

	conns map[string]*connection
	log   *slog.Logger
	// wake makes Run dial now: a connection that became needed (the first generation binds after Run started) is
	// not left to wait for the next tick, which would answer STORE_ERROR to the first requests of a healthy store.
	wake chan struct{}
	// stopped is closed when Run ends: a lookup that waits for a first dial stops waiting then.
	stopped  chan struct{}
	retry    time.Duration
	stopOnce sync.Once
	// running is true while Run runs: before it there is no dial to wait for.
	running atomic.Bool
}

type connection struct {
	owner *Connections
	// first is closed when the first dial of the connection ended, with a client or with an error. A lookup that
	// finds the connection not connected waits for it, and only until it is closed: once the first dial has ended,
	// connected or failed, nobody waits any more.
	first     chan struct{}
	dial      Dialer
	handle    atomic.Pointer[handleBox]
	name      string
	kind      string
	firstOnce sync.Once
	closeMu   sync.Mutex
	failed    atomic.Bool
	dialing   atomic.Bool
	needed    atomic.Bool
	closed    bool
}

type handleBox struct{ h Handle }

// NewConnections describes the connections of the bootstrap file and prepares their dialers. It opens nothing.
func NewConnections(cfg *bootstrap.Config, opts Options) *Connections {
	out := &Connections{
		StoreTable: New(cfg), conns: map[string]*connection{}, log: opts.Logger, retry: opts.RetryInterval,
		wake: make(chan struct{}, 1), stopped: make(chan struct{}),
	}

	if out.log == nil {
		out.log = slog.New(slog.DiscardHandler)
	}

	if out.retry <= 0 {
		out.retry = DefaultRetryInterval
	}

	for name := range cfg.Connections {
		conn := cfg.Connections[name]
		kind := conn.Type()

		dial := opts.Dialers[name]
		if dial == nil {
			dial = dialerOf(&conn, opts.Secrets)
		}

		if dial != nil {
			out.conns[name] = &connection{name: name, kind: kind, dial: dial, owner: out, first: make(chan struct{})}
		}
	}

	return out
}

// Bind implements [flagruntime.SegmentBinder]. It marks the connection as needed and returns at once: the lookups
// are answered when the connection is up.
func (c *Connections) Bind(
	connection string, missing segment.MissingKey, defs []flagruntime.BoundDefinition,
) (*flagruntime.Binding, error) {
	conn, ok := c.conns[connection]
	if !ok {
		if _, known := c.StoreTable[connection]; known {
			return nil, fmt.Errorf("%q: %w", connection, ErrCannotServe)
		}

		return nil, fmt.Errorf("%q: %w", connection, ErrUnknownConnection)
	}

	if !conn.needed.Swap(true) {
		select {
		case c.wake <- struct{}{}:
		default: // a wake-up is already pending
		}
	}

	lazy := &lazyBinding{conn: conn, missing: missing, defs: defs}

	return &flagruntime.Binding{Provider: lazy, Versioner: lazy, Source: lazy}, nil
}

// Run dials the connections that are needed and not connected, now and every retry interval, until ctx ends. It is
// owned by the run context of the relay.
func (c *Connections) Run(ctx context.Context) {
	ticker := time.NewTicker(c.retry)
	defer ticker.Stop()

	c.running.Store(true)

	defer func() {
		c.running.Store(false)
		c.stopOnce.Do(func() { close(c.stopped) })
	}()

	var dials sync.WaitGroup

	// the dials end with the context (see dialUntil): Run leaves nothing behind
	defer dials.Wait()

	for {
		c.connect(ctx, &dials)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.wake:
		}
	}
}

// States returns the state of each connection that can serve segments, sorted by name.
func (c *Connections) States() []ConnectionState {
	out := make([]ConnectionState, 0, len(c.conns))

	for _, name := range c.names() {
		conn := c.conns[name]
		state := "idle"

		conn.closeMu.Lock()
		closed := conn.closed
		conn.closeMu.Unlock()

		switch {
		case closed:
			state = "closed"
		case conn.handle.Load() != nil:
			state = "connected"
		case conn.failed.Load():
			state = "error"
		case conn.needed.Load():
			state = "connecting"
		}

		out = append(out, ConnectionState{Name: name, Kind: conn.kind, State: state})
	}

	return out
}

// Shutdown closes the clients. It is called with a fresh context, after the loops ended; a connection that is
// being dialled when it is called is closed as soon as the dial returns.
func (c *Connections) Shutdown(_ context.Context) error {
	var errs []error

	for _, name := range c.names() {
		conn := c.conns[name]

		conn.closeMu.Lock()
		conn.closed = true
		box := conn.handle.Swap(nil)
		conn.closeMu.Unlock()

		if box != nil {
			err := box.h.Close()
			if err != nil {
				errs = append(errs, fmt.Errorf("close the connection %q: %w", name, err))
			}
		}
	}

	return errors.Join(errs...)
}

// lazyBinding is the provider of a generation on a connection: it binds the definitions to the client when the
// connection is up, and answers ErrNotConnected before.
type lazyBinding struct {
	conn    *connection
	inner   atomic.Pointer[flagruntime.Binding]
	missing segment.MissingKey
	defs    []flagruntime.BoundDefinition
}

// Contains implements [segment.Provider].
func (l *lazyBinding) Contains(ctx context.Context, lookups []segment.Lookup) error {
	b, err := l.get(ctx)
	if err != nil {
		return err
	}

	err = b.Provider.Contains(ctx, lookups)
	if err != nil {
		return fmt.Errorf("the segment provider: %w", err)
	}

	return nil
}

// Version implements [segment.Versioner].
func (l *lazyBinding) Version(ctx context.Context, name string) (string, error) {
	b, err := l.get(ctx)
	if err != nil {
		return "", err
	}

	if b.Versioner == nil {
		return "", segment.ErrNoVersion
	}

	version, err := b.Versioner.Version(ctx, name)
	if err != nil {
		return "", fmt.Errorf("the segment version: %w", err)
	}

	return version, nil
}

// Members implements [segment.SnapshotSource]: a connection that is not connected yields the error once and ends.
func (l *lazyBinding) Members(ctx context.Context, name string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		b, err := l.get(ctx)
		if err != nil {
			yield("", err)

			return
		}

		if b.Source == nil {
			yield("", ErrCannotServe)

			return
		}

		for id, err := range b.Source.Members(ctx, name) {
			if !yield(id, err) {
				return
			}
		}
	}
}

// Health implements [segment.HealthReporter].
func (l *lazyBinding) Health() segment.Health {
	if b := l.inner.Load(); b != nil {
		if reporter, ok := b.Provider.(segment.HealthReporter); ok {
			return reporter.Health()
		}

		return segment.Health{Ready: true}
	}

	if l.conn.handle.Load() != nil {
		return segment.Health{Ready: true} // connected, and nothing has asked it yet
	}

	return segment.Health{Failure: "unavailable"}
}

func (c *Connections) connect(ctx context.Context, dials *sync.WaitGroup) {
	for _, name := range c.names() {
		conn := c.conns[name]
		if !conn.needed.Load() || conn.handle.Load() != nil || ctx.Err() != nil {
			continue
		}

		// one dial in flight for each connection, apart from the others: a store that does not answer delays no other
		if !conn.dialing.CompareAndSwap(false, true) {
			continue
		}

		dials.Go(func() {
			defer conn.dialing.Store(false)

			c.dial(ctx, name, conn)
		})
	}
}

// dial connects one connection and keeps the client.
func (c *Connections) dial(ctx context.Context, name string, conn *connection) {
	res := dialUntil(ctx, conn.dial)

	// the first outcome, whichever it is, ends the wait of the lookups; a retry later waits for nobody
	defer conn.firstOnce.Do(func() { close(conn.first) })

	if res.err != nil {
		if ctx.Err() == nil && conn.failed.CompareAndSwap(false, true) {
			c.log.LogAttrs(ctx, slog.LevelWarn, "a segment store is not reachable: its segments answer STORE_ERROR",
				slog.String("connection", name), slog.String("kind", conn.kind), slog.Any("error", res.err))
		}

		return
	}

	conn.closeMu.Lock()
	if conn.closed {
		conn.closeMu.Unlock()

		_ = res.handle.Close()

		return
	}

	conn.handle.Store(&handleBox{h: res.handle})
	conn.closeMu.Unlock()
	conn.failed.Store(false)
	c.log.LogAttrs(ctx, slog.LevelInfo, "a segment store is connected",
		slog.String("connection", name), slog.String("kind", conn.kind))
}

// dialed is the result of a dial that runs apart from the loop.
type dialed struct {
	handle Handle
	err    error
}

// dialUntil runs a dial that may ignore its context (the client of a store that dials while it is created) and
// returns when it ends or when ctx does, whichever is first. A client that arrives after ctx ended is closed by the
// goroutine of the dial, so nothing is left open.
func dialUntil(ctx context.Context, dial Dialer) dialed {
	result := make(chan dialed, 1)

	go func() {
		handle, err := dial(ctx)
		result <- dialed{handle: handle, err: err}
	}()

	select {
	case <-ctx.Done():
		// whatever the dial gives, now or later, is closed: nothing is left open
		go func() {
			if res := <-result; res.handle != nil {
				_ = res.handle.Close()
			}
		}()

		return dialed{err: fmt.Errorf("dial: %w", context.Cause(ctx))}
	case res := <-result:
		return res
	}
}

func (c *Connections) names() []string {
	names := make([]string, 0, len(c.conns))
	for name := range c.conns {
		names = append(names, name)
	}

	slices.Sort(names)

	return names
}

// await waits for the first dial of the connection to end, when one is under way: a connection that became needed
// with a generation is dialled at once by Run, and a lookup that arrives in that window gets the answer of the dial
// instead of an error. It returns at once when the first dial has ended, or when Run does not run. The wait ends
// with ctx.
func (c *connection) await(ctx context.Context) error {
	select {
	case <-c.first:
		return nil
	default:
	}

	if !c.owner.running.Load() {
		return nil
	}

	select {
	case <-c.first:
	case <-c.owner.stopped:
	case <-ctx.Done():
		return fmt.Errorf("%w: waiting for the first connection: %w", ErrNotConnected, context.Cause(ctx))
	}

	return nil
}

func (l *lazyBinding) get(ctx context.Context) (*flagruntime.Binding, error) {
	if b := l.inner.Load(); b != nil {
		return b, nil
	}

	box := l.conn.handle.Load()
	if box == nil {
		err := l.conn.await(ctx)
		if err != nil {
			return nil, err
		}

		box = l.conn.handle.Load()
	}

	if box == nil {
		return nil, ErrNotConnected
	}

	b, err := box.h.Bind(l.missing, l.defs)
	if err != nil {
		return nil, fmt.Errorf("bind the segments: %w", err)
	}

	l.inner.CompareAndSwap(nil, b)

	return l.inner.Load(), nil
}
