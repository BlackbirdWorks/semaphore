package corehttp

import (
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/flagruntime"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/service/stream"
	"github.com/blackbirdworks/semaphore/modules/core/flagdef"
	"github.com/blackbirdworks/semaphore/notifier"
)

// DefaultMaxPending is the number of flag sets whose change can wait for a stream service before the oldest one is
// dropped.
const DefaultMaxPending = 1024

// ErrNotStarted is returned by [Streams.AwaitStarted] when ctx ended before the workers started.
var ErrNotStarted = errors.New("corehttp: the stream workers did not start")

// ErrNotStopped is returned by [Streams.Close] when a stream service did not finish its last delivery before the
// context ended. The worker ends, and closes its service, as soon as that delivery returns.
var ErrNotStopped = errors.New("corehttp: the stream workers did not stop")

// StreamsConfig configures [NewStreamsWith].
type StreamsConfig struct {
	// Websocket and SSE are the services of the streams.
	Websocket stream.WebsocketService
	SSE       stream.SSEService
	// Labels is the context whose profiler labels the workers keep; nil means none.
	Labels context.Context //nolint:containedctx // only its profiler labels are read
	// Label is the value of the pprof label of the workers; it tells the goroutines of one Streams apart.
	Label string
	// MaxPending bounds the flag sets that wait for a service; 0 means [DefaultMaxPending].
	MaxPending int
}

// Streams are the SSE and websocket services of the streams of flag changes. A core calls [Streams.Broadcast] for
// every change (set it as the Broadcast of the dependencies of the core), and the server serves the connections.
// A change never waits for a client: each service has its own worker and queue, so a stalled websocket delays
// neither the core nor the SSE clients.
type Streams struct {
	ws   stream.WebsocketService
	sse  stream.SSEService
	lane [2]*lane
	// reaper closes the websocket connections that Revalidate took out of the list, away from the swap.
	reaper *reaper
	// publicClosed is set by [Streams.Revalidate] while the runtime has no public flags: the changes of the public flag
	// set that are still queued then are not sent, so that no client without a key gets one after its access ended.
	publicClosed atomic.Bool
}

// NewStreams returns the streams on the stream package services, built with its options. The workers carry the
// profiler labels of labels, so that what started them can find them (a leak test lists them by label).
func NewStreams(labels context.Context, opts ...stream.Option) *Streams {
	return NewStreamsWith(&StreamsConfig{
		Labels:    labels,
		Websocket: stream.NewWebsocketService(opts...), SSE: stream.NewSSEService(opts...),
	})
}

// NewStreamsWith returns the streams on the given services and starts the workers. Call [Streams.Close] to stop them.
func NewStreamsWith(cfg *StreamsConfig) *Streams {
	limit := cfg.MaxPending
	if limit <= 0 {
		limit = DefaultMaxPending
	}

	out := &Streams{ws: cfg.Websocket, sse: cfg.SSE}
	out.reaper = startReaper(cfg.Labels, cfg.Label+"/reaper")
	out.lane[0] = startLane(cfg.Labels, cfg.Label+"/websocket", limit, func(change *flagruntime.Change) {
		// a merged change that undid itself has nothing to tell: the diff is the websocket message
		if !change.Diff.HasDiff() {
			return
		}

		out.ws.BroadcastFlagChanges(change.FlagSet, change.Diff)
	}, out.ws.Close)
	out.lane[1] = startLane(cfg.Labels, cfg.Label+"/sse", limit, func(change *flagruntime.Change) {
		// a change that undid itself still tells the client to fetch again: it may have fetched the flag meanwhile
		// the SSE service fails only when it can not encode its fixed event, it has nothing to tell the core
		var config stream.ConfigIdentity
		if change.Config != nil {
			config = stream.ConfigIdentity{ETag: change.Config.ETag, ModifiedAt: change.Config.ModifiedAt}
		}

		_ = out.sse.BroadcastConfigChange(change.FlagSet, config)
	}, out.sse.Close)

	return out
}

// Revalidate gives an open stream the rights of a new request. It closes the streams of the clients without a key
// when the runtime has no public flags anymore, and the streams of the clients with a key that does not route to the
// flag set of their stream anymore (the key was removed or rotated, the flag set was withdrawn or the key moved to
// another flag set). A key that is still valid is not touched, so a rotation with an overlap disconnects nobody. Set
// it as the AfterSwap hook of the core: it runs before the changes of the swap are queued, so a closed client gets
// none of them. It never waits for a client: the clients are out of the lists when it returns, and the sockets are
// closed by a worker of their own.
func (s *Streams) Revalidate(current *flagruntime.Runtime) {
	keep := ""
	if current.Public != nil {
		keep = flagruntime.PublicName
	}

	s.publicClosed.Store(keep == "")

	if keep == "" {
		// what waits for the clients without a key is dropped with them: nothing queued before the withdrawal can
		// reach a client that connects after the public flags are back
		for _, l := range s.lane {
			l.discard(flagruntime.PublicName)
		}
	}

	s.ws.DisconnectAnonymous(keep)
	s.sse.DisconnectAnonymous(keep)

	routes := func(yield func(string, string) bool) {
		for key, set := range current.ByKey {
			if !yield(key, set.Name) {
				return
			}
		}
	}

	s.sse.DisconnectRevoked(routes)
	s.reaper.offer(s.ws.DisconnectRevoked(routes))
}

// ClientCounts are the clients connected to each stream service.
type ClientCounts struct {
	Websocket, SSE int
}

// Connected is the number of clients connected to each stream service.
func (s *Streams) Connected() ClientCounts {
	return ClientCounts{Websocket: s.ws.Connections(), SSE: s.sse.Connections()}
}

// Broadcast queues the change of a flag set for the clients connected to it and returns at once. A change that
// changes no flag is not queued. Changes of one flag set that wait together are merged into one (the SSE event only
// tells the client to fetch again, the websocket message carries the diff, so the diffs are merged to their net
// effect). When more flag sets wait than the bound, the oldest is dropped and counted.
func (s *Streams) Broadcast(change flagruntime.Change) {
	if !change.Diff.HasDiff() || (change.FlagSet == flagruntime.PublicName && s.publicClosed.Load()) {
		return
	}

	for _, l := range s.lane {
		l.offer(&change)
	}
}

// Dropped returns the number of flag set changes that were dropped because a service did not keep up.
func (s *Streams) Dropped() DropCounts {
	return DropCounts{Websocket: s.lane[0].dropped.Load(), SSE: s.lane[1].dropped.Load()}
}

// DropCounts are the flag set changes that a stream service did not keep up with, by service.
type DropCounts struct {
	Websocket, SSE uint64
}

// Close stops the workers and, once a worker is idle, closes its service. It returns when both ended or when ctx ends,
// whichever is first; a worker that is stuck in a delivery closes its service when the delivery returns.
func (s *Streams) Close(ctx context.Context) error {
	for _, l := range s.lane {
		l.close()
	}

	s.reaper.close()

	for _, done := range []chan struct{}{s.lane[0].done, s.lane[1].done, s.reaper.done} {
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrNotStopped, ctx.Err())
		}
	}

	return nil
}

// AwaitStarted returns once the two workers entered their run loops, or when ctx ends first.
func (s *Streams) AwaitStarted(ctx context.Context) error {
	for _, l := range s.lane {
		select {
		case <-l.started:
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrNotStarted, ctx.Err())
		}
	}

	return nil
}

// lane is the bounded queue and the worker of one stream service.
type lane struct {
	pending map[string]*flagruntime.Change
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	// started is closed when the worker entered its run loop.
	started chan struct{}
	send    func(*flagruntime.Change)
	onExit  func()
	order   []string
	limit   int
	dropped atomic.Uint64
	mu      sync.Mutex
	closed  bool
}

func startLane(labels context.Context, label string, limit int, send func(*flagruntime.Change), onExit func()) *lane {
	queue := &lane{
		pending: make(map[string]*flagruntime.Change), wake: make(chan struct{}, 1), stop: make(chan struct{}),
		done: make(chan struct{}), started: make(chan struct{}), send: send, onExit: onExit, limit: limit,
	}

	go func() {
		// done closes after the labels are restored: a closed worker is joined
		defer close(queue.done)

		pprof.Do(
			labelsOrBackground(labels),
			pprof.Labels("corehttp_streams", label),
			func(context.Context) { queue.run() },
		)
	}()

	return queue
}

func (l *lane) offer(change *flagruntime.Change) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return
	}

	if queued, ok := l.pending[change.FlagSet]; ok {
		l.pending[change.FlagSet] = mergeChanges(queued, change)
	} else {
		if len(l.order) >= l.limit {
			delete(l.pending, l.order[0])
			l.order = l.order[1:]
			l.dropped.Add(1)
		}

		queued := *change
		l.pending[change.FlagSet] = &queued
		l.order = append(l.order, change.FlagSet)
	}

	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// discard drops the change that waits for a flag set.
func (l *lane) discard(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, waiting := l.pending[name]; !waiting {
		return
	}

	delete(l.pending, name)

	l.order = slices.DeleteFunc(l.order, func(queued string) bool { return queued == name })
}

func (l *lane) close() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.closed {
		l.closed = true
		close(l.stop)
	}
}

// next takes the oldest queued change.
func (l *lane) next() (*flagruntime.Change, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.order) == 0 {
		return nil, false
	}

	name := l.order[0]
	l.order = l.order[1:]
	change := l.pending[name]
	delete(l.pending, name)

	return change, true
}

func (l *lane) run() {
	close(l.started)

	defer l.onExit()

	for {
		select {
		case <-l.stop:
			return
		case <-l.wake:
		}

		for {
			select {
			case <-l.stop:
				return
			default:
			}

			change, ok := l.next()
			if !ok {
				break
			}

			l.send(change)
		}
	}
}

// keyState is what a diff says of one flag: its value before and after.
type keyState struct {
	before, after       flagdef.Flag
	hadBefore, hasAfter bool
}

// mergeChanges returns the change that has the net effect of the older change followed by the newer one. The
// generation, the snapshot and the removal flag are the newer ones.
func mergeChanges(older, newer *flagruntime.Change) *flagruntime.Change {
	states := map[string]*keyState{}

	record := func(diff *notifier.DiffCache) {
		for key, flag := range diff.Added {
			apply(states, key, false, nil, true, flag)
		}

		for key, flag := range diff.Deleted {
			apply(states, key, true, flag, false, nil)
		}

		for key, upd := range diff.Updated {
			apply(states, key, true, upd.Before, true, upd.After)
		}
	}

	record(&older.Diff)
	record(&newer.Diff)

	merged := *newer
	merged.Diff = notifier.DiffCache{
		Added:   map[string]flagdef.Flag{},
		Deleted: map[string]flagdef.Flag{},
		Updated: map[string]notifier.DiffUpdated{},
	}

	for key, state := range states {
		switch {
		case !state.hadBefore && state.hasAfter:
			merged.Diff.Added[key] = state.after
		case state.hadBefore && !state.hasAfter:
			merged.Diff.Deleted[key] = state.before
		case state.hadBefore && state.hasAfter:
			merged.Diff.Updated[key] = notifier.DiffUpdated{Before: state.before, After: state.after}
		}
	}

	return &merged
}

// apply folds one step of a flag into its state: the first step fixes the value before, the last one the value after.
func apply(states map[string]*keyState, key string, hadBefore bool, before flagdef.Flag, hasAfter bool,
	after flagdef.Flag,
) {
	state, ok := states[key]
	if !ok {
		state = &keyState{hadBefore: hadBefore, before: before}
		states[key] = state
	}

	state.hasAfter, state.after = hasAfter, after
}

// labelsOrBackground is the context whose profiler labels the workers keep: [pprof.Do] adds its own to them.
func labelsOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}

	return context.WithoutCancel(ctx)
}
