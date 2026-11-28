package relaycore

import (
	"context"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/bootstrap"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/controlsource"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/events"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/flagruntime"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/metric"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/segmentobs"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/segmentstore"
	"github.com/blackbirdworks/semaphore/internal/connection"
	"github.com/blackbirdworks/semaphore/internal/manifestreader"
	"github.com/blackbirdworks/semaphore/internal/manifestreader/statedir"
	"github.com/blackbirdworks/semaphore/modules/core/clock"
	"github.com/blackbirdworks/semaphore/modules/core/flagdef"
)

// Deps are the injected dependencies of a [Core]. Every field is optional.
type Deps struct {
	// Logger receives the logs of every part; nil discards them.
	Logger *slog.Logger
	// Registerer receives the collector of the events; nil registers nothing.
	Registerer prometheus.Registerer
	// Scheduler replaces the timer of the polling loop of the reader (tests drive the rounds with it).
	Scheduler manifestreader.Scheduler
	// SegmentWait replaces the timer of the watcher of the version keys of the dynamic segments; nil is a timer.
	SegmentWait flagruntime.Wait
	// Jitter replaces the random source of the reader: it returns a value in [0, 1).
	Jitter func() float64
	// TuneReader changes the configuration of the reader after it was made from the bootstrap file.
	TuneReader func(*manifestreader.Config)
	// StateFS is the file system of the state directory; nil is the real one.
	StateFS statedir.FS
	// NewManager builds the exporter managers, see [events.RuntimeConfig.NewManager]; nil takes the default.
	NewManager events.NewManagerFunc
	// Broadcast is called with every change of a flag set, see [events.RuntimeConfig.Broadcast].
	Broadcast func(flagruntime.Change)
	// Now dates the flag sets that a build creates; nil is clock.Now.
	Now func() time.Time
	// Hooks are the hooks of the reconcile engine.
	Hooks flagruntime.Hooks
	// Secrets reads the secrets of the file (environment and files).
	Secrets bootstrap.LoadOptions
	// Connection options for the factory (an HTTP client or round tripper, an observer).
	Connection []connection.Option
}

// Status is the status of a core: the joined status of the reader, the source and the engine, the one of the
// exporters and notifiers, and the state directory. It holds no API key and no secret.
type Status struct {
	// Segments is the state of the dynamic segments: the stores, the served segments of each flag set, the counters.
	Segments SegmentsStatus `json:"segments"`
	// StateDirDisabled is the reason there is no state directory although one was asked for; empty otherwise.
	StateDirDisabled string `json:"stateDirDisabled,omitempty"`
	// Events is the status of the exporters and notifiers.
	Events events.Status `json:"events"`
	// Reloads counts the rounds of the engine by outcome since the core was built.
	Reloads ReloadStats `json:"reloads"`
	// Source is the status of the reader, the engine and the running runtime.
	Source controlsource.Status `json:"source"`
	// ShuttingDown is true from the first call of [Core.Shutdown].
	ShuttingDown bool `json:"shuttingDown"`
	// Bootstrapped is true once the first round that carried the bootstrap file ended, whatever its outcome. It is a
	// fact of the engine, not inferred from the snapshot: a file equal to the empty runtime swaps nothing.
	Bootstrapped bool `json:"bootstrapped"`
}

// ReloadStats counts the rounds of the engine (the bootstrap round, every manifest generation and every reload of
// the file) by outcome, and keeps the last ones.
type ReloadStats struct {
	// LastRound is the most recent round of any outcome; nil before the first one.
	LastRound *RoundSummary `json:"lastRound,omitempty"`
	// LastApplied is the most recent applied round; nil when none was applied.
	LastApplied *RoundSummary `json:"lastApplied,omitempty"`
	// LastRejected is the most recent rejected round, kept until a later applied round supersedes it; nil then.
	LastRejected *RoundSummary `json:"lastRejected,omitempty"`
	// LastSuccess is the time of the last applied round, zero when none was applied.
	LastSuccess time.Time `json:"lastSuccess"`
	// FlagsRejected counts the flags rejected at load by what happened to them (RejectedKept, RejectedDropped). A
	// rejected flag counts once, in the applied round whose content brought the rejection (a new object checksum
	// or a flag that was not rejected before); a poll or a generation that leaves it as it was counts nothing.
	FlagsRejected map[string]uint64 `json:"flagsRejected"`
	// Applied, Rejected and Ignored count the rounds by outcome.
	Applied  uint64 `json:"applied"`
	Rejected uint64 `json:"rejected"`
	Ignored  uint64 `json:"ignored"`
}

// RoundSummary is one round of the engine, without its free text: the outcome and the stable code, the generation
// the round was about, and when it ended.
type RoundSummary struct {
	At         time.Time `json:"at"`
	Outcome    string    `json:"outcome"`
	Code       string    `json:"code"`
	Generation uint64    `json:"generation"`
}

// The reasons of semaphore_flag_rejected_total: a flag that failed validation either keeps the definition it had
// (last known good) or, with none to fall back on, is dropped. They are the whole set; the flag set is no label.
const (
	RejectedKept    = "kept"
	RejectedDropped = "dropped"
)

// rejection identifies a rejected flag of one version of an object.
type rejection struct{ object, checksum, flag string }

// reloadCounter keeps the [ReloadStats]; the engine goroutine writes, Status reads.
type reloadCounter struct {
	// seen are the rejections of the content that runs, to count only the new ones; the engine goroutine only.
	seen  map[rejection]struct{}
	stats ReloadStats
	mu    sync.Mutex
}

// countRejected counts the rejected flags of the objects that were not rejected in the previous content.
func (r *reloadCounter) countRejected(objects map[string]*flagruntime.ObjectStatus) map[string]uint64 {
	added := map[string]uint64{}
	now := make(map[rejection]struct{}, len(r.seen))

	for _, obj := range objects {
		for _, rej := range obj.Rejected {
			key := rejection{object: obj.Name, checksum: obj.Checksum, flag: rej.Flag}
			now[key] = struct{}{}

			if _, known := r.seen[key]; known {
				continue
			}

			if rej.Kept {
				added[RejectedKept]++
			} else {
				added[RejectedDropped]++
			}
		}
	}

	r.seen = now

	return added
}

func (r *reloadCounter) record(res *flagruntime.RoundResult) {
	now := clock.Now().UTC()
	round := &RoundSummary{At: now, Outcome: string(res.Outcome), Code: res.Code, Generation: res.Generation}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.stats.LastRound = round

	switch res.Outcome {
	case flagruntime.OutcomeApplied:
		r.stats.LastSuccess = now
		r.stats.LastApplied = round
		r.stats.LastRejected = nil
		r.stats.Applied++
	case flagruntime.OutcomeRejected:
		r.stats.LastRejected = round
		r.stats.Rejected++
	case flagruntime.OutcomeIgnored:
		r.stats.Ignored++
	}
}

// recordRejected counts the flags that the round of res rejected, when it stored content.
func (r *reloadCounter) recordRejected(res *flagruntime.RoundResult, objects map[string]*flagruntime.ObjectStatus) {
	if res.Outcome != flagruntime.OutcomeApplied || !res.ContentApplied {
		return
	}

	added := r.countRejected(objects)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stats.FlagsRejected == nil {
		r.stats.FlagsRejected = map[string]uint64{}
	}

	for reason, n := range added {
		r.stats.FlagsRejected[reason] += n
	}
}

// snapshot returns a copy; the summaries it points to are never changed.
func (r *reloadCounter) snapshot() ReloadStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := r.stats
	out.FlagsRejected = maps.Clone(r.stats.FlagsRejected)

	return out
}

type phase uint8

const (
	phaseNew phase = iota
	phaseRunning
	phaseClosed
)

// Core is the assembled relay core.
type Core struct {
	log     *slog.Logger
	factory *connection.Factory
	svc     *flagdef.Service
	holder  *flagruntime.Holder
	engine  *flagruntime.Engine
	eval    *flagruntime.Evaluator
	reader  *manifestreader.Reader
	source  *controlsource.Source
	events  *events.Runtime
	state   *statedir.Store
	// segs owns the stores of the dynamic segments.
	segs *segmentstore.Connections
	// segObs counts what the resolvers of the dynamic segments do, for each connection.
	segObs *segmentobs.Observers
	// segWait is the timer of the watcher of the version keys.
	segWait flagruntime.Wait
	// jitter is the serialized source of Deps.Jitter, nil when there is none.
	jitter func() float64
	// segJitter spreads the first poll of each binding of the dynamic segments.
	segJitter func(time.Duration) time.Duration
	// readerMetrics is the collector of the reader of the manifest, registered when there is a registry.
	readerMetrics *metric.ManifestReader
	collector     prometheus.Collector
	registry      prometheus.Registerer
	cancel        context.CancelFunc
	// runDone is closed when the loops of Run ended and the state directory was flushed.
	runDone chan struct{}
	// finished is closed when the first Shutdown did its work, with its error in shutdownErr (written before the
	// close).
	finished chan struct{}
	// released is closed when the cleanup ran, exactly once, after the loops ended.
	released      chan struct{}
	shutdownErr   error
	unknownFields unknownTracker

	stateDisabled string
	reloads       reloadCounter

	// mu guards phase and cancel: the one place the lifecycle is decided.
	mu sync.Mutex
	// releaseOnce runs the cleanup once.
	releaseOnce sync.Once
	// shutdownStarted lets the first Shutdown do the work.
	shutdownStarted atomic.Bool
	phase           phase
	// segObsRegistered is true when segObs is registered with the registry.
	segObsRegistered bool
}

// Holder returns the holder of the current runtime: handlers load the runtime from it for every request.
func (c *Core) Holder() *flagruntime.Holder { return c.holder }

// Evaluator returns the evaluator of the flag sets. The events of the flags that track them reach the exporters.
func (c *Core) Evaluator() *flagruntime.Evaluator { return c.eval }

// Collect hands an event to the exporters, the way the evaluator does for the events of the flags that track them.
// It reports whether the event was accepted. The event belongs to the exporters from the call: the caller must not
// change it afterwards.
func (c *Core) Collect(event events.Event) bool { return c.events.Collect(event) }

// DropNotPublic counts the events of a caller without a key that were refused: their flag is not a public one.
func (c *Core) DropNotPublic(count int) { c.events.DropNotPublic(count) }

// Status returns a snapshot of the status. It reads atomics and immutable values and never blocks.
func (c *Core) Status() Status {
	source := c.source.Status()
	if c.state != nil {
		// the writes end between the rounds of the reader: the state of the directory is read now
		source.Reader.State = c.state.Hooks().StateStatus()
	}

	return Status{
		StateDirDisabled: c.stateDisabled, Events: c.events.Status(), Source: source,
		Reloads: c.reloads.snapshot(), ShuttingDown: c.shutdownStarted.Load(),
		Bootstrapped: c.engine.Bootstrapped(), Segments: c.segmentsStatus(),
	}
}

// readerStatus is the status of the reader with the state of the state directory as it is now.
func (c *Core) readerStatus() manifestreader.Status {
	status := c.reader.Status()
	if c.state != nil {
		status.State = c.state.Hooks().StateStatus()
	}

	return status
}
