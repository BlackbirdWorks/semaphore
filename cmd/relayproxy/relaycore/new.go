package relaycore

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/api/opentelemetry"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/bootstrap"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/controlsource"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/events"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/flagruntime"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/metric"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/segmentobs"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/segmentstore"
	"github.com/blackbirdworks/semaphore/internal/connection"
	"github.com/blackbirdworks/semaphore/internal/manifestreader"
	"github.com/blackbirdworks/semaphore/modules/core/flagdef"
	"github.com/blackbirdworks/semaphore/modules/core/segment/resolver"
)

// New builds the core of a validated bootstrap file. The context bounds the build (the exporters and notifiers); it
// does not outlive New. It starts no goroutine of the loops (see [Core.Run]), but it
// opens the state directory and starts the exporters and notifiers, so a core that was built must be stopped with
// [Core.Shutdown]. An error names the setting at fault (see [BuildError]) and never holds a secret. The core
// keeps cfg: the caller must not change it afterwards.
func New(ctx context.Context, cfg *bootstrap.Config, deps *Deps) (*Core, error) {
	if deps == nil {
		deps = &Deps{}
	}

	if cfg == nil {
		return nil, ErrNilConfig
	}

	plane := cfg.ControlPlane
	if plane == nil {
		return nil, &BuildError{Path: "controlPlane", Err: ErrNoControlPlane}
	}

	if _, ok := cfg.Connections[plane.Connection]; !ok {
		return nil, &BuildError{Path: "controlPlane.connection", Err: ErrUnknownConnection}
	}

	// the file asked for these admin keys: the relay never serves with fewer than that
	_, err := flagruntime.ResolveAdminKeys(cfg, deps.Secrets)
	if err != nil {
		return nil, &BuildError{Path: "auth.adminKeys", Err: err}
	}

	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	core := &Core{
		log:      logger,
		factory:  connection.New(append([]connection.Option{connection.WithLogger(logger)}, deps.Connection...)...),
		holder:   flagruntime.NewHolder(),
		registry: deps.Registerer,
		runDone:  make(chan struct{}),
		finished: make(chan struct{}),
		released: make(chan struct{}),
		// the reader and the poll of the segments draw from one source, from two goroutines: it is serialized once
		jitter: SerializeJitter(deps.Jitter),
	}

	err = core.assemble(ctx, cfg, deps)
	if err != nil {
		core.release(ctx)

		return nil, err
	}

	return core, nil
}

func (c *Core) assemble(ctx context.Context, cfg *bootstrap.Config, deps *Deps) error {
	plane := cfg.ControlPlane

	err := registerConnections(c.factory, cfg, deps.Secrets)
	if err != nil {
		return err
	}

	stores, err := objectStores(c.factory, cfg)
	if err != nil {
		return err
	}

	rcfg := readerConfig(plane)
	if deps.TuneReader != nil {
		deps.TuneReader(rcfg)
	}

	err = checkPolling(rcfg)
	if err != nil {
		return err
	}

	//nolint:contextcheck // the state directory opens without a context
	state, hooks, reason := openStateDir(plane, rcfg, deps, c.log)
	c.state, c.stateDisabled = state, reason

	// the state directory holds what the engine applied: the reader offers its snapshots, the engine decides
	var persist *persister

	if hooks.Save != nil {
		persist = newPersister(hooks.Save, func() uint64 { return c.engine.Taken() })
		hooks.Save = persist.offer
	}

	opts := []manifestreader.Option{
		manifestreader.WithLogger(c.log), manifestreader.WithHooks(hooks), manifestreader.WithObjectStores(stores),
	}
	if deps.Scheduler != nil {
		opts = append(opts, manifestreader.WithScheduler(deps.Scheduler))
	}

	// the reader and the poll of the segments draw from one source, from two goroutines: it is serialized once
	if c.jitter != nil {
		opts = append(opts, manifestreader.WithJitter(c.jitter))
	}

	c.reader, err = manifestreader.New(stores[plane.Connection], rcfg, opts...)
	if err != nil {
		return &BuildError{Path: "controlPlane.manifest", Err: err}
	}

	c.svc = flagdef.NewService(flagdef.WithLogger(c.log))
	engineHooks := deps.Hooks
	afterRound := engineHooks.AfterRound
	engineHooks.AfterRound = func(res flagruntime.RoundResult) {
		c.recordRound(ctx, &res)
		c.reloads.recordRejected(&res, c.holder.Load().Status.Objects)

		if persist != nil {
			persist.decided(&res, c.holder.Load().Status.Objects)
		}

		if afterRound != nil {
			afterRound(res)
		}
	}

	c.engine = c.newEngine(ctx, cfg, deps, engineHooks)
	c.source = controlsource.New(c.reader, c.engine, c.holder, c.log)

	return c.assembleEvents(ctx, cfg, deps)
}

// newEngine builds the engine and the stores of the dynamic segments that it binds its generations to.
func (c *Core) newEngine(
	ctx context.Context, cfg *bootstrap.Config, deps *Deps, hooks flagruntime.Hooks,
) *flagruntime.Engine {
	c.segWait = deps.SegmentWait
	if c.segWait == nil {
		c.segWait = timerWait
	}

	c.segJitter = flagruntime.RandomSpread
	if c.jitter != nil {
		c.segJitter = func(limit time.Duration) time.Duration { return time.Duration(c.jitter() * float64(limit)) }
	}

	metrics := cfg.Observability.Metrics
	c.segObs = segmentobs.New(segmentobs.Options{
		PerSegment: metrics.PerSegmentLabels, MaxSegments: int(metrics.MaxSegmentLabels),
	})
	c.registerSegmentSeries(ctx)

	c.segs = segmentstore.NewConnections(cfg, segmentstore.Options{Logger: c.log, Secrets: deps.Secrets})

	// the series of every connection that can serve segments exist from the start, at zero
	for _, store := range c.segs.States() {
		c.segObs.For(store.Name)
	}

	return flagruntime.NewEngine(c.log, c.svc, c.holder,
		flagruntime.WithHooks(hooks),
		flagruntime.WithNow(deps.Now),
		flagruntime.WithSegmentStores(c.segs),
		flagruntime.WithSegments(&flagruntime.SegmentEnv{
			Binder:      c.segs,
			ObserverFor: func(connection string) resolver.Observer { return c.segObs.For(connection) },
			Resolver:    resolverSettings(cfg),
			Traced: opentelemetry.Enabled(&opentelemetry.Settings{
				OTLPEndpoint: cfg.Observability.Tracing.OTLPEndpoint,
			}),
		}),
		flagruntime.WithSecretOptions(deps.Secrets),
		flagruntime.WithInitialBootstrap(cfg),
	)
}

// assembleEvents builds the exporters and notifiers and the evaluator that feeds them. It is the last step: the
// exporters are running when it succeeds, and nothing after it can fail.
func (c *Core) assembleEvents(ctx context.Context, cfg *bootstrap.Config, deps *Deps) error {
	c.events = events.NewRuntime(&events.RuntimeConfig{
		Logger:     c.log,
		Broadcast:  deps.Broadcast,
		NewManager: deps.NewManager,
		Builder:    events.Builder{Factory: c.factory, Secrets: deps.Secrets},
	})

	_, err := c.events.Reload(ctx, events.SpecsFrom(cfg))
	if err != nil {
		return &BuildError{Path: "exporters, notifiers", Err: err}
	}

	c.eval = flagruntime.NewEvaluator(c.svc, &flagruntime.EvaluatorConfig{
		EvaluationContextEnrichment: cfg.Evaluation.ContextEnrichment,
		Environment:                 cfg.Evaluation.Environment,
		DisableSemaphoreMetadata:    cfg.Evaluation.DisableSemaphoreMetadata,
		TrackEvent:                  func(event events.Event) { c.events.Collect(event) },
	})

	// the admin keys of the file reach the first runtime through the first round of the engine
	c.engine.Reload(cfg)

	if c.registry != nil {
		c.collector = events.NewCollector(c.events)

		err = c.registry.Register(c.collector)
		if err != nil {
			c.collector = nil

			return &BuildError{Path: "observability.metrics", Err: fmt.Errorf("register the events collector: %w", err)}
		}

		// the reader of the manifest: its polls and the writes of the state directory, with the connection as the one label
		c.readerMetrics, err = metric.NewManifestReader(c.registry, cfg.ControlPlane.Connection, c.readerStatus)
		if err != nil {
			return &BuildError{Path: "observability.metrics", Err: fmt.Errorf("register the reader metrics: %w", err)}
		}
	}

	return nil
}

// resolverSettings is the resolver section of the file with the defaults applied (the defaults alone when the file has
// no segments section), the same that the segment tool of the CLI reads.
func resolverSettings(cfg *bootstrap.Config) *bootstrap.SegmentResolver {
	settings := cfg.EffectiveResolver()

	return &settings
}

// SerializeJitter makes a source of jitter (a value in [0, 1)) safe to call from several goroutines: the manifest
// reader and the poll of the dynamic segments both draw from [Deps.Jitter]. Nil stays nil.
func SerializeJitter(source func() float64) func() float64 {
	if source == nil {
		return nil
	}

	var mu sync.Mutex

	return func() float64 {
		mu.Lock()
		defer mu.Unlock()

		return source()
	}
}

// registerSegmentSeries registers the series of the resolvers. A failed registration only loses the series: the
// counters still feed /info.
func (c *Core) registerSegmentSeries(ctx context.Context) {
	if c.registry == nil {
		return
	}

	err := c.registry.Register(c.segObs)
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelWarn, "the segment series are not registered", slog.String("error", err.Error()))

		return
	}

	c.segObsRegistered = true
}

// recordRound counts a round, then follows the live segments of the generation that it applied.
func (c *Core) recordRound(ctx context.Context, res *flagruntime.RoundResult) {
	c.reloads.record(res)
	c.noteUnknownFields(ctx, res)
	c.noteLiveSegments(ctx, res)
}

// noteLiveSegments follows the live catalog: the names of the served dynamic segments are the ones that have a series
// by segment name.
func (c *Core) noteLiveSegments(ctx context.Context, res *flagruntime.RoundResult) {
	if !res.ContentApplied {
		return
	}

	runtime := c.holder.Load()
	c.segObs.SetLive(runtime.DynamicSegmentNames())
	c.warnUnresolved(ctx, runtime)
}

// warnUnresolved writes one line for the applied generation when flags use segment names that no segments object of
// their flag set defines: such a name answers "not a member" for ever, which is nearly always a typo. The flag sets and
// names are bounded; names only.
func (c *Core) warnUnresolved(ctx context.Context, current *flagruntime.Runtime) {
	var total int

	var entries []string

	for _, name := range slices.Sorted(maps.Keys(current.Status.FlagSets)) {
		counts := current.Status.FlagSets[name].Segments
		if counts == nil || counts.Unresolved == 0 {
			continue
		}

		total += counts.Unresolved

		for _, segName := range counts.UnresolvedNames {
			entries = append(entries, name+"/"+segName)
		}
	}

	if total == 0 {
		return
	}

	c.log.LogAttrs(ctx, slog.LevelWarn,
		"flags use segment names that no segments object of their flag set defines: they answer not a member",
		slog.Int("unresolved", total),
		slog.Any("names", entries[:min(len(entries), maxLoggedUnresolved)]))
}

// maxLoggedUnresolved bounds the names of the line that reports unresolved segment names.
const maxLoggedUnresolved = 10
