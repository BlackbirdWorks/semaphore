package flagruntime

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/bootstrap"
	"github.com/blackbirdworks/semaphore/modules/core/clock"
	"github.com/blackbirdworks/semaphore/modules/core/flagdef"
)

// DefaultChangeBuffer is the default size of the change channel.
const DefaultChangeBuffer = 64

// Hooks are injection points for tests and for the wiring PR. A nil hook is not called. Hooks run on the
// engine goroutine: a blocking hook blocks the engine.
type Hooks struct {
	// OnParse is called for every object parsed.
	OnParse func(ParseEvent)
	// BeforeSwap is called with the candidate just before the store. An error refuses the candidate: nothing is
	// stored and the round is rejected with [CodeVetoed].
	BeforeSwap func(candidate *Runtime) error
	// OnEmit is called after a change was handed to the channel of [Engine.Changes].
	OnEmit func(Change)
	// AfterSwap is called with the new runtime right after it is stored, before the changes of the swap are queued.
	AfterSwap func(*Runtime)
	// AfterRound is called at the end of every round, after the store and the change notifications.
	AfterRound func(RoundResult)
}

// Option configures an [Engine].
type Option func(*Engine)

// WithChangeBuffer sets the size of the change channel; values below 1 mean 1.
func WithChangeBuffer(size int) Option {
	return func(e *Engine) { e.changeBuf = max(1, size) }
}

// WithWithdrawnCap sets how many withdrawn flag-set names the status keeps (default [DefaultWithdrawnCap]).
func WithWithdrawnCap(size int) Option {
	return func(e *Engine) { e.withdrawnCap = size }
}

// WithNow sets the clock that dates the flag sets a build creates ([FlagSet.ModifiedAt]); the default is
// [clock.Now].
func WithNow(now func() time.Time) Option {
	return func(e *Engine) { e.now = now }
}

// WithHooks sets the hooks.
func WithHooks(hooks Hooks) Option {
	return func(e *Engine) { e.hooks = hooks }
}

// WithSecretOptions sets how the admin keys that are secret references are resolved.
func WithSecretOptions(opts bootstrap.LoadOptions) Option {
	return func(e *Engine) { e.secrets = opts }
}

// WithInitialBootstrap sets the configuration the process started with: the baseline for pendingRestart. Without
// it the first configuration given to [Engine.Reload] is the baseline.
func WithInitialBootstrap(cfg *bootstrap.Config) Option {
	return func(e *Engine) { e.baseline = cfg }
}

// WithSegmentStores tells the engine what the connections of the bootstrap file can serve, so that a dynamic segment
// that names one that cannot is rejected when its generation is built. The connections are settings that need a
// restart, so the table of the process start is the one that counts.
func WithSegmentStores(stores SegmentStores) Option {
	return func(e *Engine) { e.stores = stores }
}

// WithSegments tells the engine how to serve the dynamic segments: the process-level binder, the settings of the
// resolvers and the observer that counts what they do. Without it the dynamic segments are not served.
func WithSegments(env *SegmentEnv) Option {
	return func(e *Engine) { e.segEnv = env }
}

// Engine is the reconcile engine. One goroutine ([Engine.Run]) is the only writer of the [Holder]; the other
// methods only hand it work through channels.
//
// Ownership: a *Content or a *bootstrap.Config passed to Submit or Reload belongs to the engine from that moment.
// The engine does not copy it, and the caller must not change it afterwards (nor the maps and byte slices it
// reaches). The same holds for everything a Runtime reaches.
//
// Withdrawn flag sets: a flag set that the publisher removes leaves FlagSets and ByKey in that generation. Its
// keys are unknown from then on (the relay answers as for any unknown key) and are free for another flag set at
// once. The Status lists the most recently withdrawn names with the generation, up to a cap (default
// [DefaultWithdrawnCap]); the engine keeps no other state for a withdrawn flag set.
//
// Events: the events of one flag set reach the consumer in order; events of different flag sets are not ordered
// by generation (a pending event keeps the place of the first change it merged).
//
// A round: take the latest content and the latest bootstrap configuration (a burst is coalesced); validate the
// content (generation, shape); build the candidate off to the side (parse what changed, rebuild the flag sets
// that use it); compose the candidate Runtime with the admin keys and the status; store it with one atomic
// pointer store; then commit the builder (the old parse results are released), compute one change per rebuilt
// flag set and queue it for the consumer. A rejected candidate stores nothing.
//
// Change events: the consumer reads [Engine.Changes], a channel of bounded size. The engine never blocks on
// it: an event that does not fit in the channel is kept per flag set in a pending table and merged with later
// changes of the same flag set (the diff goes from the state the consumer last saw to the newest one), and it is
// handed to the channel when there is room. An event can be merged or cancelled only while it is in the pending
// table, before it was handed to the channel; with free room in the channel it is handed over at once, so a
// creation and a withdrawal can arrive as two events. The table holds at most one entry per flag set name, so it
// is bounded by the number of flag sets, and no name is lost. Events still in the pending table when Run returns
// are dropped, and the channel is closed.
type Engine struct {
	stores     SegmentStores
	segEnv     *SegmentEnv
	hooks      Hooks
	builder    *Builder
	announced  map[string]struct{}
	content    chan *Content
	boot       chan *bootstrap.Config
	changes    chan Change
	queue      *pendingQueue
	roundDone  atomic.Pointer[chan struct{}]
	logger     *slog.Logger
	last       atomic.Pointer[RoundResult]
	running    map[string]runningObject
	cur        *Runtime
	rejected   *Reload
	baseline   *bootstrap.Config
	holder     *Holder
	decided    atomic.Pointer[decision]
	secrets    bootstrap.LoadOptions
	roundStart time.Time
	heldByKey  map[string]*FlagSet
	heldSets   map[string]*FlagSetStatus
	heldAdmin  map[string]struct{}
	nextAdmin  map[string]struct{}
	nextByKey  map[string]*FlagSet
	nextSets   map[string]*FlagSetStatus
	collided   map[string]struct{}
	// roundRejected is the rejection of the content of the round, when it was rejected: the decision of its revision.
	roundRejected *Reload
	now           func() time.Time
	nextCollided  []string
	taken         atomic.Uint64
	withdrawnCap  int
	changeBuf     int
	roundStats    reloadStats
	snapshot      uint64
	roundRevision uint64
	bootstrapped  atomic.Bool
	roundContent  bool
	roundHadBoot  bool
}

// NewEngine returns an engine that stores into holder. svc validates the flags and owns the compiled-query
// caches; the caller owns it. A nil logger discards.
func NewEngine(logger *slog.Logger, svc *flagdef.Service, holder *Holder, opts ...Option) *Engine {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	eng := &Engine{
		logger:    logger,
		holder:    holder,
		content:   make(chan *Content, 1),
		boot:      make(chan *bootstrap.Config, 1),
		queue:     newPendingQueue(),
		announced: map[string]struct{}{},
		changeBuf: DefaultChangeBuffer,
		cur:       holder.Load(),
	}

	for _, opt := range opts {
		opt(eng)
	}

	eng.changes = make(chan Change, eng.changeBuf)
	eng.builder = NewBuilder(svc, BuilderOptions{
		OnParse: eng.hooks.OnParse, WithdrawnCap: eng.withdrawnCap, SegmentStores: eng.stores, Segments: eng.segEnv,
		Now: eng.now,
	})
	eng.snapshot = eng.cur.Snapshot

	first := make(chan struct{})
	eng.roundDone.Store(&first)

	return eng
}

// Submit hands a content to the engine. It never blocks: a content that the engine has not taken yet is
// replaced (latest wins). Submit may be called from any goroutine.
func (e *Engine) Submit(content *Content) {
	for {
		select {
		case e.content <- content:
			return
		default:
		}

		select {
		case <-e.content:
		default:
		}
	}
}

// Reload hands a bootstrap configuration to the engine, with the same latest-wins rule as [Engine.Submit].
func (e *Engine) Reload(cfg *bootstrap.Config) {
	for {
		select {
		case e.boot <- cfg:
			return
		default:
		}

		select {
		case <-e.boot:
		default:
		}
	}
}

// Changes returns the channel of change events; it is closed when Run returns. See [Engine] for when an event can
// still be merged or cancelled: only before the engine hands it to this channel.
func (e *Engine) Changes() <-chan Change { return e.changes }

// Bootstrapped reports whether the first round that carried a bootstrap file has ended, applied, ignored or
// rejected: the relay then knows what its bootstrap file says, although the runtime may not have swapped (a file
// equal to the empty runtime leaves the snapshot at zero).
func (e *Engine) Bootstrapped() bool { return e.bootstrapped.Load() }

// Result returns the result of the last round, applied, ignored or rejected. A rejected round stores nothing, so
// its reason is not in the status of the running Runtime: it is read here (the wiring of the relay exposes it),
// and it is carried by the Status of the next Runtime as LastRejected. Result is safe to call from any
// goroutine; before the first round it returns the zero RoundResult. The result of a rejected or ignored round
// differs from the Status.LastReload of the stored runtime, which keeps the last applied round.
func (e *Engine) Result() RoundResult {
	if res := e.last.Load(); res != nil {
		return *res
	}

	return RoundResult{}
}

// decision is what the engine decided of the content of one revision: applied (the runtime serves it, or a later
// one), ignored (it changed nothing) or rejected, with its code.
type decision struct {
	reload   Reload
	revision uint64
	// running is the generation of the runtime the round left stored: read in the same step as the decision.
	running uint64
}

// Decision is what the engine decided of a revision, and the generation it served when it decided: the two are one
// fact, so a swap that comes after the decision never changes the answer.
type Decision struct {
	Reload Reload
	// Running is the generation of the runtime that was stored when the decision was made: the generation of the
	// decision when it was applied, the last known good one when it was rejected or ignored.
	Running uint64
}

// Taken is the revision of the content the engine took last, zero before any. The engine decides it next, and no
// older revision is ever decided: whoever keeps content by revision can forget the older ones.
func (e *Engine) Taken() uint64 { return e.taken.Load() }

// Await returns when the engine has decided the content of the revision, or of a later one (the later content
// replaces it: the engine keeps the latest only): the Reload is the decision, applied, ignored or rejected with its
// code. Revision 0 means nothing to wait for. It waits on the end of the rounds, never polls, and ends with ctx.
func (e *Engine) Await(ctx context.Context, revision uint64) (Reload, error) {
	decided, err := e.AwaitDecision(ctx, revision)

	return decided.Reload, err
}

// AwaitDecision is [Engine.Await] with the generation the relay served when the decision was made, taken with the
// decision and not read afterwards: a later swap does not change it.
func (e *Engine) AwaitDecision(ctx context.Context, revision uint64) (Decision, error) {
	if revision == 0 {
		current := e.holder.Load()

		return Decision{Reload: current.Status.LastReload, Running: current.Generation}, nil
	}

	for {
		done := *e.roundDone.Load()

		if decided := e.decided.Load(); decided != nil && decided.revision >= revision {
			return Decision{Reload: decided.reload, Running: decided.running}, nil
		}

		select {
		case <-done:
		case <-ctx.Done():
			return Decision{}, fmt.Errorf("waiting for the revision %d: %w", revision, ctx.Err())
		}
	}
}

// Run is the reconcile loop. It returns when ctx is done; it starts no other goroutine, so nothing is left
// behind. Run must be called once.
func (e *Engine) Run(ctx context.Context) {
	defer close(e.changes)

	for {
		var (
			out  chan<- Change
			head Change
		)

		front := e.queue.front()
		if front != nil {
			out, head = e.changes, front.change
		}

		select {
		case <-ctx.Done():
			return
		case content := <-e.content:
			var cfg *bootstrap.Config

			takeLatest(e.boot, &cfg)
			e.round(ctx, content, cfg)
		case cfg := <-e.boot:
			var content *Content

			takeLatest(e.content, &content)
			e.round(ctx, content, cfg)
		case out <- head:
			e.delivered(front)
		}
	}
}

// delivered takes a change that was handed to the consumer out of the queue.
func (e *Engine) delivered(item *pendingChange) {
	e.queue.remove(item)

	if item.change.Removed {
		delete(e.announced, item.change.FlagSet)
	}

	if e.hooks.OnEmit != nil {
		e.hooks.OnEmit(item.change)
	}
}

// takeLatest stores the value waiting on a latest-wins channel in out; out is left alone when there is none.
func takeLatest[T any](ch chan T, out *T) {
	select {
	case value := <-ch:
		*out = value
	default:
	}
}

type contentPlan struct {
	built   *Built
	running map[string]runningObject
	res     Reload
}

type bootPlan struct {
	admin   map[string]struct{}
	pending []string
	res     Reload
	apply   bool
}

// round runs one reconcile round for the inputs it was woken with.
func (e *Engine) round(ctx context.Context, content *Content, cfg *bootstrap.Config) {
	var (
		plan    contentPlan
		boot    bootPlan
		results []Reload
	)

	e.roundHadBoot = cfg != nil
	e.roundStart, e.roundStats, e.roundRejected = clock.Now(), reloadStats{}, nil

	e.roundRevision, e.roundContent = 0, false
	if content != nil {
		e.roundRevision = content.Revision
		e.taken.Store(content.Revision)
	}

	if content != nil {
		plan = e.planContent(ctx, content)
		if plan.res.Outcome == OutcomeRejected {
			rejected := plan.res
			e.roundRejected = &rejected
		}
	}

	if cfg != nil {
		boot = e.planBootstrap(cfg)
	}

	if plan.res.Outcome != "" {
		results = append(results, plan.res)
	}

	if boot.res.Outcome != "" {
		results = append(results, boot.res)
	}

	bootChanged := boot.apply && (!maps.Equal(boot.admin, e.rawAdmin()) ||
		!slices.Equal(boot.pending, e.cur.Status.PendingRestart))

	if plan.built == nil && !bootChanged {
		e.adoptBaseline(&boot, cfg)
		e.finish(ctx, results, nil, 0)

		return
	}

	veto, events := e.swap(ctx, &plan, &boot)
	if veto != nil {
		e.finish(ctx, append(results, *veto), nil, 0)

		return
	}

	e.adoptBaseline(&boot, cfg)

	e.roundContent = plan.built != nil

	applied := e.cur.Status.LastReload

	e.finish(ctx, results, &applied, events)
}

// adoptBaseline makes the first accepted configuration the baseline of pendingRestart: a configuration that was
// rejected or vetoed is never the baseline.
func (e *Engine) adoptBaseline(boot *bootPlan, cfg *bootstrap.Config) {
	if boot.apply && e.baseline == nil {
		e.baseline = cfg
	}
}

// swap assembles the candidate and stores it with one atomic store. It returns the rejection when the
// BeforeSwap hook refused the candidate; nothing is stored then and the builder is not committed.
func (e *Engine) swap(ctx context.Context, plan *contentPlan, boot *bootPlan) (*Reload, int) {
	candidate := e.assemble(plan.built, boot)

	if e.hooks.BeforeSwap != nil {
		err := e.hooks.BeforeSwap(candidate)
		if err != nil {
			rej := Reload{
				Outcome: OutcomeRejected, Code: CodeVetoed, Generation: candidate.Generation,
				Detail: fmt.Sprintf("%s: %s", ErrVetoed, err),
			}
			e.noteRejected(rej)

			return &rej, 0
		}
	}

	prior := e.disabledAdminKeys()

	// the one store: a reader sees the old Runtime or the new one, never a mix
	e.holder.store(candidate)
	e.cur = candidate
	e.snapshot = candidate.Snapshot
	e.heldByKey, e.heldSets, e.heldAdmin = e.nextByKey, e.nextSets, e.nextAdmin
	e.logCollisions(ctx, candidate.Generation, e.nextCollided, candidate.Status.DisabledAdminKeys, prior)

	if e.hooks.AfterSwap != nil {
		e.hooks.AfterSwap(candidate)
	}

	events := 0

	if plan.built != nil {
		e.builder.Commit(plan.built)
		e.running = plan.running
		events, e.roundStats = e.enqueue(plan.built, candidate)
	}

	return nil, events
}

// assemble builds the candidate Runtime: the built content (or the running one) plus the admin keys and the
// status of the round.
func (e *Engine) assemble(built *Built, boot *bootPlan) *Runtime {
	base := e.cur
	status := *base.Status
	next := &Runtime{
		FlagSets: base.FlagSets, ByKey: base.ByKey, AdminKeys: base.AdminKeys, Generation: base.Generation,
		Public: base.Public, Snapshot: e.snapshot + 1,
	}

	if built != nil {
		next.FlagSets, next.ByKey, next.Generation = built.Runtime.FlagSets, built.Runtime.ByKey, built.Runtime.Generation
		status.FlagSets, status.Objects = built.Runtime.Status.FlagSets, built.Runtime.Status.Objects
		status.Entries, status.Withdrawn = built.Runtime.Status.Entries, built.Runtime.Status.Withdrawn
		next.Public, status.Public = built.Runtime.Public, built.Runtime.Status.Public
	}

	if boot.apply {
		next.AdminKeys = boot.admin
		status.PendingRestart = boot.pending
	}

	// the admin keys win over the keys of the flag sets: a colliding key is not routed, and the flag set reports it
	raw := e.held()
	if built != nil {
		raw = heldRouting{byKey: built.Runtime.ByKey, sets: built.Runtime.Status.FlagSets}
	}

	admin := e.rawAdmin()
	if boot.apply {
		admin = boot.admin
	}

	derived, enabled, names := deriveRouting(admin, raw)
	next.ByKey, next.AdminKeys, status.FlagSets = derived.byKey, enabled, derived.sets
	status.DisabledAdminKeys = len(admin) - len(enabled)
	e.nextByKey, e.nextSets, e.nextAdmin, e.nextCollided = raw.byKey, raw.sets, admin, names

	status.Generation, status.Snapshot = next.Generation, next.Snapshot
	status.LastRejected = e.rejected
	status.LastReload = Reload{
		Outcome: OutcomeApplied, Code: CodeApplied, Generation: next.Generation, Snapshot: next.Snapshot,
	}
	next.Status = &status

	return next
}

func (e *Engine) noteRejected(res Reload) {
	e.rejected = &res
}

func (e *Engine) planContent(ctx context.Context, content *Content) contentPlan {
	content = withChecksums(content)

	err := validateContent(content)
	if err != nil {
		return e.reject(content.Generation, CodeInvalidContent, err)
	}

	if content.Generation < e.cur.Generation {
		return contentPlan{res: Reload{
			Outcome:    OutcomeIgnored,
			Code:       CodeStaleGeneration,
			Generation: content.Generation,
			Detail: fmt.Sprintf(
				"%s: generation %d is older than %d", ErrStaleGeneration, content.Generation, e.cur.Generation,
			),
		}}
	}

	if content.Generation == e.cur.Generation {
		switch compareRunning(e.running, content) {
		case sameContent:
			return contentPlan{res: Reload{
				Outcome: OutcomeIgnored, Code: CodeDuplicate, Generation: content.Generation,
				Detail: fmt.Sprintf("%s: generation %d is already running", ErrStaleGeneration, content.Generation),
			}}
		case conflictingContent:
			return e.reject(content.Generation, CodeGenerationConflict, fmt.Errorf(
				"%w: generation %d is running with other content", ErrGenerationConflict, content.Generation))
		case stateChange:
		}
	}

	built, err := e.builder.Build(ctx, content)
	if err != nil {
		return e.reject(content.Generation, CodeBuildFailed, err)
	}

	for i := range built.SegmentRejections {
		rejection := &built.SegmentRejections[i]

		e.logRejection(ctx, rejection, content.Generation)
	}

	return contentPlan{built: built, running: describe(content)}
}

// logRejection logs a segments object that the build met with a problem: rejected whole (its last good content stays)
// or, when some of its dynamic definitions are not served by the connections, rejected in part.
func (e *Engine) logRejection(ctx context.Context, rejection *SegmentRejection, generation uint64) {
	attrs := []slog.Attr{
		slog.String("object", rejection.Object), slog.String("checksum", rejection.Checksum),
		slog.Uint64("generation", generation), slog.String("reason", rejection.Reason),
		slog.Int("definitions", rejection.Definitions),
	}

	if rejection.Definitions > 0 {
		e.logger.LogAttrs(ctx, slog.LevelWarn,
			"dynamic segment definitions rejected: the connections cannot serve them, the others are applied", attrs...)

		return
	}

	e.logger.LogAttrs(ctx, slog.LevelWarn, "segments object rejected: its last good content stays", attrs...)
}

func (e *Engine) reject(generation uint64, code string, err error) contentPlan {
	res := Reload{Outcome: OutcomeRejected, Code: code, Generation: generation, Detail: err.Error()}
	e.noteRejected(res)

	return contentPlan{res: res}
}

func (e *Engine) planBootstrap(cfg *bootstrap.Config) bootPlan {
	admin, err := ResolveAdminKeys(cfg, e.secrets)
	if err != nil {
		res := Reload{
			Outcome: OutcomeRejected, Code: CodeInvalidBootstrap, Generation: e.cur.Generation, Detail: err.Error(),
		}
		e.noteRejected(res)

		return bootPlan{res: res}
	}

	base := e.baseline
	if base == nil {
		base = cfg
	}

	return bootPlan{
		admin: admin, pending: pendingRestart(base, cfg), apply: true,
	}
}

// validateContent checks the shape of a content.
func validateContent(content *Content) error {
	if content.Generation == 0 {
		return fmt.Errorf("%w: generation must be at least 1", ErrInvalidContent)
	}

	for name := range content.Objects {
		obj := content.Objects[name]

		err := validateObject(name, &obj)
		if err != nil {
			return err
		}
	}

	return nil
}

// withChecksums returns a content in which every object that carries bytes has a checksum: the bytes of an
// object without one are hashed here, once per round. The content of the caller is not changed.
func withChecksums(content *Content) *Content {
	missing := false

	for name := range content.Objects {
		obj := content.Objects[name]
		if obj.Checksum == "" && obj.state() == ObjectOK {
			missing = true

			break
		}
	}

	if !missing {
		return content
	}

	out := &Content{
		Generation: content.Generation,
		Revision:   content.Revision,
		Objects:    make(map[string]Object, len(content.Objects)),
	}

	for name, obj := range content.Objects {
		if obj.state() == ObjectOK {
			obj.Checksum = obj.checksum()
		}

		out.Objects[name] = obj
	}

	return out
}

// runningObject is what the engine remembers of an object of the content that runs.
type runningObject struct {
	kind     string
	checksum string // only for an object that carries content (state ok)
	reason   string
	state    ObjectState
}

func describe(content *Content) map[string]runningObject {
	out := make(map[string]runningObject, len(content.Objects))

	for name := range content.Objects {
		obj := content.Objects[name]
		held := runningObject{kind: obj.Kind, state: obj.state(), reason: obj.Reason}

		if held.state == ObjectOK {
			held.checksum = obj.Checksum
		}

		out[name] = held
	}

	return out
}

// contentVerdict is how a content at the running generation relates to the running one.
type contentVerdict uint8

const (
	// sameContent: the same objects in the same states.
	sameContent contentVerdict = iota
	// stateChange: the same objects, and only the state of some of them differs (for example pending or stale to ok),
	// or the kind of an object whose content was not current on one side (stale bytes of another kind).
	stateChange
	// conflictingContent: another set of names, or another checksum for a name where both sides carry content.
	conflictingContent
)

func compareRunning(running map[string]runningObject, content *Content) contentVerdict {
	if len(running) != len(content.Objects) {
		return conflictingContent
	}

	verdict := sameContent

	for name := range content.Objects {
		obj := content.Objects[name]

		held, ok := running[name]
		if !ok {
			return conflictingContent
		}

		switch compareObject(&held, &obj) {
		case conflictingContent:
			return conflictingContent
		case stateChange:
			verdict = stateChange
		case sameContent:
		}
	}

	return verdict
}

// compareObject relates one object of a content to the same object of the running one. The kind of an object that
// does not carry its current content is the kind of its fallback bytes: a name that changes kind while its new object
// is failing or arriving is a state change, not another manifest.
func compareObject(held *runningObject, obj *Object) contentVerdict {
	bothCurrent := held.state == ObjectOK && obj.state() == ObjectOK

	if held.kind != obj.Kind {
		if bothCurrent {
			return conflictingContent
		}

		return stateChange
	}

	if bothCurrent && held.checksum != obj.Checksum {
		return conflictingContent
	}

	if held.state != obj.state() || held.reason != obj.Reason {
		return stateChange
	}

	return sameContent
}

func validateObject(name string, obj *Object) error {
	if name == "" {
		return fmt.Errorf("%w: an object has an empty name", ErrInvalidContent)
	}

	switch obj.state() {
	case ObjectOK, ObjectStale, ObjectMissing, ObjectPending:
	case ObjectInvalid:
		return fmt.Errorf("%w: object %q: state %q is not an input state", ErrInvalidContent, name, obj.State)
	default:
		return fmt.Errorf("%w: object %q: unknown state %q", ErrInvalidContent, name, obj.State)
	}

	if obj.Kind == KindFlags {
		switch obj.Format {
		case "", "yaml", "yml", "json", "toml":
		default:
			return fmt.Errorf("%w: object %q: unsupported format %q", ErrInvalidContent, name, obj.Format)
		}
	}

	return nil
}

// enqueue computes one change per rebuilt flag set and merges it into the pending queue.
//
//   - A rebuilt flag set whose flags are all unchanged (only its state changed) produces no event.
//   - A flag set that the consumer knows (an event for it was delivered or waits) always gets a Removed event when
//     it is withdrawn, even if its flags were all deleted before. A flag set that the consumer never heard of gets
//     none, so churn of empty flag sets queues nothing.
//   - A flag set that is created and withdrawn while its event waits nets to no event.
//
// Every step is O(1) besides the diff of the flag set. It returns the number of flag sets whose change is left
// waiting (see [RoundResult.Events]).
func (e *Engine) enqueue(built *Built, next *Runtime) (int, reloadStats) {
	queuedEvents := 0

	var stats reloadStats

	for idx := range built.Rebuilt {
		item := &built.Rebuilt[idx]

		change := Change{
			FlagSet: item.Name, Diff: diffFlagSets(item.Old, item.New), Removed: item.New.State == StateWithdrawn,
			Generation: next.Generation, Snapshot: next.Snapshot,
			Config: &ConfigIdentity{ModifiedAt: item.New.ModifiedAt, ETag: item.New.ConfigETag()},
		}

		stats.tally(item, &change.Diff)

		_, known := e.announced[item.Name]
		queued := e.queue.byName[item.Name]

		switch {
		case change.Removed && !known:
			continue
		case !change.Removed && !change.Diff.HasDiff() && queued == nil:
			continue
		}

		if queued == nil {
			e.queue.push(&pendingChange{change: change, fromNothing: !known})
			e.announced[item.Name] = struct{}{}
			queuedEvents++

			continue
		}

		compose(&queued.change, &change)

		e.settle(queued)

		if e.queue.byName[item.Name] != nil {
			queuedEvents++ // a merge into a waiting change leaves it queued; a change that cancelled out does not
		}
	}

	return queuedEvents, stats
}

// settle drops a pending change that cancels out: for a flag set that the consumer never heard of (created and
// withdrawn, or flags gained and lost) nothing is told and the flag set is not known; for one that the consumer
// knows an empty change is dropped and the flag set stays known.
func (e *Engine) settle(queued *pendingChange) {
	name := queued.change.FlagSet

	switch {
	case queued.fromNothing && queued.change.Removed,
		queued.fromNothing && !queued.change.Diff.HasDiff():
		e.queue.remove(queued)
		delete(e.announced, name)
	case !queued.change.Removed && !queued.change.Diff.HasDiff():
		e.queue.remove(queued)
	}
}

// finish records and logs the results of a round and calls the hook once.
func (e *Engine) finish(ctx context.Context, results []Reload, applied *Reload, events int) {
	final := Reload{Outcome: OutcomeIgnored, Code: "unchanged", Generation: e.cur.Generation, Snapshot: e.cur.Snapshot}

	for idx := range results {
		if results[idx].Outcome == OutcomeRejected {
			e.logRejected(ctx, &results[idx])
		}

		final = results[idx]
	}

	if applied != nil {
		e.logApplied(ctx, applied, e.roundStats, e.roundDuration())

		final = *applied
	}

	result := RoundResult{Reload: final, Revision: e.roundRevision, ContentApplied: e.roundContent}
	if applied != nil {
		result.Events = events
	}

	e.last.Store(&result)

	if e.roundRevision > 0 {
		// before the end of the round is signalled: a waiter that wakes up reads this decision
		// the decision of a revision is the decision about its content: a bootstrap change that was applied in the
		// same round does not make a rejected content applied
		decided := final
		if e.roundRejected != nil {
			decided = *e.roundRejected
		}

		e.decided.Store(&decision{
			revision: e.roundRevision, reload: decided, running: e.holder.Load().Generation,
		})
	}

	next := make(chan struct{})
	close(*e.roundDone.Swap(&next))

	if e.roundHadBoot {
		e.bootstrapped.Store(true)
	}

	if e.hooks.AfterRound != nil {
		e.hooks.AfterRound(result)
	}
}
