package manifestreader

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/blackbirdworks/semaphore/controlplane"
	"github.com/blackbirdworks/semaphore/modules/core/clock"
)

// ErrAlreadyRunning is returned by a second call of [Reader.Run].
var ErrAlreadyRunning = errors.New("manifestreader: reader already running")

// Reader polls one manifest and fetches its objects. Build it with [New], start it once with [Reader.Run].
//
// Everything below the "poller" comment is touched by the polling goroutine only; other goroutines use
// [Reader.Snapshots] and [Reader.Status].
//
// Request budget: a round is one logical conditional GET of the manifest. When it fails, the SDK of the store
// may retry it: those are extra attempts, each observed once by the connection observer. The steady state (every
// round answered 304, no failing object) is exactly one request per round.
type Reader struct {
	hooks    Hooks
	lastSync time.Time
	store    ObjectStore
	sched    Scheduler
	stores   map[string]ObjectStore
	status   atomic.Pointer[Status]
	jitter   func() float64
	reqCtx   func(context.Context, time.Duration) (context.Context, context.CancelFunc)
	// retry is the schedule of the objects that failed, by checksum.
	retry        map[string]*retryState
	routes       map[string]string
	snaps        chan *Snapshot
	refresh      *refresher
	now          func() time.Time
	logged       map[string]string
	hookPanics   map[string]uint64
	held         *held
	log          *slog.Logger
	names        map[string]*nameState
	pollCounts   map[string]uint64
	rejectCounts map[string]uint64
	cache        map[string][]byte
	// seen is the ETag sent as If-None-Match: the one of the last manifest read, accepted or not.
	seen     string
	lastPoll string
	// restoredStale are the names to serve stale while the restored manifest is accepted.
	restoredStale []StaleObject
	groups        []*group
	failures      []ObjectFailure
	manifest      ManifestStatus
	cfg           Config
	backoff       BackoffStatus
	counts        Counts
	wait          time.Duration
	// pollFails is the number of consecutive failed manifest polls; wait is the wait planned after the round;
	// vnow is the virtual time: the sum of the waits the scheduler completed. The retry schedule of the objects
	// is kept on it, so the scheduler is the only clock of the reader.
	pollFails int
	// revision is the number of the last snapshot emitted.
	revision uint64
	vnow     time.Duration
	notMod   uint64
	polls    uint64
	// cacheBytes is the number of bytes in cache.
	cacheBytes int64
	started    atomic.Bool
	// spread is set by accept when the manifest replaces a held one: the object fetches of that generation
	// wait a random delay.
	spread bool
	// requested is set when the round that runs was asked for by [Reader.Refresh]: its fetches are not spread.
	requested bool
	// dirty is set when the state of a name changed since the last snapshot.
	dirty bool
}

// held is the accepted manifest. etag is its own ETag, which is not the one of a rejected manifest read later.
type held struct {
	ident      map[string]identity
	channel    string
	etag       string
	raw        []byte
	objects    []controlplane.ManifestObject
	generation uint64
}

// identity is what a name is at one generation: a change of it at an equal generation is a conflict.
type identity struct {
	sum     string
	kind    string
	format  string
	version int
}

func effectiveVersion(version int) int {
	if version == 0 {
		return controlplane.ObjectVersion1
	}

	return version
}

// good is the last verified content of a name.
type good struct {
	kind   string
	format string
	sum    controlplane.Checksum
	bytes  []byte
}

// nameState is what the reader knows of one object name.
type nameState struct {
	good      *good
	entry     *controlplane.ManifestObject
	state     State
	reason    string
	retryable bool
}

// group is the objects of a manifest that share one checksum: the content is fetched once for all of them.
type group struct {
	sum string
	// names are the names whose entries can be requested, in manifest order. Their entries are the routes to
	// the content: the next one is tried only when the one before failed.
	names []*nameState
}

// New returns a reader of store. cfg is copied and never changed; nil selects the defaults (which still need a
// manifest key and a connection name, so nil is an error today).
func New(store ObjectStore, cfg *Config, opts ...Option) (*Reader, error) {
	var own Config

	if cfg != nil {
		own = *cfg
	}

	err := own.withDefaults()
	if err != nil {
		return nil, err
	}

	reader := &Reader{
		store:        store,
		cfg:          own,
		log:          slog.New(slog.DiscardHandler),
		sched:        timerScheduler{},
		jitter:       defaultJitter,
		reqCtx:       withTimeout,
		snaps:        make(chan *Snapshot, 1),
		refresh:      newRefresher(),
		now:          clock.Now,
		cache:        map[string][]byte{},
		names:        map[string]*nameState{},
		logged:       map[string]string{},
		hookPanics:   map[string]uint64{},
		retry:        map[string]*retryState{},
		pollCounts:   map[string]uint64{},
		rejectCounts: map[string]uint64{},
		manifest:     ManifestStatus{Outcome: OutcomeNone},
	}

	for _, opt := range opts {
		opt(reader)
	}

	// the store of the manifest serves its own connection, unless WithObjectStores says otherwise
	if reader.stores == nil {
		reader.stores = map[string]ObjectStore{}
	}

	if _, ok := reader.stores[own.Connection]; !ok {
		reader.stores[own.Connection] = store
	}

	reader.status.Store(&Status{Poll: PollStatus{Result: PollNone}, Manifest: reader.manifest})

	return reader, nil
}

func withTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}

// Snapshots returns the channel of snapshots. It has room for one: a snapshot that is not consumed when the
// next one is ready is replaced by it (latest wins) and the poller never blocks. The channel is closed when
// [Reader.Run] returns.
func (r *Reader) Snapshots() <-chan *Snapshot { return r.snaps }

// Status returns the state at the end of the last round. It never blocks the poller.
func (r *Reader) Status() Status { return *r.status.Load() }

// Run polls until ctx is done and returns nil then. It runs the rounds on the calling goroutine; object
// downloads run on a bounded group of workers that ends with each round, so no goroutine outlives Run.
//
// The first round is delayed by a random start phase, jitter() x min(PollInterval, 5 s), so replicas started
// together do not poll together, unless the jitter is disabled or restored state already made a snapshot
// available. The other rounds follow the poll interval with jitter, and every wait is within
// [MinPollInterval, PollInterval x (1+jitter)]. A failed round is simply tried again at the next interval.
func (r *Reader) Run(ctx context.Context) error {
	if !r.started.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}

	defer close(r.snaps)
	defer close(r.refresh.stopped)

	r.restore(ctx)

	if r.held == nil && r.cfg.JitterFraction > 0 {
		phase := time.Duration(r.jitterValue() * float64(min(r.cfg.PollInterval, maxStartPhase)))
		if phase > 0 && r.sleep(ctx, phase) != nil {
			return nil
		}
	}

	for {
		before := r.Status().Manifest.Generation

		r.round(ctx)

		if ctx.Err() != nil {
			return nil
		}

		result := r.roundResult(before)
		r.refresh.complete(&result)

		woken, err := r.sleepOrRefresh(ctx, r.wait)
		if err != nil {
			return nil
		}

		// a poll that someone asked for does not wait to spread the fetches: the caller waits for its result
		r.requested = woken
	}
}

// jitterValue is the jitter source clamped into [0, 1).
func (r *Reader) jitterValue() float64 {
	value := r.jitter()

	switch {
	case math.IsNaN(value) || value < 0:
		return 0
	case value >= 1:
		return math.Nextafter(1, 0)
	}

	return value
}

// next is the wait before the next round: the interval spread by the jitter, within its bounds.
func (r *Reader) next() time.Duration {
	interval := float64(r.cfg.PollInterval)
	wait := interval

	if r.cfg.JitterFraction > 0 {
		wait = interval * (1 + r.cfg.JitterFraction*(2*r.jitterValue()-1))
	}

	upper := time.Duration(interval * (1 + r.cfg.JitterFraction))

	return min(max(time.Duration(wait), MinPollInterval), max(upper, MinPollInterval))
}

// safe runs a hook and recovers a panic: it is counted, logged once per hook, and polling goes on. It reports
// whether the hook returned normally.
func (r *Reader) safe(ctx context.Context, name string, call func()) bool {
	returned := false

	defer func() {
		if returned {
			return
		}

		_ = recover()

		r.hookPanics[name]++

		if r.hookPanics[name] == 1 {
			r.log.LogAttrs(ctx, slog.LevelError, "manifest reader: hook panicked, polling goes on",
				slog.String("hook", name))
		}
	}()

	call()

	returned = true

	return true
}

func (r *Reader) restore(ctx context.Context) {
	if r.hooks.Restore == nil {
		return
	}

	var (
		restored *Restored
		err      error
	)

	if !r.safe(ctx, hookRestore, func() { restored, err = r.hooks.Restore() }) {
		return
	}

	if err != nil || restored == nil {
		if err != nil && !errors.Is(err, ErrNothingRestored) {
			r.log.LogAttrs(ctx, slog.LevelWarn, "manifest reader: restore failed")
		}

		return
	}

	for _, content := range restored.Objects {
		sum := controlplane.ChecksumOf(content).Value
		if _, ok := r.cache[sum]; !ok {
			r.cacheBytes += int64(len(content))
		}

		r.cache[sum] = content
	}

	r.restoredStale = restored.Stale
	accepted := r.accept(ctx, Object{Bytes: restored.Manifest, ETag: restored.ETag}, true)
	r.restoredStale = nil

	if !accepted {
		// the saved ETag names bytes that were not accepted: the first poll must read the manifest, and the saved
		// objects belong to no manifest
		r.seen = ""

		clear(r.cache)

		r.cacheBytes = 0
	}

	r.storeStatus(ctx, PollStatus{Result: PollNone}, false)
}

// Hook names, as reported in [Status.HookPanics].
const (
	hookRestore = "restore"
	hookSave    = "save"
	hookOnRound = "onRound"
	hookState   = "stateStatus"
)

// round is one poll: the conditional GET of the manifest and what follows from the answer.
func (r *Reader) round(ctx context.Context) {
	r.polls++

	reqCtx, cancel := r.reqCtx(ctx, r.cfg.RequestTimeout)
	obj, err := r.store.Get(reqCtx, GetRequest{Key: r.cfg.ManifestKey, ETag: r.seen, Limit: r.cfg.MaxManifestBytes})
	timedOut := err != nil && reqCtx.Err() != nil && ctx.Err() == nil

	cancel()

	if ctx.Err() != nil {
		return
	}

	poll := PollStatus{Result: PollUpdated}
	confirmed := false
	throttled := false

	switch {
	case err == nil:
		confirmed = r.accept(ctx, obj, false)
	case timedOut:
		poll = PollStatus{Result: PollError, Reason: ReasonTimeout}
	case errors.Is(err, ErrNotModified):
		r.notMod++
		poll.Result = PollNotModified
		confirmed = r.manifest.Outcome == OutcomeAccepted

		// Only the objects that failed are fetched again, and only on a round whose manifest GET worked: when
		// the manifest cannot be read (an outage) the reader does not add object requests to the failing ones.
		// With no failed object nothing else happens.
		r.settle(ctx, false, true)
	default:
		poll = failedPoll(err)
		throttled = isThrottled(err)
	}

	r.planWait(poll, throttled)
	r.logPoll(ctx, poll)
	r.finish(ctx, poll, confirmed)
}

func failedPoll(err error) PollStatus {
	switch {
	case errors.Is(err, ErrNotFound):
		return PollStatus{Result: PollNotFound, Reason: ReasonMissing}
	case errors.Is(err, ErrTooLarge):
		return PollStatus{Result: PollTooLarge, Reason: ReasonTooLarge}
	}

	fetchErr, ok := errors.AsType[*FetchError](err)
	if ok {
		return PollStatus{Result: PollError, Reason: fetchErr.Reason}
	}

	return PollStatus{Result: PollError, Reason: ReasonError}
}

// logPoll logs a failed poll once per change of its reason, and the recovery.
func (r *Reader) logPoll(ctx context.Context, poll PollStatus) {
	if poll.Reason == "" {
		if r.lastPoll != "" {
			r.lastPoll = ""
			r.log.LogAttrs(
				ctx,
				slog.LevelInfo,
				"manifest reader: manifest poll recovered",
				slog.String("result", poll.Result),
			)
		}

		return
	}

	key := poll.Result + "/" + poll.Reason
	if key == r.lastPoll {
		return
	}

	r.lastPoll = key
	r.log.LogAttrs(ctx, slog.LevelWarn, "manifest reader: manifest poll failed",
		slog.String("result", poll.Result), slog.String("reason", poll.Reason))
}

// reject records a manifest that is not applied. The ETag of the rejected bytes is sent next (r.seen): the same
// bytes are rejected again with the same result, so the next 304 spares the download and the repeated log, and
// a corrected manifest has other bytes, so another ETag, and is read. The ETag of the ACCEPTED manifest is
// kept apart in held.etag, and is what Save gets. The held manifest's UnknownFields count and paths stay untouched.
func (r *Reader) reject(ctx context.Context, outcome, reason string) {
	r.manifest.At = r.now().UTC()
	r.manifest.Outcome = outcome
	r.manifest.Reason = reason
	r.rejectCounts[RejectionReason(outcome, reason)]++

	r.log.LogAttrs(ctx, slog.LevelWarn, "manifest reader: manifest rejected, previous one kept",
		slog.String("outcome", outcome), slog.String("reason", reason),
		slog.Uint64("heldGeneration", r.manifest.Generation))
}

// CapUnknownFieldPaths returns at most [MaxUnknownFieldPaths] unknown field paths, each sanitized by replacing
// control characters with spaces and truncated to [MaxUnknownFieldPathBytes] bytes on a valid UTF-8 boundary.
func CapUnknownFieldPaths(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}

	limit := min(len(paths), MaxUnknownFieldPaths)
	out := make([]string, 0, limit)

	for _, path := range paths[:limit] {
		out = append(out, sanitizeUnknownFieldPath(path, MaxUnknownFieldPathBytes))
	}

	return out
}

func sanitizeUnknownFieldPath(path string, maxBytes int) string {
	var builder strings.Builder

	for _, r := range path {
		if unicode.IsControl(r) {
			r = ' '
		}

		if builder.Len()+utf8.RuneLen(r) > maxBytes {
			break
		}

		builder.WriteRune(r)
	}

	return builder.String()
}

// RejectionReason maps a manifest outcome and raw reason to the single closed rejection-reason vocabulary
// ([RejectionReasons]) shared by /info (config.reader.reason) and semaphore_config_reader_rejections_total.
// Any rejection reason outside [RejectionReasons] maps to "invalid". It returns "" for non-rejection outcomes.
func RejectionReason(outcome, reason string) string {
	switch outcome {
	case "", OutcomeNone, OutcomeAccepted:
		return ""
	case OutcomeChannelMismatch, OutcomeLowerGeneration, OutcomeConflict:
		return outcome
	default:
		if slices.Contains(RejectionReasons(), reason) {
			return reason
		}

		return OutcomeInvalid
	}
}

// accept decodes, validates and applies the manifest of a 200 answer. It reports whether the manifest was
// accepted.
func (r *Reader) accept(ctx context.Context, obj Object, restoring bool) bool {
	r.seen = obj.ETag

	manifest, info, err := controlplane.UnmarshalManifest(obj.Bytes)
	if err == nil {
		err = controlplane.ValidateManifest(manifest, controlplane.WithMaxObjects(r.cfg.MaxObjects))
	}

	if !r.validateAccepted(ctx, manifest, err) {
		return false
	}

	ident := make(map[string]identity, len(manifest.Objects))
	for i := range manifest.Objects {
		entry := &manifest.Objects[i]
		ident[entry.Name] = identity{
			sum:     entry.Checksum.Value,
			kind:    entry.Kind,
			format:  entry.Format,
			version: effectiveVersion(entry.Version),
		}
	}

	genChanged := r.held == nil || manifest.Generation != r.held.generation

	if !r.checkGeneration(ctx, manifest, ident, genChanged) {
		return false
	}

	r.spread = r.held != nil

	r.adopt(manifest, ident, obj)

	r.restoreStale(restoring)

	r.manifest = ManifestStatus{
		At:                r.now().UTC(),
		Outcome:           OutcomeAccepted,
		Channel:           manifest.Channel,
		Generation:        manifest.Generation,
		Objects:           len(manifest.Objects),
		UnknownFields:     len(info.UnknownFields),
		UnknownFieldPaths: CapUnknownFieldPaths(info.UnknownFields),
	}

	r.settle(ctx, genChanged, !restoring)

	return true
}

// restoreStale gives the names that were stale when the state was saved their old content again, as their last
// good one: they come back stale with the bytes they were served with, not missing. It runs before the restored
// snapshot is published, so there is no request first.
func (r *Reader) restoreStale(restoring bool) {
	if !restoring {
		return
	}

	for _, saved := range r.restoredStale {
		name := r.names[saved.Name]
		data, held := r.cache[saved.Checksum.Value]

		if name == nil || !held || name.state == StateOK || name.good != nil ||
			int64(len(data)) > r.cfg.MaxObjectBytes {
			continue
		}

		name.good = &good{kind: saved.Kind, format: saved.Format, sum: saved.Checksum, bytes: data}

		if name.state == StateMissing {
			// an entry that failed its own checks is not in the fetch plan: give it its failure again, now stale
			r.applyFailName(name, result{reason: name.reason, retryable: name.retryable})
		}
	}
}

func (r *Reader) validateAccepted(ctx context.Context, manifest *controlplane.Manifest, err error) bool {
	if err != nil && !onlyUnsupportedObjectVersions(err) {
		r.reject(ctx, OutcomeInvalid, manifestReason(err))

		return false
	}

	if r.cfg.Channel != "" && manifest.Channel != r.cfg.Channel {
		r.reject(ctx, OutcomeChannelMismatch, "channel")

		return false
	}

	if !r.withinTotal(manifest.Objects) {
		r.reject(ctx, OutcomeInvalid, ReasonTotalTooLarge)

		return false
	}

	return true
}

func (r *Reader) checkGeneration(
	ctx context.Context,
	manifest *controlplane.Manifest,
	ident map[string]identity,
	genChanged bool,
) bool {
	if r.held == nil {
		return true
	}

	switch {
	case manifest.Generation < r.held.generation:
		r.reject(ctx, OutcomeLowerGeneration, "generation")

		return false
	case !genChanged && (manifest.Channel != r.held.channel || !maps.Equal(r.held.ident, ident)):
		r.reject(ctx, OutcomeConflict, "generation")

		return false
	}

	return true
}

func isUnsupportedObjectVersion(prob *controlplane.FieldError) bool {
	return prob.Kind == controlplane.EntryObject && prob.Code == controlplane.CodeUnsupportedVersion
}

func onlyUnsupportedObjectVersions(err error) bool {
	valErr, ok := errors.AsType[*controlplane.ValidationError](err)
	if !ok || len(valErr.Problems) == 0 {
		return false
	}

	for i := range valErr.Problems {
		if !isUnsupportedObjectVersion(&valErr.Problems[i]) {
			return false
		}
	}

	return true
}

// withinTotal reports whether the distinct objects of the manifest fit in MaxTotalBytes. A checksum listed with
// several sizes counts with the largest, so the answer does not depend on the order of the entries. An entry
// that fails local checks or lists a size outside [1, MaxObjectBytes] is never requested and counts for nothing.
// Each size is compared with the budget that is left, so the sum cannot overflow.
func (r *Reader) withinTotal(objects []controlplane.ManifestObject) bool {
	largest := make(map[string]int64, len(objects))

	for i := range objects {
		size := objects[i].Size
		if objects[i].Version > controlplane.ObjectVersion1 || size < 1 || size > r.cfg.MaxObjectBytes {
			continue
		}

		largest[objects[i].Checksum.Value] = max(largest[objects[i].Checksum.Value], size)
	}

	var total int64

	for _, size := range largest {
		if size > r.cfg.MaxTotalBytes-total {
			return false
		}

		total += size
	}

	return true
}

// manifestReason is the closed code of a manifest that cannot be used. The text of the error is not used: it
// may name fields and objects.
func manifestReason(err error) string {
	valErr, ok := errors.AsType[*controlplane.ValidationError](err)
	if ok {
		for i := range valErr.Problems {
			if !isUnsupportedObjectVersion(&valErr.Problems[i]) {
				return string(valErr.Problems[i].Code)
			}
		}
	}

	switch {
	case errors.Is(err, controlplane.ErrUnsupportedAPIVersion):
		return "unsupported-version"
	case errors.Is(err, controlplane.ErrWrongKind):
		return "wrong-kind"
	case errors.Is(err, controlplane.ErrDecode):
		return "decode"
	}

	return "invalid"
}

// adopt makes manifest the held one and plans the downloads. A name that stays keeps its last verified content
// and, while its checksum is the same, its state; a name that left is forgotten with it (removal is not a
// failure). Every entry first passes its own checks: one that fails them is failed with its own reason and
// never borrows the result of a sibling. The other entries with the same checksum form one group, fetched once.
func (r *Reader) adopt(manifest *controlplane.Manifest, ident map[string]identity, obj Object) {
	previous := r.names
	names := make(map[string]*nameState, len(manifest.Objects))
	byChecksum := map[string]*group{}

	var order []*group

	for i := range manifest.Objects {
		entry := &manifest.Objects[i]
		state := &nameState{entry: entry}

		if prev := previous[entry.Name]; prev != nil {
			state.good = prev.good

			if prev.entry.Checksum.Value == entry.Checksum.Value {
				state.state, state.reason, state.retryable = prev.state, prev.reason, prev.retryable
			}
		}

		names[entry.Name] = state

		if reason := r.localFailure(entry); reason != "" {
			// no request can be made for it, now or in a later round: final until the manifest changes
			r.applyFailName(state, result{reason: reason})

			continue
		}

		if data, held := r.cache[entry.Checksum.Value]; held {
			r.applyData(state, data)

			continue
		}

		grp := byChecksum[entry.Checksum.Value]
		if grp == nil {
			grp = &group{sum: entry.Checksum.Value}
			byChecksum[grp.sum] = grp
			order = append(order, grp)
		}

		grp.names = append(grp.names, state)
	}

	r.names = names
	r.groups = order

	r.noteRoutes(order)
	r.held = &held{
		generation: manifest.Generation, channel: manifest.Channel, ident: ident, raw: obj.Bytes, etag: obj.ETag,
		objects: manifest.Objects,
	}
}

// noteRoutes forgets the retry schedule of a checksum whose routes changed (another key, connection or version in
// a rewrite of the same generation): a corrected route is tried in that round, not after the deadline the old route
// earned.
func (r *Reader) noteRoutes(groups []*group) {
	routes := make(map[string]string, len(groups))

	for _, grp := range groups {
		var route strings.Builder

		for _, name := range grp.names {
			route.WriteString(name.entry.Key + "\x00" + name.entry.Connection + "\x00" + name.entry.VersionID + "\n")
		}

		routes[grp.sum] = route.String()

		if previous, known := r.routes[grp.sum]; known && previous != routes[grp.sum] {
			delete(r.retry, grp.sum)
		}
	}

	r.routes = routes
}

// localFailure is the reason an entry cannot be requested at all, or "". It depends on the entry only, so it
// costs no request, now or in any later round.
func (r *Reader) localFailure(entry *controlplane.ManifestObject) string {
	switch {
	case entry.Version > controlplane.ObjectVersion1:
		return ReasonUnsupportedVersion
	case !ValidKey(entry.Key):
		return ReasonInvalidKey
	case entry.Size < 1:
		return ReasonInvalidSize
	case entry.Size > r.cfg.MaxObjectBytes:
		return ReasonTooLarge
	}

	if _, ok := r.stores[entry.Connection]; !ok {
		return ReasonUnknownConnection
	}

	return ""
}

// result is the outcome of fetching a group.
type result struct {
	reason    string
	data      []byte
	retryable bool
}

// settle fetches the groups that have no verified content yet (the new ones, and the ones that failed in an
// earlier round, each once), applies the results, and publishes a snapshot when the generation or the state of
// a name changed. Nothing here walks the objects of the manifest unless something changed; when something did,
// the cost of rebuilding the snapshot and the status is proportional to the manifest.
func (r *Reader) settle(ctx context.Context, genChanged, fetch bool) {
	if len(r.groups) > 0 && !fetch {
		// restoring: serve what was saved, poll the live manifest first, and fetch what is missing after it
		for _, grp := range r.groups {
			r.applyFail(grp, result{reason: ReasonNotRestored, retryable: true})
		}
	}

	if genChanged {
		// a new generation starts over: what failed is tried at once, after the spreading delay
		clear(r.retry)
	}

	if due := r.dueGroups(); fetch && len(due) > 0 {
		if genChanged && r.spread && !r.spreadWait(ctx) {
			return
		}

		fetched := r.fetchAll(ctx, due)
		if ctx.Err() != nil {
			return
		}

		r.apply(due, fetched)
	}

	if !genChanged && !r.dirty {
		return
	}

	r.dirty = false

	r.prune()
	r.report(ctx)
	r.publish(ctx)
}

// apply gives the results of the fetched groups to their names. The groups that failed stay in the plan, with
// their retry time; the other groups of the plan were not due and stay as they are.
func (r *Reader) apply(due []*group, fetched []result) {
	results := make(map[*group]result, len(due))
	for i, grp := range due {
		results[grp] = fetched[i]
	}

	var keep []*group

	for _, grp := range r.groups {
		res, tried := results[grp]
		if !tried {
			keep = append(keep, grp)

			continue
		}

		if res.reason == "" {
			delete(r.retry, grp.sum)

			r.cache[grp.sum] = res.data
			r.cacheBytes += int64(len(res.data))

			for _, name := range grp.names {
				r.applyData(name, res.data)
			}

			continue
		}

		r.applyFail(grp, res)
		r.noteFailure(grp.sum, res)

		keep = append(keep, grp)
	}

	r.groups = keep
}

// applyOK gives a name the verified content.
func (r *Reader) applyOK(name *nameState, data []byte) {
	entry := name.entry

	if name.state != StateOK || name.good == nil || name.good.sum.Value != entry.Checksum.Value ||
		name.good.kind != entry.Kind || name.good.format != entry.Format {
		name.good = &good{kind: entry.Kind, format: entry.Format, sum: entry.Checksum, bytes: data}
		name.state, name.reason, name.retryable = StateOK, "", false
		r.dirty = true
	}
}

// applyData gives a name verified content, if its own listed size agrees with it. A checksum proves the bytes,
// not that this entry declares their length.
func (r *Reader) applyData(name *nameState, data []byte) {
	if int64(len(data)) != name.entry.Size {
		// the size is what this entry declares: nothing but a new manifest changes it
		r.applyFailName(name, result{reason: ReasonSizeMismatch})

		return
	}

	r.applyOK(name, data)
}

// applyFail gives every name of the group the failure.
func (r *Reader) applyFail(grp *group, res result) {
	for _, name := range grp.names {
		r.applyFailName(name, res)
	}
}

// applyFailName gives a name the failure: stale when it has earlier content, else missing.
func (r *Reader) applyFailName(name *nameState, res result) {
	state := StateMissing
	if name.good != nil {
		state = StateStale
	}

	if name.state != state || name.reason != res.reason || name.retryable != res.retryable {
		name.state, name.reason, name.retryable = state, res.reason, res.retryable
		r.dirty = true
	}
}

// prune drops the cached bytes that no name serves (an ok name, or a stale one that carries them) and that no
// pending retry will need. An entry that failed one of its own checks pins nothing.
func (r *Reader) prune() {
	keep := make(map[string]struct{}, len(r.names)+len(r.groups))

	for _, name := range r.names {
		if name.good != nil {
			keep[name.good.sum.Value] = struct{}{}
		}
	}

	for _, grp := range r.groups {
		keep[grp.sum] = struct{}{}
	}

	r.cacheBytes = 0

	for sum, data := range r.cache {
		if _, ok := keep[sum]; !ok {
			delete(r.cache, sum)

			continue
		}

		r.cacheBytes += int64(len(data))
	}
}

// report refreshes the counts and failures and logs the state changes of the objects, once per change.
func (r *Reader) report(ctx context.Context) {
	r.counts = Counts{}
	r.failures = nil

	for name, state := range r.names {
		switch state.state {
		case StateOK:
			r.counts.OK++
		case StateStale:
			r.counts.Stale++
		case StateMissing:
			r.counts.Missing++
		}

		if state.state == StateOK {
			if _, was := r.logged[name]; was {
				delete(r.logged, name)
				r.log.LogAttrs(ctx, slog.LevelInfo, "manifest reader: object recovered", slog.String("object", name))
			}

			continue
		}

		r.failures = append(r.failures, ObjectFailure{
			Name: name, State: state.state, Reason: state.reason, Checksum: state.entry.Checksum.Value,
			Retryable: state.retryable,
		})

		if r.logged[name] != state.reason {
			r.logged[name] = state.reason
			r.log.LogAttrs(ctx, slog.LevelWarn, "manifest reader: object not updated",
				slog.String("object", name), slog.String("state", string(state.state)),
				slog.String("reason", state.reason))
		}
	}

	for name := range r.logged {
		if _, ok := r.names[name]; !ok {
			delete(r.logged, name)
		}
	}

	slices.SortFunc(r.failures, func(a, b ObjectFailure) int { return cmp.Compare(a.Name, b.Name) })
}

// publish sends the snapshot of the current states and hands the accepted manifest to the Save hook.
func (r *Reader) publish(ctx context.Context) {
	objects := make(map[string]ObjectState, len(r.names))
	for name, state := range r.names {
		objects[name] = state.objectState()
	}

	// the status of the manifest is visible before the snapshot is: the round of the engine that gets the snapshot reads
	// it, and the status of the round itself is stored when the round ends
	held := *r.status.Load()
	held.Manifest = r.manifest
	r.status.Store(&held)

	r.revision++

	snap := &Snapshot{
		Generation: r.held.generation, Channel: r.held.channel, Objects: objects, Revision: r.revision,
	}

	r.send(ctx, snap)

	if r.hooks.Save != nil {
		persist := Persist{Snapshot: snap, ETag: r.held.etag, Manifest: r.held.raw}

		r.safe(ctx, hookSave, func() { r.hooks.Save(persist) })
	}
}

// send puts snap in the channel, replacing a snapshot that was not consumed. The poller is the only sender:
// after the drain the send cannot find the buffer full.
func (r *Reader) send(ctx context.Context, snap *Snapshot) {
	for {
		select {
		case r.snaps <- snap:
			r.log.LogAttrs(ctx, slog.LevelInfo, "manifest reader: snapshot published",
				slog.Uint64("generation", snap.Generation), slog.Int("objects", len(snap.Objects)),
				slog.Int("notOK", len(r.failures)))

			return
		default:
			select {
			case <-r.snaps:
			default:
			}
		}
	}
}

func (n *nameState) objectState() ObjectState {
	if n.good == nil {
		return ObjectState{
			Kind: n.entry.Kind, Format: n.entry.Format, Checksum: n.entry.Checksum, State: n.state, Reason: n.reason,
		}
	}

	// a stale object carries the kind, format and checksum of the content it carries
	return ObjectState{
		Kind: n.good.kind, Format: n.good.format, Checksum: n.good.sum, State: n.state, Reason: n.reason,
		Bytes: n.good.bytes,
	}
}

// storeStatus makes the state of the reader visible. It does not call a hook.
func (r *Reader) storeStatus(ctx context.Context, poll PollStatus, confirmed bool) *Status {
	now := clock.Now()
	poll.At = now

	if confirmed {
		r.lastSync = now
	}

	status := &Status{
		LastSync: r.lastSync, Poll: poll, Manifest: r.manifest, Failures: r.failures, Counts: r.counts,
		Cache: r.cacheStats(), Polls: r.polls, NotModified: r.notMod, Backoff: r.backoff,
		PollResults: r.pollResults(), Rejections: r.rejectionResults(),
	}

	status.Backoff.ObjectsWaiting = r.objectsWaiting()

	if r.hooks.StateStatus != nil {
		r.safe(ctx, hookState, func() { status.State = r.hooks.StateStatus() })
	}

	if len(r.hookPanics) > 0 {
		for name, count := range r.hookPanics {
			status.HookPanics = append(status.HookPanics, HookPanic{Name: name, Count: count})
		}

		slices.SortFunc(status.HookPanics, func(a, b HookPanic) int { return cmp.Compare(a.Name, b.Name) })
	}

	r.status.Store(status)

	return status
}

// cacheStats counts the verified objects held.
func (r *Reader) cacheStats() CacheStatus {
	return CacheStatus{Objects: len(r.cache), Bytes: r.cacheBytes}
}

// finish ends a live round: the status, then the OnRound hook, which means "a live poll round ended".
func (r *Reader) finish(ctx context.Context, poll PollStatus, confirmed bool) {
	status := r.storeStatus(ctx, poll, confirmed)

	if r.hooks.OnRound != nil {
		r.safe(ctx, hookOnRound, func() { r.hooks.OnRound(status) })
	}
}

// fetchAll downloads the groups with at most FetchConcurrency requests in flight and returns the results in
// the order of the groups. It returns after every worker ended.
func (r *Reader) fetchAll(ctx context.Context, groups []*group) []result {
	results := make([]result, len(groups))
	next := make(chan int)

	var wg sync.WaitGroup

	for range min(r.cfg.FetchConcurrency, len(groups)) {
		wg.Go(func() {
			for idx := range next {
				results[idx] = r.fetchGroup(ctx, groups[idx])
			}
		})
	}

feed:
	for idx := range groups {
		select {
		case next <- idx:
		case <-ctx.Done():
			break feed
		}
	}

	close(next)
	wg.Wait()

	return results
}

// fetchGroup fetches the content of a checksum through the first entry that lists it; only when that fails is
// the next entry tried (one more request per extra entry, and only on failure). When all fail, the result is
// retryable if any route failed retryably (a later round can recover through it), and its reason is the most
// actionable one of the failures (see [actionable]).
func (r *Reader) fetchGroup(ctx context.Context, grp *group) result {
	var failure result

	for i, name := range grp.names {
		res := r.fetch(ctx, name.entry)
		if res.reason == "" || ctx.Err() != nil {
			return res
		}

		if i == 0 {
			failure = res

			continue
		}

		if actionable(res.reason) < actionable(failure.reason) {
			failure.reason = res.reason
		}

		failure.retryable = failure.retryable || res.retryable
	}

	return failure
}

// actionable ranks the failure reasons by what an operator can do about them: a smaller rank is more actionable.
// Access is denied (fix the configuration), then the content is wrong (checksum, size: the publisher), then it is
// missing, then the failure may pass by itself (timeout, error).
func actionable(reason string) int {
	order := [...]string{ReasonDenied, ReasonChecksumMismatch, ReasonSizeMismatch, ReasonMissing}

	if idx := slices.Index(order[:], reason); idx >= 0 {
		return idx
	}

	return len(order)
}

// fetch downloads and verifies one object. The listed size is binding: it is the limit of the request, and a
// body of another length fails before the checksum is looked at.
func (r *Reader) fetch(ctx context.Context, entry *controlplane.ManifestObject) result {
	store := r.stores[entry.Connection]

	reqCtx, cancel := r.reqCtx(ctx, r.cfg.RequestTimeout)
	defer cancel()

	obj, err := store.Get(reqCtx, GetRequest{Key: entry.Key, VersionID: entry.VersionID, Limit: entry.Size})

	switch {
	case err == nil:
	case reqCtx.Err() != nil && ctx.Err() == nil:
		return result{reason: ReasonTimeout, retryable: true}
	case errors.Is(err, ErrNotFound):
		return result{reason: ReasonMissing, retryable: true}
	case errors.Is(err, ErrTooLarge):
		return result{reason: ReasonSizeMismatch, retryable: true}
	default:
		fetchErr, ok := errors.AsType[*FetchError](err)
		if ok {
			return result{reason: fetchErr.Reason, retryable: fetchErr.Retryable}
		}

		return result{reason: ReasonError, retryable: true}
	}

	if int64(len(obj.Bytes)) != entry.Size {
		return result{reason: ReasonSizeMismatch, retryable: true}
	}

	if controlplane.VerifyChecksum(obj.Bytes, entry.Checksum) != nil {
		return result{reason: ReasonChecksumMismatch, retryable: true}
	}

	return result{data: obj.Bytes}
}
