package corehttp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/events"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/flagruntime"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/relaycore"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/service"
	"github.com/blackbirdworks/semaphore/exporter"
	"github.com/blackbirdworks/semaphore/ffcontext"
	"github.com/blackbirdworks/semaphore/internal/flagstate"
	"github.com/blackbirdworks/semaphore/internal/manifestreader"
	"github.com/blackbirdworks/semaphore/modules/core/flagdef"
	"github.com/blackbirdworks/semaphore/modules/core/model"
)

// The errors of the lookup of a flag set. The texts are part of the contract: the handlers answer
// them in the body of the 400 of a key that has no flag set.
var (
	errNoAPIKey        = errors.New("no API key provided")
	errFlagSetNotFound = errors.New("flagset not found for API key")
)

// Source is what the adapter needs of a core.
type Source interface {
	// Holder returns the holder of the current runtime.
	Holder() *flagruntime.Holder
	// Evaluator returns the evaluator of the flag sets.
	Evaluator() *flagruntime.Evaluator
	// Status is the status of the core, for the probes, /info and the metrics.
	Status() relaycore.Status
	// Collect hands a feature event to the exporters.
	Collect(event events.Event) bool
	// DropNotPublic counts events refused for a flag that is not public.
	DropNotPublic(count int)
	// Refresh polls the control plane now.
	Refresh(ctx context.Context) (relaycore.RefreshResult, error)
}

// Adapter implements the seam of the HTTP layer on a core: it finds the flag set of an API key in the current runtime
// and evaluates against it. It is safe for concurrent use and owns no goroutine.
type Adapter struct {
	holder  *flagruntime.Holder
	eval    *flagruntime.Evaluator
	collect func(events.Event) bool
	drop    func(int)
	refresh func(context.Context) (relaycore.RefreshResult, error)
	// table is the flag set views of the runtime that was last served: a view is made once per flag set, so
	// resolving a key does not allocate.
	table atomic.Pointer[viewTable]
	// rebuild makes the requests that meet a new runtime at the same time build its views once.
	rebuild sync.Mutex
}

var (
	_ service.FlagSetResolver = (*Adapter)(nil)
	_ service.KeySource       = (*Adapter)(nil)
	_ service.Refresher       = (*Adapter)(nil)
)

// NewAdapter returns the adapter of a core.
func NewAdapter(src Source) *Adapter {
	return &Adapter{
		holder:  src.Holder(),
		eval:    src.Evaluator(),
		collect: src.Collect,
		drop:    src.DropNotPublic,
		refresh: src.Refresh,
	}
}

// viewTable holds the views of the flag sets of one runtime.
type viewTable struct {
	rt    *flagruntime.Runtime
	views map[*flagruntime.FlagSet]*view
}

// viewOf returns the view of a flag set of the table; a nil table has none.
func (t *viewTable) viewOf(set *flagruntime.FlagSet) (*view, bool) {
	if t == nil {
		return nil, false
	}

	found, ok := t.views[set]

	return found, ok
}

// FlagSet returns the flag set of the API key.
//
//nolint:ireturn // the seam of the HTTP layer: the core adapter and the engine clients of the tests are implementations
func (a *Adapter) FlagSet(apiKey string) (service.FlagSet, error) {
	found, err := a.lookup(apiKey)
	if err != nil {
		return nil, err
	}

	return found, nil
}

// FlagSetName returns the name of the flag set of the API key.
func (a *Adapter) FlagSetName(apiKey string) (string, error) {
	found, err := a.lookup(apiKey)
	if err != nil {
		return "", err
	}

	return found.set.Name, nil
}

// IsDefaultFlagSet is false: a core always routes the keys to flag sets.
func (*Adapter) IsDefaultFlagSet() bool { return false }

// AuthenticationEnabled is true: a request needs a key, or else the generation has public flags.
func (*Adapter) AuthenticationEnabled() bool { return true }

// KeyExists reports whether the key is a flag set key or an admin key.
func (a *Adapter) KeyExists(apiKey string) bool {
	_, route := a.holder.Load().Route(apiKey)

	return route != flagruntime.RouteUnknown
}

// AdminKeyExists reports whether the key is an admin key.
func (a *Adapter) AdminKeyExists(apiKey string) bool {
	_, route := a.holder.Load().Route(apiKey)

	return route == flagruntime.RouteAdmin
}

// Refresh polls the control plane now, through the poll of the schedule.
func (a *Adapter) Refresh(ctx context.Context) (service.RefreshOutcome, error) {
	result, err := a.refresh(ctx)
	if err != nil {
		if errors.Is(err, manifestreader.ErrNotPolling) {
			return service.RefreshOutcome{}, fmt.Errorf("%w: %w", service.ErrNotPolling, err)
		}

		return service.RefreshOutcome{}, fmt.Errorf("refreshing: %w", err)
	}

	return service.RefreshOutcome{
		Result: result.Poll.Poll.Result, Reason: result.Poll.Poll.Reason, Generation: result.Poll.Generation,
		Changed: result.Poll.Changed, Applied: result.Engine.Outcome != flagruntime.OutcomeRejected,
		AppliedGeneration: result.Running, Rejection: rejection(&result.Engine),
	}, nil
}

// views returns the views of the flag sets of the runtime. A flag set that did not change keeps its pointer across
// the generations, so its view is reused.
func (a *Adapter) views(current *flagruntime.Runtime) map[*flagruntime.FlagSet]*view {
	prev := a.table.Load()
	if prev != nil && prev.rt == current {
		return prev.views
	}

	a.rebuild.Lock()
	defer a.rebuild.Unlock()

	prev = a.table.Load()
	if prev != nil && prev.rt == current {
		return prev.views
	}

	views := make(map[*flagruntime.FlagSet]*view, len(current.FlagSets))

	for _, set := range current.FlagSets {
		if prev != nil {
			if old, ok := prev.views[set]; ok {
				views[set] = old

				continue
			}
		}

		views[set] = &view{adapter: a, set: set}
	}

	if public := current.Public; public != nil {
		if old, reused := prev.viewOf(public); reused {
			views[public] = old
		} else {
			views[public] = &view{adapter: a, set: public}
		}
	}

	a.table.Store(&viewTable{rt: current, views: views})

	return views
}

// lookup routes a key to the view of its flag set. A flag set that is known but not loaded (pending) is routed too:
// it serves no flags. An admin key, an unknown key and an empty key have no flag set.
func (a *Adapter) lookup(apiKey string) (*view, error) {
	current := a.holder.Load()

	// no key: only the public flag set serves the request. A key that is given never comes to it.
	if apiKey == "" && current.Public == nil {
		return nil, errNoAPIKey
	}

	set, route := current.Route(apiKey)
	if route == flagruntime.RouteAdmin {
		return nil, fmt.Errorf("%w", service.NewAdminKeyError(errFlagSetNotFound.Error()))
	}

	if route != flagruntime.RouteFlagSet && route != flagruntime.RoutePending {
		return nil, errFlagSetNotFound
	}

	found, ok := a.views(current)[set]
	if !ok {
		return nil, errFlagSetNotFound
	}

	return found, nil
}

// view is the flag set of one runtime, the handlers read from.
type view struct {
	adapter *Adapter
	set     *flagruntime.FlagSet
}

var (
	_ service.FlagSet         = (*view)(nil)
	_ service.EventAcceptor   = (*view)(nil)
	_ service.FlagChanger     = (*view)(nil)
	_ service.SegmentUser     = (*view)(nil)
	_ service.ConfigVersioned = (*view)(nil)
)

// RawVariation evaluates one flag.
func (v *view) RawVariation(
	ctx context.Context, flagKey string, evalCtx ffcontext.Context, defaultValue any,
) (model.RawVarResult, error) {
	res, err := v.adapter.eval.RawVariation(ctx, v.set, flagKey, evalCtx, defaultValue)

	return res, err //nolint:wrapcheck // the error of the evaluation
}

// GetFlagStates evaluates the listed flags.
func (v *view) GetFlagStates(
	ctx context.Context, evalCtx ffcontext.Context, flagsToEvaluate []string,
) flagstate.AllFlags {
	return v.adapter.eval.GetFlagStates(ctx, v.set, evalCtx, flagsToEvaluate)
}

// AllFlagsState evaluates every flag.
func (v *view) AllFlagsState(ctx context.Context, evalCtx ffcontext.Context) flagstate.AllFlags {
	return v.adapter.eval.AllFlagsState(ctx, v.set, evalCtx)
}

// FlagSegments returns the segments that the rules of a flag use and whether one is dynamic.
func (v *view) FlagSegments(flagKey string) ([]string, bool) {
	return v.set.SegmentUse(flagKey)
}

// GetFlagsFromCache returns the flags of the flag set. The flags are the ones the evaluations use: the caller reads
// them and never changes them.
func (v *view) GetFlagsFromCache() (map[string]flagdef.Flag, error) {
	return v.set.Configuration(nil), nil
}

// GetFlagsFromCacheWithContext is GetFlagsFromCache.
func (v *view) GetFlagsFromCacheWithContext(_ context.Context) (map[string]flagdef.Flag, error) {
	return v.set.Configuration(nil), nil
}

// GetCacheRefreshDate is the date the flag set was last modified.
func (v *view) GetCacheRefreshDate() time.Time { return v.set.ModifiedAt }

// ConfigETag is the identity of the configuration of the flag set.
func (v *view) ConfigETag() string { return v.set.ConfigETag() }

// ConfigModified is the date the configuration of the flag set was built.
func (v *view) ConfigModified() time.Time { return v.set.ModifiedAt }

// GetEvaluationContextEnrichment returns a copy of the enrichment of the flag set.
func (v *view) GetEvaluationContextEnrichment() map[string]any {
	return v.adapter.eval.EvaluationContextEnrichment(v.set)
}

// CollectEventData hands the event to the exporters; a full queue drops it, see [view.AcceptEventData].
func (v *view) CollectEventData(event *exporter.FeatureEvent) { v.adapter.collect(event) }

// AcceptEventData hands the event to the exporters and reports whether their queue took it.
func (v *view) AcceptEventData(event *exporter.FeatureEvent) bool { return v.adapter.collect(event) }

// FlagChange returns the hashes of the flags, computed once for the flag set. The map is shared: read it only.
func (v *view) FlagChange() (map[string]uint32, uint32, error) {
	change, err := v.set.Change()
	if err != nil {
		return nil, 0, fmt.Errorf("hashing the flags of %q: %w", v.set.Name, err)
	}

	return change.Flags, change.Hash, nil
}

// CollectTrackingEventData drops the event: the exporters of a core take feature events only.
func (*view) CollectTrackingEventData(*exporter.TrackingEvent) {}

// ForceRefresh is false: a core follows its control plane by polling and does not refresh on request.
func (*view) ForceRefresh() bool { return false }

// rejection is the closed code of a generation the engine rejected, empty when it did not.
func rejection(decided *flagruntime.Reload) string {
	if decided.Outcome == flagruntime.OutcomeRejected {
		return decided.Code
	}

	return ""
}

var _ service.PublicScope = (*view)(nil)

// IsPublic reports that the view is the public flag set.
func (v *view) IsPublic() bool { return v.set.Name == flagruntime.PublicName }

// HasFlag reports whether the flag is one of the view.
func (v *view) HasFlag(key string) bool {
	_, ok := v.set.Flag(key)

	return ok
}

// DropNotPublic counts events refused for a flag that is not public.
func (v *view) DropNotPublic(count int) { v.adapter.drop(count) }
