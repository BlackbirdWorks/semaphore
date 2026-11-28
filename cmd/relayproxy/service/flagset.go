package service

import (
	"context"
	"errors"
	"time"

	"github.com/blackbirdworks/semaphore/exporter"
	"github.com/blackbirdworks/semaphore/ffcontext"
	"github.com/blackbirdworks/semaphore/internal/flagstate"
	"github.com/blackbirdworks/semaphore/modules/core/flagdef"
	"github.com/blackbirdworks/semaphore/modules/core/model"
)

// FlagSet is what the HTTP handlers use of one flag set: the evaluation, the flag configuration, the collection of
// events and the refresh. The config v2 core satisfies it (corehttp.Adapter). The evaluations take the context of the
// request: the lookups of the dynamic segments end with it.
// A value that an evaluation returns is shared: the handlers only read it, and never change it.
type FlagSet interface {
	// RawVariation evaluates one flag without a type.
	RawVariation(
		ctx context.Context,
		flagKey string,
		evalCtx ffcontext.Context,
		defaultValue any,
	) (model.RawVarResult, error)
	// GetFlagStates evaluates the listed flags.
	GetFlagStates(ctx context.Context, evalCtx ffcontext.Context, flagsToEvaluate []string) flagstate.AllFlags
	// AllFlagsState evaluates every flag.
	AllFlagsState(ctx context.Context, evalCtx ffcontext.Context) flagstate.AllFlags
	// GetFlagsFromCache returns the flags of the flag set.
	GetFlagsFromCache() (map[string]flagdef.Flag, error)
	// GetFlagsFromCacheWithContext is GetFlagsFromCache with the context of the request.
	GetFlagsFromCacheWithContext(ctx context.Context) (map[string]flagdef.Flag, error)
	// GetCacheRefreshDate is the date the flags were last refreshed.
	GetCacheRefreshDate() time.Time
	// GetEvaluationContextEnrichment is the enrichment that every evaluation of the flag set gets.
	GetEvaluationContextEnrichment() map[string]any
	// CollectEventData hands a feature event to the exporters.
	CollectEventData(event *exporter.FeatureEvent)
	// CollectTrackingEventData hands a tracking event to the exporters.
	CollectTrackingEventData(event *exporter.TrackingEvent)
	// ForceRefresh asks the flag source for the latest flags and reports whether it did.
	ForceRefresh() bool
}

// PublicScope is a [FlagSet] that is the public flag set: a caller without an API key is a caller of the public flags
// only, so the events it sends are accepted for these flags and no other.
type PublicScope interface {
	// IsPublic reports that the flag set is the public one.
	IsPublic() bool
	// HasFlag reports whether the flag is one of the flag set.
	HasFlag(key string) bool
	// DropNotPublic counts events that were refused because their flag is not public.
	DropNotPublic(count int)
}

// ErrAdminKey is what an error of [FlagSetResolver.FlagSet] wraps (errors.Is) when the key is an admin key: it
// authenticates, and it is linked to no flag set.
var ErrAdminKey = errors.New("the key is an admin key")

type adminKeyError struct{ msg string }

func (e *adminKeyError) Error() string { return e.msg }

func (*adminKeyError) Unwrap() error { return ErrAdminKey }

// NewAdminKeyError returns the error of a resolver for an admin key, with the message of the answer: it wraps
// [ErrAdminKey].
func NewAdminKeyError(msg string) error { return &adminKeyError{msg: msg} }

// FlagSetResolver finds the flag set of an API key. It is the seam of the HTTP layer: the config v2 core implements
// it, the tests of the handlers implement it on engine clients.
type FlagSetResolver interface {
	// FlagSet returns the flag set linked to the API Key
	FlagSet(apiKey string) (FlagSet, error)
	// FlagSetName returns the name of the flagset linked to the API Key.
	// In default mode it returns the default flagset name, with flagsets it returns an
	// error if the API Key is empty or not linked to a flagset.
	FlagSetName(apiKey string) (string, error)
	// IsDefaultFlagSet returns true if the manager is in default mode (no flagsets configured)
	IsDefaultFlagSet() bool
}

// SegmentUser is a [FlagSet] that knows which segments the rules of its flags use: the flag configuration endpoint
// lists them next to the flag. A flag set that does not implement it lists none.
type SegmentUser interface {
	// FlagSegments returns the sorted names of the segments that the rules of the flag use, nil for a flag that uses
	// none, and whether one of them is dynamic (its members live in a store, so the flag needs the server to be
	// evaluated).
	FlagSegments(flagKey string) (names []string, requiresServerEvaluation bool)
}

// ConfigVersioned is a [FlagSet] that knows which configuration it serves: the OFREP bulk evaluation compares it with
// the configuration a client was notified of.
type ConfigVersioned interface {
	// ConfigETag is the identity of the configuration of the flag set, the one the change notification carries.
	ConfigETag() string
	// ConfigModified is the date the configuration of the flag set was built.
	ConfigModified() time.Time
}

// EventAcceptor is a [FlagSet] that tells whether it accepted a feature event: its queue to the exporters is bounded
// and a full queue drops the event. The collector then counts only the accepted events as ingested. A flag set that
// does not implement it accepts every event.
type EventAcceptor interface {
	// AcceptEventData hands a feature event to the exporters and reports whether it was accepted.
	AcceptEventData(event *exporter.FeatureEvent) bool
}

// FlagChanger is a [FlagSet] that keeps the hashes of its flags: the flag change endpoint answers with them instead
// of encoding every flag again. The returned map is shared: the caller only reads it.
type FlagChanger interface {
	// FlagChange returns the hash of every flag and the hash of the whole table.
	FlagChange() (flags map[string]uint32, hash uint32, err error)
}

// RefreshOutcome is what a refresh on request found.
type RefreshOutcome struct {
	Result            string
	Reason            string
	Rejection         string
	Generation        uint64
	AppliedGeneration uint64
	Changed           bool
	Applied           bool
}

// Refresher is a [FlagSetResolver] that polls its control plane on request, through the poll of the schedule: calls
// that arrive together share one poll, and ctx bounds the wait for it.
type Refresher interface {
	// Refresh polls now and returns what the poll found.
	Refresh(ctx context.Context) (RefreshOutcome, error)
}

// ErrNotPolling is the error of [Refresher.Refresh] when the relay does not poll (not started, or stopped).
var ErrNotPolling = errors.New("the relay is not polling its control plane")

// RefreshUnsupported is a [FlagSetResolver] whose flag sets can not be refreshed on request: the refresh endpoint
// answers 501 with the reason.
type RefreshUnsupported interface {
	// RefreshUnsupported is the reason, sent to the client.
	RefreshUnsupported() string
}
