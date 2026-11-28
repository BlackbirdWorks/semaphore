// Package mock holds the fixtures of the tests of the HTTP layer: flag sets served from the engine, the API keys of a
// test and canned services.
package mock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ffclient "github.com/blackbirdworks/semaphore"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/model"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/service"
	"github.com/blackbirdworks/semaphore/exporter/fileexporter"
	"github.com/blackbirdworks/semaphore/exporter/logsexporter"
	"github.com/blackbirdworks/semaphore/ffcontext"
	"github.com/blackbirdworks/semaphore/internal/flagstate"
	cmodel "github.com/blackbirdworks/semaphore/modules/core/model"
	"github.com/blackbirdworks/semaphore/pkgs/stringsx"
	"github.com/blackbirdworks/semaphore/retriever/fileretriever"
)

// The errors of the lookup of a flag set, the texts are the ones that the handlers answer in the body of a 400.
var (
	errNoAPIKey        = errors.New("no API key provided")
	errFlagSetNotFound = errors.New("flagset not found for API key")
	errKeyRequired     = errors.New("apiKey is required when using flagsets")
	errNoFlagSetOfKey  = errors.New("no flag set associated to the API key")
)

// pollingInterval is the polling of the clients of the fixtures: the files of the tests do not change.
const pollingInterval = time.Hour

// FileConfig is the engine configuration of a flag file: the file retriever and no logs.
func FileConfig(path string) ffclient.Config {
	return ffclient.Config{
		PollingInterval: pollingInterval,
		Retriever:       &fileretriever.Retriever{Path: path},
		LeveledLogger:   slog.New(slog.DiscardHandler),
	}
}

// NewClient starts the engine client of the configuration, the test closes it.
func NewClient(tb testing.TB, cfg *ffclient.Config) *ffclient.GoFeatureFlag {
	tb.Helper()

	client, err := ffclient.New(cfg)
	require.NoError(tb, err)
	tb.Cleanup(client.Close)

	return client
}

// Engine serves an engine client as a [service.FlagSet]: the client has no evaluation that takes the context of the
// request, so the context is not used.
type Engine struct{ *ffclient.GoFeatureFlag }

var _ service.FlagSet = Engine{}

// RawVariation evaluates one flag without a type.
func (e Engine) RawVariation(
	_ context.Context, flagKey string, evalCtx ffcontext.Context, defaultValue any,
) (cmodel.RawVarResult, error) {
	res, err := e.GoFeatureFlag.RawVariation(flagKey, evalCtx, defaultValue)
	if err != nil {
		return res, fmt.Errorf("raw variation: %w", err)
	}

	return res, nil
}

// GetFlagStates evaluates the listed flags.
func (e Engine) GetFlagStates(_ context.Context, evalCtx ffcontext.Context, flags []string) flagstate.AllFlags {
	return e.GoFeatureFlag.GetFlagStates(evalCtx, flags)
}

// AllFlagsState evaluates every flag.
func (e Engine) AllFlagsState(_ context.Context, evalCtx ffcontext.Context) flagstate.AllFlags {
	return e.GoFeatureFlag.AllFlagsState(evalCtx)
}

// Named is a flag set of a fixture with several flag sets.
type Named struct {
	// Name is the name of the flag set.
	Name string
	// APIKeys are the keys that evaluate against the flag set.
	APIKeys []string
	// Config is the engine configuration of the flag set.
	Config ffclient.Config
}

// FlagSets finds the flag set of an API key in engine clients. It is a [service.FlagSetResolver] for the tests of the
// handlers and of the server: with one flag set it serves every key (the default mode), with named flag sets a key
// finds its own.
type FlagSets struct {
	byKey   map[string]string
	admin   map[string]struct{}
	sets    map[string]*ffclient.GoFeatureFlag
	def     *ffclient.GoFeatureFlag
	defName string
}

var _ service.FlagSetResolver = (*FlagSets)(nil)

// NewDefault serves every key from one flag set.
func NewDefault(tb testing.TB, cfg *ffclient.Config) *FlagSets {
	tb.Helper()

	client := NewClient(tb, cfg)

	return &FlagSets{
		def:     client,
		defName: stringsx.DefaultFlagSetName,
		sets:    map[string]*ffclient.GoFeatureFlag{stringsx.DefaultFlagSetName: client},
	}
}

// NewNamed serves the keys of each flag set from it.
func NewNamed(tb testing.TB, sets ...Named) *FlagSets {
	tb.Helper()

	fs := &FlagSets{byKey: map[string]string{}, sets: map[string]*ffclient.GoFeatureFlag{}}

	for i := range sets {
		fs.sets[sets[i].Name] = NewClient(tb, &sets[i].Config)

		for _, key := range sets[i].APIKeys {
			fs.byKey[key] = sets[i].Name
		}
	}

	return fs
}

// WithAdminKeys makes the keys admin keys: they resolve to no flag set, with the error of an admin key.
func (f *FlagSets) WithAdminKeys(keys ...string) *FlagSets {
	f.admin = make(map[string]struct{}, len(keys))
	for _, key := range keys {
		f.admin[key] = struct{}{}
	}

	return f
}

// FlagSet returns the flag set of the key.
//
//nolint:ireturn // the seam of the HTTP layer
func (f *FlagSets) FlagSet(apiKey string) (service.FlagSet, error) {
	if f.def != nil {
		return Engine{f.def}, nil
	}

	if apiKey == "" {
		return nil, errNoAPIKey
	}

	if _, isAdmin := f.admin[apiKey]; isAdmin {
		return nil, service.NewAdminKeyError(errFlagSetNotFound.Error())
	}

	name, found := f.byKey[apiKey]
	if !found {
		return nil, errFlagSetNotFound
	}

	return Engine{f.sets[name]}, nil
}

// FlagSetName returns the name of the flag set of the key.
func (f *FlagSets) FlagSetName(apiKey string) (string, error) {
	if f.def != nil {
		return f.defName, nil
	}

	if apiKey == "" {
		return "", errKeyRequired
	}

	name, found := f.byKey[apiKey]
	if !found {
		return "", errNoFlagSetOfKey
	}

	return name, nil
}

// IsDefaultFlagSet reports whether the fixture has one flag set for every key.
func (f *FlagSets) IsDefaultFlagSet() bool { return f.def != nil }

// Client returns the engine client of a flag set, nil when there is none of the name.
func (f *FlagSets) Client(name string) *ffclient.GoFeatureFlag { return f.sets[name] }

// Names returns the names of the flag sets, sorted.
func (f *FlagSets) Names() []string {
	return slices.Sorted(maps.Keys(f.sets))
}

// Keys is the [service.KeySource] of a test: the keys are fixed.
type Keys struct {
	// Admin are the admin keys.
	Admin []string
	// Evaluation are the keys that evaluate.
	Evaluation []string
}

var _ service.KeySource = Keys{}

// AuthenticationEnabled reports whether the evaluation requests must carry an accepted key: there is an evaluation key.
func (k Keys) AuthenticationEnabled() bool { return len(k.Evaluation) > 0 }

// KeyExists reports whether the key is accepted.
func (k Keys) KeyExists(apiKey string) bool {
	return slices.Contains(k.Evaluation, apiKey) || slices.Contains(k.Admin, apiKey)
}

// AdminKeyExists reports whether the key is an admin key.
func (k Keys) AdminKeyExists(apiKey string) bool { return slices.Contains(k.Admin, apiKey) }

// LogExporter adds an exporter that logs the events to the configuration.
func LogExporter(cfg ffclient.Config) ffclient.Config {
	cfg.DataExporters = append(cfg.DataExporters, ffclient.DataExporter{
		FlushInterval:    time.Minute,
		MaxEventInMemory: 1000,
		Exporter:         &logsexporter.Exporter{},
	})

	return cfg
}

// Close closes the clients of the flag sets: the exporters flush the events they hold. The test closes them anyway,
// closing twice is harmless.
func (f *FlagSets) Close() {
	for _, name := range f.Names() {
		f.sets[name].Close()
	}
}

// FileExporter is a data exporter that writes the events as JSON to the file when the client closes. An empty
// eventType exports the feature events.
func FileExporter(filename, eventType string) ffclient.DataExporter {
	return ffclient.DataExporter{
		FlushInterval:     time.Hour,
		MaxEventInMemory:  10000,
		ExporterEventType: eventType,
		Exporter:          &fileexporter.Exporter{Format: "json", Filename: filename},
	}
}

// LiveKeys is a [service.KeySource] whose keys a test replaces while the server runs, as a reload does.
type LiveKeys struct {
	keys Keys
	mu   sync.RWMutex
}

var _ service.KeySource = (*LiveKeys)(nil)

// NewLiveKeys returns a source that starts with the keys.
func NewLiveKeys(keys Keys) *LiveKeys { return &LiveKeys{keys: keys} }

// Replace swaps the keys: the next request sees them.
func (l *LiveKeys) Replace(keys Keys) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.keys = keys
}

// AuthenticationEnabled reports whether the requests must carry an accepted key.
func (l *LiveKeys) AuthenticationEnabled() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.keys.AuthenticationEnabled()
}

// KeyExists reports whether the key is accepted.
func (l *LiveKeys) KeyExists(apiKey string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.keys.KeyExists(apiKey)
}

// AdminKeyExists reports whether the key is an admin key.
func (l *LiveKeys) AdminKeyExists(apiKey string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.keys.AdminKeyExists(apiKey)
}

// monitoring is the monitoring service of the flag sets of a fixture: the information is the refresh date of each
// flag set.
type monitoring struct{ flags *FlagSets }

// NewMonitoring returns the monitoring service of the flag sets: /health says it is initialized, /info lists the
// refresh dates of the flag sets.
func NewMonitoring(flags *FlagSets) service.Monitoring { return monitoring{flags: flags} }

func (monitoring) Health() model.HealthResponse { return model.HealthResponse{Initialized: true} }

func (m monitoring) Info() (model.InfoResponse, error) {
	if m.flags.IsDefaultFlagSet() {
		date := m.flags.def.GetCacheRefreshDate()

		return model.InfoResponse{LatestCacheRefresh: &date}, nil
	}

	dates := make(map[string]time.Time, len(m.flags.sets))
	latest := time.Time{}

	for name, client := range m.flags.sets {
		dates[name] = client.GetCacheRefreshDate()
		if dates[name].After(latest) {
			latest = dates[name]
		}
	}

	return model.InfoResponse{Flagsets: dates, LatestCacheRefresh: &latest}, nil
}

// Resolver is a [service.FlagSetResolver] that answers what a test gives it.
type Resolver struct {
	// Set is the flag set of every key; it is not served when Err is set or when it is nil.
	Set service.FlagSet
	// Err is the error of the lookup of the flag set and of its name.
	Err error
	// Name is the name of the flag set of every key.
	Name string
	// Default is the answer of IsDefaultFlagSet.
	Default bool
}

var _ service.FlagSetResolver = (*Resolver)(nil)

// FlagSet returns the flag set of the key.
//
//nolint:ireturn // the seam of the HTTP layer
func (r *Resolver) FlagSet(_ string) (service.FlagSet, error) {
	if r.Set == nil {
		return nil, r.Err
	}

	return r.Set, r.Err
}

// FlagSetName returns the name of the flag set of the key.
func (r *Resolver) FlagSetName(_ string) (string, error) { return r.Name, r.Err }

// IsDefaultFlagSet reports whether the resolver serves one flag set for every key.
func (r *Resolver) IsDefaultFlagSet() bool { return r.Default }
