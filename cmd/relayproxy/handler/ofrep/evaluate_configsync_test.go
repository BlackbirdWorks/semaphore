package ofrep_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/configsync"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/handler/ofrep"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/metric"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/service"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/testdata/mock"
	"github.com/blackbirdworks/semaphore/internal/protocol"
)

const (
	servedETag  = "etag-served"
	loadedETag  = "etag-loaded"
	valueOld    = "old"
	valueFresh  = "fresh"
	oldFlagYAML = "f:\n  variations: {a: old}\n  defaultRule: {variation: a}\n"
	newFlagYAML = "f:\n  variations: {a: fresh}\n  defaultRule: {variation: a}\n"
)

// versionedSet is a flag set that knows the configuration it serves.
type versionedSet struct {
	service.FlagSet

	etag string
}

func (v versionedSet) ConfigETag() string { return v.etag }

func (versionedSet) ConfigModified() time.Time { return time.Unix(1000, 0) }

// behindResolver is a replica that serves the old flags until its control plane check loads the new ones.
type behindResolver struct {
	*mock.Resolver

	fresh     service.FlagSet
	refreshes atomic.Int64
}

func (b *behindResolver) Refresh(ctx context.Context) (service.RefreshOutcome, error) {
	b.refreshes.Add(1)
	b.Set = b.fresh

	return service.RefreshOutcome{Changed: true, Applied: true}, ctx.Err()
}

func newBehindResolver(t *testing.T) *behindResolver {
	t.Helper()

	write := func(content string) string {
		path := filepath.Join(t.TempDir(), "flags.yaml")
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

		return path
	}

	oldCfg, freshCfg := mock.FileConfig(write(oldFlagYAML)), mock.FileConfig(write(newFlagYAML))
	oldSet := mock.Engine{GoFeatureFlag: mock.NewClient(t, &oldCfg)}
	freshSet := mock.Engine{GoFeatureFlag: mock.NewClient(t, &freshCfg)}

	return &behindResolver{
		Resolver: &mock.Resolver{Set: versionedSet{FlagSet: oldSet, etag: servedETag}, Default: true},
		fresh:    versionedSet{FlagSet: freshSet, etag: loadedETag},
	}
}

func TestBulkEvaluationNamesAConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		query       string
		wantValue   string
		wantRefresh int64
	}{
		{name: "no parameter", wantValue: valueOld},
		{name: "the configuration that is served", query: "?flagConfigEtag=" + servedETag, wantValue: valueOld},
		{name: "a configuration the replica has not loaded", query: "?flagConfigEtag=" + loadedETag,
			wantValue: valueFresh, wantRefresh: 1},
		{name: "a date after the build", query: "?flagConfigLastModified=2000", wantValue: valueFresh,
			wantRefresh: 1},
		{name: "a date before the build", query: "?flagConfigLastModified=5", wantValue: valueOld},
		{name: "a value that is not a date", query: "?flagConfigLastModified=soon", wantValue: valueOld},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resolver := newBehindResolver(t)
			ctrl := ofrep.NewOFREPEvaluate(resolver, &metric.Metrics{}, ofrep.EventStream{})

			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost,
				"/ofrep/v1/evaluate/flags"+tt.query, strings.NewReader(`{"context":{"targetingKey":"u"}}`))
			req.Header.Set(protocol.HeaderContentType, protocol.MIMEApplicationJSON)

			require.NoError(t, ctrl.BulkEvaluate(rec, req))
			require.Equal(t, http.StatusOK, rec.Code)

			var body struct {
				Flags []struct {
					Value string `json:"value"`
				} `json:"flags"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Len(t, body.Flags, 1)
			assert.Equal(t, tt.wantValue, body.Flags[0].Value)
			assert.Equal(t, tt.wantRefresh, resolver.refreshes.Load())
		})
	}
}

func TestBulkEvaluationChecksOnceForAMadeUpConfiguration(t *testing.T) {
	t.Parallel()

	resolver := newBehindResolver(t)
	ctrl := ofrep.NewOFREPEvaluate(resolver, &metric.Metrics{}, ofrep.EventStream{})

	// the clock of the package is frozen: the requests come within one interval
	const requests = 40

	for range requests {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost,
			"/ofrep/v1/evaluate/flags?flagConfigEtag=made-up", strings.NewReader(`{"context":{}}`))
		req.Header.Set(protocol.HeaderContentType, protocol.MIMEApplicationJSON)

		require.NoError(t, ctrl.BulkEvaluate(rec, req))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	assert.EqualValues(t, 1, resolver.refreshes.Load(), "one control plane check for %d requests (interval %s)",
		requests, configsync.MinInterval)
}
