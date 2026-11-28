package ofrep_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/handler/ofrep"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/metric"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/testdata/mock"
	"github.com/blackbirdworks/semaphore/internal/protocol"
)

const nestedMetadataFlags = `f:
  variations: {on: true}
  defaultRule: {variation: on}
  metadata: {team: payments, revision: 3, experimental: false, labels: {a: 1}, owners: [x, y], none: null}
`

// The pinned document types every metadata entry as a boolean, a string or a number: the entries that hold an object,
// a list or null are not sent, in a single and in a bulk evaluation, and the metadata of the flag keeps them.
func TestEvaluationMetadataHoldsOnlyWhatTheDocumentAllows(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "flags.yaml")
	require.NoError(t, os.WriteFile(path, []byte(nestedMetadataFlags), 0o600))

	cfg := mock.FileConfig(path)
	resolver := mock.NewDefault(t, &cfg)
	ctrl := ofrep.NewOFREPEvaluate(resolver, &metric.Metrics{}, ofrep.EventStream{})

	want := map[string]any{"team": "payments", "revision": float64(3), "experimental": false}

	send := func(target string, single bool) map[string]any {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, target,
			strings.NewReader(`{"context":{"targetingKey":"u"}}`))
		req.Header.Set(protocol.HeaderContentType, protocol.MIMEApplicationJSON)
		req.SetPathValue("flagKey", "f")

		rec := httptest.NewRecorder()
		if single {
			require.NoError(t, ctrl.Evaluate(rec, req))
		} else {
			require.NoError(t, ctrl.BulkEvaluate(rec, req))
		}

		require.Equal(t, http.StatusOK, rec.Code)

		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

		if single {
			return body
		}

		flags, _ := body["flags"].([]any)
		require.Len(t, flags, 1)

		item, _ := flags[0].(map[string]any)

		return item
	}

	// twice each: an evaluation must not leave a trace in the next
	for range 2 {
		for _, single := range []bool{true, false} {
			item := send("/ofrep/v1/evaluate/flags/f", single)
			got, _ := item["metadata"].(map[string]any)
			assert.Subset(t, got, want, "single: %t", single)

			for _, dropped := range []string{"labels", "owners", "none"} {
				assert.NotContains(t, got, dropped, "single: %t", single)
			}
		}
	}

	flags, err := mock.Engine{GoFeatureFlag: resolver.Client("default")}.GetFlagsFromCache()
	require.NoError(t, err)
	assert.Contains(t, flags["f"].GetMetadata(), "labels", "the metadata of the flag is not changed")
	assert.Contains(t, flags["f"].GetMetadata(), "owners")
}
