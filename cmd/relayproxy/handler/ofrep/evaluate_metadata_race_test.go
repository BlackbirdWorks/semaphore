package ofrep_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/handler/ofrep"
	controller "github.com/blackbirdworks/semaphore/cmd/relayproxy/handler/semaphore"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/metric"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/testdata/mock"
	"github.com/blackbirdworks/semaphore/internal/protocol"
)

const (
	metaOwner             = "owner"
	metaCacheable         = "semaphore_cacheable"
	metaLegacyCacheable   = "gofeatureflag_cacheable"
	metadataFlagsLocation = testdataDir + "/ofrep/metadata_flags.yaml"
	raceWorkers           = 8
	raceRounds            = 100
)

// A parsed OFREP success response, reduced to what the tests compare.
type evaluateResponse struct {
	Value    any            `json:"value"`
	Metadata map[string]any `json:"metadata"`
	Reason   string         `json:"reason"`
}

func TestEvaluate_DoesNotWriteIntoTheCachedFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantValue    any
		wantMetadata map[string]any
		// wantCachedMetadata is the metadata of the flag as the configuration endpoint serves it, with no
		// cacheable key: the evaluations must leave the cached flag as it was loaded.
		wantCachedMetadata map[string]any
		name               string
		flagKey            string
		wantReason         string
	}{
		{
			name:       "disabled flag with metadata",
			flagKey:    "disabled-with-metadata",
			wantValue:  nil,
			wantReason: "DISABLED",
			// the document types the entries of the metadata as booleans, strings and numbers: the object is not sent
			wantMetadata: map[string]any{
				metaOwner:           "team-a",
				metaCacheable:       true,
				metaLegacyCacheable: true,
			},
			wantCachedMetadata: map[string]any{
				metaOwner: "team-a",
				"links":   map[string]any{"docs": "https://example.com/docs"},
			},
		},
		{
			name:       "enabled flag with metadata",
			flagKey:    "enabled-with-metadata",
			wantValue:  "on",
			wantReason: "STATIC",
			wantMetadata: map[string]any{
				metaOwner:           "team-b",
				metaCacheable:       true,
				metaLegacyCacheable: true,
			},
			wantCachedMetadata: map[string]any{metaOwner: "team-b"},
		},
		{
			name:    "flag without metadata",
			flagKey: "object-flag",
			wantValue: map[string]any{
				"theme":  "light",
				"limits": map[string]any{"max": float64(3)},
				"tags":   []any{"a", "b"},
			},
			wantReason: "STATIC",
			wantMetadata: map[string]any{
				metaCacheable:       true,
				metaLegacyCacheable: true,
			},
			wantCachedMetadata: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := mock.FileConfig(metadataFlagsLocation)
			cfg.FileFormat = protocol.FormatYAML
			flagsetManager := mock.NewDefault(t, &cfg)

			evalCtrl := ofrep.NewOFREPEvaluate(flagsetManager, &metric.Metrics{}, ofrep.EventStream{})
			configCtrl := controller.NewAPIFlagConfiguration(flagsetManager, &metric.Metrics{})

			body, err := os.ReadFile(testdataDir + ofrepValidRequestJSON)
			require.NoError(t, err)

			evaluate := func() (*httptest.ResponseRecorder, error) {
				req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, ofrepEvaluatePrefix+tt.flagKey,
					strings.NewReader(string(body)))
				req.SetPathValue("flagKey", tt.flagKey)
				req.Header.Set(protocol.HeaderContentType, protocol.MIMEApplicationJSON)

				rec := httptest.NewRecorder()

				return rec, evalCtrl.Evaluate(rec, req)
			}

			// the evaluations and the configuration polls start together and overlap: with the flag's own map
			// handed out, the race detector reports the write, or the runtime stops on the concurrent iteration.
			start := make(chan struct{})
			evalResults := make([][]int, raceWorkers)
			evalErrs := make([][]error, raceWorkers)
			pollErrs := make([][]error, raceWorkers)

			var wg sync.WaitGroup

			for worker := range raceWorkers {
				wg.Go(func() {
					<-start

					for range raceRounds {
						rec, evalErr := evaluate()
						evalErrs[worker] = append(evalErrs[worker], evalErr)
						evalResults[worker] = append(evalResults[worker], rec.Code)
					}
				})
				wg.Go(func() {
					<-start

					for range raceRounds {
						_, pollErr := configCtrl.Handle(t.Context(), &controller.FlagConfigurationInput{
							Request: controller.NewRequest("", protocol.MIMEApplicationJSON),
							RawBody: []byte(`{}`),
						})
						pollErrs[worker] = append(pollErrs[worker], pollErr)
					}
				})
			}

			close(start)
			wg.Wait()

			for worker := range raceWorkers {
				require.Len(t, evalResults[worker], raceRounds)
				require.Len(t, pollErrs[worker], raceRounds)

				for round := range raceRounds {
					require.NoError(t, evalErrs[worker][round])
					require.NoError(t, pollErrs[worker][round])
					require.Equal(t, http.StatusOK, evalResults[worker][round])
				}
			}

			// the provider-facing response is unchanged: the cacheable keys are in the metadata.
			rec, err := evaluate()
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, rec.Code)

			var got evaluateResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			assert.Equal(t, tt.wantValue, got.Value)
			assert.Equal(t, tt.wantReason, got.Reason)
			assert.Equal(t, tt.wantMetadata, got.Metadata)

			// and the cached flag is as it was loaded: the configuration endpoint serves no cacheable key.
			out, err := configCtrl.Handle(t.Context(), &controller.FlagConfigurationInput{
				Request: controller.NewRequest("", protocol.MIMEApplicationJSON),
				RawBody: []byte(`{}`),
			})
			require.NoError(t, err)
			require.Contains(t, out.Body.Flags, tt.flagKey)
			assert.Equal(t, tt.wantCachedMetadata, out.Body.Flags[tt.flagKey].GetMetadata())
		})
	}
}
