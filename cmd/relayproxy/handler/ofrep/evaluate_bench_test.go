package ofrep_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/handler/ofrep"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/metric"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/testdata/mock"
	"github.com/blackbirdworks/semaphore/internal/protocol"
)

// BenchmarkEvaluate measures one OFREP single-flag evaluation through the handler, request binding and response
// writing included. It guards the evaluation hot path: allocations per operation must not increase.
func BenchmarkEvaluate(b *testing.B) {
	cfg := mock.FileConfig(configFlagsLocation)
	flagsetManager := mock.NewDefault(b, &cfg)

	body, err := os.ReadFile(testdataDir + ofrepValidRequestJSON)
	require.NoError(b, err)

	ctrl := ofrep.NewOFREPEvaluate(flagsetManager, &metric.Metrics{}, ofrep.EventStream{})
	payload := string(body)

	b.ReportAllocs()

	for b.Loop() {
		req := httptest.NewRequestWithContext(b.Context(), http.MethodPost, ofrepEvaluatePrefix+numberFlag,
			strings.NewReader(payload))
		req.SetPathValue("flagKey", numberFlag)
		req.Header.Set(protocol.HeaderContentType, protocol.MIMEApplicationJSON)

		rec := httptest.NewRecorder()
		require.NoError(b, ctrl.Evaluate(rec, req))
		require.Equal(b, http.StatusOK, rec.Code)
	}
}

// BenchmarkBulkEvaluate measures one OFREP bulk evaluation through the handler. It guards the hot path of the bulk
// operation: allocations per operation must not increase, with or without a query string.
func BenchmarkBulkEvaluate(b *testing.B) {
	cfg := mock.FileConfig(configFlagsLocation)
	flagsetManager := mock.NewDefault(b, &cfg)

	body, err := os.ReadFile(testdataDir + ofrepValidRequestJSON)
	require.NoError(b, err)

	ctrl := ofrep.NewOFREPEvaluate(flagsetManager, &metric.Metrics{}, ofrep.EventStream{})
	payload := string(body)

	for _, query := range []string{"", "?other=1"} {
		b.Run("query"+query, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				req := httptest.NewRequestWithContext(b.Context(), http.MethodPost,
					"/ofrep/v1/evaluate/flags"+query, strings.NewReader(payload))
				req.Header.Set(protocol.HeaderContentType, protocol.MIMEApplicationJSON)

				rec := httptest.NewRecorder()
				require.NoError(b, ctrl.BulkEvaluate(rec, req))
				require.Equal(b, http.StatusOK, rec.Code)
			}
		})
	}
}
