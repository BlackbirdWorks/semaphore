package ofrep_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/handler/ofrep"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/helper"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/metric"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/testdata/mock"
	"github.com/blackbirdworks/semaphore/internal/protocol"
)

const (
	ofrepBulkPath         = "/ofrep/v1/evaluate/flags"
	errInvalidContextCode = "INVALID_CONTEXT"
	tooLargeText          = "too large"
	setKey                = "set-key"
)

// unknownLengthReader hides the length of the body, like a chunked request does.
type unknownLengthReader struct{ io.Reader }

func TestEvaluate_BodyLimit(t *testing.T) {
	t.Parallel()

	big := `{"context":{"targetingKey":"u","padding":"` + strings.Repeat("a", ofrep.MaxBodyBytes) + `"}}`
	small := `{"context":{"targetingKey":"u"}}`

	tests := []struct {
		body            io.Reader
		name            string
		path            string
		wantErrorCode   string
		wantErrContains string
		contentLength   int64
		wantStatus      int
	}{
		{name: "single, known length over the limit", path: ofrepEvaluatePrefix + targetingKeyRule,
			body: strings.NewReader(big), contentLength: int64(len(big)),
			wantStatus: http.StatusBadRequest, wantErrorCode: errInvalidContextCode, wantErrContains: tooLargeText},
		{name: "single, unknown length over the limit", path: ofrepEvaluatePrefix + targetingKeyRule,
			body: unknownLengthReader{strings.NewReader(big)}, contentLength: -1,
			wantStatus: http.StatusBadRequest, wantErrorCode: errInvalidContextCode, wantErrContains: tooLargeText},
		{name: "single, unknown length under the limit", path: ofrepEvaluatePrefix + targetingKeyRule,
			body: unknownLengthReader{strings.NewReader(small)}, contentLength: -1, wantStatus: http.StatusOK},
		{name: "bulk, known length over the limit", path: ofrepBulkPath,
			body: strings.NewReader(big), contentLength: int64(len(big)),
			wantStatus: http.StatusBadRequest, wantErrorCode: errInvalidContextCode, wantErrContains: tooLargeText},
		{name: "bulk, unknown length over the limit", path: ofrepBulkPath,
			body: unknownLengthReader{strings.NewReader(big)}, contentLength: -1,
			wantStatus: http.StatusBadRequest, wantErrorCode: errInvalidContextCode, wantErrContains: tooLargeText},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := mock.FileConfig(configFlagsLocation)
			flagsetManager := mock.NewDefault(t, &cfg)

			defer flagsetManager.Close()

			ctrl := ofrep.NewOFREPEvaluate(flagsetManager, &metric.Metrics{}, ofrep.EventStream{})
			mux := http.NewServeMux()
			mux.Handle(postOfrepVOneEvaluate, helper.Handle(ctrl.Evaluate, false))
			mux.Handle("POST /ofrep/v1/evaluate/flags", helper.Handle(ctrl.BulkEvaluate, false))

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, tt.path, tt.body)
			req.ContentLength = tt.contentLength
			req.Header.Set(protocol.HeaderContentType, protocol.MIMEApplicationJSON)

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantErrorCode != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrorCode)
				assert.Contains(t, rec.Body.String(), tt.wantErrContains)
			}
		})
	}
}

func TestEvaluate_FlagSetFailureStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		apiKey     string
		wantStatus int
	}{
		{name: "single, admin key", path: ofrepEvaluatePrefix + targetingKeyRule, apiKey: "admin",
			wantStatus: http.StatusForbidden},
		{name: "bulk, admin key", path: ofrepBulkPath, apiKey: "admin", wantStatus: http.StatusForbidden},
		{name: "single, unknown key", path: ofrepEvaluatePrefix + targetingKeyRule, apiKey: "unknown",
			wantStatus: http.StatusBadRequest},
		{name: "bulk, unknown key", path: ofrepBulkPath, apiKey: "unknown", wantStatus: http.StatusBadRequest},
		{name: "single, no key", path: ofrepEvaluatePrefix + targetingKeyRule, apiKey: "",
			wantStatus: http.StatusBadRequest},
		{name: "single, flag set key", path: ofrepEvaluatePrefix + targetingKeyRule, apiKey: setKey,
			wantStatus: http.StatusOK},
		{name: "bulk, flag set key", path: ofrepBulkPath, apiKey: setKey, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			manager := mock.NewNamed(t, mock.Named{
				Name: "set", APIKeys: []string{setKey}, Config: mock.FileConfig(configFlagsLocation),
			}).WithAdminKeys("admin")

			defer manager.Close()

			ctrl := ofrep.NewOFREPEvaluate(manager, &metric.Metrics{}, ofrep.EventStream{})
			mux := http.NewServeMux()
			mux.Handle(postOfrepVOneEvaluate, helper.Handle(ctrl.Evaluate, false))
			mux.Handle("POST "+ofrepBulkPath, helper.Handle(ctrl.BulkEvaluate, false))

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, tt.path,
				strings.NewReader(`{"context":{"targetingKey":"u"}}`))
			req.Header.Set(protocol.HeaderContentType, protocol.MIMEApplicationJSON)

			if tt.apiKey != "" {
				req.Header.Set(helper.XAPIKeyHeader, tt.apiKey)
			}

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}
