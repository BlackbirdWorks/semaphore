package api_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/api"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/helper"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/metric"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/service"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/service/stream"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/testdata/mock"
	"github.com/blackbirdworks/semaphore/internal/protocol"
	"github.com/blackbirdworks/semaphore/testhelpers"
	"github.com/blackbirdworks/semaphore/testhelpers/fixtures"
)

// flagSetKey is the key of the flag set of the tests.
const flagSetKey = "flagset-key"

// TestFlagsetMode_EvaluationKeysOutsideFlagsets checks that, in flag set mode, a key that is not
// linked to a flag set is rejected by the authentication of every evaluation endpoint, instead of
// being accepted and failing later on the flag set lookup.
func TestFlagsetMode_EvaluationKeysOutsideFlagsets(t *testing.T) {
	t.Parallel()

	listener, port := testhelpers.ListenLocal(t)
	// in flag set mode only the keys of the flag sets evaluate: authorized-evaluation-key is not one of them
	proxyConf := &testConf{
		settings: api.Settings{Mode: api.ServerModeHTTP, Port: port},
		keys:     mock.Keys{Evaluation: []string{flagSetKey}, Admin: []string{adminKey}},
	}

	log := newTestLogger(t)

	wsService := stream.NewWebsocketService()
	t.Cleanup(wsService.Close)

	sseService := stream.NewSSEService()
	t.Cleanup(sseService.Close)
	proxyConf.flags = mock.NewNamed(t, mock.Named{
		Name: "tenant", APIKeys: []string{flagSetKey}, Config: mock.FileConfig(fixtures.FlagConfigYAMLParentPath),
	}).WithAdminKeys(adminKey)

	srv := newTestServer(t, proxyConf, &service.Services{
		WebsocketService: wsService,
		SSEService:       sseService,
		Metrics:          &metric.Metrics{},
	}, log.Slog, api.WithListener(listener))
	go srv.StartWithContext(context.Background())

	t.Cleanup(func() { srv.Stop(context.Background()) })

	baseURL := fmt.Sprintf(httpLocalhostD, port)
	waitForServer(t, baseURL)

	const evalBody = contextTargetingKeyUserOne

	endpoints := []struct {
		name   string
		method string
		path   string
		body   string
		// keyInQuery is true for the streaming endpoints, which read the key from the apiKey query param.
		keyInQuery bool
		// wantAdmin is the status for an admin key when it is not 400: OFREP documents 403 for it.
		wantAdmin int
	}{
		{
			name: "ofrep single", method: http.MethodPost, path: ofrepEvaluateTestFlag, body: evalBody,
			wantAdmin: http.StatusForbidden,
		},
		{
			name: "ofrep bulk", method: http.MethodPost, path: fixtures.OFREPEvaluateFlagsPath, body: evalBody,
			wantAdmin: http.StatusForbidden,
		},
		{
			name: "eval", method: http.MethodPost, path: v1FeatureTest,
			body: `{"evaluationContext":{"key":"user-1"},"defaultValue":false}`,
		},
		{
			name: "allflags", method: http.MethodPost, path: v1Allflags,
			body: `{"evaluationContext":{"key":"user-1"}}`,
		},
		{name: "flag configuration", method: http.MethodPost, path: v1FlagConfiguration, body: `{}`},
		{name: "flag change", method: http.MethodGet, path: v1FlagChange},
		{name: "data collector", method: http.MethodPost, path: v1DataCollector, body: emptyEventsBody},
		{name: "manifest", method: http.MethodGet, path: openfeatureV0Manifest},
		{name: "websocket", method: http.MethodGet, path: streamV1Ws, keyInQuery: true},
		{name: "sse", method: http.MethodGet, path: streamV1Sse, keyInQuery: true},
	}
	keys := []struct {
		name string
		key  string
		want int
		// skipStreams is true when the key is accepted: a stream would then stay open.
		skipStreams bool
	}{
		{name: "flag set", key: flagSetKey, want: http.StatusOK, skipStreams: true},
		// An admin key authenticates but it is not linked to a flag set: documented 400 (403 for OFREP, see wantAdmin).
		{name: fixtures.UserAdmin, key: adminKey, want: http.StatusBadRequest},
		{name: "authorizedKeys.evaluation", key: "authorized-evaluation-key", want: http.StatusUnauthorized},
		{name: "unknown", key: "unknown-key", want: http.StatusUnauthorized},
	}

	for _, endpoint := range endpoints {
		for _, keyCase := range keys {
			t.Run(endpoint.name+" with "+keyCase.name+" key", func(t *testing.T) {
				t.Parallel()

				if endpoint.keyInQuery && keyCase.skipStreams {
					t.Skip("a stream stays open when the key is accepted")
				}

				target := baseURL + endpoint.path
				if endpoint.keyInQuery {
					target += apiKeyQuery + url.QueryEscape(keyCase.key)
				}

				req, requestWithContextErr := http.NewRequestWithContext(
					t.Context(), endpoint.method, target, strings.NewReader(endpoint.body))
				require.NoError(t, requestWithContextErr)
				req.Header.Set(protocol.HeaderContentType, protocol.MIMEApplicationJSON)

				if !endpoint.keyInQuery {
					req.Header.Set(helper.XAPIKeyHeader, keyCase.key)
				}

				resp, requestWithContextErr := http.DefaultClient.Do(req)
				require.NoError(t, requestWithContextErr)

				defer func() { _ = resp.Body.Close() }()

				body, requestWithContextErr := io.ReadAll(resp.Body)
				require.NoError(t, requestWithContextErr)

				want := keyCase.want
				if keyCase.key == adminKey && endpoint.wantAdmin != 0 {
					want = endpoint.wantAdmin
				}

				assert.Equal(t, want, resp.StatusCode, string(body))
			})
		}
	}
}
