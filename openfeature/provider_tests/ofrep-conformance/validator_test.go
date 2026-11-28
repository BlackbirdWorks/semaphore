package ofrepconformance_test

import (
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ofrepconformance "github.com/blackbirdworks/semaphore/openfeature/provider_tests/ofrep-conformance"
)

const (
	singleFlagPath   = "/ofrep/v1/evaluate/flags/{key}"
	bulkFlagsPath    = "/ofrep/v1/evaluate/flags"
	headerRetryAfter = "Retry-After"
	emptyFlagsJSON   = `{"flags":[]}`
)

type validatorCase struct {
	headers http.Header
	name    string
	path    string
	method  string
	body    string
	status  int
	wantErr bool
}

func jsonHeaders(extra ...string) http.Header {
	hdrs := make(http.Header)
	hdrs.Set("Content-Type", "application/json; charset=utf-8")

	for idx := 0; idx+1 < len(extra); idx += 2 {
		hdrs.Set(extra[idx], extra[idx+1])
	}

	return hdrs
}

func runValidatorCases(t *testing.T, spec *ofrepconformance.Spec, cases []validatorCase) {
	t.Helper()

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotErr := spec.ValidateResponse(tt.path, tt.method, tt.status, tt.headers, []byte(tt.body))
			if tt.wantErr {
				require.Error(t, gotErr)

				return
			}

			require.NoError(t, gotErr)
		})
	}
}

func TestLoadSpec(t *testing.T) {
	t.Parallel()

	spec, err := ofrepconformance.LoadSpec(filepath.Join("testdata", "openapi.yaml"))
	require.NoError(t, err)
	assert.True(t, spec.HasHeader(bulkFlagsPath, http.MethodPost, http.StatusOK, "ETag"))
	assert.True(t, spec.HasHeader(singleFlagPath, http.MethodPost, http.StatusTooManyRequests, headerRetryAfter))
	assert.True(t, spec.HasHeader(bulkFlagsPath, http.MethodPost, http.StatusTooManyRequests, headerRetryAfter))
	assert.False(t, spec.HasHeader("/missing", http.MethodPost, http.StatusOK, "ETag"))

	_, err = ofrepconformance.LoadSpec(filepath.Join("testdata", "missing.yaml"))
	require.Error(t, err)
}

func TestSpec_ValidateSingleFlagResponse(t *testing.T) {
	t.Parallel()

	spec, err := ofrepconformance.LoadSpec(filepath.Join("testdata", "openapi.yaml"))
	require.NoError(t, err)

	runValidatorCases(t, spec, []validatorCase{
		{
			name:    "bool success",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    `{"key":"f","value":true,"reason":"TARGETING_MATCH","variant":"on","metadata":{"team":"core"}}`,
		},
		{
			name:    "string success",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    `{"key":"f","value":"hello","reason":"STATIC","variant":"a"}`,
		},
		{
			name:    "int success",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    `{"key":"f","value":42,"reason":"SPLIT","variant":"b"}`,
		},
		{
			name:    "float success",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    `{"key":"f","value":3.14,"reason":"TARGETING_MATCH","variant":"pi"}`,
		},
		{
			name:    "object success",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    `{"key":"f","value":{"nested":true},"reason":"TARGETING_MATCH"}`,
		},
		{
			name:    "code default omits value",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    `{"key":"f","reason":"DISABLED","variant":"default"}`,
		},
		{
			name:    "null value rejected",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    `{"key":"f","value":null,"reason":"DISABLED"}`,
			wantErr: true,
		},
		{
			name:    "array value accepted: the named deviation of the object flag",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    `{"key":"f","value":[1,2],"reason":"STATIC"}`,
			wantErr: false,
		},
		{
			name:    "invalid reason DEFAULT rejected",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    `{"key":"f","value":true,"reason":"DEFAULT"}`,
			wantErr: true,
		},
		{
			name:    "UNKNOWN reason accepted",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    `{"key":"f","value":true,"reason":"UNKNOWN"}`,
		},
		{
			name:    "invalid metadata type rejected",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    `{"key":"f","value":true,"reason":"STATIC","metadata":{"nested":{"a":1}}}`,
			wantErr: true,
		},
		{
			name:    "400 evaluationFailure",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusBadRequest,
			headers: jsonHeaders(),
			body:    `{"key":"f","errorCode":"INVALID_CONTEXT","errorDetails":"bad"}`,
		},
		{
			name:    "404 flagNotFound",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusNotFound,
			headers: jsonHeaders(),
			body:    `{"key":"f","errorCode":"FLAG_NOT_FOUND","errorDetails":"missing"}`,
		},
		{
			name:    "404 invalid errorCode rejected",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusNotFound,
			headers: jsonHeaders(),
			body:    `{"key":"f","errorCode":"GENERAL","errorDetails":"wrong"}`,
			wantErr: true,
		},
		{
			name:    "429 RFC3339 Retry-After",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusTooManyRequests,
			headers: http.Header{headerRetryAfter: []string{"2026-11-27T12:00:00Z"}},
		},
		{
			name:    "429 missing Retry-After rejected",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusTooManyRequests,
			headers: http.Header{},
			wantErr: true,
		},
		{
			name:    "429 invalid Retry-After rejected",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusTooManyRequests,
			headers: http.Header{headerRetryAfter: []string{"not-a-date"}},
			wantErr: true,
		},
		{
			name:    "429 signed positive Retry-After rejected",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusTooManyRequests,
			headers: http.Header{headerRetryAfter: []string{"+30"}},
			wantErr: true,
		},
		{
			name:    "429 negative Retry-After rejected",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusTooManyRequests,
			headers: http.Header{headerRetryAfter: []string{"-1"}},
			wantErr: true,
		},
	})
}

func TestSpec_ValidateBulkAndTransportResponse(t *testing.T) {
	t.Parallel()

	spec, err := ofrepconformance.LoadSpec(filepath.Join("testdata", "openapi.yaml"))
	require.NoError(t, err)

	runValidatorCases(t, spec, []validatorCase{
		{
			name:    "bulk 200 with ETag and eventStream",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders("ETag", `"abc"`),
			body: `{"flags":[{"key":"a","value":true,"reason":"STATIC"},` +
				`{"key":"b","errorCode":"GENERAL","errorDetails":"eval error"}],` +
				`"eventStreams":[{"type":"sse","url":"https://example.com/sse","inactivityDelaySec":60}]}`,
		},
		{
			name:    "bulk 200 eventStream endpoint form",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders("ETag", `"abc"`),
			body: `{"flags":[],` +
				`"eventStreams":[{"type":"sse","endpoint":{"origin":"https://example.com","requestUri":"/sse"}}]}`,
		},
		{
			name:    "bulk 200 eventStream invalid uri rejected",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders("ETag", `"abc"`),
			body:    `{"flags":[],"eventStreams":[{"type":"sse","url":"not a url"}]}`,
			wantErr: true,
		},
		{
			name:    "bulk 200 eventStream zero inactivityDelaySec rejected",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders("ETag", `"abc"`),
			body:    `{"flags":[],"eventStreams":[{"type":"sse","url":"https://example.com/sse","inactivityDelaySec":0}]}`,
			wantErr: true,
		},
		{
			name:    "bulk 200 eventStream decimal whole number inactivityDelaySec",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders("ETag", `"abc"`),
			body:    `{"flags":[],"eventStreams":[{"type":"sse","url":"https://example.com/sse","inactivityDelaySec":60.0}]}`,
		},
		{
			name:    "bulk 200 eventStream fractional inactivityDelaySec rejected",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders("ETag", `"abc"`),
			body:    `{"flags":[],"eventStreams":[{"type":"sse","url":"https://example.com/sse","inactivityDelaySec":60.5}]}`,
			wantErr: true,
		},
		{
			name:    "bulk 200 eventStream both url and endpoint rejected",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders("ETag", `"abc"`),
			body: `{"flags":[],"eventStreams":[{"type":"sse","url":"https://example.com/sse",` +
				`"endpoint":{"requestUri":"/sse"}}]}`,
			wantErr: true,
		},
		{
			name:    "bulk 200 missing ETag rejected",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    emptyFlagsJSON,
			wantErr: true,
		},
		{
			name:    "bulk 200 weak ETag",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders("ETag", `W/"abc"`),
			body:    emptyFlagsJSON,
		},
		{
			name:    "bulk 200 unquoted ETag rejected",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders("ETag", "abc"),
			body:    emptyFlagsJSON,
			wantErr: true,
		},
		{
			name:    "bulk 200 lowercase weak ETag rejected",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders("ETag", `w/"abc"`),
			body:    emptyFlagsJSON,
			wantErr: true,
		},
		{
			name:    "bulk 200 ETag with space rejected",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders("ETag", `"a b"`),
			body:    emptyFlagsJSON,
			wantErr: true,
		},
		{
			name:    "bulk 304 empty body",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusNotModified,
			headers: http.Header{"ETag": []string{`"abc"`}},
		},
		{
			name:    "bulk 304 non-empty body rejected",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusNotModified,
			headers: http.Header{"ETag": []string{`"abc"`}},
			body:    `{}`,
			wantErr: true,
		},
		{
			name:    "bulk 400 failure",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusBadRequest,
			headers: jsonHeaders(),
			body:    `{"errorCode":"INVALID_CONTEXT","errorDetails":"missing context"}`,
		},
		{
			name:    "bulk 400 missing errorCode rejected",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusBadRequest,
			headers: jsonHeaders(),
			body:    `{"errorDetails":"missing"}`,
			wantErr: true,
		},
		{
			name:    "bulk 429 integer Retry-After",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusTooManyRequests,
			headers: http.Header{headerRetryAfter: []string{"30"}},
		},
		{
			name:    "bulk 429 HTTP-date Retry-After",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusTooManyRequests,
			headers: http.Header{headerRetryAfter: []string{"Fri, 27 Nov 2026 12:00:00 GMT"}},
		},
		{
			name:    "bulk 500 generalErrorResponse",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusInternalServerError,
			headers: jsonHeaders(),
			body:    `{"errorDetails":"internal error"}`,
		},
		{
			name:    "wrong content type rejected",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: http.Header{"Content-Type": []string{contentTypePlain}},
			body:    `{"key":"f","value":true,"reason":"STATIC"}`,
			wantErr: true,
		},
		{
			name:    "missing content type rejected",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: http.Header{},
			body:    `{"key":"f","value":true,"reason":"STATIC"}`,
			wantErr: true,
		},
		{
			name:    "trailing json rejected",
			path:    singleFlagPath,
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: jsonHeaders(),
			body:    `{"key":"f","value":true,"reason":"STATIC"}{}`,
			wantErr: true,
		},
		{
			name:    "undocumented path rejected",
			path:    "/unknown",
			method:  http.MethodPost,
			status:  http.StatusOK,
			headers: http.Header{},
			wantErr: true,
		},
		{
			name:    "undocumented method rejected",
			path:    bulkFlagsPath,
			method:  http.MethodGet,
			status:  http.StatusOK,
			headers: http.Header{},
			wantErr: true,
		},
		{
			name:    "undocumented status rejected",
			path:    bulkFlagsPath,
			method:  http.MethodPost,
			status:  http.StatusTeapot,
			headers: http.Header{},
			wantErr: true,
		},
	})
}

func TestSpec_URIFailureRedactsValue(t *testing.T) {
	t.Parallel()

	spec, err := ofrepconformance.LoadSpec(filepath.Join("testdata", "openapi.yaml"))
	require.NoError(t, err)

	body := []byte(`{"flags":[],"eventStreams":[{"type":"sse","url":"not a url?token=secret-token"}]}`)
	err = spec.ValidateResponse(bulkFlagsPath, http.MethodPost, http.StatusOK, jsonHeaders("ETag", `"abc"`), body)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "secret-token")
}

// The validator follows the pinned document with one named deviation.
func TestDeviationsListsExactlyTheArrayValue(t *testing.T) {
	t.Parallel()

	got := ofrepconformance.Deviations()
	require.Len(t, got, 1)

	assert.Equal(t, "objectFlag", got[0].Schema)
	assert.Equal(t, "value", got[0].Property)
	assert.NotEmpty(t, got[0].Reason)
	assert.Len(t, got[0].Evidence, 5)
}

func TestSpec_ArrayValueIsAcceptedOnlyForTheObjectFlag(t *testing.T) {
	t.Parallel()

	spec, err := ofrepconformance.LoadSpec(filepath.Join("testdata", "openapi.yaml"))
	require.NoError(t, err)

	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "array value of a flag", body: `{"key":"a","reason":"STATIC","variant":"v","value":["x",1]}`},
		{name: "empty array value", body: `{"key":"a","reason":"STATIC","variant":"v","value":[]}`},
		{name: "object value", body: `{"key":"a","reason":"STATIC","variant":"v","value":{"x":1}}`},
		{name: "null value", body: `{"key":"a","reason":"STATIC","variant":"v","value":null}`, wantErr: true},
		{name: "array as the key", body: `{"key":["a"],"reason":"STATIC","value":true}`, wantErr: true},
		{name: "array as the reason", body: `{"key":"a","reason":["STATIC"],"value":true}`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotErr := spec.ValidateResponse(
				singleFlagPath,
				http.MethodPost,
				http.StatusOK,
				jsonHeaders(),
				[]byte(tt.body),
			)
			if tt.wantErr {
				require.Error(t, gotErr)

				return
			}

			require.NoError(t, gotErr)
		})
	}
}
