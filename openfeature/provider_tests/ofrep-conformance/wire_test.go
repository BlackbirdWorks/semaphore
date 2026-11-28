package ofrepconformance_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ofrepconformance "github.com/blackbirdworks/semaphore/openfeature/provider_tests/ofrep-conformance"
)

const (
	oversizedBytes      = 2 << 20
	contentTypePlain    = "text/plain"
	relayAuth           = "auth"
	relayPublic         = "public"
	relayAdmin          = "admin"
	relayStream         = "stream"
	adminToken          = "admin_token"
	streamBaseURL       = "https://flags.example.test"
	streamInactivitySec = 90
)

type wireCase struct {
	headers       map[string]string
	name          string
	method        string
	path          string
	body          string
	contentType   string
	relay         string
	wantErrorCode string
	wantKey       string
	wantItemKeys  []string
	wantStatus    int
}

func (hns *harness) relayURL(name string) string {
	switch name {
	case relayAuth:
		return hns.auth
	case relayPublic:
		return hns.public
	case relayAdmin:
		return hns.admin
	case relayStream:
		return hns.stream
	default:
		return hns.base
	}
}

func keyHeader() map[string]string { return map[string]string{headerXAPIKey: validToken} }

func oversizedBody() string {
	return `{"context":{"targetingKey":"user-1","padding":"` + strings.Repeat("a", oversizedBytes) + `"}}`
}

func wireCases() []wireCase {
	single := singleFlagPrefix + flagBoolMatch

	return []wireCase{
		{name: "single: charset parameter on the content type", method: http.MethodPost, path: single,
			body: userOneCtxJSON, contentType: "application/json; charset=utf-8", headers: keyHeader(),
			wantStatus: http.StatusOK, wantKey: flagBoolMatch},
		{name: "single: text/plain content type", method: http.MethodPost, path: single, body: userOneCtxJSON,
			contentType: contentTypePlain, headers: keyHeader(),
			wantStatus: http.StatusBadRequest, wantErrorCode: errCodeInvalidContext, wantKey: flagBoolMatch},
		{name: "single: unknown top level field is ignored", method: http.MethodPost, path: single,
			body: `{"context":{"targetingKey":"u"},"extra":{"a":1}}`, headers: keyHeader(),
			wantStatus: http.StatusOK, wantKey: flagBoolMatch},
		{name: "single: unknown context attributes of every JSON type", method: http.MethodPost, path: single,
			body:    `{"context":{"targetingKey":"u","a":1,"b":[1,"x"],"c":{"d":null},"e":true,"f":null}}`,
			headers: keyHeader(), wantStatus: http.StatusOK, wantKey: flagBoolMatch},
		{name: "single: empty body", method: http.MethodPost, path: single, headers: keyHeader(),
			wantStatus: http.StatusBadRequest, wantErrorCode: errCodeInvalidContext, wantKey: flagBoolMatch},
		{name: "single: null context", method: http.MethodPost, path: single, body: `{"context":null}`,
			headers: keyHeader(), wantStatus: http.StatusBadRequest, wantErrorCode: errCodeInvalidContext,
			wantKey: flagBoolMatch},
		{name: "single: context is a string", method: http.MethodPost, path: single, body: `{"context":"x"}`,
			headers: keyHeader(), wantStatus: http.StatusBadRequest, wantErrorCode: errCodeInvalidContext,
			wantKey: flagBoolMatch},
		{name: "single: context is an array", method: http.MethodPost, path: single, body: `{"context":[]}`,
			headers: keyHeader(), wantStatus: http.StatusBadRequest, wantErrorCode: errCodeInvalidContext,
			wantKey: flagBoolMatch},
		{name: "single: oversized body", method: http.MethodPost, path: single, body: oversizedBody(),
			headers: keyHeader(), wantStatus: http.StatusBadRequest, wantErrorCode: errCodeInvalidContext,
			wantKey: flagBoolMatch},
		{name: "single: key that needs URL escaping", method: http.MethodPost,
			path: singleFlagPrefix + "no%20such%2Fflag%3F", body: userOneCtxJSON, headers: keyHeader(),
			wantStatus: http.StatusNotFound, wantErrorCode: "FLAG_NOT_FOUND", wantKey: "no such/flag?"},
		{name: "single: GET is not allowed", method: http.MethodGet, path: single, headers: keyHeader(),
			wantStatus: http.StatusNotFound},
		{name: "single: PUT is not allowed", method: http.MethodPut, path: single, body: userOneCtxJSON,
			headers: keyHeader(), wantStatus: http.StatusNotFound},
		{name: "single: DELETE is not allowed", method: http.MethodDelete, path: single, headers: keyHeader(),
			wantStatus: http.StatusNotFound},
		{name: "single: bearer scheme is case insensitive", method: http.MethodPost, path: single, relay: relayAuth,
			body: userOneCtxJSON, headers: map[string]string{headerAuth: "bearer " + validToken},
			wantStatus: http.StatusOK, wantKey: flagBoolMatch},
		{name: "single: public flag set without a key", method: http.MethodPost, path: single, relay: relayPublic,
			body: userOneCtxJSON, wantStatus: http.StatusOK, wantKey: flagBoolMatch},
		{name: "single: key in the Authorization header without the Bearer scheme", method: http.MethodPost,
			path: single, relay: relayAuth, body: userOneCtxJSON, headers: map[string]string{headerAuth: validToken},
			wantStatus: http.StatusUnauthorized},
		{name: "single: admin key", method: http.MethodPost, path: single, relay: relayAdmin, body: userOneCtxJSON,
			headers: map[string]string{headerXAPIKey: adminToken}, wantStatus: http.StatusForbidden},
		{name: "bulk: admin key", method: http.MethodPost, path: bulkFlagsPath, relay: relayAdmin, body: userOneCtxJSON,
			headers: map[string]string{headerXAPIKey: adminToken}, wantStatus: http.StatusForbidden},
		{name: "bulk: charset parameter on the content type", method: http.MethodPost, path: bulkFlagsPath,
			body: userOneCtxJSON, contentType: "application/json; charset=utf-8", headers: keyHeader(),
			wantStatus: http.StatusOK},
		{name: "bulk: text/plain content type", method: http.MethodPost, path: bulkFlagsPath, body: userOneCtxJSON,
			contentType: contentTypePlain, headers: keyHeader(),
			wantStatus: http.StatusBadRequest, wantErrorCode: errCodeInvalidContext},
		{name: "bulk: empty body", method: http.MethodPost, path: bulkFlagsPath, headers: keyHeader(),
			wantStatus: http.StatusBadRequest, wantErrorCode: errCodeInvalidContext},
		{name: "bulk: null context", method: http.MethodPost, path: bulkFlagsPath, body: `{"context":null}`,
			headers: keyHeader(), wantStatus: http.StatusBadRequest, wantErrorCode: errCodeInvalidContext},
		{name: "bulk: context is a string", method: http.MethodPost, path: bulkFlagsPath, body: `{"context":"x"}`,
			headers: keyHeader(), wantStatus: http.StatusBadRequest, wantErrorCode: errCodeInvalidContext},
		{name: "bulk: oversized body", method: http.MethodPost, path: bulkFlagsPath, body: oversizedBody(),
			headers: keyHeader(), wantStatus: http.StatusBadRequest, wantErrorCode: errCodeInvalidContext},
		{name: "bulk: unknown top level field is ignored", method: http.MethodPost, path: bulkFlagsPath,
			body: `{"context":{"targetingKey":"u"},"extra":1}`, headers: keyHeader(), wantStatus: http.StatusOK},
		{
			name:    "bulk: unknown flag in the flag list",
			method:  http.MethodPost,
			path:    bulkFlagsPath,
			headers: keyHeader(),
			body: `{"context":{"targetingKey":"u","gofeatureflag":{"flagList":["no_such_flag","` +
				flagBoolMatch + `"]}}}`,
			wantStatus:   http.StatusOK,
			wantItemKeys: []string{flagBoolMatch},
		},
		{name: "bulk: no flag evaluated still answers a flags array", method: http.MethodPost, path: bulkFlagsPath,
			headers:    keyHeader(),
			body:       `{"context":{"targetingKey":"u","gofeatureflag":{"flagList":["no_such_flag"]}}}`,
			wantStatus: http.StatusOK, wantItemKeys: []string{}},
		{name: "bulk: GET is not allowed", method: http.MethodGet, path: bulkFlagsPath, headers: keyHeader(),
			wantStatus: http.StatusNotFound},
		{name: "bulk: public flag set without a key", method: http.MethodPost, path: bulkFlagsPath, relay: relayPublic,
			body: userOneCtxJSON, wantStatus: http.StatusOK},
		{
			name:       "bulk: change notification query parameters, epoch seconds",
			method:     http.MethodPost,
			path:       bulkFlagsPath + "?flagConfigEtag=550e8400-e29b-41d4-a716-446655440000&flagConfigLastModified=1771622898",
			body:       userOneCtxJSON,
			headers:    keyHeader(),
			wantStatus: http.StatusOK,
		},
		{name: "bulk: change notification query parameters, ISO date", method: http.MethodPost,
			path: bulkFlagsPath + "?flagConfigLastModified=2026-02-20T21%3A28%3A18Z", body: userOneCtxJSON,
			headers: keyHeader(), wantStatus: http.StatusOK},
	}
}

func conformanceWire(t *testing.T, hns *harness) {
	t.Helper()

	cases := wireCases()

	for idx := range cases {
		tt := &cases[idx]

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := hns.send(t, &request{
				method: tt.method, url: hns.relayURL(tt.relay) + tt.path, headers: tt.headers,
				body: tt.body, contentType: tt.contentType,
			})
			res.wantStatus(t, tt.wantStatus)

			parsed := decodeObject(t, res.body)
			if tt.wantItemKeys != nil {
				assert.Contains(t, decodeObject(t, res.body), "flags", "the providers read the flags field")
				assert.ElementsMatch(t, tt.wantItemKeys, slices.Collect(maps.Keys(parseBulkFlags(t, res.body))))
			}

			if tt.wantErrorCode != "" {
				if assert.Equal(t, tt.wantErrorCode, parsed[jsonErrorCodeField]) {
					hns.rec.add([]string{ofrepconformance.EnumID(jsonErrorCodeField, tt.wantErrorCode)})
				}
			}

			if tt.wantKey != "" {
				assert.Equal(t, tt.wantKey, parsed[jsonKeyField])
				res.bodyAsserted()
			}

			if tt.wantItemKeys != nil {
				res.bodyAsserted()
			}
		})
	}
}

func decodeObject(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var parsed map[string]any

	require.NoError(t, json.Unmarshal(body, &parsed), "body: %s", string(body))

	return parsed
}

func conformanceBulkETag(t *testing.T, hns *harness) {
	t.Helper()

	bulkURL := hns.base + bulkFlagsPath

	first := hns.post(t, bulkURL, keyHeader(), userOneCtxJSON)
	first.wantStatus(t, http.StatusOK)

	etag := first.header.Get("ETag")
	first.wantStrongETag(t)

	other := hns.post(
		t,
		bulkURL,
		keyHeader(),
		`{"context":{"targetingKey":"user-1","gofeatureflag":{"flagList":["`+flagStringKey+`"]}}}`,
	)
	other.wantStatus(t, http.StatusOK)

	otherETag := other.header.Get("ETag")

	tests := []struct {
		name        string
		ifNoneMatch string
		wantStatus  int
	}{
		{name: "strong tag", ifNoneMatch: etag, wantStatus: http.StatusNotModified},
		{name: "weak form of the tag", ifNoneMatch: "W/" + etag, wantStatus: http.StatusNotModified},
		{name: "list with the tag last", ifNoneMatch: `"a", ` + etag, wantStatus: http.StatusNotModified},
		{name: "list with the tag in the middle, no spaces", ifNoneMatch: `"a",` + etag + `,"b"`,
			wantStatus: http.StatusNotModified},
		{name: "list with the weak tag", ifNoneMatch: `"a" , W/` + etag, wantStatus: http.StatusNotModified},
		{name: "star", ifNoneMatch: "*", wantStatus: http.StatusNotModified},
		{name: "list without the tag", ifNoneMatch: `"a", "b"`, wantStatus: http.StatusOK},
		{name: "tag of another context", ifNoneMatch: otherETag, wantStatus: http.StatusOK},
		{name: "tag without quotes", ifNoneMatch: strings.Trim(etag, `"`), wantStatus: http.StatusOK},
	}

	assert.NotEqual(t, etag, otherETag)

	t.Run("the tag on the second line of the header", func(t *testing.T) {
		t.Parallel()

		res := hns.send(t, &request{
			method: http.MethodPost, url: bulkURL, headers: keyHeader(), body: userOneCtxJSON,
			repeated: map[string][]string{headerIfNoneMatch: {`"a"`, etag}},
		})
		res.wantStatus(t, http.StatusNotModified)
		res.wantETag(t, etag)
		assert.Empty(t, res.body)
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			send := hns.post
			if tt.wantStatus == http.StatusNotModified {
				// the wire bytes show that the 304 has no body
				send = hns.postRaw
			}

			res := send(t, bulkURL, map[string]string{headerXAPIKey: validToken, headerIfNoneMatch: tt.ifNoneMatch},
				userOneCtxJSON)
			res.wantStatus(t, tt.wantStatus)
			res.wantETag(t, etag)

			if tt.wantStatus == http.StatusNotModified {
				assert.Empty(t, res.body)
			}
		})
	}
}

// conformanceEventStreams checks the eventStreams field that a relay configured for it adds to the bulk evaluation:
// the response is validated against the document on every request, and the entry has to be one a provider can use.
func conformanceEventStreams(t *testing.T, hns *harness) {
	t.Helper()

	res := hns.send(t, &request{
		method: http.MethodPost, url: hns.stream + bulkFlagsPath, headers: keyHeader(), body: userOneCtxJSON,
	})
	res.wantStatus(t, http.StatusOK)

	var payload struct {
		EventStreams []struct {
			Type               string `json:"type"`
			URL                string `json:"url"`
			InactivityDelaySec int    `json:"inactivityDelaySec"`
		} `json:"eventStreams"`
	}

	require.NoError(t, json.Unmarshal(res.body, &payload))
	require.Len(t, payload.EventStreams, 1)

	assert.Equal(t, "sse", payload.EventStreams[0].Type)
	assert.Equal(t, streamInactivitySec, payload.EventStreams[0].InactivityDelaySec)
	assert.Equal(t, streamBaseURL+"/stream/v1/sse/flag/change?apiKey="+validToken, payload.EventStreams[0].URL)
	res.bodyAsserted()

	plain := hns.send(t, &request{
		method: http.MethodPost, url: hns.base + bulkFlagsPath, headers: keyHeader(), body: userOneCtxJSON,
	})
	plain.wantStatus(t, http.StatusOK)
	assert.NotContains(
		t,
		string(plain.body),
		"eventStreams",
		"a relay that is not configured for it advertises no stream",
	)
}
