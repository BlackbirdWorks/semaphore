package ofrepconformance_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ofrepconformance "github.com/blackbirdworks/semaphore/openfeature/provider_tests/ofrep-conformance"
)

const (
	validToken = "authorized_token"

	headerXAPIKey      = "X-API-Key"
	headerAuth         = "Authorization"
	headerIfNoneMatch  = "If-None-Match"
	jsonKeyField       = "key"
	jsonValueField     = "value"
	jsonReasonField    = "reason"
	jsonVariantField   = "variant"
	jsonErrorCodeField = "errorCode"
	reasonTargetMatch  = "TARGETING_MATCH"
	variantTrue        = "True"

	flagBoolMatch         = "bool_targeting_match"
	flagDisabledBool      = "disabled_bool"
	flagDisabledFloat     = "disabled_float"
	flagDisabledInt       = "disabled_int"
	flagDisabledInterface = "disabled_interface"
	flagDisabledString    = "disabled_string"
	flagStringKey         = "string_key"
	flagIntegerKey        = "integer_key"
	flagDoubleKey         = "double_key"
	flagObjectKey         = "object_key"
	flagEnrichment        = "flag-use-evaluation-context-enrichment"
	flagSemver            = "boolean_semver_targeting_match"

	errCodeTargetingKeyMissing = "TARGETING_KEY_MISSING"
	errCodeInvalidContext      = "INVALID_CONTEXT"
	contentTypeJSON            = "application/json"
	reasonDisabled             = "DISABLED"
	reasonSplit                = "SPLIT"
	reasonStatic               = "STATIC"

	defaultCtxJSON    = `{"context":{"targetingKey":"user-1","email":"john.doe@gofeatureflag.org","version":"10.0.0-10"}}`
	userOneCtxJSON    = `{"context":{"targetingKey":"user-1"}}`
	activeFlagListCtx = `{"context":{"targetingKey":"user-1","email":"john.doe@gofeatureflag.org",` +
		`"gofeatureflag":{"flagList":["bool_targeting_match","string_key","integer_key","double_key","object_key"]}}}`
)

type singleFlagCase struct {
	headers          map[string]string
	wantFields       map[string]any
	name             string
	targetURL        string
	flagKey          string
	body             string
	wantAbsentFields []string
	wantStatus       int
}

func singleFlagTestCases(baseURL, authURL string) []singleFlagCase {
	return []singleFlagCase{
		{
			name:       "bool targeting match",
			targetURL:  baseURL,
			flagKey:    flagBoolMatch,
			body:       defaultCtxJSON,
			wantStatus: http.StatusOK,
			wantFields: map[string]any{
				jsonKeyField:     flagBoolMatch,
				jsonValueField:   true,
				jsonReasonField:  reasonTargetMatch,
				jsonVariantField: variantTrue,
			},
		},
		{
			name:       "string targeting match",
			targetURL:  baseURL,
			flagKey:    flagStringKey,
			body:       defaultCtxJSON,
			wantStatus: http.StatusOK,
			wantFields: map[string]any{
				jsonKeyField:     flagStringKey,
				jsonValueField:   "CC0000",
				jsonReasonField:  reasonTargetMatch,
				jsonVariantField: variantTrue,
			},
		},
		{
			name:       "integer targeting match",
			targetURL:  baseURL,
			flagKey:    flagIntegerKey,
			body:       defaultCtxJSON,
			wantStatus: http.StatusOK,
			wantFields: map[string]any{
				jsonKeyField:     flagIntegerKey,
				jsonValueField:   float64(100),
				jsonReasonField:  reasonTargetMatch,
				jsonVariantField: variantTrue,
			},
		},
		{
			name:       "float targeting match",
			targetURL:  baseURL,
			flagKey:    flagDoubleKey,
			body:       defaultCtxJSON,
			wantStatus: http.StatusOK,
			wantFields: map[string]any{
				jsonKeyField:     flagDoubleKey,
				jsonValueField:   100.25,
				jsonReasonField:  reasonTargetMatch,
				jsonVariantField: variantTrue,
			},
		},
		{
			name:       "object targeting match",
			targetURL:  baseURL,
			flagKey:    flagObjectKey,
			body:       defaultCtxJSON,
			wantStatus: http.StatusOK,
			wantFields: map[string]any{
				jsonKeyField:     flagObjectKey,
				jsonValueField:   expectedObjectValue(),
				jsonReasonField:  reasonTargetMatch,
				jsonVariantField: variantTrue,
			},
		},
		{
			name:       "empty context without targetingKey",
			targetURL:  baseURL,
			flagKey:    flagEnrichment,
			body:       `{"context":{}}`,
			wantStatus: http.StatusOK,
			wantFields: map[string]any{
				jsonKeyField:    flagEnrichment,
				jsonValueField:  "A",
				jsonReasonField: reasonTargetMatch,
			},
		},
		{
			name:       "bucketing flag without targetingKey 400",
			targetURL:  baseURL,
			flagKey:    flagBoolMatch,
			body:       `{"context":{"email":"john.doe@gofeatureflag.org"}}`,
			wantStatus: http.StatusBadRequest,
			wantFields: map[string]any{jsonKeyField: flagBoolMatch, jsonErrorCodeField: errCodeTargetingKeyMissing},
		},
		{
			name:       "context enrichment on bootstrap.yaml",
			targetURL:  baseURL,
			flagKey:    flagEnrichment,
			body:       userOneCtxJSON,
			wantStatus: http.StatusOK,
			wantFields: map[string]any{
				jsonKeyField:    flagEnrichment,
				jsonValueField:  "A",
				jsonReasonField: reasonTargetMatch,
			},
		},
		{
			name:       "semver targeting match",
			targetURL:  baseURL,
			flagKey:    flagSemver,
			body:       defaultCtxJSON,
			wantStatus: http.StatusOK,
			wantFields: map[string]any{
				jsonKeyField:    flagSemver,
				jsonValueField:  true,
				jsonReasonField: reasonTargetMatch,
			},
		},
		{
			name:             "disabled bool flag",
			targetURL:        baseURL,
			flagKey:          flagDisabledBool,
			body:             defaultCtxJSON,
			wantStatus:       http.StatusOK,
			wantAbsentFields: []string{jsonValueField},
			wantFields: map[string]any{
				jsonKeyField:    flagDisabledBool,
				jsonReasonField: reasonDisabled,
			},
		},
		{
			name:             "disabled string flag",
			targetURL:        baseURL,
			flagKey:          flagDisabledString,
			body:             defaultCtxJSON,
			wantStatus:       http.StatusOK,
			wantAbsentFields: []string{jsonValueField},
			wantFields: map[string]any{
				jsonKeyField:    flagDisabledString,
				jsonReasonField: reasonDisabled,
			},
		},
		{
			name:             "disabled int flag",
			targetURL:        baseURL,
			flagKey:          flagDisabledInt,
			body:             defaultCtxJSON,
			wantStatus:       http.StatusOK,
			wantAbsentFields: []string{jsonValueField},
			wantFields: map[string]any{
				jsonKeyField:    flagDisabledInt,
				jsonReasonField: reasonDisabled,
			},
		},
		{
			name:             "disabled float flag",
			targetURL:        baseURL,
			flagKey:          flagDisabledFloat,
			body:             defaultCtxJSON,
			wantStatus:       http.StatusOK,
			wantAbsentFields: []string{jsonValueField},
			wantFields: map[string]any{
				jsonKeyField:    flagDisabledFloat,
				jsonReasonField: reasonDisabled,
			},
		},
		{
			name:             "disabled object flag",
			targetURL:        baseURL,
			flagKey:          flagDisabledInterface,
			body:             defaultCtxJSON,
			wantStatus:       http.StatusOK,
			wantAbsentFields: []string{jsonValueField},
			wantFields: map[string]any{
				jsonKeyField:    flagDisabledInterface,
				jsonReasonField: reasonDisabled,
			},
		},
		{
			name:       "default rule fallback maps to STATIC",
			targetURL:  baseURL,
			flagKey:    flagSemver,
			body:       `{"context":{"targetingKey":"user-1","version":"10.0.0-2"}}`,
			wantStatus: http.StatusOK,
			wantFields: map[string]any{
				jsonKeyField:    flagSemver,
				jsonValueField:  false,
				jsonReasonField: reasonStatic,
			},
		},
		{
			name:       "flag not found 404",
			targetURL:  baseURL,
			flagKey:    "does_not_exists",
			body:       defaultCtxJSON,
			wantStatus: http.StatusNotFound,
			wantFields: map[string]any{jsonKeyField: "does_not_exists", jsonErrorCodeField: "FLAG_NOT_FOUND"},
		},
		{
			name:       "missing context 400",
			targetURL:  baseURL,
			flagKey:    flagBoolMatch,
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
			wantFields: map[string]any{jsonKeyField: flagBoolMatch, jsonErrorCodeField: errCodeInvalidContext},
		},
		{
			name:       "malformed JSON 400",
			targetURL:  baseURL,
			flagKey:    flagBoolMatch,
			body:       `{`,
			wantStatus: http.StatusBadRequest,
			wantFields: map[string]any{jsonKeyField: flagBoolMatch, jsonErrorCodeField: errCodeInvalidContext},
		},
		{
			name:       "non-string targetingKey 400",
			targetURL:  baseURL,
			flagKey:    flagBoolMatch,
			body:       `{"context":{"targetingKey":123}}`,
			wantStatus: http.StatusBadRequest,
			wantFields: map[string]any{jsonKeyField: flagBoolMatch, jsonErrorCodeField: errCodeTargetingKeyMissing},
		},
		{
			name:       "authenticated valid X-API-Key",
			targetURL:  authURL,
			flagKey:    flagBoolMatch,
			headers:    map[string]string{headerXAPIKey: validToken},
			body:       defaultCtxJSON,
			wantStatus: http.StatusOK,
			wantFields: map[string]any{jsonKeyField: flagBoolMatch, jsonValueField: true},
		},
		{
			name:       "authenticated valid Bearer token",
			targetURL:  authURL,
			flagKey:    flagBoolMatch,
			headers:    map[string]string{headerAuth: "Bearer " + validToken},
			body:       defaultCtxJSON,
			wantStatus: http.StatusOK,
			wantFields: map[string]any{jsonKeyField: flagBoolMatch, jsonValueField: true},
		},
		{
			name:       "authenticated missing auth 401",
			targetURL:  authURL,
			flagKey:    flagBoolMatch,
			body:       defaultCtxJSON,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "authenticated invalid X-API-Key 401",
			targetURL:  authURL,
			flagKey:    flagBoolMatch,
			headers:    map[string]string{headerXAPIKey: "wrong"},
			body:       defaultCtxJSON,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "authenticated invalid Bearer 401",
			targetURL:  authURL,
			flagKey:    flagBoolMatch,
			headers:    map[string]string{headerAuth: "Bearer wrong"},
			body:       defaultCtxJSON,
			wantStatus: http.StatusUnauthorized,
		},
	}
}

func runSingleFlagCase(
	t *testing.T,
	hns *harness,
	spec *ofrepconformance.Spec,
	baseURL string,
	testCase *singleFlagCase,
) {
	t.Helper()

	headers := testCase.headers
	if headers == nil && testCase.targetURL == baseURL {
		headers = map[string]string{headerXAPIKey: validToken}
	}

	res := hns.post(
		t,
		testCase.targetURL+"/ofrep/v1/evaluate/flags/"+testCase.flagKey,
		headers,
		testCase.body,
	)
	res.wantStatus(t, testCase.wantStatus)

	valErr := spec.ValidateResponse(singleFlagPath, http.MethodPost, res.status, res.header, res.body)
	require.NoError(t, valErr, "body: %s", string(res.body))

	if len(testCase.wantFields) > 0 || len(testCase.wantAbsentFields) > 0 {
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(res.body, &parsed))

		defer res.bodyAsserted()

		for key, want := range testCase.wantFields {
			got, exists := parsed[key]
			assert.True(t, exists, "missing field %s", key)
			assert.Equal(t, want, got, "field %s", key)
		}

		for _, key := range testCase.wantAbsentFields {
			assert.NotContains(t, parsed, key, "unexpected field %s", key)
		}
	}
}

func conformanceSingleFlag(t *testing.T, hns *harness) {
	t.Helper()

	spec := hns.spec

	baseURL, authURL := hns.base, hns.auth

	for _, tt := range singleFlagTestCases(baseURL, authURL) {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runSingleFlagCase(t, hns, spec, baseURL, &tt)
		})
	}
}

func expectedObjectValue() map[string]any {
	return map[string]any{
		"test":  "test1",
		"test2": false,
		"test3": 123.3,
		"test4": float64(1),
	}
}

func expectedActiveBulkValues() map[string]any {
	return map[string]any{
		flagBoolMatch:  true,
		flagStringKey:  "CC0000",
		flagIntegerKey: float64(100),
		flagDoubleKey:  100.25,
		flagObjectKey:  expectedObjectValue(),
	}
}

func parseBulkFlags(t *testing.T, body []byte) map[string]map[string]any {
	t.Helper()

	var payload struct {
		Flags []map[string]any `json:"flags"`
	}
	require.NoError(t, json.Unmarshal(body, &payload), "body: %s", string(body))

	byKey := make(map[string]map[string]any, len(payload.Flags))
	for _, entry := range payload.Flags {
		key, ok := entry[jsonKeyField].(string)
		require.True(t, ok, "bulk flag item missing string key: %v", entry)
		require.NotContains(t, byKey, key, "duplicate bulk flag key %q", key)
		byKey[key] = entry
	}

	return byKey
}

func assertActiveBulkFlags(t *testing.T, body []byte) {
	t.Helper()

	byKey := parseBulkFlags(t, body)
	wantValues := expectedActiveBulkValues()
	require.Len(t, byKey, len(wantValues))

	for key, wantVal := range wantValues {
		entry, ok := byKey[key]
		require.True(t, ok, "missing flag %q in bulk response", key)
		assert.Equal(t, wantVal, entry[jsonValueField], "flag %q", key)
	}
}

type bulkTestCase struct {
	headers             map[string]string
	wantFlagErrors      map[string]string
	wantReasonByKey     map[string]string
	name                string
	targetURL           string
	body                string
	wantErrorCode       string
	wantFlagKeys        []string
	wantAbsentValueKeys []string
	wantStatus          int
}

func assertBulkValidResponse(t *testing.T, body []byte, valErr error, testCase *bulkTestCase) {
	t.Helper()

	require.NoError(t, valErr, "body: %s", string(body))

	if testCase.wantErrorCode != "" {
		var errBody map[string]any
		require.NoError(t, json.Unmarshal(body, &errBody))
		assert.Equal(t, testCase.wantErrorCode, errBody[jsonErrorCodeField])
	}

	hasFlagChecks := len(testCase.wantFlagKeys) > 0 || len(testCase.wantFlagErrors) > 0 ||
		len(testCase.wantAbsentValueKeys) > 0 || len(testCase.wantReasonByKey) > 0
	if !hasFlagChecks {
		return
	}

	flags := parseBulkFlags(t, body)

	if len(testCase.wantFlagKeys) > 0 {
		assert.Len(t, flags, len(testCase.wantFlagKeys))

		for _, key := range testCase.wantFlagKeys {
			assert.Contains(t, flags, key)
		}
	}

	for key, wantErrCode := range testCase.wantFlagErrors {
		entry, ok := flags[key]
		require.True(t, ok, "missing expected error flag %q", key)
		assert.Equal(t, wantErrCode, entry[jsonErrorCodeField], "flag %q", key)
	}

	for _, key := range testCase.wantAbsentValueKeys {
		entry, ok := flags[key]
		require.True(t, ok, "missing expected flag %q", key)
		assert.NotContains(t, entry, jsonValueField, "flag %q", key)
	}

	for key, wantReason := range testCase.wantReasonByKey {
		entry, ok := flags[key]
		require.True(t, ok, "missing expected flag %q", key)
		assert.Equal(t, wantReason, entry[jsonReasonField], "flag %q", key)
	}
}

func conformanceBulk(t *testing.T, hns *harness) {
	t.Helper()

	spec := hns.spec

	baseURL, authURL := hns.base, hns.auth

	t.Run("active flags ETag and If-None-Match 304", func(t *testing.T) {
		t.Parallel()

		authHdr := map[string]string{headerXAPIKey: validToken}
		res := hns.post(t, baseURL+bulkFlagsPath, authHdr, activeFlagListCtx)
		res.wantStatus(t, http.StatusOK)
		require.NoError(t, spec.ValidateResponse(bulkFlagsPath, http.MethodPost, res.status, res.header, res.body))
		assertActiveBulkFlags(t, res.body)
		res.bodyAsserted()

		etag := res.header.Get("ETag")
		res.wantStrongETag(t)

		notModified := hns.postRaw(
			t, baseURL+bulkFlagsPath,
			map[string]string{headerXAPIKey: validToken, headerIfNoneMatch: etag}, activeFlagListCtx,
		)
		notModified.wantStatus(t, http.StatusNotModified)
		assert.Empty(t, notModified.body)
		notModified.wantETag(t, etag)
		require.NoError(t, spec.ValidateResponse(bulkFlagsPath, http.MethodPost, notModified.status,
			notModified.header, notModified.body))

		weak := hns.postRaw(
			t, baseURL+bulkFlagsPath,
			map[string]string{headerXAPIKey: validToken, headerIfNoneMatch: "W/" + etag}, activeFlagListCtx,
		)
		weak.wantStatus(t, http.StatusNotModified)
		assert.Empty(t, weak.body)

		stale := hns.post(
			t, baseURL+bulkFlagsPath,
			map[string]string{headerXAPIKey: validToken, headerIfNoneMatch: `"stale"`}, activeFlagListCtx,
		)
		stale.wantStatus(t, http.StatusOK)
		assertActiveBulkFlags(t, stale.body)
		stale.wantETag(t, etag)
	})

	activeFlagKeys := []string{flagBoolMatch, flagStringKey, flagIntegerKey, flagDoubleKey, flagObjectKey}

	tests := []bulkTestCase{
		{
			name:       "full store bulk evaluation",
			targetURL:  baseURL,
			body:       defaultCtxJSON,
			wantStatus: http.StatusOK,
			wantAbsentValueKeys: []string{
				flagDisabledBool,
				flagDisabledFloat,
				flagDisabledInt,
				flagDisabledInterface,
				flagDisabledString,
			},
			wantReasonByKey: map[string]string{
				flagDisabledBool:      reasonDisabled,
				flagDisabledFloat:     reasonDisabled,
				flagDisabledInt:       reasonDisabled,
				flagDisabledInterface: reasonDisabled,
				flagDisabledString:    reasonDisabled,
			},
		},
		{
			name:            "bulk default rule fallback maps to STATIC",
			targetURL:       baseURL,
			body:            `{"context":{"targetingKey":"user-1","gofeatureflag":{"flagList":["bool_targeting_match"]}}}`,
			wantStatus:      http.StatusOK,
			wantFlagKeys:    []string{flagBoolMatch},
			wantReasonByKey: map[string]string{flagBoolMatch: reasonStatic},
		},
		{
			name:                "bulk individual flag error item",
			targetURL:           baseURL,
			body:                `{"context":{"gofeatureflag":{"flagList":["bool_targeting_match"]}}}`,
			wantStatus:          http.StatusOK,
			wantFlagKeys:        []string{flagBoolMatch},
			wantFlagErrors:      map[string]string{flagBoolMatch: errCodeTargetingKeyMissing},
			wantAbsentValueKeys: []string{flagBoolMatch},
		},
		{
			name:          "missing context 400",
			targetURL:     baseURL,
			body:          `{}`,
			wantStatus:    http.StatusBadRequest,
			wantErrorCode: errCodeInvalidContext,
		},
		{
			name:          "malformed JSON 400",
			targetURL:     baseURL,
			body:          `{`,
			wantStatus:    http.StatusBadRequest,
			wantErrorCode: errCodeInvalidContext,
		},
		{
			name:         "authenticated valid X-API-Key",
			targetURL:    authURL,
			headers:      map[string]string{headerXAPIKey: validToken},
			body:         activeFlagListCtx,
			wantStatus:   http.StatusOK,
			wantFlagKeys: activeFlagKeys,
		},
		{
			name:         "authenticated valid Bearer",
			targetURL:    authURL,
			headers:      map[string]string{headerAuth: "Bearer " + validToken},
			body:         activeFlagListCtx,
			wantStatus:   http.StatusOK,
			wantFlagKeys: activeFlagKeys,
		},
		{
			name:       "authenticated missing auth 401",
			targetURL:  authURL,
			body:       activeFlagListCtx,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "authenticated invalid key 401",
			targetURL:  authURL,
			headers:    map[string]string{headerXAPIKey: "wrong"},
			body:       activeFlagListCtx,
			wantStatus: http.StatusUnauthorized,
		},
	}

	for idx := range tests {
		tt := &tests[idx]

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			headers := tt.headers
			if headers == nil && tt.targetURL == baseURL {
				headers = map[string]string{headerXAPIKey: validToken}
			}

			res := hns.post(t, tt.targetURL+bulkFlagsPath, headers, tt.body)
			res.wantStatus(t, tt.wantStatus)

			valErr := spec.ValidateResponse(bulkFlagsPath, http.MethodPost, res.status, res.header, res.body)
			assertBulkValidResponse(t, res.body, valErr, tt)
			res.bodyAsserted()
		})
	}
}

func conformanceReasons(t *testing.T, hns *harness) {
	t.Helper()

	const reasonFlagsYAML = `static_flag:
  variations:
    v1: "static"
  defaultRule:
    variation: v1
default_rule_flag:
  variations:
    match: "matched"
    fallback: "default"
  targeting:
    - query: tier eq "vip"
      variation: match
  defaultRule:
    variation: fallback
targeting_flag:
  variations:
    match: "matched"
    fallback: "default"
  targeting:
    - query: tier eq "vip"
      variation: match
  defaultRule:
    variation: fallback
split_flag:
  variations:
    a: "a"
    b: "b"
  defaultRule:
    percentage:
      a: 50
      b: 50
targeting_split_flag:
  variations:
    a: "a"
    b: "b"
    fallback: "default"
  targeting:
    - query: tier eq "vip"
      percentage:
        a: 50
        b: 50
  defaultRule:
    variation: fallback
disabled_flag:
  variations:
    v1: true
  defaultRule:
    variation: v1
  disable: true
`

	spec := hns.spec

	baseURL := startRelayWithFlags(t, reasonFlagsYAML)
	hdrs := map[string]string{headerXAPIKey: validToken}

	tests := []struct {
		name           string
		flagKey        string
		contextJSON    string
		wantReason     string
		wantBulkReason string
		wantErrorCode  string
		wantStatus     int
		wantOmitValue  bool
	}{
		{
			name:           "engine STATIC maps to STATIC",
			flagKey:        "static_flag",
			contextJSON:    userOneCtxJSON,
			wantStatus:     http.StatusOK,
			wantReason:     reasonStatic,
			wantBulkReason: reasonStatic,
		},
		{
			name:           "engine DEFAULT maps to STATIC",
			flagKey:        "default_rule_flag",
			contextJSON:    `{"context":{"targetingKey":"user-1","tier":"free"}}`,
			wantStatus:     http.StatusOK,
			wantReason:     reasonStatic,
			wantBulkReason: reasonStatic,
		},
		{
			name:           "engine TARGETING_MATCH maps to TARGETING_MATCH",
			flagKey:        "targeting_flag",
			contextJSON:    `{"context":{"targetingKey":"user-1","tier":"vip"}}`,
			wantStatus:     http.StatusOK,
			wantReason:     reasonTargetMatch,
			wantBulkReason: reasonTargetMatch,
		},
		{
			name:           "engine SPLIT maps to SPLIT",
			flagKey:        "split_flag",
			contextJSON:    userOneCtxJSON,
			wantStatus:     http.StatusOK,
			wantReason:     reasonSplit,
			wantBulkReason: reasonSplit,
		},
		{
			name:           "engine TARGETING_MATCH_SPLIT maps to SPLIT",
			flagKey:        "targeting_split_flag",
			contextJSON:    `{"context":{"targetingKey":"user-1","tier":"vip"}}`,
			wantStatus:     http.StatusOK,
			wantReason:     reasonSplit,
			wantBulkReason: reasonSplit,
		},
		{
			name:           "engine DISABLED maps to DISABLED and omits value",
			flagKey:        "disabled_flag",
			contextJSON:    userOneCtxJSON,
			wantStatus:     http.StatusOK,
			wantReason:     reasonDisabled,
			wantBulkReason: reasonDisabled,
			wantOmitValue:  true,
		},
		{
			name:           "engine ERROR returns 400 on single and ERROR item on bulk",
			flagKey:        "split_flag",
			contextJSON:    `{"context":{"tier":"vip"}}`,
			wantStatus:     http.StatusBadRequest,
			wantBulkReason: "ERROR",
			wantErrorCode:  errCodeTargetingKeyMissing,
			wantOmitValue:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			single := hns.post(
				t,
				baseURL+"/ofrep/v1/evaluate/flags/"+tt.flagKey,
				hdrs,
				tt.contextJSON,
			)
			single.wantStatus(t, tt.wantStatus)
			require.NoError(t, spec.ValidateResponse(singleFlagPath, http.MethodPost, single.status, single.header,
				single.body))

			var singleParsed map[string]any
			require.NoError(t, json.Unmarshal(single.body, &singleParsed))

			defer single.bodyAsserted()

			if tt.wantReason != "" {
				assert.Equal(t, tt.wantReason, singleParsed[jsonReasonField])
			}

			if tt.wantErrorCode != "" {
				assert.Equal(t, tt.wantErrorCode, singleParsed[jsonErrorCodeField])
			}

			if tt.wantOmitValue {
				assert.NotContains(t, singleParsed, jsonValueField)
			}

			bulk := hns.post(t, baseURL+bulkFlagsPath, hdrs, tt.contextJSON)
			bulk.wantStatus(t, http.StatusOK)
			require.NoError(t, spec.ValidateResponse(bulkFlagsPath, http.MethodPost, bulk.status, bulk.header,
				bulk.body))

			defer bulk.bodyAsserted()

			flags := parseBulkFlags(t, bulk.body)
			entry, ok := flags[tt.flagKey]
			require.True(t, ok, "missing flag %q in bulk response", tt.flagKey)
			assert.Equal(t, tt.wantBulkReason, entry[jsonReasonField])

			if tt.wantErrorCode != "" {
				assert.Equal(t, tt.wantErrorCode, entry[jsonErrorCodeField])
			}

			if tt.wantOmitValue {
				assert.NotContains(t, entry, jsonValueField)
			}
		})
	}
}
