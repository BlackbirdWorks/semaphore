package ofrepconformance_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ofrepconformance "github.com/blackbirdworks/semaphore/openfeature/provider_tests/ofrep-conformance"
)

const (
	specPath = "testdata/openapi.yaml"
	pinPath  = "testdata/openapi.pin.json"
)

// pin is the record of where the vendored OFREP document comes from.
type pin struct {
	Repository string `json:"repository"`
	Path       string `json:"path"`
	Source     string `json:"source"`
	Tag        string `json:"tag"`
	Commit     string `json:"commit"`
	Version    string `json:"version"`
	SHA256     string `json:"sha256"`
}

func readPin(t *testing.T) pin {
	t.Helper()

	raw, err := os.ReadFile(pinPath)
	require.NoError(t, err)

	var got pin

	require.NoError(t, json.Unmarshal(raw, &got))

	return got
}

// TestPinnedSpecMatchesRecordedHash fails when the vendored document is not the one the pin file records.
func TestPinnedSpecMatchesRecordedHash(t *testing.T) {
	t.Parallel()

	got := readPin(t)

	raw, err := os.ReadFile(specPath)
	require.NoError(t, err)

	sum := sha256.Sum256(raw)

	assert.Equal(t, got.SHA256, hex.EncodeToString(sum[:]), "openapi.yaml differs from the pinned document")
	assert.Equal(t, "open-feature/protocol", got.Repository)
	assert.Equal(t, "service/openapi.yaml", got.Path)
	assert.Regexp(t, `^[0-9a-f]{40}$`, got.Commit)
	assert.Equal(t, "https://raw.githubusercontent.com/"+got.Repository+"/"+got.Commit+"/"+got.Path, got.Source)
	assert.Contains(t, string(raw), "version: "+got.Version, "the document states another version than the pin")
}

// exempt lists the requirements of the pinned document that the relay cannot be made to produce, with the reason.
// The coverage test fails when an exempt item is covered after all, so the list cannot go stale.
func exempt() map[string]string {
	const (
		noRateLimit = "the relay has no rate limit, so it never answers 429"
		noFault     = "the relay answers 500 only for a fault of its own, which no request can provoke"
	)

	out := make(map[string]string)

	for _, path := range []string{singleFlagPath, bulkFlagsPath} {
		out[ofrepconformance.ResponseID(http.MethodPost, path, http.StatusTooManyRequests)] = noRateLimit
		out[ofrepconformance.HeaderID(http.MethodPost, path, http.StatusTooManyRequests, headerRetryAfter)] = noRateLimit
		out[ofrepconformance.ResponseID(http.MethodPost, path, http.StatusInternalServerError)] = noFault
		out[ofrepconformance.SchemaID(http.MethodPost, path, http.StatusInternalServerError)] = noFault
	}

	out[ofrepconformance.EnumID("reason", "UNKNOWN")] = "the engine maps the reasons UNKNOWN and OFFLINE to UNKNOWN, " +
		"and no flag evaluation of a running relay ends with one of them (the handler unit tests cover the mapping)"
	out[ofrepconformance.EnumID("errorCode", "PARSE_ERROR")] = "the relay never sends PARSE_ERROR: a flag " +
		"that cannot be parsed is rejected when the flags are loaded, a request body that cannot be parsed is " +
		"INVALID_CONTEXT"
	out[ofrepconformance.EnumID("errorCode", "GENERAL")] = "GENERAL answers a request without a flag key, which " +
		"the router refuses before the handler (404), and an evaluation without a service, which a running relay " +
		"does not have"

	return out
}

// TestOFREPConformance runs every conformance case against real relays, then checks that the cases exercised every
// requirement the pinned document defines: a new version of the document cannot be adopted with silent gaps.
func TestOFREPConformance(t *testing.T) {
	t.Parallel()

	spec, err := ofrepconformance.LoadSpec(specPath)
	require.NoError(t, err)

	flags, err := os.ReadFile(filepath.Join("..", "flags.yaml"))
	require.NoError(t, err)

	hns := &harness{
		spec:   spec,
		rec:    &recorder{seen: make(map[string]struct{})},
		base:   startRelay(t, "bootstrap.yaml"),
		auth:   startRelay(t, "bootstrap-authenticated.yaml"),
		public: startRelayWithFlagSet(t, string(flags), false),
		admin:  startRelayWithAdminKey(t, adminToken),
		stream: startRelayWithEventStream(t, streamBaseURL, streamInactivitySec),
	}

	cases := map[string]func(*testing.T, *harness){
		"single":    conformanceSingleFlag,
		"bulk":      conformanceBulk,
		"reasons":   conformanceReasons,
		"wire":      conformanceWire,
		"etag":      conformanceBulkETag,
		"scenarios": conformanceScenarios,
		"stream":    conformanceEventStreams,
	}

	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fn(t, hns)
		})
	}

	// the subtests run in parallel and are finished when the cleanup of their parent runs
	t.Cleanup(func() { checkCoverage(t, spec, hns.rec) })
}

func checkCoverage(t *testing.T, spec *ofrepconformance.Spec, rec *recorder) {
	t.Helper()

	skip := exempt()

	for _, id := range spec.Requirements() {
		reason, isExempt := skip[id]
		if isExempt {
			assert.False(t, rec.has(id), "%s is covered: remove it from the exempt list", id)
			assert.NotEmpty(t, reason, id)

			continue
		}

		assert.True(t, rec.has(id), "the pinned document defines %q and no conformance case exercises it", id)
	}
}
