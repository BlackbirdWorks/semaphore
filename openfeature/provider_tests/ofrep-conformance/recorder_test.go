package ofrepconformance_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ofrepconformance "github.com/blackbirdworks/semaphore/openfeature/provider_tests/ofrep-conformance"
)

// A response that a case only received claims nothing: the response, its header and its schema are recorded when the
// case asserts on the status, on the header and on the body.
func TestRecorderCountsOnlyWhatACaseAsserted(t *testing.T) {
	t.Parallel()

	spec, err := ofrepconformance.LoadSpec(specPath)
	require.NoError(t, err)

	hns := &harness{spec: spec, rec: &recorder{seen: make(map[string]struct{})}, base: startRelay(t, "bootstrap.yaml")}

	var (
		response = ofrepconformance.ResponseID(http.MethodPost, bulkFlagsPath, http.StatusOK)
		header   = ofrepconformance.HeaderID(http.MethodPost, bulkFlagsPath, http.StatusOK, "ETag")
		schema   = ofrepconformance.SchemaID(http.MethodPost, bulkFlagsPath, http.StatusOK)
	)

	res := hns.post(t, hns.base+bulkFlagsPath, keyHeader(), userOneCtxJSON)
	require.Equal(t, http.StatusOK, res.status)

	assert.False(t, hns.rec.has(response), "a response that was only returned")
	assert.False(t, hns.rec.has(header))
	assert.False(t, hns.rec.has(schema))

	res.wantStatus(t, http.StatusOK)
	assert.True(t, hns.rec.has(response))
	assert.False(t, hns.rec.has(header), "the status was asserted, the header was not")
	assert.False(t, hns.rec.has(schema))

	res.wantStrongETag(t)
	assert.True(t, hns.rec.has(header))
	assert.False(t, hns.rec.has(schema), "the body was not asserted")

	res.bodyAsserted()
	assert.True(t, hns.rec.has(schema))
}
