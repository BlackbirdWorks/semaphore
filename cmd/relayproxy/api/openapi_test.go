package api_test

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/api"
)

// TestOpenAPI compares the document generated from the registered operations with the committed copy: a change of
// the API shows in the review. GOLDEN_UPDATE=1 rewrites the files.
func TestOpenAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		file string
	}{
		{name: "document", file: "testdata/openapi.golden.json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := api.OpenAPIJSON()
			require.NoError(t, err)

			if os.Getenv(goldenUpdate) != "" {
				require.NoError(t, os.WriteFile(tt.file, got, 0o600))

				return
			}

			want, err := os.ReadFile(tt.file)
			require.NoError(t, err, "missing golden file, run with GOLDEN_UPDATE=1")
			assert.Equal(t, string(want), string(got))
		})
	}
}

// The operations the public flags can reach document an optional key: the key schemes they accept, and the empty
// requirement next to them, with the note that says why. The admin operations keep a required key.
func TestOpenAPIOptionalKey(t *testing.T) {
	t.Parallel()

	const (
		header = "ApiKeyAuth"
		xKey   = "XApiKeyAuth"
		bearer = "BearerAuth"
		query  = "ApiKeyQueryAuth"
	)

	both := [][]string{{header}, {xKey}, {}}
	want := map[string][][]string{
		"allFlags": both, "collectEvalData": both, "flagChange": both, "flagConfiguration": both, "flagEval": both,
		"ofrepBulkEvaluate": {{xKey}, {bearer}, {}}, "ofrepEvaluate": {{xKey}, {bearer}, {}},
		"getManifest":        {{header}, {}},
		"sseFlagChange":      {{query}, {}},
		"wsFlagChange":       {{query}, {}},
		"wsFlagChangeLegacy": {{query}, {}},
		// the admin operations: a key is required
		"validateFlags":    {{header}},
		"retrieverRefresh": {{header}},
	}

	got := map[string][][]string{}
	noted := map[string]bool{}

	for _, item := range api.OpenAPI().Paths {
		for _, operation := range []*huma.Operation{item.Get, item.Post, item.Put, item.Delete} {
			if operation == nil || len(operation.Security) == 0 {
				continue
			}

			requirements := make([][]string, 0, len(operation.Security))

			for _, requirement := range operation.Security {
				names := make([]string, 0, len(requirement))
				for name := range requirement {
					names = append(names, name)
				}

				slices.Sort(names)

				requirements = append(requirements, names)
			}

			got[operation.OperationID] = requirements
			noted[operation.OperationID] = strings.Contains(operation.Description, "published without API keys")
		}
	}

	assert.Equal(t, want, got, "the security of every operation that has one")

	for _, item := range api.OpenAPI().Paths {
		for _, operation := range []*huma.Operation{item.Get, item.Post, item.Put, item.Delete} {
			if operation == nil {
				continue
			}

			for _, param := range operation.Parameters {
				assert.False(t, param.In == "query" && param.Name == "apiKey",
					"%s declares the key as a parameter and as a security scheme", operation.OperationID)
			}
		}
	}

	for operationID, requirements := range got {
		hasEmpty := slices.ContainsFunc(requirements, func(names []string) bool { return len(names) == 0 })

		assert.Equal(t, hasEmpty, noted[operationID], "the note of "+operationID+" says what its security says")
	}
}
