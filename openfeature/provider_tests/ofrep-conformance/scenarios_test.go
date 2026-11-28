package ofrepconformance_test

import (
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
	bulkSame    = "same"
	bulkError   = "error"
	bulkOmitted = "omitted"
)

// scenario is one row of ../ofrep-scenarios/scenarios.json, the language neutral table of what the relay answers.
type scenario struct {
	Context  map[string]any `json:"context"`
	Body     map[string]any `json:"body"`
	Metadata map[string]any `json:"metadata"`
	Name     string         `json:"name"`
	Source   string         `json:"source"`
	Flag     string         `json:"flag"`
	Bulk     string         `json:"bulk"`
	Absent   []string       `json:"absent"`
	Status   int            `json:"status"`
}

func loadScenarios(t *testing.T) []scenario {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("..", "ofrep-scenarios", "scenarios.json"))
	require.NoError(t, err)

	var table struct {
		Scenarios []scenario `json:"scenarios"`
	}

	require.NoError(t, json.Unmarshal(raw, &table))
	require.NotEmpty(t, table.Scenarios)

	for idx := range table.Scenarios {
		assert.Contains(
			t,
			[]string{"spec", "provider", "relay"},
			table.Scenarios[idx].Source,
			table.Scenarios[idx].Name,
		)
	}

	return table.Scenarios
}

// assertFields checks the fields a scenario expects of one evaluation result.
func assertFields(t *testing.T, rec *recorder, got map[string]any, scen *scenario, withReason bool) {
	t.Helper()

	for field, want := range scen.Body {
		if field == jsonReasonField && !withReason {
			continue
		}

		if assert.Equal(t, want, got[field], "field %s of %v", field, got) &&
			(field == jsonReasonField || field == jsonErrorCodeField) {
			rec.add([]string{ofrepconformance.EnumID(field, text(want))})
		}
	}

	for _, field := range scen.Absent {
		if field == jsonReasonField && !withReason {
			continue
		}

		assert.NotContains(t, got, field)
	}

	if len(scen.Metadata) > 0 {
		gotMeta, _ := got["metadata"].(map[string]any)
		for key, want := range scen.Metadata {
			assert.Equal(t, want, gotMeta[key], "metadata %s of %v", key, got)
		}
	}
}

func conformanceScenarios(t *testing.T, hns *harness) {
	t.Helper()

	flags, err := os.ReadFile(filepath.Join("..", "ofrep-scenarios", "flags.yaml"))
	require.NoError(t, err)

	base := startRelayWithFlags(t, string(flags))

	for _, scen := range loadScenarios(t) {
		body, marshalErr := json.Marshal(map[string]any{"context": scen.Context})
		require.NoError(t, marshalErr)

		t.Run("single: "+scen.Name, func(t *testing.T) {
			t.Parallel()

			res := hns.send(t, &request{
				method: http.MethodPost, url: base + singleFlagPrefix + scen.Flag,
				headers: keyHeader(), body: string(body),
			})
			assert.Equal(t, scen.Status, res.status, string(res.body))
			assertFields(t, hns.rec, decodeObject(t, res.body), &scen, true)
		})

		t.Run("bulk: "+scen.Name, func(t *testing.T) {
			t.Parallel()

			res := hns.send(t, &request{
				method: http.MethodPost, url: base + bulkFlagsPath, headers: keyHeader(), body: string(body),
			})
			require.Equal(t, http.StatusOK, res.status, string(res.body))

			item, found := parseBulkFlags(t, res.body)[scen.Flag]

			switch scen.Bulk {
			case bulkOmitted:
				assert.False(t, found, "the bulk response has an item for %s", scen.Flag)
			case bulkError:
				require.True(t, found)
				assert.Equal(t, scen.Body[jsonErrorCodeField], item[jsonErrorCodeField])
				assert.NotContains(t, item, jsonValueField)
			case bulkSame:
				require.True(t, found)
				assertFields(t, hns.rec, item, &scen, true)
			}
		})
	}
}
