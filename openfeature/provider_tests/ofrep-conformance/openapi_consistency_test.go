package ofrepconformance_test

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	ofrepconformance "github.com/blackbirdworks/semaphore/openfeature/provider_tests/ofrep-conformance"
)

type object = map[string]any

func readYAML(t *testing.T, path string) object {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	var doc object

	require.NoError(t, yaml.Unmarshal(raw, &doc))

	return doc
}

func readJSON(t *testing.T, path string) object {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	var doc object

	require.NoError(t, json.Unmarshal(raw, &doc))

	return doc
}

// sub follows the keys down an object; a missing step gives an empty object.
func sub(from object, keys ...string) object {
	cur := from

	for _, key := range keys {
		next, _ := cur[key].(object)
		cur = next
	}

	return cur
}

func text(val any) string {
	str, _ := val.(string)

	return str
}

func keysOf(from object) []string {
	out := make([]string, 0, len(from))
	for key := range from {
		out = append(out, key)
	}

	slices.Sort(out)

	return out
}

// paramIDs lists the parameters of an operation as "in:name", resolving the references of the document.
func paramIDs(doc, operation object) []string {
	list, _ := operation["parameters"].([]any)

	ids := make([]string, 0, len(list))

	for _, raw := range list {
		param, _ := raw.(object)
		if ref := text(param["$ref"]); ref != "" {
			param = sub(doc, "components", "parameters", strings.TrimPrefix(ref, "#/components/parameters/"))
		}

		ids = append(ids, text(param["in"])+":"+text(param["name"]))
	}

	slices.Sort(ids)

	return ids
}

func headerNames(resp object) []string {
	names := make([]string, 0, len(sub(resp, "headers")))

	for name := range sub(resp, "headers") {
		names = append(names, strings.ToLower(name))
	}

	slices.Sort(names)

	return names
}

// securityKinds describes the security schemes an operation accepts by what a client sends.
func securityKinds(doc, operation object) []string {
	reqs, ok := operation["security"].([]any)
	if !ok {
		reqs, _ = doc["security"].([]any)
	}

	var kinds []string

	for _, raw := range reqs {
		req, _ := raw.(object)
		for name := range req {
			scheme := sub(doc, "components", "securitySchemes", name)

			switch text(scheme["type"]) {
			case "http":
				kinds = append(kinds, "http:"+strings.ToLower(text(scheme["scheme"])))
			case "apiKey":
				kinds = append(kinds, "apiKey:"+text(scheme["in"])+":"+strings.ToLower(text(scheme["name"])))
			}
		}
	}

	slices.Sort(kinds)

	return slices.Compact(kinds)
}

// TestRelayOpenAPIFollowsPinnedDocument compares what the relay publishes about its OFREP operations with the pinned
// document: the paths, the parameters, the status codes, the headers, the media types and the security of every
// operation.
func TestRelayOpenAPIFollowsPinnedDocument(t *testing.T) {
	t.Parallel()

	pinned := readYAML(t, specPath)
	relay := readJSON(t, filepath.Join("..", "..", "..", "cmd", "relayproxy", "api", "testdata", "openapi.golden.json"))

	for _, path := range keysOf(sub(pinned, "paths")) {
		for _, method := range keysOf(sub(pinned, "paths", path)) {
			t.Run(strings.ToUpper(method)+" "+path, func(t *testing.T) {
				t.Parallel()

				want := sub(pinned, "paths", path, method)
				got := sub(relay, "paths", path, method)
				require.NotEmpty(t, got, "the relay documents no %s %s", method, path)

				assert.Equal(t, paramIDs(pinned, want), paramIDs(relay, got), "parameters")
				assert.Equal(t, securityKinds(pinned, want), securityKinds(relay, got), "security")
				assert.Equal(
					t,
					sub(want, "requestBody")["required"],
					sub(got, "requestBody")["required"],
					"request body",
				)
				assert.Equal(t, keysOf(sub(want, "requestBody", "content")), keysOf(sub(got, "requestBody", "content")))

				assertResponsesFollow(t, path, want, got)
			})
		}
	}
}

func assertResponsesFollow(t *testing.T, path string, want, got object) {
	t.Helper()

	wantStatuses := keysOf(sub(want, "responses"))
	gotStatuses := keysOf(sub(got, "responses"))

	for _, status := range gotStatuses {
		assert.Contains(t, wantStatuses, status, "the relay documents a status the pinned document does not")
	}

	for _, status := range wantStatuses {
		code, err := strconv.Atoi(status)
		require.NoError(t, err)

		_, isExempt := exempt()[ofrepconformance.ResponseID(http.MethodPost, path, code)]
		if isExempt && !slices.Contains(gotStatuses, status) {
			continue
		}

		require.Contains(t, gotStatuses, status, "the pinned document defines this status")

		wantResp := sub(want, "responses", status)
		gotResp := sub(got, "responses", status)
		assert.Equal(t, headerNames(wantResp), headerNames(gotResp), "headers of %s", status)

		if len(sub(wantResp, "content")) > 0 {
			assert.Equal(
				t,
				keysOf(sub(wantResp, "content")),
				keysOf(sub(gotResp, "content")),
				"media types of %s",
				status,
			)
		}
	}
}
