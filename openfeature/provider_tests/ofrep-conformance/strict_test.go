package ofrepconformance_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ofrepconformance "github.com/blackbirdworks/semaphore/openfeature/provider_tests/ofrep-conformance"
)

// miniSpec is a document with one operation; schema is the YAML of the schema of a component and of a response.
func miniSpec(schema string) []byte {
	indent := func(spaces int) string {
		pad := strings.Repeat(" ", spaces)

		return pad + strings.ReplaceAll(strings.TrimLeft(schema, " "), "\n        ", "\n"+pad)
	}

	return []byte(`openapi: 3.1.0
paths:
  /x:
    post:
      responses:
        "200":
          content:
            application/json:
              schema:
` + indent(16) + `
components:
  schemas:
    objectFlag:
      type: object
      properties:
        value: {type: object}
    thing:
` + indent(6) + `
`)
}

func TestParseSpecRejectsKeywordsItDoesNotImplement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		schema      string
		wantKeyword string
	}{
		{name: "anyOf", schema: "        type: object\n        anyOf: [{type: string}]", wantKeyword: "anyOf"},
		{name: "maxLength", schema: "        type: string\n        maxLength: 4", wantKeyword: "maxLength"},
		{name: "nullable", schema: "        type: string\n        nullable: true", wantKeyword: "nullable"},
		{name: "unknown keyword in a property", schema: "        type: object\n        properties:\n" +
			"          a: {type: string, minLength: 1}", wantKeyword: "minLength"},
		{
			name:        "unknown keyword in items",
			schema:      "        type: array\n        items: {type: string, uniqueItems: true}",
			wantKeyword: "uniqueItems",
		},
		{name: "unknown keyword in oneOf", schema: "        oneOf:\n          - {type: string, const: a}",
			wantKeyword: "const"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ofrepconformance.ParseSpec(miniSpec(tt.schema))
			require.Error(t, err)
			require.ErrorContains(t, err, "does not implement")
			require.ErrorContains(t, err, tt.wantKeyword)
		})
	}
}

func TestParseSpecAcceptsImplementedKeywordsAndAnnotations(t *testing.T) {
	t.Parallel()

	schema := "        type: object\n        description: d\n        example: {a: 1}\n        title: t\n" +
		"        required: [a]\n        properties:\n          a: {type: integer, minimum: 1, default: 3}\n" +
		"          example: {type: string, description: a property that is named like a keyword}\n" +
		"        additionalProperties: true"

	spec, err := ofrepconformance.ParseSpec(miniSpec(schema))
	require.NoError(t, err)
	assert.NotNil(t, spec)
}
