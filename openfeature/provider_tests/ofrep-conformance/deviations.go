package ofrepconformance

import (
	"fmt"
)

// Deviation is a place where the validator accepts what the pinned document alone would reject. Each one is named,
// scoped to one property of one schema, and has to be justified; a test pins the list.
type Deviation struct {
	// Schema and Property say where it applies: the property of a component schema.
	Schema, Property string
	// Rule is what the validator accepts.
	Rule string
	// Reason says why the document is not followed.
	Reason string
	// Evidence are the sources that show the clients rely on it.
	Evidence []string
}

// Deviations lists every deviation of the validator from the pinned document. There is one.
func Deviations() []Deviation {
	return []Deviation{
		{
			Schema:   "objectFlag",
			Property: "value",
			Rule:     "the value of an object flag may be a JSON array",
			Reason: "the document types the value of an object flag as `object`, with no array type, while every official " +
				"OFREP provider takes a JSON array as the value of an object evaluation and GO Feature Flag sends arrays",
			Evidence: []string{
				"Go provider: the value of the result is an `any`",
				"JavaScript `ofrep-core`: an object value is checked with `typeof value === 'object'`, which holds for an array",
				"Java provider: the value goes through `Value`, which has a list form",
				"`.NET` provider: `ConvertJsonArray` converts an array value",
				"Python provider: an object value is checked with `isinstance(value, (dict, list))`",
			},
		},
	}
}

// applyDeviations marks the schemas the deviations apply to.
func applyDeviations(doc *openAPIDoc) error {
	for _, dev := range Deviations() {
		target := doc.Components.Schemas[dev.Schema]
		if target == nil || target.Properties[dev.Property] == nil {
			return fmt.Errorf("%w: deviation of %s.%s", errInvalidSpec, dev.Schema, dev.Property)
		}

		target.Properties[dev.Property].allowArray = true
	}

	return nil
}
