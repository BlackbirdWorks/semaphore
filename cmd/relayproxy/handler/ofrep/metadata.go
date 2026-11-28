package ofrep

// documentMetadata returns the metadata an OFREP response may carry: the pinned document types every entry of a
// metadata object as a boolean, a string or a number, so an entry that holds an object, a list or null is left out
// (the JavaScript providers drop such entries too). Metadata that is already valid is returned as it is, with no
// allocation. With own set the map belongs to the caller and is changed in place; without it a map that has to change
// is copied first, the metadata of a flag is shared and never changed.
func documentMetadata(metadata map[string]any, own bool) map[string]any {
	valid := true

	for _, value := range metadata {
		if !isDocumentValue(value) {
			valid = false

			break
		}
	}

	if valid {
		return metadata
	}

	if own {
		for key, value := range metadata {
			if !isDocumentValue(value) {
				delete(metadata, key)
			}
		}

		return metadata
	}

	out := make(map[string]any, len(metadata))

	for key, value := range metadata {
		if isDocumentValue(value) {
			out[key] = value
		}
	}

	return out
}

// isDocumentValue reports whether the value is a boolean, a string or a number.
func isDocumentValue(value any) bool {
	switch value.(type) {
	case bool, string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}
