package model

// OFREPSSEEventTypeRefetchEvaluation tells the provider that the flag configuration has changed
// and that it must re-fetch its evaluations.
const OFREPSSEEventTypeRefetchEvaluation = "refetchEvaluation"

// OFREPSSEEvent is the payload of the events sent on the OFREP SSE stream,
// as defined by OpenFeature ADR-0008.
type OFREPSSEEvent struct {
	// Type of the event, providers must ignore the types they don't know.
	Type string `example:"refetchEvaluation" json:"type"`
	// ETag identifies the flag configuration that the change leads to. A provider sends it back as the
	// flagConfigEtag query parameter of its re-fetch, so that a relay that has not loaded that configuration yet
	// checks its control plane before it answers.
	ETag string `example:"3f9a1c" json:"etag,omitempty"`
	// LastModified is the unix timestamp (in seconds) of the flag configuration change.
	LastModified int64 `example:"1771622898" json:"lastModified,omitempty"`
}
