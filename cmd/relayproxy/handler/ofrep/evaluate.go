package ofrep

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/helper"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/metric"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/model"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/service"
	"github.com/blackbirdworks/semaphore/cmdhelpers/configfile"
	"github.com/blackbirdworks/semaphore/internal/flagstate"
	"github.com/blackbirdworks/semaphore/modules/core/ffcontext"
	"github.com/blackbirdworks/semaphore/modules/core/flagdef"
	"github.com/blackbirdworks/semaphore/pkgs/evalcore"
)

// EvaluateCtrl serves the OFREP evaluation requests of the relay proxy.
type EvaluateCtrl struct {
	flagsetManager service.FlagSetResolver
	metrics        *metric.Metrics
	eventStream    EventStream
	// legacyMetadataOnly drops MetadataCacheable, LegacyMetadataCacheable is always sent.
	legacyMetadataOnly bool
}

// Option customizes an EvaluateCtrl.
type Option func(*EvaluateCtrl)

// WithLegacyMetadataOnly stops the controller from adding the Semaphore metadata keys
// (semaphore_cacheable) next to the legacy ones.
func WithLegacyMetadataOnly(legacyOnly bool) Option {
	return func(h *EvaluateCtrl) { h.legacyMetadataOnly = legacyOnly }
}

// NewOFREPEvaluate returns the controller that evaluates flags through the OFREP API.
func NewOFREPEvaluate(
	flagsetManager service.FlagSetResolver,
	metrics *metric.Metrics,
	eventStream EventStream,
	opts ...Option,
) EvaluateCtrl {
	ctrl := EvaluateCtrl{
		flagsetManager: flagsetManager,
		metrics:        metrics,
		eventStream:    eventStream,
	}
	for _, opt := range opts {
		opt(&ctrl)
	}

	return ctrl
}

// Evaluate is the entry point to evaluate a flag using the OpenFeature Remote Evaluation Protocol.
func (h *EvaluateCtrl) Evaluate(w http.ResponseWriter, r *http.Request) error {
	flagKey := r.PathValue("flagKey")
	if flagKey == "" {
		return writeJSON(w, http.StatusBadRequest, NewEvaluateError(flagKey, flagdef.ErrorCodeGeneral,
			"No key provided in the URL"))
	}

	reqBody := new(model.OFREPEvalFlagRequest)

	err := bindBody(w, r, reqBody)
	if err != nil {
		return writeJSON(
			w,
			http.StatusBadRequest,
			NewEvaluateError(flagKey, flagdef.ErrorCodeInvalidContext, err.Error()),
		)
	}

	validateErr := assertOFREPEvaluateRequest(reqBody)
	if validateErr != nil {
		return writeJSON(w, http.StatusBadRequest, model.OFREPEvaluateResponseError{
			OFREPCommonResponseError: *validateErr,
			Key:                      flagKey,
		})
	}

	evalCtx, err := evaluationContextFromOFREPRequest(reqBody)
	if err != nil {
		return writeJSON(w, http.StatusBadRequest, err)
	}

	tracer := otel.GetTracerProvider().Tracer(configfile.OtelTracerName)

	ctx, span := tracer.Start(r.Context(), "flagEvaluation")
	defer span.End()

	flagset, httpErr := helper.RequestFlagSet(ctx, h.flagsetManager, helper.APIKey(r))
	if httpErr != nil {
		return fmt.Errorf("getting the flag set of the evaluation: %w", flagSetError(httpErr))
	}

	// we set a nil value to the default value to avoid the default value to be used.
	var defaultValue any

	flagValue, _ := flagset.RawVariation(ctx, flagKey, evalCtx, defaultValue)
	h.countEvaluation(ctx, flagKey, flagValue.ErrorCode)

	if flagValue.Reason == flagdef.ReasonError {
		httpStatus := http.StatusBadRequest
		if flagValue.ErrorCode == flagdef.ErrorCodeFlagNotFound {
			httpStatus = http.StatusNotFound
		}

		return writeJSON(w, httpStatus, NewEvaluateError(flagKey, flagValue.ErrorCode,
			"Error while evaluating the flag: "+flagKey))
	}

	span.SetAttributes(
		attribute.String("flagEvaluation.flagName", flagKey),
		attribute.Bool("flagEvaluation.trackEvents", flagValue.TrackEvents),
		attribute.String("flagEvaluation.variant", flagValue.VariationType),
		attribute.Bool("flagEvaluation.failed", flagValue.Failed),
		attribute.String("flagEvaluation.version", flagValue.Version),
		attribute.String("flagEvaluation.reason", flagValue.Reason),
		attribute.String("flagEvaluation.errorCode", flagValue.ErrorCode),
		attribute.Bool("flagEvaluation.cacheable", flagValue.Cacheable),
		// we convert to string because there is no attribute for interface{}
		attribute.String("flagEvaluation.value", fmt.Sprintf("%v", flagValue.Value)),
	)

	// The metadata of an evaluation is a map the engine made for this evaluation (never the flag's own, which
	// the other evaluations and the configuration endpoint read), so the cacheable keys are added to it
	// without a copy: the hot path must not allocate more.
	metadata := flagValue.Metadata
	if flagValue.Cacheable {
		if metadata == nil {
			metadata = make(map[string]any)
		}

		if !h.legacyMetadataOnly {
			metadata[MetadataCacheable] = true
		}

		metadata[LegacyMetadataCacheable] = true
	}

	if !h.legacyMetadataOnly {
		metadata = flagstate.WithSegmentStatus(metadata, flagValue.SegmentStatus)
	}

	return writeJSON(w, http.StatusOK, model.OFREPEvaluateSuccessResponse{
		Key:      flagKey,
		Value:    ofrepValue(flagValue.Reason, flagValue.Value),
		Reason:   ofrepReason(flagValue.Reason),
		Variant:  flagValue.VariationType,
		Metadata: metadata,
	})
}

// BulkEvaluate is the entry point to evaluate in bulk flags using the OpenFeature Remote Evaluation Protocol.
func (h *EvaluateCtrl) BulkEvaluate(w http.ResponseWriter, r *http.Request) error {
	request := new(model.OFREPEvalFlagRequest)

	err := bindBody(w, r, request)
	if err != nil {
		return writeJSON(w, http.StatusBadRequest, NewOFREPCommonError("INVALID_CONTEXT", err.Error()))
	}

	validateErr := assertOFREPEvaluateRequest(request)
	if validateErr != nil {
		return writeJSON(w, http.StatusBadRequest, validateErr)
	}

	evalCtx, err := evaluationContextFromOFREPRequest(request)
	if err != nil {
		return writeJSON(w, http.StatusBadRequest, err)
	}

	// if no flag is not provided, we evaluate all available flags
	response := model.OFREPBulkEvaluateSuccessResponse{
		Flags:        make([]model.OFREPFlagBulkEvaluateSuccessResponse, 0),
		EventStreams: nil,
	}
	tracer := otel.GetTracerProvider().Tracer(configfile.OtelTracerName)

	ctx, span := tracer.Start(r.Context(), "AllFlagsState")
	defer span.End()

	flagset, httpErr := helper.RequestFlagSet(ctx, h.flagsetManager, helper.APIKey(r))
	if httpErr != nil {
		return fmt.Errorf("getting the flag set of the bulk evaluation: %w", flagSetError(httpErr))
	}

	var allFlagsResp flagstate.AllFlags
	if len(evalCtx.ExtractGOFFProtectedFields().FlagList) > 0 {
		// if we have a list of flags to evaluate in the evaluation context, we evaluate only those flags.
		allFlagsResp = flagset.GetFlagStates(ctx, evalCtx, evalCtx.ExtractGOFFProtectedFields().FlagList)
	} else {
		allFlagsResp = flagset.AllFlagsState(ctx, evalCtx)
	}

	flagNames := make([]string, 0, len(allFlagsResp.GetFlags()))
	for key, val := range allFlagsResp.GetFlags() {
		flagNames = append(flagNames, key)

		response.Flags = append(response.Flags, model.OFREPFlagBulkEvaluateSuccessResponse{
			Key:          key,
			Value:        ofrepValue(val.Reason, val.Value),
			Reason:       ofrepBulkReason(val.Reason),
			Variant:      val.VariationType,
			Metadata:     h.bulkMetadata(&val),
			ErrorCode:    val.ErrorCode,
			ErrorDetails: val.ErrorDetails,
		})
	}

	if ctx.Err() == nil {
		h.metrics.IncAllFlag(flagNames...)
	}

	slices.SortFunc(response.Flags, func(a, b model.OFREPFlagBulkEvaluateSuccessResponse) int {
		return cmp.Compare(a.Key, b.Key)
	})

	span.SetAttributes(
		attribute.Int("AllFlagsState.numberEvaluation", len(response.Flags)),
	)

	if h.eventStream.IsEnabled() {
		response.EventStreams = h.buildEventStreams(r)
	}

	return writeJSON(w, http.StatusOK, response)
}

// ofrepSSEPath is the path of the relay proxy endpoint streaming flag changes over SSE.
const (
	// MetadataCacheable is the response metadata key telling a provider that it can cache the value.
	MetadataCacheable = "semaphore_cacheable"
	// LegacyMetadataCacheable is the key GO Feature Flag used for the same information. The upstream
	// providers read it, so it is still sent next to MetadataCacheable.
	LegacyMetadataCacheable = "gofeatureflag_cacheable"
)

const ofrepSSEPath = "/stream/v1/sse/flag/change"

// buildEventStreams returns the eventStreams advertised in the OFREP bulk evaluation
// response (OpenFeature ADR-0008).
// The URL is built from the configured base URL
// and contains the API key of the caller, so the provider can connect without extra credentials.
// This URL is sensitive, it must never be logged.
func (h *EvaluateCtrl) buildEventStreams(r *http.Request) []model.OFREPEventStream {
	baseURL := h.eventStream.baseURL

	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return nil
	}

	parsedURL = parsedURL.JoinPath(ofrepSSEPath)
	if apiKey := helper.APIKey(r); apiKey != "" {
		parsedURL.RawQuery = url.Values{"apiKey": []string{apiKey}}.Encode()
	}

	return []model.OFREPEventStream{{
		Type:               "sse",
		URL:                parsedURL.String(),
		InactivityDelaySec: h.eventStream.inactivityDelaySec,
	}}
}

func assertOFREPEvaluateRequest(
	ofrepEvalReq *model.OFREPEvalFlagRequest,
) *model.OFREPCommonResponseError {
	if ofrepEvalReq.Context == nil {
		return NewOFREPCommonError(flagdef.ErrorCodeInvalidContext,
			"Semaphore requires an evaluation context in the request.")
	}

	// An empty context object is allowed since the evaluation context is optional.
	// If the context does not have any targetingKey, this is fine since the core
	// evaluation logic will handle if it is required or not.

	return nil
}

func evaluationContextFromOFREPRequest(req *model.OFREPEvalFlagRequest) (ffcontext.EvaluationContext, error) {
	if req == nil || req.Context == nil {
		return ffcontext.EvaluationContext{}, NewOFREPCommonError(
			flagdef.ErrorCodeInvalidContext,
			"Semaphore has received an invalid context.")
	}

	ctx := req.Context

	// targetingKey is optional, it is only required if the flag needs bucketing and
	// the check is done in the core evaluation logic.
	// If we don't have a targetingKey, we return an empty string.
	targetingKey := ""
	if key, ok := ctx["targetingKey"].(string); ok {
		targetingKey = key
	}

	// Create evaluation context (empty targeting key is allowed)
	evalCtx := evalcore.ConvertEvaluationCtxFromRequest(targetingKey, ctx)

	return evalCtx, nil
}

// ofrepValue clears the evaluated value when a flag is disabled or errored so OFREP
// responses omit "value" instead of serializing "value": null.
func ofrepValue(reason string, value any) any {
	if reason == flagdef.ReasonError || reason == flagdef.ReasonDisabled {
		return nil
	}

	return value
}

// countEvaluation counts a served evaluation: no client received the answer of a request that ended, so it is not
// counted.
func (h *EvaluateCtrl) countEvaluation(ctx context.Context, flagKey string, code flagdef.ErrorCode) {
	if ctx.Err() != nil {
		return
	}

	if code == flagdef.ErrorCodeFlagNotFound {
		h.metrics.IncFlagNotFoundEvaluation()
	} else {
		h.metrics.IncFlagEvaluation(flagKey)
	}
}

// MaxBodyBytes is the size of the largest request body of an evaluation. An evaluation context is a few hundred
// bytes, a bulk request with a list of flag keys a few kilobytes.
const MaxBodyBytes = 1 << 20

// ErrBodyTooLarge is the error of a request body larger than MaxBodyBytes.
var ErrBodyTooLarge = errors.New("the request body is too large")

// bindBody decodes the JSON body of the request. A body that is larger than MaxBodyBytes is refused before it is
// read when its length is known, and while it is read when it is not (no allocation on the way).
func bindBody(w http.ResponseWriter, r *http.Request, target any) error {
	if r.ContentLength > MaxBodyBytes {
		return fmt.Errorf("%w: %d bytes, the limit is %d", ErrBodyTooLarge, r.ContentLength, MaxBodyBytes)
	}

	if r.ContentLength < 0 {
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	}

	err := helper.BindBody(r, target)
	if err != nil {
		if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
			return fmt.Errorf("%w: the limit is %d", ErrBodyTooLarge, MaxBodyBytes)
		}

		return fmt.Errorf("%w", err)
	}

	return nil
}

// flagSetError turns the failure to find the flag set of a request into the error to send. A key that
// authenticated and is an admin key may not evaluate flags: the client is forbidden to. Every other failure keeps
// its own status.
func flagSetError(err *helper.HTTPError) error {
	if errors.Is(err, service.ErrAdminKey) {
		return fmt.Errorf("refusing the key: %w", helper.NewHTTPError(http.StatusForbidden,
			"the API key does not give access to a flag set"))
	}

	return err
}

// writeJSON sends v as the JSON body of the response, with the status code.
func writeJSON(w http.ResponseWriter, code int, v any) error {
	err := helper.WriteJSON(w, code, v)
	if err != nil {
		return fmt.Errorf("writing the OFREP response: %w", err)
	}

	return nil
}

// bulkMetadata is the metadata of a flag of a bulk evaluation: the metadata of the evaluation, and the segment
// status when the lookups were not healthy, unless the Semaphore metadata is disabled. The metadata of the flag is
// not changed.
func (h *EvaluateCtrl) bulkMetadata(val *flagstate.FlagState) map[string]any {
	if h.legacyMetadataOnly {
		return val.Metadata
	}

	return val.MetadataWithStatus()
}
