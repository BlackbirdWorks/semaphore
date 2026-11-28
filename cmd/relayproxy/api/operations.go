package api

import (
	"net/http"
	"reflect"
	"slices"
	"strconv"

	"github.com/danielgtaylor/huma/v2"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/api/middleware"
	controller "github.com/blackbirdworks/semaphore/cmd/relayproxy/handler/semaphore"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/helper"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/model"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/modeldocs"
	"github.com/blackbirdworks/semaphore/notifier"
)

// Tags of the operations.
const (
	tagEvaluation = "Semaphore Evaluation API"
	tagStream     = "Semaphore Evaluation Stream API"
	tagOFREP      = "OpenFeature Remote Evaluation Protocol (OFREP)"
	tagManifest   = "OpenFeature Manisfest API"
	tagAdmin      = "Admin API to manage Semaphore"
	tagMonitoring = "Monitoring"
	tagProfiling  = "Profiling"
)

const (
	inHeader    = "header"
	inQuery     = "query"
	typeString  = "string"
	typeObject  = "object"
	contentJSON = "application/json"
	etagExample = `Etag: "117-0193435c612c50d93b798619d9464856263dbf9f"`
)

// keylessNote is added to the description of the operations that accept a request without a key.
const keylessNote = "\n\nThe key is optional when a flag set is published without API keys (the public flags): " +
	"a request without a key is then served by those flags. Without such a flag set every request needs a key. " +
	"A wrong key is always refused."

// securityAll is the security of the endpoints that accept an API key in any of the two headers.
func securityAll() []map[string][]string {
	return []map[string][]string{{securityAPIKey: {}}, {securityXAPIKey: {}}}
}

// securityOFREP is the security of the OFREP endpoints: the key in the X-API-Key header or as a bearer token.
func securityOFREP() []map[string][]string {
	return []map[string][]string{{securityXAPIKey: {}}, {securityBearer: {}}}
}

// optionalKey documents the key of an operation as optional when the middleware that guards it lets a request without
// a key through (the public flags): the empty requirement is how OpenAPI says that the security is optional.
// The operations that the public flags can reach are the ones registered through it, so the document follows
// the middleware.
func optionalKey(auth middleware.KeyAuth, operation *huma.Operation) *huma.Operation {
	if !auth.AllowsAnonymous() || slices.ContainsFunc(operation.Security, func(req map[string][]string) bool {
		return len(req) == 0
	}) {
		return operation
	}

	operation.Security = append(slices.Clone(operation.Security), map[string][]string{})
	operation.Description += keylessNote

	return operation
}

// securityQuery is the security of the streams: the key is in the query.
func securityQuery() []map[string][]string {
	return []map[string][]string{{securityQueryAPIKey: {}}}
}

// securityAuthorization is the security of the endpoints that only document the Authorization header.
func securityAuthorization() []map[string][]string {
	return []map[string][]string{{securityAPIKey: {}}}
}

// schemaOf returns the schema of the type of v, registered in the document.
func schemaOf(api huma.API, v any) *huma.Schema {
	return api.OpenAPI().Components.Schemas.Schema(reflect.TypeOf(v), true, "")
}

func jsonResponse[T any](api huma.API, description string) *huma.Response {
	return &huma.Response{
		Description: description,
		Content: map[string]*huma.MediaType{
			contentJSON: {Schema: api.OpenAPI().Components.Schemas.Schema(reflect.TypeFor[T](), true, "")},
		},
	}
}

func errorResponse(api huma.API, description string) *huma.Response {
	return jsonResponse[helper.ErrorBody](api, description)
}

func notModified() *huma.Response {
	return &huma.Response{Description: etagExample}
}

func jsonBody(api huma.API, description string, required bool, v any) *huma.RequestBody {
	return &huma.RequestBody{
		Description: description,
		Required:    required,
		Content:     map[string]*huma.MediaType{contentJSON: {Schema: schemaOf(api, v)}},
	}
}

// withETagHeader documents the ETag header of a response.
func withETagHeader(resp *huma.Response) *huma.Response {
	resp.Headers = map[string]*huma.Param{
		"ETag": {Description: "Entity tag of the evaluation: send it back in If-None-Match.",
			Schema: &huma.Schema{Type: typeString}},
	}

	return resp
}

// changeNotificationParam is a query parameter that a provider copies from a change notification of the stream.
func changeNotificationParam(name string, schema *huma.Schema) *huma.Param {
	return &huma.Param{
		Name: name, In: inQuery, Schema: schema,
		Description: "Metadata of the change notification that triggered this request. The relay accepts it and " +
			"evaluates the flags as for any other request.",
	}
}

func ifNoneMatch() *huma.Param {
	return &huma.Param{
		Name: "If-None-Match", In: inHeader,
		Description: "The request will be processed only if ETag doesn't match.",
		Schema:      &huma.Schema{Type: typeString},
	}
}

func flagKeyParam() *huma.Param {
	return &huma.Param{
		Name: "flag_key", In: "path", Required: true,
		Description: "Name of your feature flag", Schema: &huma.Schema{Type: typeString},
	}
}

// setRequestBody replaces the request body huma derived from a RawBody field (a binary document) by the JSON
// document the handler decodes: the handler decodes the bytes itself to keep its error bodies.
func setRequestBody(api huma.API, id string, body *huma.RequestBody) {
	for _, item := range api.OpenAPI().Paths {
		for _, op := range []*huma.Operation{item.Get, item.Post, item.Put, item.Delete, item.Patch} {
			if op != nil && op.OperationID == id {
				op.RequestBody = body
			}
		}
	}
}

func statusKey(code int) string { return strconv.Itoa(code) }

// operationAllFlags documents POST /v1/allflags.
func operationAllFlags(api huma.API) *huma.Operation {
	return &huma.Operation{
		OperationID: "allFlags",
		Method:      http.MethodPost,
		Path:        "/v1/allflags",
		Summary:     "All flags variations for a user",
		Tags:        []string{tagEvaluation},
		Security:    securityAll(),
		Description: "Making a **POST** request to the URL `/v1/allflags` will give you the values of all the flags for\n" +
			"this user.\n\nTo get a variation you should provide information about the user.\n" +
			"For that you should provide some user information in JSON in the request body.",
		RequestBody: jsonBody(api, "Evaluation context to challenge against the flags.", true, model.AllFlagRequest{}),
		Responses: map[string]*huma.Response{
			statusKey(http.StatusOK):                  jsonResponse[modeldocs.AllFlags](api, "Success"),
			statusKey(http.StatusBadRequest):          errorResponse(api, "Bad Request"),
			statusKey(http.StatusInternalServerError): errorResponse(api, "Internal server error"),
		},
	}
}

// operationFlagEval documents POST /v1/feature/{flag_key}/eval.
func operationFlagEval(api huma.API) *huma.Operation {
	return &huma.Operation{
		OperationID: "flagEval",
		Method:      http.MethodPost,
		Path:        "/v1/feature/{flag_key}/eval",
		Summary:     "Evaluate a feature flag",
		Tags:        []string{tagEvaluation},
		Security:    securityAll(),
		Description: "Making a **POST** request to the URL `/v1/feature/<your_flag_name>/eval` will give you the value of\n" +
			"the\n" +
			"flag for this user.\n\nTo get a variation you should provide information about the user:\n" +
			"- User information in JSON in the request body.\n" +
			"- A default value in case there is an error while evaluating the flag.\n\n" +
			"Note that you will always have a usable value in the response, you can use the field `failed` to know if\n" +
			"an issue has occurred during the validation of the flag, in that case the value returned will be the\n" +
			"default value.",
		Parameters:  []*huma.Param{flagKeyParam()},
		RequestBody: jsonBody(api, "Evaluation context and default value for the flag.", true, model.EvalFlagRequest{}),
		Responses: map[string]*huma.Response{
			statusKey(http.StatusOK):                  jsonResponse[modeldocs.EvalFlagDoc](api, "Success"),
			statusKey(http.StatusBadRequest):          errorResponse(api, "Bad Request"),
			statusKey(http.StatusInternalServerError): errorResponse(api, "Internal server error"),
		},
	}
}

// operationOFREPEval documents POST /ofrep/v1/evaluate/flags/{key}.
func operationOFREPEval(api huma.API) *huma.Operation {
	return &huma.Operation{
		OperationID: "ofrepEvaluate",
		Method:      http.MethodPost,
		Path:        "/ofrep/v1/evaluate/flags/{key}",
		Summary:     "Evaluate a feature flag using the OpenFeature Remote Evaluation Protocol",
		Tags:        []string{tagOFREP},
		Security:    securityOFREP(),
		Description: "Making a **POST** request to the URL `/ofrep/v1/evaluate/flags/{your_flag_name}` will give you the\n" +
			"value of the flag for this evaluation context",
		Parameters: []*huma.Param{{
			Name: "key", In: "path", Required: true,
			Description: "Name of your feature flag", Schema: &huma.Schema{Type: typeString},
		}},
		RequestBody: jsonBody(api, "Evaluation Context for this API call", true, model.OFREPEvalFlagRequest{}),
		Responses: map[string]*huma.Response{
			statusKey(http.StatusOK): jsonResponse[model.OFREPEvaluateSuccessResponse](api, "Success"),
			statusKey(http.StatusBadRequest): jsonResponse[model.OFREPEvaluateResponseError](
				api,
				"Bad Request",
			),
			statusKey(http.StatusUnauthorized): errorResponse(api, "Unauthorized"),
			statusKey(http.StatusForbidden): errorResponse(
				api,
				"Forbidden - The API key does not give access to a flag set (an admin key)",
			),
			statusKey(http.StatusNotFound): jsonResponse[model.OFREPEvaluateResponseError](
				api,
				"Flag Not Found",
			),
			statusKey(http.StatusInternalServerError): errorResponse(api, "Internal server error"),
		},
	}
}

// operationOFREPBulk documents POST /ofrep/v1/evaluate/flags.
func operationOFREPBulk(api huma.API) *huma.Operation {
	return &huma.Operation{
		OperationID: "ofrepBulkEvaluate",
		Method:      http.MethodPost,
		Path:        "/ofrep/v1/evaluate/flags",
		Summary:     "Open-Feature Remote Evaluation Protocol bulk evaluation API.",
		Tags:        []string{tagOFREP},
		Security:    securityOFREP(),
		Description: "Making a **POST** request to the URL `/ofrep/v1/evaluate/flags` will give you the value of the list\n" +
			"of feature flags for this evaluation context.\n\n" +
			"If no flags are provided, the API will evaluate all available flags in the configuration.",
		Parameters: []*huma.Param{
			ifNoneMatch(),
			changeNotificationParam("flagConfigEtag", &huma.Schema{Type: typeString}),
			changeNotificationParam("flagConfigLastModified", &huma.Schema{OneOf: []*huma.Schema{
				{Type: "integer", Minimum: new(float64)}, {Type: typeString, Format: "date-time"},
			}}),
		},
		RequestBody: jsonBody(api, "Evaluation Context and list of flag for this API call", true,
			model.OFREPEvalFlagRequest{}),
		Responses: map[string]*huma.Response{
			statusKey(http.StatusOK): withETagHeader(jsonResponse[model.OFREPBulkEvaluateSuccessResponse](
				api,
				"OFREP successful evaluation response",
			)),
			statusKey(http.StatusNotModified): notModified(),
			statusKey(http.StatusBadRequest): jsonResponse[model.OFREPCommonResponseError](
				api,
				"Bad evaluation request",
			),
			statusKey(http.StatusUnauthorized): errorResponse(
				api,
				"Unauthorized - You need credentials to access the API",
			),
			statusKey(http.StatusForbidden): errorResponse(
				api,
				"Forbidden - You are not authorized to access the API",
			),
			statusKey(http.StatusInternalServerError): errorResponse(api, "Internal server error"),
		},
	}
}

// operationValidate documents POST /admin/v1/flags/validate.
func operationValidate(api huma.API) *huma.Operation {
	content := map[string]*huma.MediaType{}
	for _, ct := range []string{
		contentJSON, "application/yaml", "application/x-yaml", "text/yaml", "application/toml",
	} {
		content[ct] = &huma.MediaType{Schema: &huma.Schema{Type: "object"}}
	}

	return &huma.Operation{
		OperationID: "validateFlags", Method: http.MethodPost, Path: "/admin/v1/flags/validate",
		Summary: "Validate a flag file", Tags: []string{tagAdmin}, Security: securityAuthorization(),
		Description: "Validates the flag file sent in the body of the request, using the same rules as the\n" +
			"`goff lint` command.\n" +
			"The flag file is not loaded, nothing is changed on the flags served by the relay proxy.\n" +
			"The format of the body is selected with the `Content-Type` header (`application/json`,\n" +
			"`application/yaml`, `application/x-yaml`, `text/yaml` or `application/toml`).\n" +
			"The maximum size of the body is configurable per format with `server.validateFlags`\n" +
			"(defaults: 1 MiB for JSON, 64 KiB for YAML and TOML).",
		RequestBody: &huma.RequestBody{Description: "The flag file to validate", Required: true, Content: content},
		Responses: map[string]*huma.Response{
			statusKey(http.StatusOK): jsonResponse[controller.FlagValidationResponse](
				api,
				"All the flags are valid",
			),
			statusKey(http.StatusBadRequest):            errorResponse(api, "Unparseable body"),
			statusKey(http.StatusRequestEntityTooLarge): errorResponse(api, "Body too large"),
			statusKey(http.StatusUnsupportedMediaType):  errorResponse(api, "Missing or unsupported content type"),
			statusKey(http.StatusUnprocessableEntity): jsonResponse[controller.FlagValidationResponse](
				api,
				"At least one flag is invalid",
			),
		},
	}
}

// operationSSE documents GET /stream/v1/sse/flag/change.
func operationSSE(api huma.API) *huma.Operation {
	return &huma.Operation{
		OperationID: "sseFlagChange",
		Method:      http.MethodGet,
		Path:        "/stream/v1/sse/flag/change",
		Summary:     "SSE endpoint to be notified about flag changes",
		Tags:        []string{tagStream},
		Description: "Server-Sent Events endpoint pushing flag change notifications, following OpenFeature ADR-0008.\n" +
			"It is advertised in the `eventStreams` field of the OFREP bulk evaluation response.\n" +
			"Each event is sent as `event: message` with a `model.OFREPSSEEvent` JSON payload,\n" +
			"a `refetchEvaluation` event means that the provider must re-fetch its evaluations.\n" +
			"The stream also sends SSE comment lines (`: connected` when the connection opens,\n" +
			"`: heartbeat` every 30 seconds), they are ignored by SSE clients.\n" +
			"The full URL (including query string) is sensitive and must not be logged\n" +
			"or persisted by intermediaries.",
		Security: securityQuery(),
		Responses: map[string]*huma.Response{
			statusKey(http.StatusOK): {
				Description: "SSE stream of OFREP flag change events",
				Content: map[string]*huma.MediaType{
					"text/event-stream": {Schema: schemaOf(api, model.OFREPSSEEvent{})},
				},
			},
			statusKey(http.StatusBadRequest):          errorResponse(api, "Bad Request"),
			statusKey(http.StatusUnauthorized):        errorResponse(api, "Unauthorized"),
			statusKey(http.StatusInternalServerError): errorResponse(api, "Internal server error"),
		},
	}
}

// operationWebsocket documents GET /stream/v1/ws/flag/change, and its deprecated alias GET /ws/v1/flag/change.
func operationWebsocket(api huma.API, deprecated bool) *huma.Operation {
	operation := &huma.Operation{
		OperationID: "wsFlagChange", Method: http.MethodGet, Path: "/stream/v1/ws/flag/change",
		Summary: "Websocket endpoint to be notified about flag changes", Tags: []string{tagStream},
		Description: "This endpoint is a websocket endpoint to be notified about flag changes;\n" +
			"every change pushes a notifier.DiffCache message to the client.\n" +
			"When disableFlagDetailsInStream is true, values are empty objects.",
		Security: securityQuery(),
		Responses: map[string]*huma.Response{
			statusKey(http.StatusOK):                  jsonResponse[notifier.DiffCache](api, "Success"),
			statusKey(http.StatusBadRequest):          errorResponse(api, "Bad Request"),
			statusKey(http.StatusUnauthorized):        errorResponse(api, "Unauthorized"),
			statusKey(http.StatusInternalServerError): errorResponse(api, "Internal server error"),
		},
	}
	if deprecated {
		operation.OperationID = "wsFlagChangeLegacy"
		operation.Path = "/ws/v1/flag/change"
		operation.Deprecated = true
		operation.Description = "Deprecated: use /stream/v1/ws/flag/change instead. This endpoint\n" +
			"is a websocket endpoint to be notified about flag changes; every\n" +
			"change pushes a notifier.DiffCache message to the client.\n" +
			"When disableFlagDetailsInStream is true, values are empty objects."
	}

	return operation
}

func operationMetrics() *huma.Operation {
	return &huma.Operation{
		OperationID: "metrics", Method: http.MethodGet, Path: "/metrics",
		Summary: "Prometheus endpoint", Tags: []string{tagMonitoring},
		Description: "This endpoint is providing metrics about the relay proxy in the prometheus format.",
		Responses: map[string]*huma.Response{
			statusKey(http.StatusOK): {
				Description: "OK",
				Content:     map[string]*huma.MediaType{"text/plain": {Schema: &huma.Schema{Type: typeString}}},
			},
		},
	}
}

func probeOperation(id, path, summary, description string) *huma.Operation {
	return &huma.Operation{
		OperationID: id, Method: http.MethodGet, Path: path, Summary: summary, Tags: []string{tagMonitoring},
		Description: description,
		Responses: map[string]*huma.Response{
			statusKey(http.StatusOK): {
				Description: "OK",
				Content:     map[string]*huma.MediaType{contentJSON: {Schema: &huma.Schema{Type: typeObject}}},
			},
			statusKey(http.StatusServiceUnavailable): {
				Description: "Not ready: {\"ready\": false, \"reason\": \"starting|waiting-for-manifest|shutting-down\"}",
				Content:     map[string]*huma.MediaType{contentJSON: {Schema: &huma.Schema{Type: typeObject}}},
			},
		},
	}
}

func operationLivez() *huma.Operation {
	op := probeOperation("livez", "/livez", "Liveness",
		"200 `{\"live\": true}` while the process serves. It never depends on the control plane or any flag set. "+
			"Served by the config v2 runtime. No API key.")
	delete(op.Responses, statusKey(http.StatusServiceUnavailable))

	return op
}

func operationReadyz() *huma.Operation {
	return probeOperation("readyz", "/readyz", "Readiness",
		"200 `{\"ready\": true, \"state\": \"empty|serving|degraded\"}` when the relay is able to serve, else 503 "+
			"with a reason. Once ready the relay stays ready until shutdown. Served by the config v2 runtime. No API key.")
}

func operationStartupz() *huma.Operation {
	return probeOperation("startupz", "/startupz", "Startup",
		"200 once the first bootstrap round has ended, else 503 `{\"ready\": false, \"reason\": \"starting\"}`. "+
			"Served by the config v2 runtime. No API key.")
}

func operationPprof() *huma.Operation {
	return &huma.Operation{
		OperationID: "pprof", Method: http.MethodGet, Path: "/debug/pprof/",
		Summary: "pprof endpoint", Tags: []string{tagProfiling},
		Description: "This endpoint is provided by the standard library pprof handlers.\n" +
			"To know more please check this blogpost from the GO team https://go.dev/blog/pprof.\n" +
			"Visit the page /debug/pprof/ to see the available endpoints, all endpoint are not in the OpenAPI " +
			"documentation because they are standard pprof endpoints.\n" +
			"This endpoint is only available in debug mode.",
		Responses: map[string]*huma.Response{
			statusKey(http.StatusOK): {
				Description: "OK",
				Content:     map[string]*huma.MediaType{"text/plain": {Schema: &huma.Schema{Type: typeString}}},
			},
		},
	}
}

// typedOperations are the operations huma serves (typed input and output). Their descriptions are here, the
// parameters and the responses come from the input and output types.
func operationHealth() *huma.Operation {
	return &huma.Operation{
		OperationID: "health",
		Method:      http.MethodGet,
		Path:        "/health",
		Summary:     "Health",
		Tags:        []string{tagMonitoring},
		Description: "Making a **GET** request to the URL path `/health` will tell you if the relay proxy is ready to\n" +
			"serve\n" +
			"traffic.\n\nThis is useful especially for loadbalancer to know that they can send traffic to the service.",
	}
}

func operationInfo() *huma.Operation {
	return &huma.Operation{
		OperationID: "info",
		Method:      http.MethodGet,
		Path:        "/info",
		Summary:     "Info",
		Tags:        []string{tagMonitoring},
		Description: "Making a **GET** request to the URL path `/info` will give you information about the actual state\n" +
			"of the relay proxy.\n\nAs of Today the level of information is small be we can improve this endpoint to\n" +
			"returns more\n" +
			"information.",
	}
}

func operationRefresh() *huma.Operation {
	return &huma.Operation{
		OperationID: "retrieverRefresh", Method: http.MethodPost, Path: "/admin/v1/retriever/refresh",
		Summary: "This endpoint is used to force the refresh of the flags in the cache.", Tags: []string{tagAdmin},
		Security: securityAuthorization(),
		Description: "This endpoint is used to force the refresh of the flags in the cache.\n" +
			"The relay polls the control plane at once, through the poll of its schedule: calls that arrive together\n" +
			"share one poll. The answer tells what the poll found: its result, the generation the relay holds and\n" +
			"whether the poll changed it. A failed poll is an answer with its reason, not an error of the request.\n" +
			"Only an admin key may call it.",
		Errors: []int{
			http.StatusBadRequest, http.StatusInternalServerError, http.StatusNotImplemented,
			http.StatusServiceUnavailable, http.StatusGatewayTimeout,
		},
	}
}

func operationManifest() *huma.Operation {
	return &huma.Operation{
		OperationID: "getManifest",
		Method:      http.MethodGet,
		Path:        "/openfeature/v0/manifest",
		Summary:     "GetManifest is a GET endpoint to return the flags configured in Semaphore.",
		Tags:        []string{tagManifest},
		Security:    securityAuthorization(),
		Description: "**GET** request to the URL `/openfeature/v0/manifest` returns the project manifest\n" +
			"containing active flags. Archived flags are excluded.",
		Errors: []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusInternalServerError},
	}
}

func operationFlagChange() *huma.Operation {
	return &huma.Operation{
		OperationID: "flagChange", Method: http.MethodGet, Path: "/v1/flag/change",
		Summary: "Endpoint to poll if you want to know if there is a configuration change in the flags",
		Tags:    []string{tagEvaluation}, Security: securityAll(),
		Description: "Making a **GET** request to the URL `/v1/flag/change` will give you the hash of the current\n" +
			"configuration, you can use this hash to know if the configuration has changed.",
		Responses: map[string]*huma.Response{statusKey(http.StatusNotModified): notModified()},
		Errors:    []int{http.StatusInternalServerError},
	}
}

func operationFlagConfiguration() *huma.Operation {
	return &huma.Operation{
		OperationID: "flagConfiguration", Method: http.MethodPost, Path: "/v1/flag/configuration",
		Summary: "Endpoint to poll if you want to get the configuration of the flags.",
		Tags:    []string{tagEvaluation}, Security: securityAll(),
		Description: "Making a **POST** request to the URL `/v1/flag/configuration` will give you the list of\n" +
			"the flags to use them for local evaluation in your provider.",
		Responses: map[string]*huma.Response{statusKey(http.StatusNotModified): notModified()},
		Errors:    []int{http.StatusInternalServerError},
	}
}

func operationCollector() *huma.Operation {
	return &huma.Operation{
		OperationID: "collectEvalData",
		Method:      http.MethodPost,
		Path:        "/v1/data/collector",
		Summary:     "Endpoint to send usage of your flags to be collected",
		Tags:        []string{tagEvaluation},
		Security:    securityAll(),
		Description: "This endpoint is receiving the events of your flags usage to send them in the data collector.\n\n" +
			"It is used by the different Open Feature providers to send in bulk all the cached events to avoid\n" +
			"to lose track of what happen when a cached flag is used.",
		Errors: []int{http.StatusBadRequest, http.StatusInternalServerError},
	}
}
