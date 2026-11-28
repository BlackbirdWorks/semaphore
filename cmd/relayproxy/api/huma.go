package api

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/danielgtaylor/huma/v2"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/helper"
	reqlog "github.com/blackbirdworks/semaphore/cmdhelpers/api/middleware"
	"github.com/blackbirdworks/semaphore/internal/protocol"
)

const (
	// securityAPIKey and securityXAPIKey are the names of the security schemes of the document.
	securityAPIKey  = "ApiKeyAuth"  //nolint:gosec // the name of a security scheme, not a credential
	securityXAPIKey = "XApiKeyAuth" //nolint:gosec // the name of a security scheme, not a credential
	// securityQueryAPIKey is the scheme of the streams: they take the key in the apiKey query parameter only.
	securityBearer      = "BearerAuth"
	securityQueryAPIKey = "ApiKeyQueryAuth" //nolint:gosec // the name of a security scheme, not a credential
	// schemeAPIKey is the type of the security schemes: an API key.
	schemeAPIKey = "apiKey"
	// queryAPIKey is the name of the query parameter that carries the key of a stream.
	queryAPIKey = "apiKey"
)

// introduction is the description of the API, the first paragraphs of the document.
const introduction = `# Introduction

This API is documented in **OpenAPI format** and describe the REST API of the **Semaphore relay proxy**.

The relay proxy is a component to evaluate your feature flags remotely when using **Semaphore**.  
This API is mostly used by all the OpenFeature providers.`

// jsonFormat is the JSON format of the relay proxy: the encoding of encoding/json, the one of the responses
// before huma (HTML characters escaped, a newline after the document).
func jsonFormat() huma.Format {
	return huma.Format{
		Marshal: func(w io.Writer, v any) error {
			return json.NewEncoder(w).Encode(v)
		},
		Unmarshal: json.Unmarshal,
	}
}

// errorTransformer gives the bodies of the errors the shape of the relay proxy, and reports the error to the
// access log. exposeError adds the wrapped error to the body, at the debug level.
//
// The errors the handlers return are [helper.HTTPError]. The errors huma raises itself (a failure it did not get a
// status error for, which it wraps in its RFC 9457 model) are turned into the same body: huma has no error factory
// per API (its factories are package variables), the transformer of the API is the place that is not global.
func errorTransformer(exposeError bool) huma.Transformer {
	return func(ctx huma.Context, _ string, val any) (any, error) {
		if model, ok := val.(*huma.ErrorModel); ok {
			ctx.SetHeader(protocol.HeaderContentType, protocol.MIMEApplicationJSON)

			herr := modelError(model)
			reqlog.RecordError(ctx.Context(), herr)
			_, body := helper.ErrorResponse(herr, exposeError)

			return body, nil
		}

		herr, ok := errors.AsType[*helper.HTTPError](asError(val))
		if !ok {
			return val, nil
		}

		_, body := helper.ErrorResponse(herr, exposeError)
		reqlog.RecordError(ctx.Context(), herr)

		return body, nil
	}
}

// detailError is a detail that huma reports about an error: the text it gave.
type detailError string

// Error returns the text of the detail.
func (e detailError) Error() string { return string(e) }

// modelError is the error of the relay proxy for an error of the RFC 9457 model of huma: the text of the status as
// message, what huma reports as the wrapped error.
func modelError(model *huma.ErrorModel) *helper.HTTPError {
	details := make([]error, 0, len(model.Errors)+1)
	if model.Detail != "" {
		details = append(details, detailError(model.Detail))
	}

	for _, d := range model.Errors {
		details = append(details, detailError(d.Message))
	}

	if len(details) == 0 {
		return helper.NewHTTPError(model.Status, "")
	}

	return helper.WrapHTTPError(model.Status, errors.Join(details...))
}

func asError(v any) error {
	err, _ := v.(error)

	return err
}

// newHumaConfig returns the huma configuration that holds the operations of the relay proxy. The document is
// generated from them. huma serves nothing by itself unless docs is enabled: the relay proxy owns its routes.
func newHumaConfig(exposeError, docs bool) huma.Config {
	cfg := huma.DefaultConfig("Semaphore relay proxy endpoints", "1.0.0")
	// huma serves the document and the Swagger UI under the swagger path, only when it is enabled: the relay proxy
	// owns every other route.
	cfg.OpenAPIPath, cfg.DocsPath, cfg.SchemasPath = "", "", ""
	if docs {
		cfg.OpenAPIPath, cfg.DocsPath = "/swagger/openapi", "/swagger/index.html"
		cfg.DocsRenderer = huma.DocsRendererSwaggerUI
	}

	cfg.CreateHooks = nil
	cfg.Formats = map[string]huma.Format{"application/json": jsonFormat(), "json": jsonFormat()}
	cfg.DefaultFormat = "application/json"
	cfg.Transformers = []huma.Transformer{errorTransformer(exposeError)}
	cfg.Info.Description = introduction
	cfg.Info.Contact = &huma.Contact{Name: "Semaphore relay proxy"}
	cfg.Info.License = &huma.License{
		Name: "MIT",
		URL:  "https://github.com/BlackbirdWorks/semaphore/blob/main/LICENSE",
	}
	cfg.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		securityAPIKey: {
			Type: "apiKey",
			In:   "header",
			Name: "Authorization",
			Description: "Use configured APIKeys in yaml config as authorization keys, " +
				"disabled when this yaml config is not set.",
		},
		securityQueryAPIKey: {
			Type: schemeAPIKey, In: inQuery, Name: queryAPIKey,
			Description: "The API key of the flag set in the apiKey query parameter, as the streams of flag changes " +
				"take it. The URL with the key is sensitive and must not be logged.",
		},
		securityBearer: {
			Type: "http", Scheme: "bearer",
			Description: "The API key of the flag set as a bearer token: Authorization: Bearer <key>.",
		},
		securityXAPIKey: {
			Type: schemeAPIKey, In: inHeader, Name: "X-API-Key",
			Description: "Use configured APIKeys in yaml config as authorization keys via X-API-Key header,\n" +
				"disabled when this yaml config is not set.",
		},
	}

	return cfg
}
