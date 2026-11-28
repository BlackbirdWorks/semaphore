package ofrepconformance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

// Requirement identifiers, as returned by [Spec.Requirements]. A conformance case reports what it exercised with
// the same identifiers, so that a document that grows is a failing test until every new item has a case.
const (
	kindResponse = "response"
	kindHeader   = "header"
	kindSchema   = "schema"
	kindParam    = "param"
	kindSecurity = "security"
	kindBranch   = "value"
	kindEnum     = "enum"

	codeDefaultName = "codeDefaultFlag"
	schemaSuccess   = "evaluationSuccess"
	schemaFailure   = "evaluationFailure"
	schemaNotFound  = "flagNotFound"
)

// ResponseID identifies a documented response: operation and status code.
func ResponseID(method, path string, status int) string {
	return fmt.Sprintf("%s %s %s %d", kindResponse, strings.ToUpper(method), path, status)
}

// HeaderID identifies a response header documented for a response.
func HeaderID(method, path string, status int, name string) string {
	return fmt.Sprintf("%s %s %s %d %s", kindHeader, strings.ToUpper(method), path, status, canonicalHeader(name))
}

// SchemaID identifies the JSON schema documented for a response.
func SchemaID(method, path string, status int) string {
	return fmt.Sprintf("%s %s %s %d", kindSchema, strings.ToUpper(method), path, status)
}

// ParamID identifies a request parameter ("header" or "query") of an operation.
func ParamID(method, path, location, name string) string {
	return fmt.Sprintf("%s %s %s %s:%s", kindParam, strings.ToUpper(method), path, location, name)
}

// EnumID identifies one value of an enumerated property of a response: "reason" or "errorCode".
func EnumID(property, value string) string { return fmt.Sprintf("%s %s %s", kindEnum, property, value) }

// SecurityID identifies a security scheme.
func SecurityID(name string) string { return kindSecurity + " " + name }

// BranchID identifies one branch of the value of a successful evaluation.
func BranchID(name string) string { return kindBranch + " " + name }

func canonicalHeader(name string) string {
	parts := strings.Split(strings.ToLower(name), "-")
	for idx, part := range parts {
		if part != "" {
			parts[idx] = strings.ToUpper(part[:1]) + part[1:]
		}
	}

	return strings.Join(parts, "-")
}

// Requirements lists everything the document defines that a conformance suite has to exercise: every response
// (status code) of every operation, each header and JSON schema of those responses, each request parameter, each
// security scheme, each value type of a successful evaluation, and each value of the `reason` and
// `errorCode` enumerations (a case records those it asserts with [EnumID]; the validator checks every response against
// them as well). The list is sorted.
func (s *Spec) Requirements() []string {
	var ids []string

	for path, methods := range s.doc.Paths {
		for method, oper := range methods {
			ids = append(ids, s.operationRequirements(path, method, &oper)...)
		}
	}

	for name := range s.doc.Components.SecuritySchemes {
		ids = append(ids, SecurityID(name))
	}

	for name := range s.ValueBranches() {
		ids = append(ids, BranchID(name))
	}

	for _, spot := range []struct{ schemaName, property string }{
		{schemaSuccess, "reason"}, {schemaFailure, "errorCode"}, {schemaNotFound, "errorCode"},
	} {
		for _, val := range s.propertyEnum(spot.schemaName, spot.property) {
			ids = append(ids, EnumID(spot.property, val))
		}
	}

	return uniqueSorted(ids)
}

func (s *Spec) propertyEnum(schemaName, property string) []string {
	sch := s.doc.Components.Schemas[schemaName]
	if sch == nil {
		return nil
	}

	var out []string

	collect := func(from *schema) {
		if prop := from.Properties[property]; prop != nil {
			for _, val := range prop.Enum {
				out = append(out, fmt.Sprint(val))
			}
		}
	}

	collect(sch)

	for _, part := range sch.AllOf {
		collect(part)
	}

	return out
}

func (s *Spec) operationRequirements(path, method string, oper *operation) []string {
	var ids []string

	for code, resp := range oper.Responses {
		var status int

		_, err := fmt.Sscanf(code, "%d", &status)
		if err != nil {
			continue
		}

		ids = append(ids, ResponseID(method, path, status))

		for header := range resp.Headers {
			ids = append(ids, HeaderID(method, path, status, header))
		}

		if media, ok := resp.Content[mimeJSON]; ok && media.Schema != nil {
			ids = append(ids, SchemaID(method, path, status))
		}
	}

	for _, param := range oper.Parameters {
		param = s.resolveParameter(param)
		ids = append(ids, ParamID(method, path, param.In, param.Name))
	}

	return ids
}

func (s *Spec) resolveParameter(param parameter) parameter {
	name, ok := strings.CutPrefix(param.Ref, "#/components/parameters/")
	if !ok {
		return param
	}

	return s.doc.Components.Parameters[name]
}

// ValueBranches returns the value types of a successful evaluation: branch name to the JSON type of its `value`
// (empty for the code default, which has no value).
func (s *Spec) ValueBranches() map[string]string {
	branches := make(map[string]string)

	success := s.doc.Components.Schemas[schemaSuccess]
	if success == nil {
		return branches
	}

	for _, part := range success.AllOf {
		for _, branch := range part.OneOf {
			name := strings.TrimPrefix(branch.Ref, schemaRefPrefix)

			target := s.doc.Components.Schemas[name]
			if target == nil {
				continue
			}

			if val := target.Properties[fieldValue]; val != nil {
				branches[name] = val.Type
			} else {
				branches[name] = ""
			}
		}
	}

	return branches
}

// Observed derives the requirement identifiers that one request and its response exercised, among the ones the
// document defines. The caller validated the response against the document first.
func (s *Spec) Observed(path, method string, req *Exchange) []string {
	var ids []string

	status := req.Status

	resp, err := s.lookupResponse(path, method, status)
	if err != nil {
		return nil
	}

	ids = append(ids, ResponseID(method, path, status))

	for header := range resp.Headers {
		if req.RespHeaders.Get(header) != "" {
			ids = append(ids, HeaderID(method, path, status, header))
		}
	}

	if media, ok := resp.Content[mimeJSON]; ok && media.Schema != nil {
		ids = append(ids, SchemaID(method, path, status))
	}

	ids = append(ids, s.observedParams(path, method, req)...)
	ids = append(ids, observedBody(s, req)...)

	return ids
}

func (s *Spec) observedParams(path, method string, req *Exchange) []string {
	var ids []string

	oper := s.doc.Paths[path][strings.ToLower(method)]
	for _, param := range oper.Parameters {
		param = s.resolveParameter(param)

		switch param.In {
		case "header":
			if req.ReqHeaders.Get(param.Name) != "" {
				ids = append(ids, ParamID(method, path, param.In, param.Name))
			}
		case "path":
			ids = append(ids, ParamID(method, path, param.In, param.Name))
		case "query":
			if req.Query.Has(param.Name) {
				ids = append(ids, ParamID(method, path, param.In, param.Name))
			}
		}
	}

	return ids
}

func uniqueSorted(ids []string) []string {
	slices.Sort(ids)

	out := ids[:0]

	for idx, id := range ids {
		if idx == 0 || id != ids[idx-1] {
			out = append(out, id)
		}
	}

	return out
}

// Exchange is one HTTP exchange with the relay, as a conformance case saw it.
type Exchange struct {
	ReqHeaders  http.Header
	RespHeaders http.Header
	Query       url.Values
	Body        []byte
	Status      int
	// Authenticated is true when the relay under test requires a key and accepted the request.
	Authenticated bool
}

func (s *Spec) observedSecurity(req *Exchange) []string {
	if !req.Authenticated || req.Status >= http.StatusBadRequest {
		return nil
	}

	var ids []string

	for name, scheme := range s.doc.Components.SecuritySchemes {
		switch {
		case scheme.Type == "apiKey" && scheme.In == "header" && req.ReqHeaders.Get(scheme.Name) != "":
			ids = append(ids, SecurityID(name))
		case scheme.Type == "http" && strings.EqualFold(scheme.Scheme, "bearer") &&
			strings.HasPrefix(req.ReqHeaders.Get("Authorization"), "Bearer "):
			ids = append(ids, SecurityID(name))
		}
	}

	return ids
}

func observedBody(s *Spec, req *Exchange) []string {
	ids := s.observedSecurity(req)

	var doc map[string]any

	dec := json.NewDecoder(bytes.NewReader(req.Body))
	dec.UseNumber()

	if dec.Decode(&doc) != nil {
		return ids
	}

	items := []map[string]any{doc}

	if flags, ok := doc["flags"].([]any); ok {
		items = items[:0]

		for _, raw := range flags {
			if item, isMap := raw.(map[string]any); isMap {
				items = append(items, item)
			}
		}
	}

	for _, item := range items {
		ids = append(ids, observedItem(item)...)
	}

	return ids
}

func observedItem(item map[string]any) []string {
	if _, failed := item["errorCode"].(string); failed {
		return nil
	}

	if _, ok := item["reason"].(string); !ok {
		return nil
	}

	val, present := item[fieldValue]
	if !present {
		return []string{BranchID(codeDefaultName)}
	}

	switch typed := val.(type) {
	case bool:
		return []string{BranchID("booleanFlag")}
	case string:
		return []string{BranchID("stringFlag")}
	case json.Number:
		if strings.ContainsAny(typed.String(), ".eE") {
			return []string{BranchID("floatFlag")}
		}

		return []string{BranchID("integerFlag")}
	case map[string]any, []any:
		return []string{BranchID("objectFlag")}
	}

	return nil
}

// HasResponse reports whether the document defines a response with that status code for the operation.
func (s *Spec) HasResponse(path, method string, status int) bool {
	_, err := s.lookupResponse(path, method, status)

	return err == nil
}
