package ofrepconformance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"mime"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	headerETag        = "ETag"
	headerRetryAfter  = "Retry-After"
	headerContentType = "Content-Type"
	mimeJSON          = "application/json"

	schemaRefPrefix    = "#/components/schemas/"
	refCodeDefaultFlag = "#/components/schemas/codeDefaultFlag"
	refIntegerFlag     = "#/components/schemas/integerFlag"
	refFloatFlag       = "#/components/schemas/floatFlag"
	fieldValue         = "value"

	typeObject  = "object"
	typeArray   = "array"
	typeString  = "string"
	typeBoolean = "boolean"
	typeInteger = "integer"
	typeNumber  = "number"

	decimalBase        = 10
	bitSize64          = 64
	numericBranchCount = 2
	minQuotedETagLen   = 2
	etagBang           = 0x21
	etagRangeLow       = 0x23
	etagRangeHigh      = 0x7E
	etagObsTextMin     = 0x80
)

var (
	errInvalidSpec          = errors.New("invalid OpenAPI specification")
	errUndocumentedPath     = errors.New("undocumented OpenAPI path")
	errUndocumentedMethod   = errors.New("undocumented OpenAPI method")
	errUndocumentedStatus   = errors.New("undocumented HTTP status code")
	errMissingHeader        = errors.New("missing required response header")
	errInvalidHeader        = errors.New("invalid response header")
	errUnexpectedBody       = errors.New("unexpected non-empty response body")
	errTrailingJSON         = errors.New("unexpected trailing JSON data")
	errUnknownSchemaRef     = errors.New("unknown schema $ref")
	errSchemaTypeMismatch   = errors.New("schema type mismatch")
	errMissingRequiredField = errors.New("missing required object property")
	errDisallowedExtraProp  = errors.New("disallowed additional property")
	errEnumMismatch         = errors.New("value is not in enum")
	errMinimumViolation     = errors.New("value is below schema minimum")
	errPatternMismatch      = errors.New("value does not match schema pattern")
	errFormatMismatch       = errors.New("value does not match schema format")
	errNotConstraintFailed  = errors.New("value matched negated schema")
	errOneOfMismatch        = errors.New("value matched zero oneOf branches")
	errOneOfAmbiguous       = errors.New("value matched multiple oneOf branches")
)

// Spec is a parsed OpenAPI 3.1 specification used to validate OFREP HTTP responses.
type Spec struct {
	doc openAPIDoc
}

type openAPIDoc struct {
	Components components                      `yaml:"components"`
	Paths      map[string]map[string]operation `yaml:"paths"`
	Security   []map[string][]string           `yaml:"security"`
}

type components struct {
	Schemas         map[string]*schema        `yaml:"schemas"`
	Parameters      map[string]parameter      `yaml:"parameters"`
	SecuritySchemes map[string]securityScheme `yaml:"securitySchemes"`
}

type operation struct {
	Responses  map[string]responseSpec `yaml:"responses"`
	Parameters []parameter             `yaml:"parameters"`
	Security   []map[string][]string   `yaml:"security"`
}

type parameter struct {
	Ref  string `yaml:"$ref"`
	Name string `yaml:"name"`
	In   string `yaml:"in"`
}

type securityScheme struct {
	Type   string `yaml:"type"`
	Scheme string `yaml:"scheme"`
	In     string `yaml:"in"`
	Name   string `yaml:"name"`
}

type responseSpec struct {
	Headers map[string]any       `yaml:"headers"`
	Content map[string]mediaType `yaml:"content"`
}

type mediaType struct {
	Schema *schema `yaml:"schema"`
}

type schema struct {
	AdditionalProperties *additionalProps   `yaml:"additionalProperties"`
	Items                *schema            `yaml:"items"`
	Not                  *schema            `yaml:"not"`
	Minimum              *float64           `yaml:"minimum"`
	Properties           map[string]*schema `yaml:"properties"`
	Ref                  string             `yaml:"$ref"`
	Type                 string             `yaml:"type"`
	Format               string             `yaml:"format"`
	Pattern              string             `yaml:"pattern"`
	AllOf                []*schema          `yaml:"allOf"`
	OneOf                []*schema          `yaml:"oneOf"`
	Required             []string           `yaml:"required"`
	Enum                 []any              `yaml:"enum"`
	// allowArray is set by a deviation: see [Deviations].
	allowArray bool
}

type additionalProps struct {
	Schema *schema
	Allow  *bool
}

// UnmarshalYAML decodes an OpenAPI additionalProperties field, which may be a boolean or a schema object.
func (a *additionalProps) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		var allow bool

		err := value.Decode(&allow)
		if err != nil {
			return fmt.Errorf("decode additionalProperties boolean: %w", err)
		}

		a.Allow = &allow

		return nil
	}

	var sub schema

	err := value.Decode(&sub)
	if err != nil {
		return fmt.Errorf("decode additionalProperties schema: %w", err)
	}

	a.Schema = &sub

	return nil
}

// LoadSpec parses an OpenAPI YAML file from path.
func LoadSpec(path string) (*Spec, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read OpenAPI spec: %w", err)
	}

	return ParseSpec(raw)
}

// ParseSpec parses an OpenAPI YAML document.
func ParseSpec(raw []byte) (*Spec, error) {
	err := checkKeywords(raw)
	if err != nil {
		return nil, err
	}

	var doc openAPIDoc

	err = yaml.Unmarshal(raw, &doc)
	if err != nil {
		return nil, fmt.Errorf("unmarshal OpenAPI spec: %w", err)
	}

	if len(doc.Paths) == 0 || len(doc.Components.Schemas) == 0 {
		return nil, fmt.Errorf("%w: missing paths or component schemas", errInvalidSpec)
	}

	err = applyDeviations(&doc)
	if err != nil {
		return nil, err
	}

	return &Spec{doc: doc}, nil
}

// HasHeader reports whether the documented response for path, method, and statusCode declares headerName.
func (s *Spec) HasHeader(path, method string, statusCode int, headerName string) bool {
	resp, err := s.lookupResponse(path, method, statusCode)
	if err != nil {
		return false
	}

	for name := range resp.Headers {
		if strings.EqualFold(name, headerName) {
			return true
		}
	}

	return false
}

// ValidateResponse checks that an HTTP response conforms to the documented status code, headers, and JSON schema.
func (s *Spec) ValidateResponse(path, method string, statusCode int, headers http.Header, body []byte) error {
	resp, err := s.lookupResponse(path, method, statusCode)
	if err != nil {
		return err
	}

	err = validateResponseHeaders(resp, headers)
	if err != nil {
		return err
	}

	if statusCode == http.StatusNotModified {
		if len(body) != 0 {
			return fmt.Errorf("%w: got %d bytes on 304", errUnexpectedBody, len(body))
		}

		return nil
	}

	media, hasJSON := resp.Content[mimeJSON]
	if !hasJSON || media.Schema == nil {
		return nil
	}

	err = validateJSONContentType(headers.Get(headerContentType))
	if err != nil {
		return err
	}

	val, err := decodeSingleJSON(body)
	if err != nil {
		return err
	}

	return s.validateSchema(media.Schema, val)
}

func (s *Spec) lookupResponse(path, method string, statusCode int) (responseSpec, error) {
	methods, ok := s.doc.Paths[path]
	if !ok {
		return responseSpec{}, fmt.Errorf("%w: %s", errUndocumentedPath, path)
	}

	op, ok := methods[strings.ToLower(method)]
	if !ok {
		return responseSpec{}, fmt.Errorf("%w: %s %s", errUndocumentedMethod, method, path)
	}

	resp, ok := op.Responses[strconv.Itoa(statusCode)]
	if !ok {
		return responseSpec{}, fmt.Errorf("%w: %s %s -> %d", errUndocumentedStatus, method, path, statusCode)
	}

	return resp, nil
}

func validateResponseHeaders(resp responseSpec, headers http.Header) error {
	for name := range resp.Headers {
		if strings.EqualFold(name, headerETag) {
			err := validateETag(headers.Get(headerETag))
			if err != nil {
				return err
			}
		}

		if strings.EqualFold(name, headerRetryAfter) {
			err := validateRetryAfter(headers.Get(headerRetryAfter))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func validateETag(val string) error {
	if val == "" {
		return fmt.Errorf("%w: %s", errMissingHeader, headerETag)
	}

	tag := strings.TrimPrefix(val, "W/")
	if len(tag) < minQuotedETagLen || tag[0] != '"' || tag[len(tag)-1] != '"' {
		return fmt.Errorf("%w %s: %q", errInvalidHeader, headerETag, val)
	}

	for idx := 1; idx < len(tag)-1; idx++ {
		if !isETagOctet(tag[idx]) {
			return fmt.Errorf("%w %s: %q", errInvalidHeader, headerETag, val)
		}
	}

	return nil
}

func isETagOctet(octet byte) bool {
	return octet == etagBang || (octet >= etagRangeLow && octet <= etagRangeHigh) || octet >= etagObsTextMin
}

func validateRetryAfter(val string) error {
	if val == "" {
		return fmt.Errorf("%w: %s", errMissingHeader, headerRetryAfter)
	}

	if isASCIIDigits(val) {
		_, err := strconv.ParseUint(val, decimalBase, bitSize64)
		if err == nil {
			return nil
		}
	}

	_, err := time.Parse(time.RFC3339, val)
	if err == nil {
		return nil
	}

	_, err = http.ParseTime(val)
	if err == nil {
		return nil
	}

	return fmt.Errorf("%w %s: %q", errInvalidHeader, headerRetryAfter, val)
}

func isASCIIDigits(raw string) bool {
	if raw == "" {
		return false
	}

	for idx := range len(raw) {
		if raw[idx] < '0' || raw[idx] > '9' {
			return false
		}
	}

	return true
}

func validateJSONContentType(contentType string) error {
	if contentType == "" {
		return fmt.Errorf("%w: %s", errMissingHeader, headerContentType)
	}

	mediaTypeStr, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return fmt.Errorf("parse %s: %w", headerContentType, err)
	}

	if mediaTypeStr != mimeJSON {
		return fmt.Errorf("%w %s: got %q, want %q", errInvalidHeader, headerContentType, mediaTypeStr, mimeJSON)
	}

	return nil
}

func decodeSingleJSON(body []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	var val any

	err := dec.Decode(&val)
	if err != nil {
		return nil, fmt.Errorf("decode JSON body: %w", err)
	}

	var extra any

	err = dec.Decode(&extra)
	if !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: %w", errTrailingJSON, err)
	}

	return val, nil
}

func (s *Spec) resolveRef(ref string) (*schema, error) {
	name, ok := strings.CutPrefix(ref, schemaRefPrefix)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownSchemaRef, ref)
	}

	target, ok := s.doc.Components.Schemas[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownSchemaRef, ref)
	}

	return target, nil
}

func (s *Spec) validateSchema(sch *schema, val any) error {
	if sch.Ref != "" {
		target, err := s.resolveRef(sch.Ref)
		if err != nil {
			return err
		}

		return s.validateSchema(target, val)
	}

	if isDeviantArray(sch, val) {
		return nil
	}

	err := s.validateComposite(sch, val)
	if err != nil {
		return err
	}

	err = validateScalar(sch, val)
	if err != nil {
		return err
	}

	if sch.Type == typeObject || len(sch.Required) > 0 || len(sch.Properties) > 0 || sch.AdditionalProperties != nil {
		err = s.validateObject(sch, val)
		if err != nil {
			return err
		}
	}

	if sch.Type == typeArray && sch.Items != nil {
		return s.validateArray(sch.Items, val)
	}

	return nil
}

func (s *Spec) validateComposite(sch *schema, val any) error {
	for _, sub := range sch.AllOf {
		err := s.validateSchema(sub, val)
		if err != nil {
			return err
		}
	}

	if len(sch.OneOf) > 0 {
		err := s.validateOneOf(sch.OneOf, val)
		if err != nil {
			return err
		}
	}

	if sch.Not != nil && s.validateSchema(sch.Not, val) == nil {
		return errNotConstraintFailed
	}

	return nil
}

func (s *Spec) validateOneOf(branches []*schema, val any) error {
	if hasCodeDefaultBranch(branches) {
		return s.validateValueOneOf(branches, val)
	}

	matches := 0

	for _, branch := range branches {
		if s.validateSchema(branch, val) == nil {
			matches++
		}
	}

	if matches == 0 {
		return errOneOfMismatch
	}

	if matches > 1 {
		return fmt.Errorf("%w (%d matched)", errOneOfAmbiguous, matches)
	}

	return nil
}

func hasCodeDefaultBranch(branches []*schema) bool {
	for _, branch := range branches {
		if branch.Ref == refCodeDefaultFlag {
			return true
		}
	}

	return false
}

// validateValueOneOf validates evaluationSuccess's value-type oneOf.
// Per ADR-0006, codeDefaultFlag matches only when "value" is absent; numeric values match integerFlag and floatFlag.
func (s *Spec) validateValueOneOf(branches []*schema, val any) error {
	obj, ok := val.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: expected object, got %T", errSchemaTypeMismatch, val)
	}

	_, hasValue := obj[fieldValue]

	var matchedRefs []string

	for _, branch := range branches {
		if branch.Ref == refCodeDefaultFlag {
			if !hasValue {
				matchedRefs = append(matchedRefs, branch.Ref)
			}

			continue
		}

		if hasValue && s.validateSchema(branch, val) == nil {
			matchedRefs = append(matchedRefs, branch.Ref)
		}
	}

	if len(matchedRefs) == 0 {
		return errOneOfMismatch
	}

	if len(matchedRefs) == 1 || isNumericPair(matchedRefs) {
		return nil
	}

	return fmt.Errorf("%w: %v", errOneOfAmbiguous, matchedRefs)
}

func isNumericPair(refs []string) bool {
	if len(refs) != numericBranchCount {
		return false
	}

	return (refs[0] == refIntegerFlag && refs[1] == refFloatFlag) ||
		(refs[0] == refFloatFlag && refs[1] == refIntegerFlag)
}

func validateScalar(sch *schema, val any) error {
	err := validateType(sch.Type, val)
	if err != nil {
		return err
	}

	err = validateEnum(sch.Enum, val)
	if err != nil {
		return err
	}

	err = validateNumberConstraints(sch, val)
	if err != nil {
		return err
	}

	return validateStringConstraints(sch, val)
}

func validateType(wantType string, val any) error {
	if wantType == "" {
		return nil
	}

	if val == nil {
		return fmt.Errorf("%w: expected %s, got null", errSchemaTypeMismatch, wantType)
	}

	if isMatchingType(wantType, val) {
		return nil
	}

	return fmt.Errorf("%w: expected %s, got %v (%T)", errSchemaTypeMismatch, wantType, val, val)
}

func isMatchingType(wantType string, val any) bool {
	switch wantType {
	case typeObject:
		_, ok := val.(map[string]any)

		return ok
	case typeArray:
		_, ok := val.([]any)

		return ok
	case typeString:
		_, ok := val.(string)

		return ok
	case typeBoolean:
		_, ok := val.(bool)

		return ok
	case typeInteger:
		return isIntegerValue(val)
	case typeNumber:
		return isNumberValue(val)
	default:
		return false
	}
}

func isIntegerValue(val any) bool {
	num, ok := val.(json.Number)
	if !ok {
		return false
	}

	_, err := strconv.ParseInt(num.String(), decimalBase, bitSize64)
	if err == nil {
		return true
	}

	rat, ok := new(big.Rat).SetString(num.String())

	return ok && rat.IsInt()
}

func isNumberValue(val any) bool {
	num, ok := val.(json.Number)
	if !ok {
		return false
	}

	_, err := num.Float64()

	return err == nil
}

func validateEnum(allowed []any, val any) error {
	if len(allowed) == 0 {
		return nil
	}

	for _, candidate := range allowed {
		if fmt.Sprint(candidate) == fmt.Sprint(val) {
			return nil
		}
	}

	return fmt.Errorf("%w: got %v", errEnumMismatch, val)
}

func validateNumberConstraints(sch *schema, val any) error {
	if sch.Minimum == nil {
		return nil
	}

	num, ok := val.(json.Number)
	if !ok {
		return nil
	}

	parsed, err := num.Float64()
	if err != nil {
		return fmt.Errorf("parse number: %w", err)
	}

	if parsed < *sch.Minimum {
		return fmt.Errorf("%w: %v < %v", errMinimumViolation, parsed, *sch.Minimum)
	}

	return nil
}

func validateStringConstraints(sch *schema, val any) error {
	str, ok := val.(string)
	if !ok {
		return nil
	}

	if sch.Pattern != "" {
		re, err := regexp.Compile(sch.Pattern)
		if err != nil {
			return fmt.Errorf("compile pattern %q: %w", sch.Pattern, err)
		}

		if !re.MatchString(str) {
			return fmt.Errorf("%w: %q does not match %q", errPatternMismatch, str, sch.Pattern)
		}
	}

	switch sch.Format {
	case "uri":
		parsed, err := url.ParseRequestURI(str)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return fmt.Errorf("%w: invalid uri", errFormatMismatch)
		}
	case "date-time":
		_, err := time.Parse(time.RFC3339, str)
		if err != nil {
			return fmt.Errorf("%w: %q is not RFC3339 date-time", errFormatMismatch, str)
		}
	}

	return nil
}

func (s *Spec) validateObject(sch *schema, val any) error {
	obj, ok := val.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: expected object, got %T", errSchemaTypeMismatch, val)
	}

	for _, reqKey := range sch.Required {
		if _, exists := obj[reqKey]; !exists {
			return fmt.Errorf("%w: %s", errMissingRequiredField, reqKey)
		}
	}

	for key, propVal := range obj {
		if propSchema, defined := sch.Properties[key]; defined {
			err := s.validateSchema(propSchema, propVal)
			if err != nil {
				return fmt.Errorf("property %s: %w", key, err)
			}

			continue
		}

		err := s.validateAdditionalProp(sch.AdditionalProperties, key, propVal)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Spec) validateAdditionalProp(add *additionalProps, key string, propVal any) error {
	if add == nil {
		return nil
	}

	if add.Allow != nil && !*add.Allow {
		return fmt.Errorf("%w: %s", errDisallowedExtraProp, key)
	}

	if add.Schema != nil {
		err := s.validateSchema(add.Schema, propVal)
		if err != nil {
			return fmt.Errorf("additional property %s: %w", key, err)
		}
	}

	return nil
}

func (s *Spec) validateArray(itemSchema *schema, val any) error {
	arr, ok := val.([]any)
	if !ok {
		return fmt.Errorf("%w: expected array, got %T", errSchemaTypeMismatch, val)
	}

	for idx, elem := range arr {
		err := s.validateSchema(itemSchema, elem)
		if err != nil {
			return fmt.Errorf("index %d: %w", idx, err)
		}
	}

	return nil
}

// isDeviantArray reports whether the value is the array that a deviation accepts for the schema.
func isDeviantArray(sch *schema, val any) bool {
	_, isList := val.([]any)

	return isList && sch.allowArray
}
