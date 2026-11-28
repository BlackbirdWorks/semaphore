# Client compatibility

This page tracks what the provider integration suites in `openfeature/provider_tests/` test against the version 2 relay proxy.

The current suites run unmodified `GO Feature Flag` OpenFeature providers against two relay proxy instances:

- `bootstrap.yaml` (`:1031`) with `evaluation.contextEnrichment` enabled
- `bootstrap-authenticated.yaml` (`:1032`) for API key authentication checks

Both instances set `evaluation.disableSemaphoreMetadata: true` so response metadata matches what the upstream providers assert. Data collector and tracking event coverage depends on `#9` and is omitted until that pipeline is ready.

## Provider coverage

| Language | Provider | Pinned version | Value types | Bulk evaluation | Evaluation context | Error codes | Data collector / tracking | Flag change polling | WebSocket |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Go | `github.com/open-feature/go-sdk-contrib/providers/go-feature-flag` (SDK `v1.20.0`, `EvaluationTypeRemote`) | `v1.1.3` (`go.mod`) | bool, string, float, int, object (match, disabled, not-found; type mismatch for bool/string/float/int) | Not covered | Targeting key and `email` targeting asserted; semver `version` targeting and server `contextEnrichment` asserted; other fixture attributes (bool, number, list, nested object) sent without targeting assertions | `FLAG_NOT_FOUND`, `TYPE_MISMATCH`, `GENERAL` (invalid/missing API key) | Not covered (depends on `#9`) | Not covered | Not covered |
| Java | `dev.openfeature.contrib.providers:go-feature-flag` (SDK `1.23.0`) | `2.0.2` (`pom.xml`) | bool, string, double, int, object (match, disabled, not-found; type mismatch for bool/string/double/int; disabled object uses `disabled_int`) | Not covered | Targeting key and `email` targeting asserted; other fixture attributes (bool, number, list, nested structure) sent without targeting assertions | `FLAG_NOT_FOUND`, `TYPE_MISMATCH`, `PROVIDER_FATAL` (invalid/empty API key: init throws `AuthenticationFailure`, next evaluation returns `PROVIDER_FATAL`) | Not covered (depends on `#9`) | Not covered | Not covered |
| .NET | `OpenFeature.Contrib.GOFeatureFlag` (SDK `2.14.1`) | `0.2.1` (`ProviderTests.csproj`) | bool, string, double, int, object (match, disabled, not-found; type mismatch for bool/string/double/int; disabled object uses `disabled_int`) | Not covered | Targeting key and `email` targeting asserted; other fixture attributes (bool, number, list, nested structure) sent without targeting assertions | `FLAG_NOT_FOUND`, `TYPE_MISMATCH`, `GENERAL` (invalid/empty API key) | Not covered (depends on `#9`) | Not covered | Not covered |
| JavaScript | `@openfeature/go-feature-flag-provider` (SDK `1.23.0`; peer `@openfeature/ofrep-provider` `0.2.4`) | `^2.2.0` (`package.json`), locked `2.2.0` (`package-lock.json`) | bool, string, number (float only), object (match, disabled, type mismatch, not-found; disabled object uses `disabled_int`) | Not covered | Targeting key and `email` targeting asserted; other fixture attributes (bool, number, array, nested object) and global `gofeatureflag.flagList` sent without assertions | `FLAG_NOT_FOUND`, `TYPE_MISMATCH`, `PROVIDER_FATAL` (invalid/empty API key: init rejects with `UnauthorizedError`, next evaluation returns `PROVIDER_FATAL`) | Not covered (depends on `#9`) | Not covered | Not covered |
| HTTP (Go suite) | Direct OFREP conformance (`openfeature/provider_tests/ofrep-conformance`) | OFREP OpenAPI `0.4.0` (commit `98c4e0de`, SHA-256 pinned) | bool, string, int, float, object (disabled flags omit `value` per `codeDefaultFlag`; live OFREP reasons `STATIC`, `TARGETING_MATCH`, `SPLIT`, `DISABLED`; `UNKNOWN` covered in schema and handler unit tests) | Every operation and status code of the document, checked for schema and headers: active flag set, full store with disabled flags, `ETag`, `If-None-Match` (`304`; strong, weak, list, `*`; stale), `flagConfigEtag` and `flagConfigLastModified`, `eventStreams`, per-flag error items | `targetingKey` optional check, `TARGETING_KEY_MISSING`, semver `version`, server `contextEnrichment`, non-string `targetingKey` (`400`), missing, null and malformed `context`, wrong content type, oversized body, escaped flag key | `FLAG_NOT_FOUND` (`404`), `INVALID_CONTEXT` (`400`), `TARGETING_KEY_MISSING` (`400`), `401` and `403` (`X-API-Key`, `Bearer`, public flag set, admin key); the relay has no rate limit, so no `429` | Not covered (OFREP spec has no tracking endpoint) | Not covered | Not covered |

## Known gaps

- No dedicated OFREP-only provider suites yet (Go, JavaScript server/web, Java, .NET, Python).
- Live HTTP evaluation against the relay proxy does not trigger `ReasonUnknown` or `ReasonOffline`; their mapping to `UNKNOWN` is verified in `cmd/relayproxy/handler/ofrep` unit tests and the OpenAPI schema validator.
- Bulk evaluation, flag change polling, and WebSocket notifications are uncovered across the four `GO Feature Flag` provider suites (bulk evaluation is covered in the direct HTTP OFREP suite).
- Object `TYPE_MISMATCH` is missing in Go, Java, and .NET; the Java, .NET, and JavaScript disabled-object tests evaluate `disabled_int` instead of `disabled_interface`; JavaScript number tests cover floats only (not integers).
- Ordinary targeting rules only assert `email` matching (`flags.yaml`); non-string, list, and nested context attributes are included in test fixtures without targeting-rule assertions.
- Server-side `evaluation.contextEnrichment` and semver targeting rules are only tested in Go and the direct HTTP OFREP suite.
- Across the four `GO Feature Flag` provider suites, error codes `PARSE_ERROR`, `TARGETING_KEY_MISSING`, `INVALID_CONTEXT`, and rate limiting (`429` / `Retry-After`) are not covered. In the direct HTTP OFREP suite, `TARGETING_KEY_MISSING` and `INVALID_CONTEXT` are covered; the relay never sends `PARSE_ERROR` and has no rate limit.
- JavaScript `package.json` uses a caret range (`^2.2.0`) rather than an exact pinned version (resolved to `2.2.0` in `package-lock.json`).
- Data collector and tracking events depend on `#9` and are intentionally left out.
