# OpenFeature Remote Evaluation Protocol (OFREP)

## Overview

Applications integrate with Semaphore through standard [OpenFeature](https://openfeature.dev) SDKs and generic OpenFeature Remote Evaluation Protocol (OFREP) providers. This repository does not ship language-specific providers.

## What is OFREP?

OFREP is a vendor-neutral API specification that lets OpenFeature SDKs evaluate flags against any compatible backend without custom providers or vendor lock-in. See the [OpenFeature protocol repository](https://github.com/open-feature/protocol/) for the full specification.

## Use OFREP with Semaphore

Run the [relay proxy](../relay-proxy/README.md) in your infrastructure. It exposes these OFREP endpoints:

| Endpoint | Usage |
|---|---|
| `POST /ofrep/v1/evaluate/flags/{key}` | Evaluate a single flag (server-side SDKs). |
| `POST /ofrep/v1/evaluate/flags` | Bulk-evaluate all flags (client-side SDKs). |

- See the [endpoints reference](../relay-proxy/endpoints.md) and `cmd/relayproxy/api/testdata/openapi.golden.json` for full request and response schemas.
- Point your language's OFREP provider at the relay proxy base URL (for example, `http://localhost:1031`).
- If [API keys](../relay-proxy/bootstrap-config.md) are configured, pass the key via `X-API-Key` or `Authorization: Bearer <key>`.
- The relay proxy does not implement `GET /ofrep/v1/configuration`: the pinned specification (below) does not define it.
- Request bodies are limited to 1 MiB (`400` with `INVALID_CONTEXT`).
- `If-None-Match` on the bulk endpoint takes one tag, a comma separated list of tags (weak or strong), or `*`; a match gives `304` without a body. See [ETag](#etag-on-the-bulk-evaluation).
- An admin key is not linked to a flag set: it gets `403` on the OFREP endpoints.
- Set `server.ofrepEventStream` to advertise the flag change stream in the `eventStreams` field of the bulk response.

### ETag on the bulk evaluation

The `ETag` is a hash of the response body, so it follows what the client receives:

- The same request twice gives the same tag; `If-None-Match` with it gives `304` and no body.
- A published change to a flag the request evaluates (value, rule result for that context, disabled, added, removed) gives a new tag, and the old tag gives `200` with the new body.
- A change that does not alter the result for the request keeps the tag: a rule that only matches another user, a flag outside the requested `flagList`, another flag set, or a new generation with the same content. The client keeps its cache (`304`); nothing is re-sent for nothing.
- A tag of one context never validates a context whose result differs. Two contexts with the same result share a tag, which is correct: the bodies are the same.
- Flag sets never share a tag unless their results for the request are identical.

### Configuration parameters

The bulk operation accepts `flagConfigEtag` and `flagConfigLastModified` (query). A provider sends them only on a re-fetch that a change notification triggered, with the values of the notification (`etag`, `lastModified`).

- The relay names the configuration of a flag set in its notification: `etag` is a hash of the flags of the flag set (the same on every replica that serves the same content), `lastModified` is the time (unix seconds) the replica built that configuration.
- If the request names a configuration the replica has not loaded (another `etag`, or a `lastModified` later than its own build), the replica checks its control plane at once, shares that check with other requests, and answers with what it has afterwards. It does not wait for the fetch spreading that scheduled polls use.
- The wait is bounded to 2 seconds and ends with the request context. The answer is never an error: when the check finds nothing newer (a made-up value, a notification from a replica that is ahead), the answer is the current configuration.
- A replica starts at most one such check per second for the whole relay, however many requests carry a value it does not know. A request that arrives while a check runs waits for that check.
- A value that is not a unix time or an ISO 8601 date-time is ignored. Without the parameters nothing changes and nothing is allocated.
- `If-None-Match` still decides `304` after the check: a provider that sends its previous tag with the parameters gets the new body (`200`) from a replica that was behind, not a `304` from the old configuration.

### Change stream loop

With `server.ofrepEventStream` set, the bulk response has `eventStreams: [{"type": "sse", "url": ..., "inactivityDelaySec": ...}]`. The entry uses the `url` form (the document allows `url` or `endpoint`): browsers cannot send headers to an `EventSource`, so the URL carries the API key of the request. Treat it as secret.

1. The provider evaluates in bulk and connects to the `url`.
2. A change is published. Every relay that serves the flag set sends `{"type": "refetchEvaluation", "etag": "...", "lastModified": 1771622898}`.
3. The provider evaluates in bulk again with `flagConfigEtag`, `flagConfigLastModified` and its last `ETag` in `If-None-Match`.
4. It receives the new values (`200`) or `304` when the change does not touch its result.

The relay sends no response level `metadata` (the document makes it optional metadata of the flag set, and a flag set has none). Flag `metadata` holds the entries of the flag that are booleans, strings and numbers, the only types the document allows; an entry that holds an object, a list or null is not sent.

### Flag sets

A key reaches the flags of its flag set only, for a single and a bulk evaluation:

- A flag that exists in another flag set is `404` `FLAG_NOT_FOUND` (single) or absent from the bulk response; a flag of the same name in two flag sets returns the value of each set's own flag.
- Flags published without an API key (the public flag set) are served to requests without a key only, and a keyed request never sees them.
- An unknown or missing key is `401`; an admin key is `403`.

### JavaScript SDK examples

Server side (Node.js, `npm install @openfeature/server-sdk @openfeature/ofrep-provider`):

```javascript
import { OpenFeature } from '@openfeature/server-sdk';
import { OFREPProvider } from '@openfeature/ofrep-provider';

await OpenFeature.setProviderAndWait(
  new OFREPProvider({
    baseUrl: 'http://localhost:1031',
    headers: [['X-API-Key', 'my-api-key']], // only if API keys are configured
  })
);
const client = OpenFeature.getClient();
const enabled = await client.getBooleanValue('my-flag', false, { targetingKey: 'user-123' });
```

Browser (`npm install @openfeature/web-sdk @openfeature/ofrep-web-provider`) uses the bulk endpoint and `OpenFeature.setContext(...)`:

```javascript
import { OpenFeature } from '@openfeature/web-sdk';
import { OFREPWebProvider } from '@openfeature/ofrep-web-provider';

await OpenFeature.setContext({ targetingKey: 'user-123' });
await OpenFeature.setProviderAndWait(new OFREPWebProvider({ baseUrl: 'http://localhost:1031' }));
const enabled = OpenFeature.getClient().getBooleanValue('my-flag', false);
```

For other languages, use the official OFREP provider in the [OpenFeature ecosystem](https://openfeature.dev/ecosystem).

### curl example

```shell
curl 'http://localhost:1031/ofrep/v1/evaluate/flags/my-flag' -X POST \
  -H 'Content-Type: application/json' \
  --data-raw '{"context": {"targetingKey": "user-123"}}'
```

## Specification version and what is verified

The relay proxy follows the OFREP OpenAPI document version `0.4.0`, vendored byte for byte in `openfeature/provider_tests/ofrep-conformance/testdata/openapi.yaml`. `openapi.pin.json` next to it records the source (`open-feature/protocol`, `service/openapi.yaml`), the commit and the SHA-256; a test fails if the file differs from it. The protocol repository publishes no release tag, so the pin is a commit.

The conformance suite (`task ofrep-conformance`, also part of `go test ./...`) starts real relays and checks every response against that document:

- Both operations and every status code the relay sends (`200`, `304`, `400`, `401`, `403`, `404`), with the response schema, the `ETag` header, the `Content-Type` and the error bodies.
- All value types (boolean, string, integer, float, object, and no `value` for a disabled flag), the `reason` and `errorCode` enumerations, and `metadata` value types.
- The behaviour of the protocol, as a language neutral table (`openfeature/provider_tests/ofrep-scenarios`: the fixture flags and the expected `value`, `variant`, `reason`, `metadata` and errors of each scenario, for a single and for a bulk evaluation): every value type (arrays included), targeting rules on string, number, boolean, nested and date-time attributes, percentage rollouts (stable for a key, different across keys), default rule, disabled flag, scheduled and progressive rollouts, flag metadata, flag not found, missing and invalid targeting keys.
- Request handling: missing, null or malformed `context`, wrong content type, empty and oversized bodies, unknown fields, a flag key that needs URL escaping, other HTTP methods.
- Authentication: `X-API-Key` and `Authorization: Bearer`, with and without keys (public flag sets), and an admin key.
- `If-None-Match` (strong, weak, list, `*`) and the `flagConfigEtag` and `flagConfigLastModified` query parameters.
- The `eventStreams` field of the bulk response.
- Sequences that need time (`openfeature/provider_tests/ofrep-scenarios/sequences.json`, language neutral: steps to request, publish a new generation, connect to the stream, wait for an event): the `ETag` after published changes, a replica that is held back and a refetch with the configuration parameters, the notification loop, flag set isolation.
- The OpenAPI document that the relay publishes about itself agrees with the pinned one for these paths.

A coverage test lists every operation, status code, header, schema, parameter, security scheme and value type of the pinned document and fails when one has no case that asserted on it (a response that a case only received counts for nothing), so a new version of the document cannot be adopted with silent gaps. Two responses cannot be provoked: the relay has no rate limit, so it never answers `429` (and sends no `Retry-After`), and it answers `500` only for a fault of its own.

### Array values

The pinned document types the value of an object flag as `object` and has no array type. The relay returns a JSON array as is when a flag's variation is a list: every official OFREP provider accepts it as an object value, and `GO Feature Flag` sends arrays. The conformance validator follows the document with this one named deviation, scoped to the `value` of the object flag; a test pins the list of deviations.

### Warm-up

Before a flag set has loaded its flags, a single evaluation answers `404` with `FLAG_NOT_FOUND`, and a bulk evaluation answers `200` with no flags: the document defines no `503` for these operations.

## Evaluation context

The OFREP context maps directly to the OpenFeature evaluation context. See [evaluation context](../concepts/evaluation-context.md) for reserved keys understood by the relay proxy (such as `semaphore`, and its legacy name `gofeatureflag`).

## Not available through OFREP

- **In-process evaluation**: Language-specific in-process providers are not supported; only the Go module evaluates flags in-process.
- **Flag usage events**: The relay proxy generates flag-usage events at evaluation time; see [flag usage tracking](../tracking/flag-usage-tracking.md).
