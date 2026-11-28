# Pinned OFREP OpenAPI document

- `openapi.yaml`: the published document, byte for byte, with no local edit.
- `openapi.pin.json`: where it comes from (repository, path, commit, version) and its SHA-256.
- The repository publishes no git tag or release; the pin is the commit that last changed `service/openapi.yaml`
  and that was the head of the default branch when it was pinned. `tag` is empty for that reason.
- A test fails when `openapi.yaml` does not match the recorded hash.

To adopt a new version: download the file from the new commit, update the pin file, then add the conformance
cases the coverage test asks for.

## Rules the validator adds to the document

The document leaves two things open that the validator closes with the intent of the specification
([`validateValueOneOf`](../validator.go)):

- `codeDefaultFlag` matches only when `value` is absent (so `"value": null` fails).
- A typed flag matches only when `value` is present. A whole JSON number matches both `integerFlag` and `floatFlag`
  and is accepted as an unambiguous numeric match.
