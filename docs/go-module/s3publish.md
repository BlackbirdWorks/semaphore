# Publish to S3 (`s3publish`)

`github.com/blackbirdworks/semaphore/controlplane/s3publish` is the upload protocol of the control plane. A control plane
service imports it next to [`controlplane`](controlplane.md) and gets "objects first, manifest last" with conditional
writes. [`semaphore-cli publish`](../tooling/publish.md) is built on it.

- It imports no `internal` package. It needs an `API`: the three calls `GetObject`, `HeadObject` and `PutObject`, which a
  `*s3.Client` of the AWS SDK for Go v2 implements.
- It never sends a `LIST` or a `DELETE`.

## Example

```go
client := s3.NewFromConfig(awsCfg) // your client, region, endpoint and credentials

pub, err := s3publish.New(client, &s3publish.Config{
    Bucket:     "acme-control",
    Channel:    "stable",
    Connection: "control", // the connection name of the relay's bootstrap file
    // ManifestKey defaults to manifests/stable.yaml; Prefix goes before every key.
})
if err != nil {
    return err
}

res, err := pub.Publish(ctx, s3publish.Request{
    Objects: []s3publish.Object{
        {Name: "checkout-flags", Kind: controlplane.KindFlags, Format: controlplane.FormatYAML, Body: flagsYAML},
        {Name: "flagsets", Kind: controlplane.KindFlagSets, Format: controlplane.FormatYAML, Body: setsYAML},
    },
})
switch {
case errors.Is(err, s3publish.ErrConflict):
    // another publisher won; nothing visible changed. Read again and decide.
    return err
case errors.Is(err, s3publish.ErrInvalid):
    // nothing was written; the error wraps the controlplane error.
    return err
case err != nil:
    // a store call failed (*s3publish.OperationError); repeating is safe.
    return err
}

// res is nil whenever err is not nil: use it only here.
fmt.Println(res.Generation, res.Uploaded, res.Present, res.Written)
```

## API

| Name | Meaning |
|---|---|
| `New(api API, cfg *Config) (*Publisher, error)` | No S3 call. `Bucket`, `Channel` and `Connection` are required. |
| `(*Publisher).Publish(ctx, Request) (*Result, error)` | Validate, upload absent objects, write the manifest last. |
| `(*Publisher).Verify(ctx) (*Report, error)` | Read the manifest and every object of the connection; per-object problems in the report. |
| `Request.DryRun` | The same reads and no write. |
| `Request.Generation` | Zero: the generation in the bucket plus one. An explicit value must be higher. |
| `Request.ReplaceInvalidManifest` | Replace a manifest in the bucket that is unreadable, invalid or over the limits. Needs an explicit `Generation`; still `If-Match`. |
| `Config.Check` | `func(*Object) error`, run for every object before any call, and by `Verify` for every object it read: the hook for validation beyond `controlplane`, such as the engine (see below). |
| `Config.MaxObjectBytes`, `MaxTotalBytes`, `MaxManifestBytes`, `MaxObjects` | The limits of the reader; zero means its defaults (16 MiB, 1 GiB, 16 MiB, 20000). |
| `Config.Concurrency` | Object calls at a time; zero means 8. |
| `Object.Source` | Where a document came from (a path), used only in error messages. |

Errors (test with `errors.Is` and `errors.As`): `ErrInvalid` (wraps the `controlplane` error), `ErrConfig`, `ErrConflict`,
`ErrGeneration`, `ErrChannelMismatch`, `ErrPreviousManifest`, `ErrObjectMismatch`, `ErrNoManifest` and `*OperationError`
(operation, key and the client error). A missing bucket (`NoSuchBucket`, any 404 that is not `NoSuchKey` or `NotFound`) is an
`*OperationError`, never "no manifest" and never a conflict.

## Keys and the reader

A manifest key that is also the key of one of the objects (its key is decided by its content) is refused (`ErrManifestKey`): the
manifest would overwrite the object it lists.

The keys written in the manifest are absolute keys of the bucket: `Config.Prefix` is put before them by the publisher and the
reader must not add a prefix. `Config.Connection` must be the control-plane connection name of the relay.

## Limits

Before any call the publisher refuses what the reader rejects, with `ErrInvalid` naming the object (never its content):
an object of 0 bytes or over `MaxObjectBytes`, the distinct objects over `MaxTotalBytes`, a marshalled manifest over
`MaxManifestBytes`, more than `MaxObjects` entries. `Verify` applies the same limits. The defaults are exported by
`controlplane` (`DefaultMaxObjectBytes`, `DefaultMaxTotalBytes`, `DefaultMaxManifestBytes`, `DefaultMaxObjects`).

## Publishing to a directory

`API` is the three calls of an S3 client. For a file store, `controlplane/filepublish` has a `Store` that implements it over
a directory, so the same `Publisher` and the same protocol write a `file` connection: `filepublish.New(dir)` and
`s3publish.New(store, &cfg)` (any non-empty `Bucket`). The write rules (rename, lock, permissions) are in the
[publish page](../tooling/publish.md#file-store).

## Checking flag files with the engine

`s3publish` imports only `controlplane` and the AWS SDK, on purpose. To also run the engine of the relay on every flag file,
pass the `controlplane/flagcheck` package through the hook:

```go
cfg.Check = func(obj *s3publish.Object) error {
    if obj.Kind != controlplane.KindFlags {
        return nil
    }

    return flagcheck.Check(obj.Body, obj.Format) // the loader and IsValid the relay uses; names the flag
}
```

`flagcheck` imports the engine (heavy): only importers of `flagcheck` pull it. A refusal is `ErrInvalid` and writes nothing. `Verify`
runs the same hook, so a stored flag file that the relay would reject is `invalid-content` even when its size and checksum match.
A flag file that a manifest lists with `format: toml` is not read by the format package: `Verify` accepts it as far as the
structure goes and leaves it to the hook (`flagcheck` reads TOML).

A generation of `math.MaxUint64` in the bucket has no successor: a changed publication is refused with `ErrGeneration`.

## Protocol

1. Validate the request with `controlplane.ValidateManifest`, `UnmarshalFlags` and `ValidateFlags`, `UnmarshalFlagSet` and
   `ValidateFlagSet` (names, kinds, formats, source references, duplicate keys, object limit). No call is made when it fails.
   Flag-set documents are validated together (`controlplane.ValidateFlagSetDocuments`): duplicate flag-set names and API keys
   across documents, unresolved sources.
2. `GetObject` the manifest: its bytes and ETag. Invalid, over the limits or of another channel: an error, never an overwrite
   (unless `ReplaceInvalidManifest` with an explicit generation).
3. Objects sorted by name get `Prefix + controlplane.ObjectKey(checksum, format)`. Same documents as the manifest in the bucket
   and no higher explicit generation: no manifest is written, but step 4 still runs, so a missing listed object is uploaded again.
4. Per distinct key, at most `Concurrency` at a time: `HeadObject` (checksum mode on). Present with the expected size and the
   expected sha256: `StatusPresent`. When the store reports no sha256, or a composite one, the object is read and hashed; a
   mismatch is `ErrObjectMismatch`. Absent: `PutObject` with `If-None-Match: *` and `ChecksumSHA256`. A `412` there means another
   writer created it: the key is checked again and accepted if it matches. The first error cancels the rest.
5. `PutObject` the manifest, alone and last: `If-Match: <ETag>`, or `If-None-Match: *` when there was none. `412`,
   `409 ConditionalRequestConflict` and `404 NoSuchKey` (manifest deleted) trigger one `GetObject` of the manifest: if it is
   the whole manifest we tried to write (same channel, generation, entries and `createdAt`), an earlier attempt of ours
   committed and the result is success with its ETag; otherwise `ErrConflict`.

The manifest is YAML, or JSON when the manifest key ends in `.json`.

## Guarantees

- A manifest in the bucket never lists an object that is not there.
- Of two publishers that write different manifests over the same one, exactly one wins and the other gets `ErrConflict`.
  Two publishers that write the identical whole manifest both succeed: the second finds its own manifest in the bucket
  (the write is idempotent).
- The generation strictly increases.
- After `ErrConflict` the caller decides; the library does not retry. With an automatic generation (`Generation` zero) a
  repeat reads the manifest of the winner and publishes over it with the next generation: the last writer wins, by
  intent. With an explicit generation the repeat fails until the generation is higher than the winner's. The CLI exits 3
  and does not retry on its own.
- Failures of the store: a transient one (a timeout, a 5xx) is safe to repeat, and the repeat converges, because nothing
  visible changed before the manifest and objects are content-addressed. A permanent one (access denied, a missing
  bucket) fails the same way again: fix the cause first.
- Old objects are not deleted: garbage collection is a later change. Objects left by an interrupted publish are harmless.
- The IAM identity needs `s3:GetObject` (also for `HeadObject`) and `s3:PutObject`; `Verify` of a manifest that pins a
  `versionId` also needs `s3:GetObjectVersion`. Without `s3:ListBucket` S3 answers a missing key with 403 instead of 404
  (documented S3 behaviour): the publisher cannot tell it from a real denial, so it returns an `*OperationError` whose text
  names the cause, and never takes the object for present or the manifest for absent. A manifest that `Verify` cannot read (denied or failed) is that `*OperationError` (CLI exit code 4), not a report; only an object that it cannot read is a `unreadable` problem in the report.

## Verify

Like the relay, `Verify` fails some entries alone and rejects the manifest for the others (a test pins this table):

| Problem of an entry | Classification |
|---|---|
| size 0 | `invalid-entry` of that entry; the others are checked |
| size over the object limit (16 MiB by default) | `invalid-entry` of that entry; the others are checked |
| key with a leading slash | `invalid-entry` of that entry; the others are checked |
| negative size | `manifest-invalid`: nothing is read |
| empty key | `manifest-invalid`: nothing is read |
| bad checksum, bad name, unknown kind or format, any other rule of `ValidateManifest` | `manifest-invalid`: nothing is read |
| not parseable, wrong header, too many entries, distinct objects over the total limit | `manifest-invalid`: nothing is read |

In the first three rows the entry is a `ProblemInvalidEntry`, it is not read, and the other entries are still checked; in the
others the report holds one `ProblemManifestInvalid` and nothing is read. A publication refuses to write such
an entry, but a manifest in the bucket that lists one is a valid manifest to publish over (no `ReplaceInvalidManifest`).

`Verify` reads the manifest and each object of `Config.Connection` (the pinned `versionId` of an entry, when it has one; an absent
version is `missing`) and reports `Problem{Object, Key, Code, Detail}`:
`missing`, `unreadable`, `invalid-entry`, `size-mismatch`, `checksum-mismatch`, `invalid-content`, `manifest-invalid`, or the `controlplane`
code of a validation problem such as `unresolved-source`. Objects of other connections are in `Skipped`. The `Detail` this package writes never holds
document content, so no API key. The text of an error returned by a `Config.Check` hook is the hook's own: it is reported as
given, except that it is made single-line (control characters and newlines become spaces) and, when longer, cut so that the result including a closing "…" is at most `MaxHookDetail` (200)
characters. The messages of `controlplane/flagcheck` are made of facts that do not depend on the content of the file: the flag key (a name, made single-line and cut to 80 characters), the line and column or byte the decoder reports, the kind of value found and the type expected, and the category of the engine's validation (a fixed phrase such as `invalid query` or `all variations should have the same type`). They never repeat the text of the decoder or of the engine, which can quote a value or a query. The original errors stay reachable (`errors.Unwrap` for a decode failure, `FlagProblem.Err` for the engine's reason, `FlagProblem.Message()` for the safe text): they may quote the file, so do not log them where an API key matters.

`Problems` is in manifest order (the entries are sorted by name), cross-document problems included, each at the position of
its entry. A problem that belongs to no entry (a channel that is not the expected one, or a failure across documents that
names none) comes last. `Attempted` counts the objects for which a read was attempted; `Checked` counts the objects whose
bytes were read and compared with the listed size and checksum, so an object that is missing or unreadable is in `Attempted`
only. The CLI line "checked N objects" prints `Checked`. A hook that rejects a document does not stop the checks across
documents: the document still takes part in them. `ErrNoManifest` and `*OperationError` are returned as errors.
