# Control-plane file store

> Status: the relay serves the file store (`FileStore` in `internal/manifestreader`) when the control plane connection is of
> `type: file`. The other retriever kinds come back with issue #57.

The [manifest reader](control-plane-polling.md) reads the manifest and its objects through a small store interface.
The file store serves them from a directory of the machine instead of S3.

## When to use it

- Development and tests: a directory with a manifest and its objects, no emulator needed.
- A mounted volume: a Kubernetes ConfigMap or Secret mount, an NFS or CSI volume that another process fills.
- Not for production fleets that already have a bucket: the S3 store is the one that spreads load and is built for it.

## Layout

Keys are relative slash paths under one root directory. The manifest key and every object `key` of the manifest are
read as `<root>/<key>`:

```text
root/
  manifests/stable.yaml
  objects/sha256/ab/ab12....json
```

`semaphore-cli publish` writes a file store (see [publish](../tooling/publish.md#file-store)): the objects first and the manifest last, as for S3. A publisher of your own writes each file atomically (write a
temporary file in the same directory, then rename it over the final name). The reader survives a non-atomic writer
(see [Partial writes](#partial-writes)), but an atomic one costs nothing.

## Keys and containment

A key is refused (reason `denied`, not retryable) when it is empty, starts with `/`, ends with `/`, has a `.`, `..`
or empty element (`a//b`), or contains a backslash or a NUL byte. `..data/manifest.yaml` is a valid key: only a whole
element `..` is refused.

Every path is resolved with `os.Root`, so it cannot leave the root:

| Case | Result |
|---|---|
| A symbolic link inside the root that points to a place inside the root (a file, a directory, a chain, `../sibling` that stays inside) | Followed |
| A symbolic link that points outside the root, directly or through a directory link | Refused: `denied` |
| An absolute symbolic link, even one that points back inside the root | Refused: `denied` |
| A symbolic link loop | Refused: `denied` |
| A dangling link, or a file used as a directory (`a.txt/child`) | Missing |

A link inside that stays inside is followed because a ConfigMap mount is a tree of such links.

Hard links are not symbolic links and cannot be told apart: a hard link inside the root to a file outside it is
an ordinary file of the root and is read. The store cannot prevent that. Whoever can create files in the root is
trusted: mount the directory read-only into the relay, and give write access to the control plane only.
A path that crosses a mount point inside the root is followed, too.

## What is served

Only regular files. A directory, device, socket or FIFO is refused without being read (`denied`, cause
`ErrNotRegular`). The file is opened without blocking, so a FIFO cannot hang a poll, and the type is read from the
open file, not from an earlier lookup. `Limit` is enforced from the size the file has when it is opened (too large:
no read at all) and again while reading: the store never reads more than `limit + 1` bytes, so a file that grows
during the read is still refused.

| Situation | Result |
|---|---|
| File does not exist (also a missing directory on the path, a dangling link, a file used as a directory) | Missing: the same result as an S3 `404` |
| Permission denied | `denied`, not retryable |
| Path leaves the root, key refused (also a name that is too long), not a regular file (also `ENXIO`, `ENODEV`), symbolic link loop | `denied`, not retryable |
| Larger than the limit | Too large |
| `Limit` of zero or below | Only an empty file is returned; any other is too large |
| `Limit` below the size and a current ETag | Too large (the size is looked at first; the S3 store answers "not modified" first; both are allowed, neither returns a body) |
| Any other I/O error (`EIO`, too many open files, a stale NFS handle, out of memory) | `error`, retryable |
| The root directory cannot be opened: missing, not a directory, out of descriptors, I/O error | `error`, retryable, the cause includes `ErrRootUnavailable` |
| The root directory cannot be opened: permission denied | `denied`, not retryable |
| `VersionID` set | Missing: a file has no versions. Manifests that list a `versionId` for a file connection are not served |
| Context ended | `error`, retryable, the cause is the context error |

`NewFileStore` itself refuses a root that is not a directory when it is built (a configuration error at start).
After that, a root that goes away (an unmounted or remounted volume, a directory that is swapped, even one replaced
by a FIFO, which is never opened) is retryable on purpose: the volume must recover with the normal backoff, without a
restart of the relay.

The root directory is opened on every call, so a root that is replaced (a remounted volume) is followed, and one that
disappears is a retryable error until it is back.

## Change detection and its exact guarantees

The ETag of an object is the quoted sha256 of its bytes: `"<64 hex digits>"`. A conditional read (the reader sends the
ETag it holds) is answered like this:

1. Open the file and check its type and size.
2. Hash the whole file in 32 KiB chunks. The bytes are not kept.
3. The same digest: answer "not modified" (no body). A different digest: read the file again into one buffer and
   answer with the bytes and the ETag of **those bytes**.

Guarantees:

- **"Not modified" is never wrong.** It is answered only after a digest of the whole current content equals the ETag
  the reader holds. The size, the modification time and the inode of the file are not used for that decision.
- **A change is seen at the next poll.** A rewrite in place that keeps the size, the modification time and the inode
  still changes the digest. This is tested.
- **The ETag always describes the bytes returned with it.** A file replaced, rewritten or cut in place between the two
  passes gives the bytes the read pass saw with the ETag of exactly those bytes (tested with a writer that rewrites
  the file in place while readers read it, and with a change injected between the two passes). A torn read has a
  hash of its own; the reader's checksum check, or the failure to decode the manifest, then rejects it.
- **The ETag is the same on every machine and after a restart**, so the one saved in the state directory is valid for the next
  process.

Why not a stat-based validator (size + modification time + inode), however it is dressed up:

- A timestamp has a granularity. The kernel stamps a write with a coarse clock (a jiffy, a few milliseconds), and
  other file systems are coarser (FAT: two seconds; some network file systems: one second). Two writes of the same
  size inside one tick carry the same modification time.
- The "racy timestamp" problem: a file written in the same tick as the moment the validator was taken is clean by
  its stat and may still change in that tick without any stat changing. A stat validator has to hash such files
  anyway (the window is as wide as the granularity), so it needs a trustworthy clock and the hash path as well.
- Network file systems (NFS and others) cache attributes for seconds and can serve stale sizes and times, and the
  client's clock is not the server's.
- Tools that preserve times (`rsync -t`, `cp -p`, `tar`) and in-place rewrites keep the modification time and the
  inode while the content changes. A validator that includes the change time (`ctime`) is not fooled by that, but
  it is still subject to the granularity, the racy window and attribute caching above, and it changes on a
  `chmod` or a hard link without any change of content (a false "changed" costs only a read, which is safe).

A wrong "not modified" on the manifest would hide a new generation, so the store hashes. A stat is used only for what
cannot be wrong: the type and the size limit before any read. The tests rewrite a file in place with the same size,
the same modification time and the same inode, and the store sees it.

Cost of the hash (commodity 4-core / 8-thread Linux machine, files in the page cache, one core; the ranges are
repeated runs, the upper values on a busy machine):

| File size | Unchanged poll | Changed poll | Floor: `stat` only |
|---|---|---|---|
| 1 KiB | 14 us | 15 us | 0.6 us |
| 100 KiB | 0.2 ms | 0.45 ms | 0.6 us |
| 4 MiB | 8 ms | 19 ms | 1 us |
| 16 MiB (the manifest limit) | 44 to 60 ms | 85 to 110 ms | 1 us |

An unchanged poll allocates a fixed 32 KiB whatever the size; a changed one allocates the file once. At the default
60 s interval a 100 KiB manifest costs about 0.0004 % of one core, a 16 MiB one about 0.1 %. Objects are not read
on a quiet poll at all. Run the benchmark with `go test -tags bench -run '^$' -bench FileStore_Poll
./internal/manifestreader`.

## Partial writes

A writer that is not atomic can be caught in the middle by a poll:

- A half-written **object** has another size or other bytes than the manifest lists: the reader refuses it
  (`size-mismatch` or `checksum-mismatch`), serves the last good content of that name, and reads the file again at
  the next poll. The other objects are not affected.
- A half-written **manifest** does not decode (or lists a generation it cannot apply): the reader rejects it and keeps
  the previous one. The next poll reads the finished file, which has another ETag.

A manifest that is cut at a place where it still decodes cannot be told from a complete one: write the manifest atomically.

## ConfigMap and Secret mounts

Kubernetes mounts a ConfigMap as a directory tree of links:

```text
mount/
  ..2024_05_01_10_00_00.123/manifest.yaml     real files
  ..data -> ..2024_05_01_10_00_00.123          swapped atomically on every update
  manifest.yaml -> ..data/manifest.yaml
```

An update writes a new timestamped directory, renames a new `..data` link over the old one, and removes the old
directory. The store follows these links (all relative and inside the mount), so `manifest.yaml` resolves to the files of
the current directory. The swap is atomic: a read sees the old directory or the new one. A read that started on the old
directory just before it was removed can find the file missing; that poll is a failed poll and the next one succeeds.
Remember that a mounted ConfigMap is updated after a delay that depends on the kubelet synchronization period and on
the propagation of its cache (there is no fixed bound), that a ConfigMap is limited to
1 MiB, and that a `subPath` mount never updates: mount the whole directory.

## Request budget

The equivalent of the S3 budget, per replica and per poll interval:

| Situation | File operations |
|---|---|
| Nothing changed (the normal case) | 1 open and read of the manifest file. Nothing else: no directory listing, no object file touched |
| A new manifest that changes `K` objects | The manifest read twice (hash, then read) plus `K` object reads (one pass each, they have no ETag) |
| Failing objects | As in [control-plane polling](control-plane-polling.md#request-budget): one read for each failing object that is due |

The counts do not depend on the number of objects, flag sets or API keys. The cost of a read is the cost of hashing the file
(see above), not of a network round trip, and a local read is not billed.

## Limits and platforms

- A read cannot be cancelled inside a single system call: the context is looked at between reads of 32 KiB, so a
  cancel or a deadline stops a read of a large file after the chunk in progress (unit-tested with a context that
  ends during the hashing pass and during the read pass). A network file system that hangs inside a read hangs that
  poll until the call returns.
- Linux, macOS and the BSDs are the supported platforms (a socket is refused on all of them: the open fails with
  `ENXIO` on Linux and `EOPNOTSUPP` on macOS and the BSDs). The code builds for Windows, where `os.Root` rules apply:
  names such as `NUL` and `COM1` are refused (as a generic error, never read), symbolic links need a privilege to
  create, and a file that is open for writing may be locked against reading. Permission denial by mode bits does not
  exist on Windows. Plan 9 and `js` are not supported: `os.Root` does not track directories there, so containment
  is not guaranteed.
- A process that runs as root (or on a file system without permission bits) reads a file whatever its mode. The
  `denied` of the tests does not rely on modes: it is a link that leaves the root.
- `os.Root` does not stop a path from crossing a mount point or reaching a device inside the root; the store
  refuses the device itself (not a regular file) but follows a mount point, and reads hard links (see above).
- An object `a` and an object `a/b` cannot exist together: a path is a file or a directory. S3 keys are not.
  Publish files under names that do not clash (the content-addressed keys of the publisher never do).

## Writing another store

Every `ObjectStore` has to pass the conformance suite in `internal/manifestreader/storetest`. Give `storetest.Run` the
capabilities the store does not offer (the waivers, with the reason) and a factory that returns a fresh store with a
backend handle:

```go
func TestConformance_MyStore(t *testing.T) {
	t.Parallel()

	waivers := storetest.Waivers{
		storetest.CapHang: "why this backend cannot hold a read",
	}

	report := storetest.Run(t, "mystore", waivers, func(t *testing.T) storetest.Fixture {
		store, backend := newStoreAndBackend(t) // backend: Put, Overwrite, Delete, Fail, Hang, HangBody, PutVersion
		return storetest.Fixture{Store: store, Backend: backend, Unsupported: waivers}
	})

	assert.Equal(t, wantUnsupportedCases, report.Unsupported)
}
```

- A case that needs a capability the backend cannot arrange is reported as unsupported, with the reason, and logged.
  `Run` fails when the fixture declares other waivers than the test passed, so a store cannot drop a guarantee
  without its test saying so, and every waiver needs a reason. There is no silent skip.
- Capabilities: `deny`, `transient`, `throttle`, `hang` (a read that does not answer), `hang-body` (an answer whose
  body stalls), `versions`, `key-validation` (unclean keys are refused), `content-addressed-etag`, `nested-keys`, `reserved-name-chars` (keys with a question mark or a colon; the file
  store waives it on Windows, where file names cannot contain them).
- The cases are in the table below. A case with a capability in the `Needs` column runs only for a store that does
  not waive that capability; the others always run. A test compares this table with `storetest.CaseNames()` and
  `storetest.CaseNeeds`, so the table cannot drift from the code.

| Case | Needs | What it checks |
|---|---|---|
| `get-unconditional` | - | the bytes and an ETag of an object |
| `conditional-unchanged-is-not-modified` | - | the current ETag is answered with `ErrNotModified` and an empty object |
| `conditional-changed-returns-new-body-and-etag` | - | a changed object returns the new bytes and a new ETag |
| `unknown-etag-returns-the-object` | - | an ETag the store never issued returns the object |
| `missing-is-not-found` | - | never written, deleted, a deleted object with an ETag, a missing directory |
| `missing-version-is-not-found` | - | a version that does not exist is not found |
| `version-id-reads-an-older-version` | `versions` | an older version is read after an overwrite |
| `denied-is-not-retryable` | `deny` | denied, not retryable, and recovery |
| `transient-is-retryable` | `transient` | an error that may pass is retryable, and recovery |
| `throttled-is-retryable-and-throttled` | `throttle` | a throttle is retryable and flagged, and recovery |
| `limit-at-and-one-over` | - | a limit at the size, one under, one over and half |
| `limit-zero-and-negative` | - | zero and negative limits admit the empty object only |
| `conditional-over-limit-never-returns-a-body` | - | over the limit with the current ETag: not modified or too large, never a body |
| `empty-object` | - | the empty object, its ETag, at limit zero |
| `large-object-near-the-limit` | - | a 3 MiB object at the limit and one under |
| `key-characters` | - | `#`, `%`, `%2F`, a space and a plus, a leading dot, unicode, `%41` against `A`: distinct objects |
| `key-characters-reserved-by-some-file-systems` | `reserved-name-chars` | a question mark and a colon in a key |
| `odd-keys-never-return-content` | - | empty, absolute and escaping keys never return an object |
| `unclean-keys-are-refused` | `key-validation` | unclean keys are denied, not retryable (also a name that is too long) |
| `object-and-child-keys-coexist` | `nested-keys` | an object `a` and an object `a/b` together |
| `etag-is-stable-for-unchanged-content` | - | the same ETag on every read of unchanged content |
| `etag-follows-content` | - | for generated contents, including same-size in-place rewrites, the ETag changes and is current for the new bytes |
| `etag-is-content-addressed` | `content-addressed-etag` | the same bytes always give the same ETag |
| `etag-describes-the-bytes-returned` | - | a read that starts after a write sees it; under concurrent writes one ETag never comes with two contents |
| `returned-bytes-are-not-aliased` | - | a later write does not change bytes already returned |
| `ended-context-is-an-error` | - | a cancelled or expired context is an error that wraps the context error, with no object |
| `cancel-while-reading` | `hang` | a cancel ends a read that waits: `context.Canceled`, no object, nothing left |
| `deadline-while-reading` | `hang` | a deadline ends a read that waits: `context.DeadlineExceeded`, no object. Done is authoritative: a store that copies `Deadline()` into a one-shot I/O deadline must also watch Done |
| `cancel-while-reading-the-body` | `hang-body` | a cancel while the body is read, then the store reads on |
| `cancel-leaves-nothing-behind` | `hang` | many cancelled reads leave no request, and the store is not poisoned |
| `gets-leave-no-goroutine-behind` | - | reads of every outcome leave no goroutine |
| `concurrent-gets` | - | 16 readers, conditional and not |

- Every case is also checked for leaked goroutines: the goroutines of a case carry a pprof label, and when the case
  is over the goroutine profile is read (with a bounded number of yields, no sleeps) and any goroutine of the case
  that is still there fails it. The loops of an idle HTTP connection are not counted; a connection whose response body
  was left open is. The check runs after the cleanup of the fixture, so a worker the fixture stops is not a leak.
  The run as a whole is checked the same way. Limit: only goroutines started by a goroutine of the case carry its
  labels; a worker that the store started before the case is not attributed to it, and the fixture of such a store
  has to check its workers itself.
- `storetest.Check(name, fixture)` runs one case with a recording test and returns its failures: the tests of this
  package use it on stores with one defect each (a stale ETag, a constant ETag, an ignored limit, a body with
  "not modified", an ignored context, a truncated body, a leaked goroutine, an aliased slice, and others) to show
  that the suite fails for each.
- - The suite assumes only what the contract says. It accepts any ETag scheme (hashes, row versions, per-key generation
  numbers), the empty object returned as nil or empty bytes, a backend that admits one read at a time, a store that
  answers a current ETag before it looks at the size, and a store that lets an abandoned request finish after its
  context ended. `Check` is used on stores of exactly these kinds in the tests of the suite, which must pass them.
- The suite runs today against the in-memory fake, the file store and the S3 store (the last one with the `docker`
  build tag, on the emulator).

What a store must do when the context ends: end the read when `Done` closes, with an error that wraps
`context.Canceled` or `context.DeadlineExceeded`, and return no object. `Deadline()` may be used to set a deadline on the
I/O the store starts, but only as an optimisation: a store that copies it into a one-shot deadline must still watch
`Done`. The suite signals a deadline the way a context does (Done closes, Err is `DeadlineExceeded`, Deadline reports a
deadline that has passed) without a timer, so a store that watches only a one-shot copy of `Deadline()` fails it.

What the reader needs of an ETag, and nothing more:

- It describes exactly the bytes returned with it.
- It differs when the bytes differ.
- A conditional read that holds the current ETag of an unchanged object is answered with `ErrNotModified` and an
  empty `Object`.

Equal bytes need not give an equal ETag: a row version, a commit SHA or a generation number is a valid ETag, and
such a store waives `content-addressed-etag`. The file store, the S3 store and the fake have it.
