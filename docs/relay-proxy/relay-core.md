# Relay core

> Status: this is what the `semaphore` binary runs (`cmd/relayproxy/main.go` wires `relaycore`, `corehttp` and the HTTP
> server). The version 1 configuration is not read: see [Run, reload and stop](bootstrap-config.md#run-reload-and-stop).

The core is the part of the relay that turns a [bootstrap file, version 2](bootstrap-config.md) into a running
flag runtime. It is one object with no global state.

## What it assembles

- **Connections**: one client per AWS connection, built by the connection factory with secrets resolved from the
  environment or files.
- **Control plane store**: S3 or a directory, by the type of the `controlPlane.connection`. A manifest can list
  objects of any `s3` or `file` connection of the file.
- **State directory**: opened if `controlPlane.stateDir` is set. A directory that cannot be used (a file, a
  world-writable directory, locked by another process) never stops the relay: it runs without one and the status
  says why.
- **Manifest reader**: polls the manifest with conditional requests and fetches only the objects that changed.
- **Engine and holder**: build an immutable runtime from each snapshot and swap it in with one atomic store.
- **Evaluator**: evaluates a flag of a flag set. It is the one place the relay hands tracked evaluations to the
  exporters.
- **Exporters and notifiers**: the runtime-level pipelines from the `exporters` and `notifiers` sections. A change
  of a flag set reaches every notifier.

## Start

1. `New` takes the loaded bootstrap file and the injected dependencies (logger, metrics registerer, scheduler,
   secrets, state directory file system). It builds everything, opens the state directory and starts the exporters
   and notifiers. It starts none of the loops. An error names the setting at fault, for example
   `connections.control.path`, and never holds a credential.
2. `Run` starts the reader, the source, the engine and the state directory writer, and blocks until its context ends
   or `Shutdown` is called.

The relay serves an empty runtime until the first manifest is applied. The first round of the engine also takes the
admin keys of the file.

## Reload in place

- A new manifest or changed object goes live after one poll, without a restart: the engine rebuilds only the flag
  sets that use a changed object, and swaps the runtime atomically. Added and removed flag sets take effect the same
  way, and the keys of a removed flag set are unknown from that generation.
- `Reload` takes a new bootstrap file. Only the admin keys apply live. Every other change (connections, control plane,
  exporters, notifiers, listeners, observability) is kept out and named in the `pendingRestart` list of the status.
  Values are never listed.
- If the store is unreachable the relay keeps serving the last runtime, and the reader status shows the failed poll.

### Measured

Three tests prove what a reload costs. The figures are from a commodity 8-thread Linux machine.

- Store requests: a reload that leaves the control plane unchanged reads no object. The bootstrap reload (SIGHUP) makes
  no request at all. A refresh (`POST /admin/v1/retriever/refresh`) and a scheduled poll make one conditional GET of the
  manifest, answered 304. A cold start reads the manifest and each object once (`task test-e2e`, gopherstack, counting
  transport).
- Memory: with a reader still holding the old runtime, a swap of 5000 flag sets x 100 flags holds 1.54 x the live heap
  of one generation (58 MB; the bound is 2 x). After the reader lets go it is 1.08 x. A change of one flags object
  (ten flag sets) adds 0.78 MB, 1.3% of one generation (`go test -tags=bench -run TestScaleMemoryDuringSwap
  ./cmd/relayproxy/flagruntime`).
- Latency: `task test-reload-load` sends 1000 evaluation requests per second over HTTP for 60 s to a relay proxy on a
  file store while a reload that changes the flags runs every 200 ms (299 reloads, none failed). 60,000 requests, no
  failure. p50 / p99 of the requests due during a reload: 0.58 ms / 1.18 ms (9,117 requests); outside: 0.62 ms / 1.18 ms
  (50,883 requests); ratio 1.00. The test fails on any failed request and when the p99 during reloads is more than 3 x the p99 outside and also more
  than 10 ms above it (without the race detector, which distorts the figure); CI runs the 10 s variant
  (`SEMAPHORE_RELOAD_LOAD_DURATION`).

## Shut down

`Shutdown` stops in order:

1. It cancels the reader, the source and the engine, so nothing new enters the runtime, and waits for them.
2. The state directory writes the last snapshot.
3. The exporters and notifiers drain until the deadline of the context of `Shutdown`.
4. It unregisters the collector and releases the connections and the state directory.

`Shutdown` is safe without `Run`, and a second call returns the result of the first. When the deadline passes first,
it returns an error that says so and what did not stop ends in the background. A relay must always call it, also
after `Run` returned.

## HTTP API from the core

`corehttp.New(core, config)` builds the HTTP server of the relay on a core. The handlers find the flag set of
an API key through `service.FlagSetResolver` and read it through `service.FlagSet`. The core adapter implements that
seam; the tests of the handlers implement it on engine clients.

- **Served from the core**: OFREP single and bulk evaluation, the legacy `/v1/feature/{flag}/eval` and `/v1/allflags`,
  flag configuration, flag change, the manifest, the collection of events, the SSE and websocket streams, `/health`,
  `/info` (with the state of the runtime), `/livez`, `/readyz` and `/startupz`; see [probes](probes.md).
- **Keys**: every request needs a key. A flag set key routes to its flag set, an admin key is accepted and then
  answered with 403 on the OFREP endpoints and with 400 on the others (a key that is both an admin key and a flag set key is neither: it is answered with 401 and the flag set reports `admin-key-collision`), an unknown key or the key of a removed flag set is answered with 401.
- **Unavailable flag set** (known, never loaded): a single evaluation answers `FLAG_NOT_FOUND`, a bulk evaluation
  answers 200 with no flag.
- **Events**: an evaluation of a flag that tracks its events and an event sent to the data collector reach the
  exporters of the core. When the queue of the exporters is full, the collector answers 200 with the number of events
  it accepted in `ingestedContentCount` (an event that fails conversion or that the queue refuses is not counted); the v1 answer, the number of submitted events, is unchanged.
- **Changes**: pass `Streams.Broadcast` as the `Broadcast` of the dependencies of the core, and give the same `Streams`
  to `corehttp.New` (it is required, the caller closes it with `Streams.Close(ctx)` after the server stopped).
  `Broadcast` returns at once: each of the SSE and websocket services has its own worker and queue, so a client that
  stopped reading delays neither the core nor the other service. Changes of one flag set that wait together are
  merged (the SSE event only tells the client to fetch again, the websocket message carries the diff, merged to its net
  effect); when more than 1024 flag sets wait, the oldest is dropped and counted. `Close` is bounded by its context.
  The end to end test follows an SSE client.
- **Flag change**: the hashes of a flag set are computed once per flag set and reused by every poll.
- **Refresh**: `/admin/v1/retriever/refresh` (admin key) polls the control plane now through the loop that polls on
  schedule, shares one poll between the calls that arrive together, and answers when the engine has decided the
  generation the poll found: `applied` says that the relay serves it, `rejection` gives the code when the engine
  refused it and kept the last known good.
- **Read only**: a result of an evaluation and the flags of a flag set are shared between requests. The handlers
  serialize them and never change them.

### Not yet in the v2 core

- Tracking events (`kind: tracking`) reach no exporter: they are accepted and dropped.
- A change of the bootstrap file is read on `SIGHUP`, not watched.

The binary builds `corehttp.NewReadiness(core)` and calls `BeginShutdown` on SIGTERM before the streams close and the
server stops. The evaluation enrichment is the `evaluation` section of the bootstrap file. A golden test serves
flags from a core and compares the status, the contract headers and the body of every evaluation endpoint with the files
in `corehttp/testdata/golden`, which record the answers of the first version of the relay.

## Tests

- Unit tests: path-addressed construction errors without credentials, the lifecycle (no goroutine left after
  shutdown, shutdown without run, twice, bounded by its context), and the wiring (a tracked evaluation reaches the
  exporter, a manifest change reaches the notifier, an unusable state directory still serves).
- HTTP: v1 and v2 answer the same requests (OFREP, legacy, flag configuration, flag change, key variants, targeting,
  buckets, scheduled flags), a handler never changes a shared result, and the allocations of the authentication and of
  one evaluation on the core are not above v1.
- End to end, driven by a test scheduler instead of timers: a publisher writes objects and manifests to a directory
  (always runs) or to a gopherstack bucket (`task test-e2e`), and the test checks a change, a new flag set, a removed
  flag set and a store outage.
