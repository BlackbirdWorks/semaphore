# Getting started with a file store

This guide is [getting started](getting-started.md) without Docker and without an S3 emulator. The control plane store
is a directory on your machine: you write a version 2 [bootstrap file](bootstrap-config.md), check it, publish flags to
the directory, start the relay, evaluate a flag, change the flag, publish again, and see the new value without a
restart.

Every command and file on this page is run by a test of the repository (`internal/docstest`), in the order shown. Run
each block in the same shell and the same directory.

You need the binaries `semaphore` (the relay) and `semaphore-cli` on your `PATH`. Build them with
`task build-relayproxy build-cli` (they land in `out/bin/`), or see [installation](install.md). `publish` writes to a
directory on Linux, macOS and Windows.

## 1. Write the bootstrap file

The file names one connection, `control`, of type `file`: the directory `./store`. It says that the control plane is the
manifest `manifests/stable.yaml` of that connection. The relay polls it every 2 seconds. The directory must exist.

<!-- docs-test: run -->
```shell
mkdir store
cat > bootstrap.yaml <<'YAML'
version: 2
connections:
  control:
    type: file
    path: ./store
controlPlane:
  connection: control
  manifest: {key: manifests/stable.yaml, channel: stable, pollInterval: 2s}
YAML
```

Check it with the loader of the relay. It reports the path and the line of every error.

<!-- docs-test: run -->
```shell
semaphore-cli config validate bootstrap.yaml
```

<!-- docs-test: output -->
```text
bootstrap.yaml: valid
```

## 2. Write a flag and a flag set

One flag file (`flags/`) and one flag-set document (`flagsets/`). The flag set `web` serves the flags of the file
`checkout` to the requests that carry its API key.

<!-- docs-test: run -->
```shell
mkdir -p flags-repo/flags flags-repo/flagsets
cat > flags-repo/flags/checkout.yaml <<'YAML'
new-checkout:
  variations:
    enabled: true
    disabled: false
  defaultRule:
    variation: enabled
YAML
cat > flags-repo/flagsets/web.yaml <<'YAML'
apiVersion: semaphore/v1
kind: FlagSet
flagSets:
  - name: web
    sources: [checkout]
    apiKeys: [8d3b5f0e-6a4f-4c8e-9c3e-2f2f5d1a7b10]
YAML
```

## 3. Publish

`publish` validates the documents, writes the objects, and writes the manifest last. Every file is written aside and
renamed, so the relay never reads a half-written file. The first line of its output says which generation it wrote.

<!-- docs-test: run -->
```shell
semaphore-cli publish ./flags-repo --config bootstrap.yaml | sed -n 1p
```

<!-- docs-test: output -->
```text
published generation 1 (was 0) to manifests/stable.yaml, channel stable
```

See [publish](../tooling/publish.md) for the flags and the exit codes.

## 4. Start the relay

The relay runs in the background here, and writes its log to `relay.log`. It listens on port 1041 so that it can run next to the [S3 variant](getting-started.md). It is ready when `/readyz` answers 200.

<!-- docs-test: run -->
```shell
semaphore --config bootstrap.yaml --listen 127.0.0.1:1041 > relay.log 2>&1 &
echo $! > relay.pid
until curl -sf http://127.0.0.1:1041/readyz > /dev/null; do sleep 1; done
```

## 5. Evaluate a flag

The API key of the flag set goes in the `X-API-Key` header. The call is an [OFREP](../openfeature/ofrep.md) evaluation.

<!-- docs-test: run -->
```shell
curl -s http://127.0.0.1:1041/ofrep/v1/evaluate/flags/new-checkout -X POST \
  -H 'Content-Type: application/json' -H 'X-API-Key: 8d3b5f0e-6a4f-4c8e-9c3e-2f2f5d1a7b10' \
  -d '{"context": {"targetingKey": "user-123"}}' | grep -o '"value":[a-z]*'
```

<!-- docs-test: output -->
```text
"value":true
```

## 6. Change the flag and publish again

Make `disabled` the default, publish generation 2, and wait until the relay serves it. The relay polls the manifest, so
the change is live within a few seconds. Nothing restarts.

<!-- docs-test: run -->
```shell
sed -i 's/variation: enabled/variation: disabled/' flags-repo/flags/checkout.yaml
semaphore-cli publish ./flags-repo --config bootstrap.yaml | sed -n 1p
for i in $(seq 30); do
  curl -s http://127.0.0.1:1041/ofrep/v1/evaluate/flags/new-checkout -X POST \
    -H 'Content-Type: application/json' -H 'X-API-Key: 8d3b5f0e-6a4f-4c8e-9c3e-2f2f5d1a7b10' \
    -d '{"context": {"targetingKey": "user-123"}}' | grep -o '"value":[a-z]*' > value.txt
  grep -q false value.txt && break
  sleep 1
done
cat value.txt
```

<!-- docs-test: output -->
```text
published generation 2 (was 1) to manifests/stable.yaml, channel stable
"value":false
```

Publishing the same documents again writes nothing: the manifest already lists them.

## 7. Clean up

<!-- docs-test: run -->
```shell
kill "$(cat relay.pid)"
```

## Next steps

- Read how `publish` writes a file store (atomic rename, the lock, permissions): [publish](../tooling/publish.md#file-store).
- Connect an application using a standard OpenFeature SDK with [OFREP](../openfeature/ofrep.md).
- Read the [bootstrap file reference](bootstrap-config.md) and the [file store](../concepts/control-plane-file-store.md).
- The same steps against an S3 bucket and a local emulator: [getting started](getting-started.md).
