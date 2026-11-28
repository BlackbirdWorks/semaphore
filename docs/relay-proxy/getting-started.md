# Getting started

This guide runs a relay proxy on your machine and changes a flag while it runs. You write a version 2
[bootstrap file](bootstrap-config.md), check it, publish flags to an S3 bucket, start the relay, evaluate a flag, change
the flag, publish again, and see the new value without a restart.

Every command and file on this page is run by a test of the repository (`internal/docstest`), in the order shown. Run
each block in the same shell and the same directory.

You need:

- Docker, for a local S3 emulator.
- The binaries `semaphore` (the relay) and `semaphore-cli` on your `PATH`. Build them with
  `task build-relayproxy build-cli` (they land in `out/bin/`), or see [installation](install.md).

The relay reads its flags from a **control plane store**: an S3 bucket (or a directory) that holds a manifest and the
objects it lists. `semaphore-cli publish` writes them. See [concepts](../concepts/control-plane-polling.md).

## 1. Start an S3 emulator and create a bucket

<!-- docs-test: run -->
```shell
docker run -d --rm --name semaphore-s3 -p 9100:8000 ghcr.io/blackbirdworks/gopherstack:1.10.0
until curl -sf http://127.0.0.1:9100/_gopherstack/health > /dev/null; do sleep 1; done
curl -sf -X PUT http://127.0.0.1:9100/getting-started
```

The emulator accepts any credentials. The next steps use `test` for both.

<!-- docs-test: run -->
```shell
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
```

## 2. Write the bootstrap file

The file names one connection, `control`, to the bucket, and says that the control plane is the manifest
`manifests/stable.yaml` of that connection. The relay polls it every 2 seconds. Secrets are references to environment
variables, never inline values.

<!-- docs-test: run -->
```shell
cat > bootstrap.yaml <<'YAML'
version: 2
connections:
  control:
    type: s3
    endpoint: http://127.0.0.1:9100
    region: us-east-1
    bucket: getting-started
    pathStyle: true
    credentials:
      type: static
      accessKeyId: {fromEnv: AWS_ACCESS_KEY_ID}
      secretAccessKey: {fromEnv: AWS_SECRET_ACCESS_KEY}
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

## 3. Write a flag and a flag set

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

## 4. Publish

`publish` validates the documents, uploads the objects, and writes the manifest last. The first line of its output says
which generation it wrote.

<!-- docs-test: run -->
```shell
semaphore-cli publish ./flags-repo --config bootstrap.yaml | sed -n 1p
```

<!-- docs-test: output -->
```text
published generation 1 (was 0) to manifests/stable.yaml, channel stable
```

See [publish](../tooling/publish.md) for the flags and the exit codes. To publish to a directory instead, with no emulator, follow [getting started with a file store](getting-started-file-store.md).

## 5. Start the relay

The relay runs in the background here, and writes its log to `relay.log`. It is ready when `/readyz` answers 200.

<!-- docs-test: run -->
```shell
semaphore --config bootstrap.yaml --listen 127.0.0.1:1031 > relay.log 2>&1 &
echo $! > relay.pid
until curl -sf http://127.0.0.1:1031/readyz > /dev/null; do sleep 1; done
```

## 6. Evaluate a flag

The API key of the flag set goes in the `X-API-Key` header. The call is an [OFREP](../openfeature/ofrep.md) evaluation.

<!-- docs-test: run -->
```shell
curl -s http://127.0.0.1:1031/ofrep/v1/evaluate/flags/new-checkout -X POST \
  -H 'Content-Type: application/json' -H 'X-API-Key: 8d3b5f0e-6a4f-4c8e-9c3e-2f2f5d1a7b10' \
  -d '{"context": {"targetingKey": "user-123"}}' | grep -o '"value":[a-z]*'
```

<!-- docs-test: output -->
```text
"value":true
```

## 7. Change the flag and publish again

Make `disabled` the default, publish generation 2, and wait until the relay serves it. The relay polls the manifest, so
the change is live within a few seconds. Nothing restarts.

<!-- docs-test: run -->
```shell
sed -i 's/variation: enabled/variation: disabled/' flags-repo/flags/checkout.yaml
semaphore-cli publish ./flags-repo --config bootstrap.yaml | sed -n 1p
for i in $(seq 30); do
  curl -s http://127.0.0.1:1031/ofrep/v1/evaluate/flags/new-checkout -X POST \
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

## 8. Clean up

<!-- docs-test: run -->
```shell
kill "$(cat relay.pid)"
docker rm -f semaphore-s3
```

## Next steps

- Connect an application using a standard OpenFeature SDK with [OFREP](../openfeature/ofrep.md).
- Read the [bootstrap file reference](bootstrap-config.md) and the [schema reference](../reference/schema.md).
- Run the relay on a directory instead of S3: [getting started with a file store](getting-started-file-store.md) and the
  [file store](../concepts/control-plane-file-store.md).
- Migrating from the GO Feature Flag relay proxy: see [the migration note](../migration/from-go-feature-flag.md).
