---
layout: home

hero:
  name: Semaphore
  text: Self-hosted feature flags, built for speed.
  tagline: A lightweight feature flag relay proxy and embeddable Go engine with native OpenFeature OFREP support.
  actions:
    - theme: brand
      text: Get Started
      link: /relay-proxy/getting-started
    - theme: alt
      text: Documentation Index
      link: /overview
    - theme: alt
      text: View on GitHub
      link: https://github.com/BlackbirdWorks/semaphore

features:
  - icon: 🔌
    title: OpenFeature & OFREP Native
    details: Use standard OpenFeature SDKs in any language over HTTP using the OpenFeature Remote Evaluation Protocol (OFREP).
  - icon: ⚡
    title: Stateless Relay Proxy
    details: Single Go binary or container image with zero database dependencies, hot reloading, Flag Sets, and FIPS 140-3 builds.
  - icon: 🗄️
    title: Store Flags Anywhere
    details: Publish YAML, JSON, or TOML flag files to S3 or a directory for the relay proxy; the Go module reads many more sources.
  - icon: 🎯
    title: Rich Rollout Strategies
    details: Target users with query rules, consistent percentage splits, progressive ramps, scheduled rollouts, experimentation windows, and custom bucketing keys.
  - icon: 📡
    title: Export & Notify
    details: Stream evaluation events to S3, Kinesis, Kafka, Pub/Sub, BigQuery, OpenTelemetry, or webhooks, and alert Slack, Discord, or Teams on changes.
  - icon: 🧩
    title: Embeddable Go Engine
    details: Embed the core evaluation engine directly inside Go services with no network hop when you do not need a separate proxy.
---

## Quick Start

Build and run the relay proxy locally on the example file store, or pull the container image from `ghcr.io/blackbirdworks/semaphore`:

```shell
task build-relayproxy        # binary: out/bin/semaphore
cd cmd/relayproxy/testdata/dockerhub-example   # a version 2 bootstrap file and a file store with flags
../../../../out/bin/semaphore --config bootstrap.yaml
```

Evaluate a flag through OFREP from another terminal (the example flag set has the API key `example-api-key`):

```shell
curl -s http://localhost:1031/ofrep/v1/evaluate/flags/flag-only-for-admin -X POST \
  -H 'Content-Type: application/json' -H 'X-API-Key: example-api-key' \
  --data-raw '{"context": {"targetingKey": "user-123", "admin": true}}'
```

Publish your own flags with `semaphore-cli publish` to an S3 bucket or a directory, see [Getting started](relay-proxy/getting-started.md) (or [without an emulator](relay-proxy/getting-started-file-store.md)).
