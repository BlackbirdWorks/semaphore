package publish_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/cli/helper"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/api"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/bootstrap"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/corehttp"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/metric"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/relaycore"
	"github.com/blackbirdworks/semaphore/controlplane"
	"github.com/blackbirdworks/semaphore/controlplane/filepublish"
	"github.com/blackbirdworks/semaphore/controlplane/s3publish"
)

const (
	fileConn   = "control"
	fileAPIKey = "8d3b5f0e-6a4f-4c8e-9c3e-2f2f5d1a7b10"
	fileWait   = 30 * time.Second
	fileTick   = 50 * time.Millisecond
)

// relayOn starts the relay core on the bootstrap file of the rig, with its own polling loop, and serves its HTTP
// handler. It returns the base URL and the core, whose status counts the polls.
func (r *fileRig) relayOn(t *testing.T) (string, *relaycore.Core) {
	t.Helper()

	raw, err := os.ReadFile(r.config)
	require.NoError(t, err)

	cfg, err := bootstrap.Parse(raw, nil, bootstrap.LoadOptions{})
	require.NoError(t, err)

	metrics, err := metric.NewMetrics()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	core, err := relaycore.New(ctx, cfg, &relaycore.Deps{Registerer: metrics.Registry})
	require.NoError(t, err)

	streams := corehttp.NewStreams(ctx)
	done := make(chan error, 1)

	go func() { done <- core.Run(ctx) }()

	t.Cleanup(func() {
		cancel()

		stop, stopCancel := context.WithTimeout(context.Background(), fileWait)
		defer stopCancel()

		assert.NoError(t, core.Shutdown(stop))
		assert.NoError(t, streams.Close(stop))
		assert.NoError(t, <-done)
	})

	srv, err := corehttp.New(core, &corehttp.Config{
		Settings: &api.Settings{}, Streams: streams, Metrics: metrics, Readiness: corehttp.NewReadiness(core),
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	return ts.URL, core
}

// evaluate returns the value of the flag, or an error while the relay does not serve it yet.
func evaluate(ctx context.Context, base, flag string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/ofrep/v1/evaluate/flags/"+flag,
		strings.NewReader(`{"context": {"targetingKey": "user-1"}}`))
	if err != nil {
		return false, fmt.Errorf("request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", fileAPIKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("evaluate: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("read: %w", err)
	}

	var out struct {
		Value bool `json:"value"`
	}

	err = json.Unmarshal(body, &out)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("%w: %d %s", errNotServed, resp.StatusCode, body)
	}

	return out.Value, nil
}

var errNotServed = errors.New("the flag is not served")

// awaitValue waits until the relay evaluates checkout-new to want.
func awaitValue(t *testing.T, base string, want bool) {
	t.Helper()

	awaitFlag(t, base, "checkout-new", want)
}

// awaitFlag waits until the relay evaluates the flag to want.
func awaitFlag(t *testing.T, base, flag string, want bool) {
	t.Helper()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		got, err := evaluate(t.Context(), base, flag)
		if assert.NoError(c, err) {
			assert.Equal(c, want, got)
		}
	}, fileWait, fileTick)
}

func flagsDefaulting(variation string) string {
	return strings.Replace(fileFlagsDoc, `variation: "off"`, `variation: "`+variation+`"`, 1)
}

func TestFileStorePublishThenRelayServesAndFollowsAChange(t *testing.T) {
	t.Parallel()

	rig := newFileRig(t)

	out, code := rig.run(t, rig.src, "--config", rig.config)
	require.Zero(t, code, out)

	base, _ := rig.relayOn(t)
	awaitValue(t, base, false)

	rig.writeFlags(t, flagsDefaulting("on"))

	out, code = rig.run(t, rig.src, "--config", rig.config)
	require.Zero(t, code, out)
	assert.Contains(t, out, "published generation 2 (was 1)")

	awaitValue(t, base, true)
}

// published is the part of the JSON output of a publish that names what it wrote.
type published struct {
	Objects []struct {
		Name string `json:"name"`
		Key  string `json:"key"`
	} `json:"objects"`
	Generation uint64 `json:"generation"`
}

func TestFileStoreConcurrentPublishesLeaveOneCompleteGeneration(t *testing.T) {
	t.Parallel()

	rig := newFileRig(t)

	_, code := rig.run(t, rig.src, "--config", rig.config)
	require.Zero(t, code)

	// two sources, each with a flag of its own that the other does not have
	sources := make([]*fileRig, 2)
	flags := make([]string, len(sources))

	for i := range sources {
		flags[i] = fmt.Sprintf("checkout-%d", i)
		sources[i] = &fileRig{src: filepath.Join(t.TempDir(), "src"), store: rig.store, config: rig.config}
		sources[i].write(t, "flags/checkout.yaml", strings.Replace(fileFlagsDoc, "checkout-new", flags[i], 1))
		sources[i].write(t, "flagsets/web.yaml", fileFlagSetDoc)
	}

	outs := make([]string, len(sources))
	codes := make([]int, len(sources))

	var wg sync.WaitGroup

	for i, source := range sources {
		wg.Go(func() { outs[i], codes[i] = source.run(t, "-o", "json", source.src, "--config", rig.config) })
	}

	wg.Wait()

	wins := map[uint64]int{} // generation to the index of the publisher

	for i, code := range codes {
		if code == 0 {
			var res published

			require.NoError(t, json.Unmarshal([]byte(outs[i]), &res), outs[i])
			wins[res.Generation] = i

			continue
		}

		assert.Equal(t, helper.ExitConflict, code, "a publisher that did not win gets the conflict code: %s", outs[i])
	}

	require.NotEmpty(t, wins, "one publisher always wins")

	raw, err := os.ReadFile(filepath.Join(rig.store, "manifests", "stable.yaml"))
	require.NoError(t, err)

	manifest, _, err := controlplane.UnmarshalManifest(raw)
	require.NoError(t, err)

	winner, ok := wins[manifest.Generation]
	require.True(
		t,
		ok,
		"the manifest is the one of a publisher that succeeded: generation %d, wins %v",
		manifest.Generation,
		wins,
	)

	var res published

	require.NoError(t, json.Unmarshal([]byte(outs[winner]), &res))

	listed := map[string]string{}
	for _, obj := range res.Objects {
		listed[obj.Name] = obj.Key
	}

	got := map[string]string{}

	for i := range manifest.Objects {
		obj := &manifest.Objects[i]
		got[obj.Name] = obj.Key

		body, readErr := os.ReadFile(filepath.Join(rig.store, filepath.FromSlash(obj.Key)))
		require.NoError(t, readErr)
		require.NoError(t, controlplane.VerifyChecksum(body, obj.Checksum), obj.Key)
	}

	assert.Equal(t, listed, got, "the manifest lists the objects of the publisher that wrote it")

	out, vcode := rig.run(t, "verify", "--config", rig.config)
	assert.Zero(t, vcode, out)

	// the relay serves the values of that publisher, never a mix
	base, _ := rig.relayOn(t)
	awaitFlag(t, base, flags[winner], false)

	_, err = evaluate(t.Context(), base, flags[1-winner])
	require.ErrorIs(t, err, errNotServed)
}

// failManifest fails the write of the manifest: the publisher was killed after the objects.
type failManifest struct {
	s3publish.API

	key string
}

var errKilled = errors.New("killed before the manifest")

func (f *failManifest) PutObject(
	ctx context.Context,
	input *s3.PutObjectInput,
	opts ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	if aws.ToString(input.Key) == f.key {
		return nil, errKilled
	}

	out, err := f.API.PutObject(ctx, input, opts...)
	if err != nil {
		return nil, fmt.Errorf("put: %w", err)
	}

	return out, nil
}

func TestFileStoreAPublishKilledBeforeTheManifestLeavesThePreviousGeneration(t *testing.T) {
	t.Parallel()

	rig := newFileRig(t)

	out, code := rig.run(t, rig.src, "--config", rig.config)
	require.Zero(t, code, out)

	base, core := rig.relayOn(t)
	awaitValue(t, base, false)

	manifestPath := filepath.Join(rig.store, "manifests", "stable.yaml")
	before, err := os.ReadFile(manifestPath)
	require.NoError(t, err)

	store, err := filepublish.New(rig.store)
	require.NoError(t, err)

	pub, err := s3publish.New(&failManifest{API: store, key: "manifests/stable.yaml"},
		&s3publish.Config{Bucket: rig.store, Channel: "stable", Connection: fileConn})
	require.NoError(t, err)

	body, err := controlplane.MarshalFlags(controlplane.Flags{
		"checkout-new": {
			Variations:  map[string]any{"on": true, "off": false},
			DefaultRule: &controlplane.Rule{Variation: new("on")},
		},
	})
	require.NoError(t, err)

	_, err = pub.Publish(t.Context(), s3publish.Request{Objects: []s3publish.Object{
		{Name: "checkout", Kind: controlplane.KindFlags, Format: controlplane.FormatYAML, Body: body},
		{Name: "web", Kind: controlplane.KindFlagSets, Format: controlplane.FormatYAML, Body: []byte(fileFlagSetDoc)},
	}})
	require.ErrorIs(t, err, errKilled)

	after, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(before, after), "the manifest is untouched")

	// the relay polls the manifest at least twice more, and still serves the previous generation
	polled := core.Status().Source.Reader.Polls

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, core.Status().Source.Reader.Polls, polled+2)
	}, fileWait, fileTick)

	got, err := evaluate(t.Context(), base, "checkout-new")
	require.NoError(t, err)
	assert.False(t, got, "the value is the one of the previous generation")
	assert.Equal(t, uint64(1), core.Status().Source.Runtime.Generation)

	// the objects of the killed run are on disk but unreferenced: the next publish reuses them and goes live
	rig.writeFlags(t, flagsDefaulting("on"))

	out, code = rig.run(t, rig.src, "--config", rig.config)
	require.Zero(t, code, out)
	assert.Contains(t, out, "published generation 2 (was 1)")

	awaitValue(t, base, true)
}
