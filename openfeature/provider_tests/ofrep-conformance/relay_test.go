package ofrepconformance_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/api"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/bootstrap"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/corehttp"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/flagruntime"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/metric"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/relaycore"
	"github.com/blackbirdworks/semaphore/cmdhelpers/cmdlog"
)

const (
	relayReadyTimeout = 15 * time.Second
	relayPollInterval = 20 * time.Millisecond
	roundsBuffer      = 16
)

func noEnv(string) (string, bool) { return "", false }

// startRelay starts an in-process Semaphore relay proxy from a bootstrap YAML file in
// openfeature/provider_tests/ and returns its loopback base URL.
func startRelay(t *testing.T, bootstrapFile string) string {
	t.Helper()

	return startRelayAt(t, filepath.Join("..", bootstrapFile), nil)
}

// startRelayWithAdminKey starts the relay of bootstrap-authenticated.yaml with an admin key added: an admin key
// authenticates on every endpoint and is then refused by the handlers that serve flag sets only.
func startRelayWithAdminKey(t *testing.T, adminKey string) string {
	t.Helper()

	const fileMode = 0o600

	raw, err := os.ReadFile(filepath.Join("..", "bootstrap-authenticated.yaml"))
	require.NoError(t, err)

	cfgPath := filepath.Join(t.TempDir(), "bootstrap.yaml")
	require.NoError(t, os.WriteFile(cfgPath, append(raw, []byte("auth:\n  adminKeys: ["+adminKey+"]\n")...), fileMode))

	return startRelayAt(t, cfgPath, nil)
}

// startRelayWithEventStream starts the relay of bootstrap-authenticated.yaml that advertises a stream of flag changes.
func startRelayWithEventStream(t *testing.T, baseURL string, inactivityDelaySec int) string {
	t.Helper()

	return startRelayAt(t, filepath.Join("..", "bootstrap-authenticated.yaml"), func(cfg *bootstrap.Config) {
		cfg.Server.OFREPEventStream = bootstrap.OFREPEventStream{
			BaseURL:            baseURL,
			InactivityDelaySec: inactivityDelaySec,
		}
	})
}

func startRelayAt(t *testing.T, cfgPath string, mutate func(*bootstrap.Config)) string {
	t.Helper()

	load := bootstrap.LoadOptions{ReadFile: os.ReadFile, LookupEnv: noEnv}

	cfg, err := bootstrap.Load(&bootstrap.Overrides{Config: cfgPath}, load)
	require.NoError(t, err)

	conn := cfg.Connections["control"]
	require.NotNil(t, conn.File)
	conn.File.Path = filepath.Join("..", "store")

	if mutate != nil {
		mutate(cfg)
	}

	return startRelayFromConfig(t, cfg, load)
}

// startRelayWithFlags starts an in-process relay proxy serving the provided flags YAML document.
func startRelayWithFlags(t *testing.T, flagsYAML string) string {
	t.Helper()

	return startRelayWithFlagSet(t, flagsYAML, true)
}

// startRelayWithFlagSet starts a relay proxy serving the flags; without a key it serves them to requests that
// carry no key (a public flag set), otherwise only to the key authorized_token.
func startRelayWithFlagSet(t *testing.T, flagsYAML string, keyed bool) string {
	t.Helper()

	storeDir := writeTempFlagStore(t, flagsYAML, keyed)
	cfgPath := filepath.Join("..", "bootstrap.yaml")
	load := bootstrap.LoadOptions{ReadFile: os.ReadFile, LookupEnv: noEnv}

	cfg, err := bootstrap.Load(&bootstrap.Overrides{Config: cfgPath}, load)
	require.NoError(t, err)

	conn := cfg.Connections["control"]
	require.NotNil(t, conn.File)
	conn.File.Path = storeDir

	return startRelayFromConfig(t, cfg, load)
}

func writeTempFlagStore(t *testing.T, flagsYAML string, keyed bool) string {
	t.Helper()

	const (
		dirMode  = 0o750
		fileMode = 0o600
	)

	storeDir := t.TempDir()

	flagSetsYAML := "apiVersion: semaphore/v1\nkind: FlagSet\nflagSets:\n" +
		"  - name: integration\n    sources:\n      - integration-flags\n"
	if keyed {
		flagSetsYAML += "    apiKeys:\n      - authorized_token\n"
	}

	writeObj := func(content string) (string, string, int) {
		sum := sha256.Sum256([]byte(content))
		hexSum := hex.EncodeToString(sum[:])
		rel := filepath.Join("objects", "sha256", hexSum[:2], hexSum+".yaml")
		full := filepath.Join(storeDir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(full), dirMode))
		require.NoError(t, os.WriteFile(full, []byte(content), fileMode))

		return filepath.ToSlash(rel), hexSum, len(content)
	}

	fsKey, fsSum, fsSize := writeObj(flagSetsYAML)
	flKey, flSum, flSize := writeObj(flagsYAML)

	manifest := "apiVersion: semaphore/v1\nkind: Manifest\nchannel: stable\ngeneration: 1\nobjects:\n" +
		"  - name: flagsets\n    kind: flagSets\n    connection: control\n" +
		"    key: " + fsKey + "\n    format: yaml\n    checksum:\n      algorithm: sha256\n" +
		"      value: " + strconv.Quote(fsSum) + "\n    size: " + strconv.Itoa(fsSize) + "\n" +
		"  - name: integration-flags\n    kind: flags\n    connection: control\n" +
		"    key: " + flKey + "\n    format: yaml\n    checksum:\n      algorithm: sha256\n" +
		"      value: " + strconv.Quote(flSum) + "\n    size: " + strconv.Itoa(flSize) + "\n"

	manifestPath := filepath.Join(storeDir, "manifests", "stable.yaml")
	require.NoError(t, os.MkdirAll(filepath.Dir(manifestPath), dirMode))
	require.NoError(t, os.WriteFile(manifestPath, []byte(manifest), fileMode))

	return storeDir
}

func startRelayFromConfig(t *testing.T, cfg *bootstrap.Config, load bootstrap.LoadOptions) string {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	var (
		streams            *corehttp.Streams
		core               *relaycore.Core
		server             api.Server
		listener           net.Listener
		loops              sync.WaitGroup
		serverOwnsListener bool
	)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), relayReadyTimeout)
		defer stopCancel()

		if streams != nil {
			_ = streams.Close(stopCtx)
		}

		if serverOwnsListener {
			server.Stop(stopCtx)
		} else if listener != nil {
			_ = listener.Close()
		}

		if core != nil {
			_ = core.Shutdown(stopCtx)
		}

		cancel()
		loops.Wait()
	})

	var err error

	listener, err = (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok, "expected *net.TCPAddr listener address")

	metrics, err := metric.NewMetrics()
	require.NoError(t, err)

	streams = corehttp.NewStreams(ctx)
	rounds := make(chan flagruntime.RoundResult, roundsBuffer)

	core, err = relaycore.New(ctx, cfg, &relaycore.Deps{
		Registerer: metrics.Registry,
		Broadcast:  streams.Broadcast,
		Secrets:    load,
		Hooks: flagruntime.Hooks{AfterRound: func(res flagruntime.RoundResult) {
			select {
			case rounds <- res:
			default:
			}
		}},
	})
	require.NoError(t, err)

	settings, err := corehttp.SettingsFrom(cfg, "conformance-test")
	require.NoError(t, err)

	settings.Host = "127.0.0.1"
	settings.Port = tcpAddr.Port

	logger := cmdlog.NewDefault(cmdlog.WithWriter(io.Discard)).Slog
	ready := corehttp.NewReadiness(core)

	server, err = corehttp.New(core, &corehttp.Config{
		Settings:  &settings,
		Logger:    logger,
		Streams:   streams,
		Metrics:   metrics,
		Readiness: ready,
	}, api.WithListener(listener))
	require.NoError(t, err)

	serverOwnsListener = true

	loops.Go(func() { _ = core.Run(ctx) })
	loops.Go(func() { server.StartWithContext(ctx) })

	waitFirstGeneration(t, rounds)

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", tcpAddr.Port)
	waitReadyz(t, baseURL)

	return baseURL
}

func waitFirstGeneration(t *testing.T, rounds <-chan flagruntime.RoundResult) {
	t.Helper()

	timer := time.NewTimer(relayReadyTimeout)
	defer timer.Stop()

	for {
		select {
		case res := <-rounds:
			if res.Generation != 1 {
				continue
			}

			require.Equal(t, flagruntime.OutcomeApplied, res.Outcome, "relay rejected test flag store")

			return
		case <-timer.C:
			require.FailNow(t, "timed out waiting for relay generation 1 to apply")
		}
	}
}

func waitReadyz(t *testing.T, baseURL string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), relayReadyTimeout)
	defer cancel()

	client := &http.Client{Timeout: relayReadyTimeout}

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/readyz", http.NoBody)
		require.NoError(t, err)

		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()

			if resp.StatusCode == http.StatusOK && ctx.Err() == nil {
				return
			}
		}

		timer := time.NewTimer(relayPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			require.FailNowf(
				t,
				"relay /readyz not ready",
				"did not become ready within %s: %v",
				relayReadyTimeout,
				ctx.Err(),
			)
		case <-timer.C:
		}
	}
}
