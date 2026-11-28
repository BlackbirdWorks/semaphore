package relayapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/api"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/bootstrap"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/flagruntime"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/relaycore"
	"github.com/blackbirdworks/semaphore/controlplane"
	"github.com/blackbirdworks/semaphore/internal/manifestreader"
	"github.com/blackbirdworks/semaphore/internal/manifestreader/statedir"
)

const (
	// waitBound bounds every wait of the tests: it is a failure bound, never a pause.
	waitBound     = 30 * time.Second
	connName      = "control"
	manifestKey   = "manifests/stable.yaml"
	channelName   = "stable"
	setsName      = "sets"
	flagsObject   = "flags-a"
	tenantSet     = "tenant"
	tenantKey     = "11111111-1111-4111-8111-111111111111"
	adminKey      = "22222222-2222-4222-8222-222222222222"
	otherAdminKey = "44444444-4444-4444-8444-444444444444"
	flagKey       = "f"
	evalPath      = "/ofrep/v1/evaluate/flags/" + flagKey
	readyPath     = "/readyz"
	reasonDown    = "shutting-down"
	rigLabel      = "relayrig"
)

// errPortInUse is the failure of the address that is already bound.
var errPortInUse = errors.New("address already in use")

// tickScheduler is the scheduler of the reader that the test drives: a round per tick, no timer.
type tickScheduler struct {
	entered chan struct{}
	release chan struct{}
}

func (s *tickScheduler) Wait(ctx context.Context, _ time.Duration) error {
	s.entered <- struct{}{}

	select {
	case <-ctx.Done():
		return fmt.Errorf("tick scheduler: %w", ctx.Err())
	case <-s.release:
		return nil
	}
}

// syncBuffer is a log sink that the test can read while the relay writes to it.
type syncBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// store is a directory that a publisher fills: the objects by content, and the manifest last.
type store struct {
	t   *testing.T
	dir string
	gen uint64
}

func (s *store) put(key string, data []byte) {
	s.t.Helper()

	path := filepath.Join(s.dir, key)
	require.NoError(s.t, os.MkdirAll(filepath.Dir(path), 0o750))
	require.NoError(s.t, os.WriteFile(path, data, 0o600))
}

// publish writes the flag set of tenantKey over one flags object that holds the flag f valued value, then the
// manifest of the next generation.
func (s *store) publish(value int) {
	s.t.Helper()

	s.publishDocs([]controlplane.FlagSet{
		{Name: tenantSet, Sources: []string{flagsObject}, APIKeys: []string{tenantKey}},
	}, map[string]string{flagsObject: flagYAML(value)})
}

// flagYAML is a flags document of the flag f valued value.
func flagYAML(value int) string { return flagYAMLOf(flagKey, value) }

// flagYAMLOf is a flags document of the flag key valued value.
func flagYAMLOf(key string, value int) string {
	return fmt.Sprintf("%s:\n  variations:\n    v: %d\n  defaultRule:\n    variation: v\n", key, value)
}

// publishDocs writes the flag sets and the flags objects (by name), then the manifest of the next generation.
func (s *store) publishDocs(sets []controlplane.FlagSet, flags map[string]string) {
	s.t.Helper()

	s.gen++

	manifest := &controlplane.Manifest{
		APIVersion: controlplane.APIVersion, Kind: controlplane.DocumentKindManifest, Channel: channelName,
		Generation: s.gen,
	}

	add := func(name, kind string, data []byte) {
		sum := controlplane.ChecksumOf(data)
		key, err := controlplane.ObjectKey(sum, controlplane.FormatYAML)
		require.NoError(s.t, err)
		s.put(key, data)

		manifest.Objects = append(manifest.Objects, controlplane.ManifestObject{
			Name: name, Kind: kind, Connection: connName, Key: key, Format: controlplane.FormatYAML,
			Checksum: sum, Size: int64(len(data)),
		})
	}

	doc, err := controlplane.MarshalFlagSet(&controlplane.FlagSetDocument{FlagSets: sets})
	require.NoError(s.t, err)

	add(setsName, controlplane.KindFlagSets, doc)

	for _, name := range slices.Sorted(maps.Keys(flags)) {
		add(name, controlplane.KindFlags, []byte(flags[name]))
	}

	data, err := controlplane.MarshalManifest(manifest)
	require.NoError(s.t, err)
	s.put(manifestKey, data)
}

// bootstrapDoc is a bootstrap file on the file store of dir.
func bootstrapDoc(dir, extra string) string { return bootstrapDocWithKey(dir, manifestKey, extra) }

func bootstrapDocWithKey(dir, key, extra string) string {
	return "version: 2\nconnections:\n  " + connName + ": {type: file, path: " + dir + "}\n" +
		"controlPlane:\n  connection: " + connName + "\n  manifest: {key: " + key + ", channel: " +
		channelName + ", jitter: 0}\n" + extra
}

// relayRig is a relay started by run on a file store, with its listener open from the start.
type relayRig struct {
	stderr      *syncBuffer
	stdout      *bytes.Buffer
	rounds      chan flagruntime.RoundResult
	reload      chan struct{}
	done        chan int
	finished    chan struct{}
	sched       *tickScheduler
	draining    chan struct{}
	timeoutEnd  chan context.CancelFunc
	timeoutFor  chan time.Duration
	hardStop    chan context.CancelFunc
	started     chan *relay
	label       string
	store       *store
	cancel      context.CancelFunc
	ctx         context.Context //nolint:containedctx // the context the relay runs with, read by the drain hook
	drainErr    error
	url         string
	cfgPath     string
	manifestKey string
	drainBody   string
	drainStatus int
	snapshot    uint64
}

type rigOptions struct {
	stateFS      statedir.FS
	boundAddress chan string
	reads        chan string
	drainWait    func(ctx context.Context, delay time.Duration) error
	onDraining   func(ctx context.Context)
	// readFile replaces the reading of the bootstrap file (a gate of the test).
	readFile    func(string) ([]byte, error)
	extra       string
	manifestKey string
	flags       []string
	// serverOptions are added to the options of the API server, listenFails makes it fail to bind its address.
	// broadcast sees every change the core hands to the streams.
	broadcast     func(flagruntime.Change)
	serverOptions []api.Option
	listenFails   bool
	watchDrain    bool
}

func startRelay(t *testing.T, ropts *rigOptions) *relayRig {
	t.Helper()

	dir := t.TempDir()
	rig := &relayRig{
		store:    &store{t: t, dir: dir},
		sched:    &tickScheduler{entered: make(chan struct{}, 64), release: make(chan struct{})},
		rounds:   make(chan flagruntime.RoundResult, 256),
		reload:   make(chan struct{}, 1),
		done:     make(chan int, 1),
		finished: make(chan struct{}),
		stderr:   &syncBuffer{},
		stdout:   &bytes.Buffer{},
		cfgPath:  filepath.Join(t.TempDir(), "bootstrap.yaml"),
		draining: make(chan struct{}),
		// the shutdown timeout never ends by itself: the test ends it through the cancel function it receives
		timeoutEnd: make(chan context.CancelFunc, 1), timeoutFor: make(chan time.Duration, 1),
		hardStop: make(chan context.CancelFunc, 1), label: t.Name(), started: make(chan *relay, 1),
	}
	rig.store.publish(1)
	rig.manifestKey = ropts.manifestKey
	rig.writeConfig(ropts.extra)

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() }) // the relay closes it when it serves; a failed bind never does

	rig.url = "http://" + listener.Addr().String()

	served := listener
	if ropts.reads != nil {
		served = &readSignalListener{Listener: listener, reads: ropts.reads}
	}

	serverOpts := make([]api.Option, 0, 1+len(ropts.serverOptions))
	serverOpts = append(serverOpts, api.WithListener(served))

	if ropts.boundAddress != nil {
		serverOpts = []api.Option{api.WithListenFunc(func(_ context.Context, _, address string) (net.Listener, error) {
			ropts.boundAddress <- address

			return listener, nil
		})}
	}

	ctx, cancel := context.WithCancel(t.Context())
	rig.cancel = cancel
	rig.ctx = ctx

	if ropts.listenFails {
		serverOpts = []api.Option{api.WithListenFunc(func(context.Context, string, string) (net.Listener, error) {
			return nil, errPortInUse
		})}
	}

	serverOpts = append(serverOpts, ropts.serverOptions...)

	opts := &runOptions{
		Args:   append([]string{configFlag, rig.cfgPath}, ropts.flags...),
		Stdout: rig.stdout, Stderr: rig.stderr,
		Load:   bootstrap.LoadOptions{ReadFile: readFileOf(ropts)},
		Reload: rig.reload,
		Deps: relaycore.Deps{
			Broadcast: ropts.broadcast,
			Scheduler: rig.sched,
			StateFS:   ropts.stateFS,
			TuneReader: func(cfg *manifestreader.Config) {
				cfg.DisableFetchSpread = true
			},
			Hooks: flagruntime.Hooks{AfterRound: func(res flagruntime.RoundResult) { rig.rounds <- res }},
		},
		ServerOptions: serverOpts,
		ShutdownContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			shutdown, cancelShutdown := context.WithCancel(context.WithoutCancel(parent))
			rig.hardStop <- cancelShutdown

			return shutdown, cancelShutdown
		},
		OnStarted: func(r *relay) { rig.started <- r },
		DrainWait: ropts.drainWait,
		WithTimeout: func(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
			bounded, endTimeout := context.WithCancel(parent)

			rig.timeoutFor <- d

			rig.timeoutEnd <- endTimeout

			return bounded, endTimeout
		},
		OnDraining: ropts.onDraining,
	}
	if ropts.watchDrain {
		opts.OnDraining = rig.onDraining
	}

	go func() {
		defer close(rig.finished)

		// every goroutine of the relay inherits this label: a test lists what is left of it
		pprof.Do(ctx, pprof.Labels(rigLabel, rig.label), func(ctx context.Context) {
			rig.done <- run(ctx, opts)
		})
	}()

	t.Cleanup(func() {
		cancel()
		<-rig.finished
	})

	return rig
}

// onDraining records what /readyz answers while the listener is still open.
func (r *relayRig) onDraining(ctx context.Context) {
	defer close(r.draining)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url+readyPath, http.NoBody)
	if err != nil {
		r.drainErr = err

		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.drainErr = err

		return
	}

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	r.drainStatus, r.drainBody, r.drainErr = resp.StatusCode, string(body), err
}

func (r *relayRig) writeConfig(extra string) {
	r.store.t.Helper()

	key := manifestKey
	if r.manifestKey != "" {
		key = r.manifestKey
	}

	require.NoError(r.store.t, os.WriteFile(r.cfgPath, []byte(bootstrapDocWithKey(r.store.dir, key, extra)), 0o600))
}

// awaitRound waits for the first applied round that satisfies ok.
func (r *relayRig) awaitRound(ok func(flagruntime.RoundResult) bool) {
	r.store.t.Helper()

	timeout := time.After(waitBound)

	for {
		select {
		case res := <-r.rounds:
			if res.Outcome == flagruntime.OutcomeApplied && ok(res) {
				r.snapshot = res.Snapshot

				return
			}
		case <-timeout:
			require.FailNow(r.store.t, "the engine did not apply the expected round", r.stderr.String())
		}
	}
}

// awaitGeneration waits for the engine to apply the generation.
func (r *relayRig) awaitGeneration(gen uint64) {
	r.store.t.Helper()
	r.awaitRound(func(res flagruntime.RoundResult) bool { return res.Generation == gen })
}

// poll lets the reader poll once and waits for the generation that was last published to be live.
func (r *relayRig) poll() {
	r.store.t.Helper()

	select {
	case <-r.sched.entered:
	case <-time.After(waitBound):
		require.FailNow(r.store.t, "the reader did not end its round")
	}

	select {
	case r.sched.release <- struct{}{}:
	case <-time.After(waitBound):
		require.FailNow(r.store.t, "the reader is not waiting for a tick")
	}

	select {
	case <-r.sched.entered:
	case <-time.After(waitBound):
		require.FailNow(r.store.t, "the reader did not end its next round")
	}

	r.awaitGeneration(r.store.gen)
}

func get(t *testing.T, url string, headers http.Header) (int, string) {
	t.Helper()

	return doRequest(t, http.MethodGet, url, "", headers)
}

func doRequest(t *testing.T, method, url, body string, headers http.Header) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err)

	maps.Copy(req.Header, headers)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer func() { require.NoError(t, resp.Body.Close()) }()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(data)
}

// evaluate evaluates the flag f with the key and returns the status and the value.
func (r *relayRig) evaluate(key string) (int, any) {
	r.store.t.Helper()

	status, body := doRequest(r.store.t, http.MethodPost, r.url+evalPath, `{"context":{"targetingKey":"u"}}`,
		http.Header{"X-Api-Key": {key}, "Content-Type": {"application/json"}})

	var out struct {
		Value any `json:"value"`
	}

	_ = json.Unmarshal([]byte(body), &out)

	return status, out.Value
}

func TestRunServesAFileStore(t *testing.T) {
	t.Parallel()

	rig := startRelay(
		t,
		&rigOptions{extra: "auth:\n  adminKeys: [" + adminKey + "]\nevaluation:\n  environment: test\n"},
	)
	rig.awaitGeneration(1)

	status, body := get(t, rig.url+readyPath, nil)
	assert.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, `{"ready":true,"state":"serving"}`, body)

	status, value := rig.evaluate(tenantKey)
	assert.Equal(t, http.StatusOK, status)
	assert.InDelta(t, 1, value, 0)

	status, _ = rig.evaluate("33333333-3333-4333-8333-333333333333")
	assert.Equal(t, http.StatusUnauthorized, status)

	// an object change is live after one poll, without a restart
	rig.store.publish(2)
	rig.poll()

	status, value = rig.evaluate(tenantKey)
	assert.Equal(t, http.StatusOK, status)
	assert.InDelta(t, 2, value, 0)

	assert.Contains(t, rig.stdout.String(), "Semaphore Relay Proxy")
}

func TestRunReloadsTheBootstrapFile(t *testing.T) {
	t.Parallel()

	rig := startRelay(t, &rigOptions{})
	rig.awaitGeneration(1)

	// the admin key is unknown: 401. After the reload it is known: the evaluation API answers 403 to an admin key.
	status, _ := rig.evaluate(adminKey)
	assert.Equal(t, http.StatusUnauthorized, status)

	rig.writeConfig("auth:\n  adminKeys: [" + adminKey + "]\n")

	rig.reload <- struct{}{}

	// every applied round is a chance to see the reload: the answer to the admin key turns from 401 to 403
	timeout := time.After(waitBound)

	for status != http.StatusForbidden {
		select {
		case res := <-rig.rounds:
			if res.Outcome == flagruntime.OutcomeApplied {
				status, _ = rig.evaluate(adminKey)
			}
		case <-timeout:
			require.FailNow(t, "the reload of the bootstrap file was not applied")
		}
	}

	assert.Equal(t, http.StatusForbidden, status)
}

func TestRunShutsDownInOrder(t *testing.T) {
	t.Parallel()

	rig := startRelay(t, &rigOptions{watchDrain: true})
	rig.awaitGeneration(1)

	rig.cancel()

	select {
	case <-rig.draining:
	case <-time.After(waitBound):
		require.FailNow(t, "the relay did not start to drain")
	}

	// the readiness fails first, while the listener still answers
	require.NoError(t, rig.drainErr)
	assert.Equal(t, http.StatusServiceUnavailable, rig.drainStatus)
	assert.JSONEq(t, `{"ready":false,"reason":"`+reasonDown+`"}`, rig.drainBody)

	select {
	case code := <-rig.done:
		assert.Equal(t, 0, code)
	case <-time.After(waitBound):
		require.FailNow(t, "the relay did not stop")
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, rig.url+readyPath, http.NoBody)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}

	require.Error(t, err, "the listener is closed after the shutdown")
}

func TestRunRefusesWhatItCanNotServe(t *testing.T) {
	t.Parallel()

	taken, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = taken.Close() })

	dir := t.TempDir()
	st := &store{t: t, dir: dir}
	st.publish(1)

	tests := []struct {
		name     string
		doc      string
		wantErr  string
		args     []string
		flags    []string
		wantCode int
	}{
		{
			name: "a version 1 file", doc: "retrievers:\n  - kind: file\n    path: flags.yaml\n", wantCode: 1,
			wantErr: "version 2 required",
		},
		{name: "version 1 declared", doc: "version: 1\n", wantCode: 1, wantErr: "version 2 required"},
		{name: "an unknown key", doc: bootstrapDoc(dir, "studio: {}\n"), wantCode: 1, wantErr: "studio"},
		{name: "no control plane", doc: "version: 2\n", wantCode: 1, wantErr: "controlPlane"},
		{
			name:     "a unix socket",
			doc:      bootstrapDoc(dir, "server:\n  unixSocketPath: /tmp/semaphore.sock\n"),
			wantCode: 1,
			wantErr:  "server.unixSocketPath",
		},
		{name: "two api listeners", doc: bootstrapDoc(dir, "server:\n  listeners:\n    - {name: a, address: \":1\"}\n"+
			"    - {name: b, address: \":2\"}\n"), wantCode: 1, wantErr: "the relay serves one api listener"},
		{name: "a port in use", doc: bootstrapDoc(dir, "server:\n  listeners:\n    - {name: api, address: \""+
			taken.Addr().String()+"\"}\n"), wantCode: 1, wantErr: "Error starting relay proxy"},
		{name: "no file", args: []string{}, wantCode: 1, wantErr: "no bootstrap file"},
		{name: "an admin key reference that does not resolve", doc: bootstrapDoc(dir,
			"auth:\n  adminKeys: ["+adminKey+", {fromEnv: ADMIN_MISSING}]\n"), wantCode: 1, wantErr: "auth.adminKeys"},
		{
			name:     "a flag without its value",
			args:     []string{"--listen"},
			wantCode: exitUsage,
			wantErr:  "invalid command line",
		},
		{
			name: "listen override", doc: bootstrapDoc(dir, ""), flags: []string{"--listen=nowhere"}, wantCode: 1,
			wantErr: "set by flag --listen",
		},
		{
			name:     "ops listen override",
			doc:      bootstrapDoc(dir, ""),
			flags:    []string{"--ops-listen=nowhere"},
			wantCode: 1,
			wantErr:  "set by flag --ops-listen",
		},
		{
			name: "log level override", doc: bootstrapDoc(dir, ""), flags: []string{"--log-level=loud"}, wantCode: 1,
			wantErr: "set by flag --log-level",
		},
		{
			name: "log format override", doc: bootstrapDoc(dir, ""), flags: []string{"--log-format=xml"}, wantCode: 1,
			wantErr: "set by flag --log-format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			args := tt.args
			if args == nil {
				path := filepath.Join(t.TempDir(), "bootstrap.yaml")
				require.NoError(t, os.WriteFile(path, []byte(tt.doc), 0o600))

				args = append([]string{configFlag, path}, tt.flags...)
			}

			var stderr syncBuffer

			code := run(t.Context(), &runOptions{
				Args: args, Stdout: io.Discard, Stderr: &stderr, Load: bootstrap.LoadOptions{ReadFile: os.ReadFile},
			})

			assert.Equal(t, tt.wantCode, code)
			assert.Contains(t, stderr.String(), tt.wantErr)
		})
	}
}

func TestRunWithoutMetrics(t *testing.T) {
	t.Parallel()

	rig := startRelay(t, &rigOptions{extra: "observability:\n  metrics: {enabled: false}\n"})
	rig.awaitGeneration(1)

	status, _ := rig.evaluate(tenantKey)
	assert.Equal(t, http.StatusOK, status)

	status, _ = get(t, rig.url+"/metrics", nil)
	assert.Equal(t, http.StatusNotFound, status, "no metrics are served when they are disabled")
}

// One strict parse decides what run does: --help wins over --version, a false flag is not a request, and a request
// does not excuse an unknown flag.
func TestRunHelpVersionPrecedence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStdout string
		wantStderr string
		args       []string
		wantCode   int
	}{
		{name: "help=false and version", args: []string{"--help=false", versionFlag}, wantStdout: "semaphore "},
		{name: "version=false starts normally", args: []string{"--version=false"}, wantCode: 1,
			wantStderr: "configuration error"},
		{name: "help and version", args: []string{helpFlag, versionFlag}, wantStdout: "Usage: semaphore"},
		{name: "version and help", args: []string{versionFlag, helpFlag}, wantStdout: "Usage: semaphore"},
		{name: "help and version, unknown flag", args: []string{helpFlag, versionFlag, unknownFlag},
			wantCode: exitUsage, wantStderr: unknownFlagText},
		{name: "version and help, unknown flag", args: []string{versionFlag, helpFlag, unknownFlag},
			wantCode: exitUsage, wantStderr: unknownFlagText},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stdout, stderr bytes.Buffer

			code := run(t.Context(), &runOptions{Args: tt.args, Stdout: &stdout, Stderr: &stderr})

			assert.Equal(t, tt.wantCode, code)
			assert.Contains(t, stdout.String(), tt.wantStdout)
			assert.Contains(t, stderr.String(), tt.wantStderr)
		})
	}
}

func TestRunPrintsTheVersion(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer

	code := run(t.Context(), &runOptions{Args: []string{versionFlag}, Stdout: &stdout, Stderr: io.Discard})

	assert.Equal(t, 0, code)
	assert.True(t, strings.HasPrefix(stdout.String(), "semaphore "), stdout.String())
}

func TestLogFormat(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, in, want string }{
		{name: "text is logfmt", in: logFormatText, want: "logfmt"},
		{name: "json stays", in: "json", want: "json"},
		{name: "empty stays", in: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, logFormat(tt.in))
		})
	}
}

var errLoad = errors.New("load failed")

func TestReloadRefusesAFileThatDoesNotLoad(t *testing.T) {
	t.Parallel()

	var logs syncBuffer

	rel := &relay{log: slog.New(slog.NewTextHandler(&logs, nil))}
	rel.reload(t.Context(), func() (*bootstrap.Config, error) { return nil, errLoad })

	assert.Contains(t, logs.String(), "the reload of the bootstrap file was refused")
	assert.Contains(t, logs.String(), errLoad.Error())
}

func TestForwardSignals(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	signals := make(chan os.Signal)
	out := make(chan struct{}, 1)
	ended := make(chan struct{})

	go func() {
		defer close(ended)

		forwardSignals(ctx, signals, out)
	}()

	// two signals that arrive while a reload waits are one request: the second finds the buffer full
	signals <- os.Interrupt

	signals <- os.Interrupt

	cancel()
	<-ended

	assert.Len(t, out, 1, "the second signal was merged into the first")
}

// The relay binds the address of its file as written: brackets of an IPv6 host included. The binder gets a function
// that records the address and hands over a listener that is already bound.
func TestRunBindsTheConfiguredAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		address     string
		wantAddress string
	}{
		{name: "ipv4 literal", address: "127.0.0.1:1031", wantAddress: "127.0.0.1:1031"},
		{name: "no host", address: ":1031", wantAddress: ":1031"},
		{name: "ipv6 wildcard in brackets", address: "[::]:1031", wantAddress: "[::]:1031"},
		{name: "ipv6 loopback in brackets", address: "[::1]:8080", wantAddress: "[::1]:8080"},
		{name: "host name", address: "localhost:1031", wantAddress: "localhost:1031"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bound := make(chan string, 1)
			rig := startRelay(t, &rigOptions{
				boundAddress: bound,
				extra:        "server:\n  listeners:\n    - {name: api, address: \"" + tt.address + "\"}\n",
			})

			select {
			case address := <-bound:
				assert.Equal(t, tt.wantAddress, address)
			case <-time.After(waitBound):
				require.FailNow(t, "the relay did not bind", rig.stderr.String())
			}

			rig.awaitGeneration(1)

			status, _ := get(t, rig.url+readyPath, nil)
			assert.Equal(t, http.StatusOK, status)
		})
	}
}

// The flags override the file at the start: the manifest key of the file names no manifest, the flag does.
func TestRunAppliesTheOverridesOfTheFlags(t *testing.T) {
	t.Parallel()

	rig := startRelay(
		t,
		&rigOptions{manifestKey: missingManifest, flags: []string{"--manifest-key=" + manifestKey}},
	)
	rig.awaitGeneration(1)

	status, value := rig.evaluate(tenantKey)
	assert.Equal(t, http.StatusOK, status)
	assert.InDelta(t, 1, value, 0)
}

func TestLoaderKeepsTheOverridesOnReload(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "bootstrap.yaml")
	require.NoError(t, os.WriteFile(path, []byte(bootstrapDoc(t.TempDir(), "")), 0o600))

	load := loaderOf(
		&bootstrap.Overrides{Config: path, Listen: "127.0.0.1:4321"},
		bootstrap.LoadOptions{ReadFile: os.ReadFile},
	)

	for range 2 {
		cfg, err := load()
		require.NoError(t, err)
		assert.Equal(t, "127.0.0.1:4321", cfg.Server.Listeners[0].Address)
	}
}

// A reload whose admin key reference does not resolve is rejected as a whole: the keys of the running set stay.
func TestReloadWithAnUnresolvedAdminKeyKeepsTheKeys(t *testing.T) {
	t.Parallel()

	rig := startRelay(t, &rigOptions{extra: "auth:\n  adminKeys: [" + adminKey + "]\n"})
	rig.awaitGeneration(1)

	status, _ := rig.evaluate(adminKey)
	require.Equal(t, http.StatusForbidden, status, "the admin key of the file is known")

	rig.writeConfig("auth:\n  adminKeys: [" + otherAdminKey + ", {fromEnv: ADMIN_MISSING}]\n")

	rig.reload <- struct{}{}

	timeout := time.After(waitBound)

	for {
		select {
		case res := <-rig.rounds:
			if res.Outcome != flagruntime.OutcomeRejected {
				continue
			}

			assert.Equal(t, flagruntime.CodeInvalidBootstrap, res.Code)
			assert.NotContains(t, res.Detail, otherAdminKey)

			status, _ = rig.evaluate(adminKey)
			assert.Equal(t, http.StatusForbidden, status, "the previous admin key still works")

			status, _ = rig.evaluate(otherAdminKey)
			assert.Equal(t, http.StatusUnauthorized, status, "the key of the rejected file is not known")

			return
		case <-timeout:
			require.FailNow(t, "the reload was not rejected")
		}
	}
}

// heldFS is a state directory file system whose first rename waits for the test: a write that the disk does not finish.
type heldFS struct {
	statedir.FS

	entered chan struct{}
	release chan struct{}
	// closed is closed when the root is closed: the core does it last, after the writer ended.
	closed    chan struct{}
	once      sync.Once
	closeOnce sync.Once
}

//nolint:ireturn // implements the FS seam, which returns an interface
func (h *heldFS) Open(dir string) (statedir.Root, error) {
	root, err := h.FS.Open(dir)
	if err != nil {
		return nil, err
	}

	return &heldRoot{Root: root, held: h}, nil
}

type heldRoot struct {
	statedir.Root

	held *heldFS
}

func (r *heldRoot) Close() error {
	err := r.Root.Close()

	r.held.closeOnce.Do(func() { close(r.held.closed) })

	return err
}

func (r *heldRoot) Rename(oldname, newname string) error {
	r.held.once.Do(func() { close(r.held.entered) })
	<-r.held.release

	return r.Root.Rename(oldname, newname)
}

// A state write that does not finish must not keep the process from exiting: the shutdown is bounded, says so, and
// the exit code is not zero.
func TestRunExitsWhenAStateWriteHoldsTheShutdown(t *testing.T) {
	t.Parallel()

	held := &heldFS{
		FS: statedir.NewOSFS(), entered: make(chan struct{}), release: make(chan struct{}),
		closed: make(chan struct{}),
	}
	stateDir := filepath.Join(t.TempDir(), "state")
	rig := startRelay(t, &rigOptions{
		stateFS: held,
		flags:   []string{"--state-dir=" + stateDir},
	})

	// registered after the temp dir and the rig: it runs first. The held write ends, and the cleanup of the core
	// (it joins the writer, then closes the root) is awaited, so nothing writes when the directory is removed
	t.Cleanup(func() {
		close(held.release)

		select {
		case <-held.closed:
		case <-time.After(waitBound):
			assert.Fail(t, "the state writer was not joined")
		}
	})

	rig.awaitGeneration(1)

	select {
	case <-held.entered:
	case <-time.After(waitBound):
		require.FailNow(t, "no state write started", rig.stderr.String())
	}

	rig.cancel()

	// the timeout of the shutdown is the one of the file, and it ends when the test says so
	assert.Equal(t, bootstrap.DefaultShutdownTimeout, rig.awaitTimeout())
	(<-rig.timeoutEnd)()

	select {
	case code := <-rig.done:
		assert.Equal(t, 1, code)
	case <-time.After(waitBound):
		require.FailNow(t, "the relay did not exit while a state write was held")
	}

	assert.Contains(t, rig.stderr.String(), "the loops of the relay did not stop within the shutdown deadline")
}

// Handing a file to the engine is not an applied reload: the log says it is queued, and names where the outcome is.
func TestReloadLogsTheHandOverAsQueued(t *testing.T) {
	t.Parallel()

	rig := startRelay(t, &rigOptions{})
	rig.awaitGeneration(1)

	// the channel holds one request: when the third send returns, the loop took the second, so it ended the first
	for range 3 {
		rig.reload <- struct{}{}
	}

	assert.Contains(t, rig.stderr.String(), "reload queued")
	assert.NotContains(t, rig.stderr.String(), "was reloaded")
}

// --help prints every flag with its environment variable on stdout and exits 0; an unknown flag prints the usage on
// stderr and exits 2. The flags listed are the ones of the code's own flag set, and the ones of the documentation.
func TestRunHelpAndUsage(t *testing.T) {
	t.Parallel()

	want := flagNames(t, true)

	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout bool
	}{
		{name: "long help", args: []string{helpFlag}, wantStdout: true},
		{name: "short help", args: []string{"-h"}, wantStdout: true},
		{name: unknownFlagText, args: []string{unknownFlag}, wantCode: exitUsage},
		{name: "unknown flag after --version", args: []string{versionFlag, unknownFlag}, wantCode: exitUsage},
		{name: "unknown flag before --version", args: []string{unknownFlag, versionFlag}, wantCode: exitUsage},
		{name: "unknown flag after --help", args: []string{helpFlag, unknownFlag}, wantCode: exitUsage},
		{
			name: "unknown flag after --version --help", args: []string{versionFlag, helpFlag, unknownFlag},
			wantCode: exitUsage,
		},
		{
			name: "unknown flag after --help --version", args: []string{helpFlag, versionFlag, unknownFlag},
			wantCode: exitUsage,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stdout, stderr bytes.Buffer

			code := run(t.Context(), &runOptions{Args: tt.args, Stdout: &stdout, Stderr: &stderr})
			assert.Equal(t, tt.wantCode, code)

			out := stderr.String()
			if tt.wantStdout {
				out = stdout.String()

				assert.Empty(t, stderr.String())
			} else {
				assert.Empty(t, stdout.String())
				assert.Contains(t, out, "unknown flag "+unknownFlag)
			}

			for _, name := range want {
				assert.Contains(t, out, name)
			}

			for _, env := range []string{
				bootstrap.EnvConfig, bootstrap.EnvLogLevel, bootstrap.EnvLogFormat, bootstrap.EnvListen,
				bootstrap.EnvOpsListen, bootstrap.EnvManifestKey, bootstrap.EnvStateDir,
			} {
				assert.Contains(t, out, env)
			}
		})
	}
}

// The documentation lists the flags of the relay: the list is the one of the code, so it can not drift.
func TestDocumentedFlagsAreTheFlagsOfTheCode(t *testing.T) {
	t.Parallel()

	page, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "relay-proxy", "bootstrap-config.md"))
	require.NoError(t, err)

	_, after, found := strings.Cut(string(page), "The flags are")
	require.True(t, found, "the page lists the flags")

	paragraph, _, _ := strings.Cut(after, "\n- ")

	matches := regexp.MustCompile("`(--[a-z-]+)`").FindAllStringSubmatch(paragraph, -1)
	documented := make([]string, 0, len(matches))

	for _, match := range matches {
		documented = append(documented, match[1])
	}

	assert.ElementsMatch(t, flagNames(t, false), documented)
}

// flagNames lists the flags of the code's own parser as --name; the answering flags (--help, --version) only when
// withAnswering is true.
func flagNames(t *testing.T, withAnswering bool) []string {
	t.Helper()

	parser, err := newParser(&cli{}, io.Discard)
	require.NoError(t, err)

	var names []string

	for _, flag := range parser.Model.Flags {
		if !withAnswering && (flag.Name == "version" || flag.Name == helpName) {
			continue
		}

		names = append(names, "--"+flag.Name)
	}

	return names
}

// awaitTimeout is the duration that the relay asked for the timeout of its shutdown.
func (r *relayRig) awaitTimeout() time.Duration {
	r.store.t.Helper()

	select {
	case d := <-r.timeoutFor:
		return d
	case <-time.After(waitBound):
		require.FailNow(r.store.t, "the relay did not start the timeout of the shutdown")

		return 0
	}
}

// readFileOf is the file reader of the rig: the one of the options, else the real one.
func readFileOf(ropts *rigOptions) func(string) ([]byte, error) {
	if ropts.readFile != nil {
		return ropts.readFile
	}

	return os.ReadFile
}
