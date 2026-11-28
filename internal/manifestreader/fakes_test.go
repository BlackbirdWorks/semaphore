package manifestreader_test

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/internal/manifestreader"
)

// wait is the bound of every wait of a test: a missing event fails the test instead of hanging it.
const wait = 10 * time.Second

// call is one request the fake store received.
type call struct {
	Key       string
	ETag      string
	VersionID string
}

// fakeStore is an in-memory ObjectStore that records every call. The ETag of an object is derived from its
// content, so an unchanged object answers a conditional call with ErrNotModified.
type fakeStore struct {
	objects     map[string][]byte
	etags       map[string]string
	fails       map[string][]error
	gate        func(ctx context.Context, key string)
	hang        map[string]bool
	hung        chan string
	order       *orderLog
	calls       []call
	inFlight    int
	maxInFlight int
	mu          sync.Mutex
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		order: &orderLog{}, objects: map[string][]byte{}, etags: map[string]string{}, fails: map[string][]error{},
		hang: map[string]bool{}, hung: make(chan string, 64),
	}
}

func etagOf(content []byte) string {
	return `"` + string(hexSum(content)) + `"`
}

func (f *fakeStore) Get(ctx context.Context, req manifestreader.GetRequest) (manifestreader.Object, error) {
	key, etag, limit := req.Key, req.ETag, req.Limit

	f.mu.Lock()
	f.calls = append(f.calls, call{Key: key, ETag: etag, VersionID: req.VersionID})

	f.order.note(key)

	f.inFlight++
	f.maxInFlight = max(f.maxInFlight, f.inFlight)
	gate := f.gate
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()

	if gate != nil && key != manifestKey {
		gate(ctx, key)
	}

	f.mu.Lock()
	hang := f.hang[key]
	f.mu.Unlock()

	if hang {
		f.hung <- key

		<-ctx.Done()
	}

	if ctx.Err() != nil {
		return manifestreader.Object{}, ctx.Err()
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if queue := f.fails[key]; len(queue) > 0 {
		f.fails[key] = queue[1:]

		return manifestreader.Object{}, queue[0]
	}

	content, ok := f.objects[key]
	if !ok {
		return manifestreader.Object{}, manifestreader.ErrNotFound
	}

	if int64(len(content)) > limit {
		return manifestreader.Object{}, manifestreader.ErrTooLarge
	}

	if etag != "" && etag == f.etags[key] {
		return manifestreader.Object{}, manifestreader.ErrNotModified
	}

	return manifestreader.Object{Bytes: content, ETag: f.etags[key]}, nil
}

func (f *fakeStore) put(key string, content []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.objects[key] = content
	f.etags[key] = etagOf(content)
}

func (f *fakeStore) remove(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.objects, key)
	delete(f.etags, key)
}

// failNext makes the next calls of key fail with errs, one each.
func (f *fakeStore) failNext(key string, errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.fails[key] = append(f.fails[key], errs...)
}

// takeArrival returns the calls since the last take, in the order they arrived.
func (f *fakeStore) takeArrival() []call {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := f.calls
	f.calls = nil
	f.order.reset()

	return out
}

// takeCalls returns the calls since the last take with the objects in key order: concurrent downloads have no
// order of their own. The order between the manifest and the objects is not sorted away: a request for an object
// before the manifest sets badOrder, which the rig checks when it stops. Tests that assert the exact arrival
// order use takeArrival.
func (f *fakeStore) takeCalls() []call {
	return sortCalls(f.takeArrival())
}

// sortCalls orders calls the way takeCalls does.
func sortCalls(calls []call) []call {
	slices.SortStableFunc(calls, func(left, right call) int {
		am, bm := left.Key == manifestKey, right.Key == manifestKey
		if am != bm {
			if am {
				return -1
			}

			return 1
		}

		return cmp.Compare(left.Key, right.Key)
	})

	return calls
}

func (f *fakeStore) maxConcurrent() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.maxInFlight
}

func (f *fakeStore) active() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.inFlight
}

// fakeSched is a Scheduler driven by the test: Wait blocks until the test ticks it.
type fakeSched struct {
	entered chan struct{}
	release chan struct{}
	waits   []time.Duration
	mu      sync.Mutex
}

func newFakeSched() *fakeSched {
	return &fakeSched{entered: make(chan struct{}, 1024), release: make(chan struct{})}
}

func (s *fakeSched) Wait(ctx context.Context, d time.Duration) error {
	s.mu.Lock()
	s.waits = append(s.waits, d)
	s.mu.Unlock()

	s.entered <- struct{}{}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return nil
	}
}

func (s *fakeSched) waited() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]time.Duration(nil), s.waits...)
}

// rig runs a reader on the fake store and scheduler.
type rig struct {
	t      testing.TB
	store  *fakeStore
	sched  *fakeSched
	reader *manifestreader.Reader
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	// revision is the revision of the last snapshot taken.
	revision uint64
	// bound is how long a round may take before the test fails; zero means [wait]. It is a failure bound, never a pause:
	// a round of the largest manifest under the race detector on a busy machine needs more than the usual one.
	bound time.Duration
}

// limit returns the failure bound of a round.
func (r *rig) limit() time.Duration {
	if r.bound > 0 {
		return r.bound
	}

	return wait
}

// startRig runs the reader with the jitter off and waits for its first round to end.
func startRig(tb testing.TB, store *fakeStore, cfg *manifestreader.Config, opts ...manifestreader.Option) *rig {
	tb.Helper()

	own := *cfg
	own.DisableJitter = true

	rdr := launchRig(tb, store, &own, opts...)
	rdr.settle()

	return rdr
}

// startBoundedRig is startRig for a manifest whose rounds take longer than usual: every wait of the rig, the first
// round included, fails only after bound.
func startBoundedRig(tb testing.TB, store *fakeStore, cfg *manifestreader.Config, bound time.Duration) *rig {
	tb.Helper()

	own := *cfg
	own.DisableJitter = true

	rdr := launchRig(tb, store, &own)
	rdr.bound = bound
	rdr.settle()

	return rdr
}

// launchRig runs the reader and returns at once; the caller waits for the rounds.
func launchRig(
	tb testing.TB,
	store manifestreader.ObjectStore,
	cfg *manifestreader.Config,
	opts ...manifestreader.Option,
) *rig {
	tb.Helper()

	own := *cfg
	if own.Connection == "" {
		own.Connection = connControl
	}

	if own.ManifestKey == "" {
		own.ManifestKey = manifestKey
	}

	switch own.Channel {
	case "":
		own.Channel = testChannel
	case anyChannel:
		own.Channel = ""
	}

	sched := newFakeSched()
	// jitter 0: no backoff draw and no spreading, unless a test sets its own source
	opts = append([]manifestreader.Option{
		manifestreader.WithScheduler(sched), manifestreader.WithJitter(func() float64 { return 0 }),
	}, opts...)

	reader, err := manifestreader.New(store, &own, opts...)
	require.NoError(tb, err)

	ctx, cancel := context.WithCancel(tb.Context())
	fake, _ := store.(*fakeStore)
	rdr := &rig{t: tb, store: fake, sched: sched, reader: reader, cancel: cancel, done: make(chan error, 1)}

	go func() { rdr.done <- reader.Run(ctx) }()

	tb.Cleanup(rdr.stop)

	return rdr
}

// settle waits until the reader finished a round and waits for the next tick.
func (r *rig) settle() {
	r.t.Helper()

	select {
	case <-r.sched.entered:
	case <-time.After(r.limit()):
		require.FailNow(r.t, "the reader did not end its round")
	}
}

// round ticks the scheduler once and returns the calls of the round (nil when the store is not the fake).
func (r *rig) round() []call {
	r.t.Helper()

	if r.store != nil {
		r.store.takeCalls()
	}

	select {
	case r.sched.release <- struct{}{}:
	case <-time.After(r.limit()):
		require.FailNow(r.t, "the reader is not waiting for a tick")
	}

	r.settle()

	if r.store == nil {
		return nil
	}

	return r.store.takeCalls()
}

// snapshot returns the snapshot waiting in the channel, or nil. It is called after a round: the reader
// published before it started to wait, so a snapshot of that round is already in the channel.
func (r *rig) snapshot() *manifestreader.Snapshot {
	r.t.Helper()

	select {
	case snap := <-r.reader.Snapshots():
		// every snapshot has a revision of its own, and the revisions only grow: it is checked here and cleared on the
		// snapshot, the tests compare the content (the reader does not touch a snapshot it sent)
		require.Greater(r.t, snap.Revision, r.revision, "the revision of a snapshot is above the one before")

		r.revision = snap.Revision
		snap.Revision = 0

		return snap
	default:
		return nil
	}
}

// stop ends the reader and waits for Run to return; it may be called more than once.
func (r *rig) stop() {
	r.once.Do(func() {
		r.cancel()

		select {
		case err := <-r.done:
			require.NoError(r.t, err)

			if r.store != nil {
				assert.False(r.t, r.store.order.bad(), "an object was requested before the manifest of its round")
			}
		case <-time.After(wait):
			assert.Fail(r.t, "Run did not return after the context ended")
		}
	})
}

func (f *fakeStore) setHang(key string, on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.hang[key] = on
}

// peekArrival returns the requests received and not taken yet, in arrival order, without taking them.
func (f *fakeStore) peekArrival() []call {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]call(nil), f.calls...)
}

// orderLog records, for all the stores that share it, whether an object was requested before the manifest of its
// round: a round always reads the manifest first.
type orderLog struct {
	sawManifest bool
	badOrder    bool
	mu          sync.Mutex
}

func (o *orderLog) note(key string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if key == manifestKey {
		o.sawManifest = true
	} else if !o.sawManifest {
		o.badOrder = true
	}
}

func (o *orderLog) reset() {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.sawManifest = false
}

func (o *orderLog) bad() bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.badOrder
}

// shareOrder makes other record into the same arrival log as f: the order is checked across stores.
func (f *fakeStore) shareOrder(other *fakeStore) { other.order = f.order }
