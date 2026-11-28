package manifestreader_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/internal/manifestreader"
)

// A refresh polls at once, with no tick of the scheduler: it reports the generation the reader holds, whether the
// round changed it, and the class of a failure.
func TestReader_Refresh(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantPoll       string
		publishGen     uint64
		wantGeneration uint64
		wantRevision   uint64
		wantChanged    bool
	}{
		{
			name:           "a new generation",
			publishGen:     2,
			wantPoll:       manifestreader.PollUpdated,
			wantGeneration: 2,
			wantRevision:   2,
			wantChanged:    true,
		},
		{name: "nothing new", wantPoll: manifestreader.PollNotModified, wantGeneration: 1, wantRevision: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := newFakeStore()
			publish(t, store, 1, threeObjects()...)

			rdr := startRig(t, store, &manifestreader.Config{})

			if tt.publishGen != 0 {
				publish(t, store, tt.publishGen, threeObjects()...)
			}

			result, err := rdr.reader.Refresh(t.Context())
			require.NoError(t, err)

			assert.Equal(t, tt.wantPoll, result.Poll.Result)
			assert.Equal(t, tt.wantGeneration, result.Generation)
			assert.Equal(t, tt.wantRevision, result.Revision, "the revision of the last snapshot")
			assert.Equal(t, tt.wantChanged, result.Changed)
		})
	}
}

// manualClock is a clock the test moves.
type manualClock struct {
	now atomic.Pointer[time.Time]
}

func (c *manualClock) Now() time.Time { return *c.now.Load() }

func (c *manualClock) advance(d time.Duration) {
	next := c.Now().Add(d)
	c.now.Store(&next)
}

// A refresh cuts the wait short, but the time that passed counts: however many refreshes interrupt the waits, the
// retry of a failed object runs once its instant came. The manifest answers 304 all along.
func TestReader_RefreshesDoNotPostponeARetry(t *testing.T) {
	t.Parallel()

	const backoffMax = 100 * time.Second

	start := time.Date(2026, time.November, 27, 12, 0, 0, 0, time.UTC)
	clk := &manualClock{}
	clk.now.Store(&start)

	store := newFakeStore()
	objs := threeObjects()
	publish(t, store, 1, objs...)
	store.failNext(keyOf(t, objs[0]), &manifestreader.FetchError{Err: errBoom, Reason: manifestreader.ReasonDenied})

	rdr := startRig(t, store, &manifestreader.Config{BackoffMax: backoffMax}, manifestreader.WithClock(clk.Now))
	store.takeCalls()

	tests := []struct {
		name          string
		advance       time.Duration
		wantObjectGet int
		wantRevision  uint64
	}{
		{name: "30s after the failure", advance: 30 * time.Second, wantRevision: 1},
		{name: "70s after it", advance: 40 * time.Second, wantRevision: 1},
		{name: "90s after it", advance: 20 * time.Second, wantRevision: 1},
		// the recovery of the object is a snapshot of its own, at the same generation
		{name: "101s after it: the retry is due", advance: 11 * time.Second, wantObjectGet: 1, wantRevision: 2},
	}

	for _, tt := range tests {
		clk.advance(tt.advance)

		result, err := rdr.reader.Refresh(t.Context())
		require.NoError(t, err, tt.name)
		assert.Equal(t, manifestreader.PollNotModified, result.Poll.Result, tt.name)
		assert.Equal(t, uint64(1), result.Generation, tt.name)
		assert.Equal(t, tt.wantRevision, result.Revision, "%s: the revision of the last snapshot", tt.name)

		// the reader entered its next wait, and took the time it started on: the clock moves only after that
		rdr.settle()

		manifestGets, objectGets := 0, 0

		for _, c := range store.takeCalls() {
			if c.Key == keyOf(t, objs[0]) {
				objectGets++
			} else if c.Key == manifestKey {
				manifestGets++
			}
		}

		assert.Equal(t, 1, manifestGets, "%s: one poll of the manifest", tt.name)
		assert.Equal(t, tt.wantObjectGet, objectGets, "%s: requests of the failed object", tt.name)
	}
}

// A refresh does not wait for the spread of the fetches: the delay that keeps replicas from fetching a new generation
// together is for the rounds of the schedule, and whoever asks for a refresh waits for its result.
func TestReader_RefreshSkipsTheFetchSpread(t *testing.T) {
	t.Parallel()

	store := newFakeStore()
	publish(t, store, 1, threeObjects()...)

	rdr := startRig(t, store, &manifestreader.Config{PollInterval: time.Hour, FetchSpread: time.Hour}, fixedJitter(0.9))
	publish(t, store, 2, threeObjects()...)

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	result, err := rdr.reader.Refresh(ctx)
	require.NoError(t, err)

	assert.Equal(t, uint64(2), result.Generation)
	assert.True(t, result.Changed)
	// the spread delay would be the jitter (0.9) of the window (an hour)
	for _, wait := range rdr.sched.waited() {
		assert.Equal(t, time.Hour, wait, "the scheduler was asked for the wait between the rounds only")
	}
}
