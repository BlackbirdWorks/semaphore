package configsync_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/configsync"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/service"
)

type versioned struct {
	modified time.Time
	etag     string
}

func (v versioned) ConfigETag() string        { return v.etag }
func (v versioned) ConfigModified() time.Time { return v.modified }

func TestFromRequestAndBehind(t *testing.T) {
	t.Parallel()

	served := versioned{etag: "abc", modified: time.Unix(1000, 0)}

	tests := []struct {
		name       string
		query      string
		wantNamed  bool
		wantBehind bool
	}{
		{name: "no query"},
		{name: "other parameters", query: "a=1&b=2"},
		{name: "the same etag", query: "flagConfigEtag=abc", wantNamed: true},
		{name: "another etag", query: "flagConfigEtag=def", wantNamed: true, wantBehind: true},
		{name: "the etag decides over the date", query: "flagConfigEtag=abc&flagConfigLastModified=2000",
			wantNamed: true},
		{name: "a newer date in seconds", query: "flagConfigLastModified=1001", wantNamed: true, wantBehind: true},
		{name: "the same date", query: "flagConfigLastModified=1000", wantNamed: true},
		{name: "an older date", query: "flagConfigLastModified=5", wantNamed: true},
		{name: "a newer ISO date", query: "flagConfigLastModified=1970-01-01T00:20:01Z", wantNamed: true,
			wantBehind: true},
		{name: "an older ISO date", query: "flagConfigLastModified=1970-01-01T00:00:01Z", wantNamed: true},
		{name: "a negative date is ignored", query: "flagConfigLastModified=-5"},
		{name: "a date that is not a date is ignored", query: "flagConfigLastModified=yesterday"},
		{name: "an empty etag is ignored", query: "flagConfigEtag="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ofrep/v1/evaluate/flags?"+tt.query,
				http.NoBody)

			target, named := configsync.FromRequest(req)
			assert.Equal(t, tt.wantNamed, named)
			assert.Equal(t, tt.wantBehind, target.Behind(served))
		})
	}
}

func TestFromRequestDoesNotAllocateWithoutTheParameters(t *testing.T) {
	t.Parallel()

	for _, query := range []string{"", "?a=1"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ofrep/v1/evaluate/flags"+query,
			http.NoBody)

		res := testing.Benchmark(func(b *testing.B) {
			b.Helper()
			b.ReportAllocs()

			for b.Loop() {
				_, _ = configsync.FromRequest(req)
			}
		})
		assert.Zero(t, res.AllocsPerOp(), "query %q", query)
	}
}

type countingRefresher struct {
	deadline atomic.Pointer[time.Time]
	err      atomic.Pointer[errBox]
	calls    atomic.Int64
}

func (c *countingRefresher) Refresh(ctx context.Context) (service.RefreshOutcome, error) {
	c.calls.Add(1)

	if deadline, ok := ctx.Deadline(); ok {
		c.deadline.Store(&deadline)
	}

	c.err.Store(&errBox{err: ctx.Err()})

	return service.RefreshOutcome{}, nil
}

type errBox struct{ err error }

func TestLimiterAllowsOneStartPerInterval(t *testing.T) {
	t.Parallel()

	var limiter configsync.Limiter

	start := time.Unix(5000, 0)
	offsets := []time.Duration{
		0, 400 * time.Millisecond, 999 * time.Millisecond, // the first starts, the next two are inside its interval
		time.Second, 1400 * time.Millisecond, // one interval after the first start
		2 * time.Second, // one interval after the second start
	}
	want := []bool{true, false, false, true, false, true}

	got := make([]bool, 0, len(want))
	for _, offset := range offsets {
		got = append(got, limiter.Allow(start.Add(offset)))
	}

	assert.Equal(t, want, got)
}

func TestGateStartsOneCheckForManyRequests(t *testing.T) {
	t.Parallel()

	var (
		gate      configsync.Gate
		refresher countingRefresher
	)

	// the clock of this package is frozen: no interval passes between the requests
	got := make([]bool, 0, 50)
	for range 50 {
		got = append(got, gate.CheckNow(t.Context(), &refresher))
	}

	assert.Equal(t, 1, countTrue(got))
	assert.EqualValues(t, 1, refresher.calls.Load(), "one control plane check, however many requests")
}

func countTrue(values []bool) int {
	total := 0

	for _, v := range values {
		if v {
			total++
		}
	}

	return total
}

func TestGateBoundsTheWait(t *testing.T) {
	t.Parallel()

	var (
		gate      configsync.Gate
		refresher countingRefresher
	)

	require.True(t, gate.CheckNow(t.Context(), &refresher))

	deadline := refresher.deadline.Load()
	require.NotNil(t, deadline, "the check has a deadline")
	assert.LessOrEqual(t, time.Until(*deadline), configsync.Wait)
	assert.Greater(t, time.Until(*deadline), configsync.Wait-time.Minute/2)
}

func TestGateRespectsTheRequestContext(t *testing.T) {
	t.Parallel()

	var (
		gate      configsync.Gate
		refresher countingRefresher
	)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	assert.True(t, gate.CheckNow(ctx, &refresher))

	box := refresher.err.Load()
	require.NotNil(t, box)
	require.ErrorIs(t, box.err, context.Canceled)
}

// blockingRefresher holds every check until it is released.
type blockingRefresher struct {
	release chan struct{}
	entered chan struct{}
	calls   atomic.Int64
}

func (b *blockingRefresher) Refresh(context.Context) (service.RefreshOutcome, error) {
	b.calls.Add(1)

	b.entered <- struct{}{}

	<-b.release

	return service.RefreshOutcome{}, nil
}

func TestGateSharesTheCheckThatRuns(t *testing.T) {
	t.Parallel()

	var (
		gate      configsync.Gate
		wait      sync.WaitGroup
		refresher = &blockingRefresher{release: make(chan struct{}), entered: make(chan struct{}, 2)}
		results   = make(chan bool, 2)
	)

	// the first request starts a check and waits for it
	wait.Go(func() { results <- gate.CheckNow(t.Context(), refresher) })
	<-refresher.entered

	// a request that arrives while it runs waits for it too, however recent the last start was
	wait.Go(func() { results <- gate.CheckNow(t.Context(), refresher) })
	<-refresher.entered

	close(refresher.release)
	wait.Wait()

	assert.True(t, <-results)
	assert.True(t, <-results)
	assert.EqualValues(t, 2, refresher.calls.Load())
}
