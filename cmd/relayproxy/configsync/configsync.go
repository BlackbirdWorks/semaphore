package configsync

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/service"
	"github.com/blackbirdworks/semaphore/modules/core/clock"
)

const (
	// Wait is the longest a request waits for the control plane check that its flagConfigEtag or
	// flagConfigLastModified asks for. The request context ends the wait earlier.
	Wait = 2 * time.Second
	// MinInterval is the shortest time between two control plane checks that requests start: a client that names a
	// configuration that never comes (a made-up value) costs the control plane one check per interval, for the
	// whole relay, however many requests it sends. A request that arrives while a check runs waits for that check.
	MinInterval = time.Second

	// QueryETag is the query parameter of the OFREP bulk evaluation that carries the etag of the change notification.
	QueryETag = "flagConfigEtag"
	// QueryLastModified is the query parameter that carries the lastModified of the change notification.
	QueryLastModified = "flagConfigLastModified"
)

// Target is the configuration a request names. The zero value names none.
type Target struct {
	etag string
	// modified is the unix time in seconds of flagConfigLastModified, valid when hasModified is set.
	modified    int64
	hasModified bool
}

// FromRequest reads the parameters of the request. It allocates nothing when the request carries neither of them
// (the common case): the raw query is searched before it is parsed. A value that cannot be read is as if it was not
// sent, a request is never refused for it.
func FromRequest(r *http.Request) (Target, bool) {
	raw := r.URL.RawQuery
	if raw == "" || !strings.Contains(raw, "flagConfig") {
		return Target{}, false
	}

	query := r.URL.Query()
	target := Target{etag: query.Get(QueryETag)}

	target.modified, target.hasModified = parseLastModified(query.Get(QueryLastModified))

	return target, target.etag != "" || target.hasModified
}

// parseLastModified reads unix seconds or an ISO 8601 date-time; anything else is as if it was not sent.
func parseLastModified(value string) (int64, bool) {
	if value == "" {
		return 0, false
	}

	secs, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		return secs, secs >= 0
	}

	when, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return 0, false
	}

	return when.Unix(), true
}

// Behind reports whether the flag set serves another configuration than the one named. The ETag decides when it is
// sent; otherwise the configuration is behind when it was built before the named date (whole seconds).
func (t Target) Behind(set service.ConfigVersioned) bool {
	if t.etag != "" {
		return t.etag != set.ConfigETag()
	}

	return t.hasModified && t.modified > set.ConfigModified().Unix()
}

// Limiter allows one start per [MinInterval]. The zero value is ready to use.
type Limiter struct {
	last    time.Time
	mu      sync.Mutex
	started bool
}

// Allow reports whether a check may start at now, and counts it as started when it may.
func (l *Limiter) Allow(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.started && now.Sub(l.last) < MinInterval {
		return false
	}

	l.last, l.started = now, true

	return true
}

// Gate starts the control plane checks of the requests: at most one per [MinInterval], and requests share the
// check that runs. The zero value is ready to use.
type Gate struct {
	limiter  Limiter
	mu       sync.Mutex
	inFlight int
}

// CheckNow asks the control plane for the latest configuration and waits for the answer, at most [Wait] and never
// longer than ctx. It reports whether it waited for a check. A request that finds no check running within
// [MinInterval] of the last one started returns false at once.
func (g *Gate) CheckNow(ctx context.Context, refresher service.Refresher) bool {
	if !g.enter() {
		return false
	}

	defer g.leave()

	waitCtx, cancel := context.WithTimeout(ctx, Wait)
	defer cancel()

	// the check itself runs in the poll loop of the relay and is shared with every other caller: the context only
	// ends this request's wait. The outcome does not matter, what the relay serves afterwards is what it answers.
	_, _ = refresher.Refresh(waitCtx)

	return true
}

func (g *Gate) enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.inFlight == 0 && !g.limiter.Allow(clock.Now()) {
		return false
	}

	g.inFlight++

	return true
}

func (g *Gate) leave() {
	g.mu.Lock()
	g.inFlight--
	g.mu.Unlock()
}
