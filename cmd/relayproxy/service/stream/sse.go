package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/r3labs/sse/v2"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/model"
	"github.com/blackbirdworks/semaphore/modules/core/clock"
	"github.com/blackbirdworks/semaphore/notifier"
)

// SSEService is the service interface that handles SSE connections.
// It dispatches OFREP flag change events (OpenFeature ADR-0008) scoped to specific
// flagset names so that only clients connected with an API key belonging to a given
// flagset receive the corresponding events.
type SSEService interface {
	// BroadcastFlagChanges sends an OFREP refetchEvaluation event to all clients
	// subscribed to the given flagset stream.
	BroadcastFlagChanges(flagsetName string, diff notifier.DiffCache) error
	// BroadcastConfigChange is BroadcastFlagChanges for a change whose configuration is known: the event carries
	// its identity, which a provider names in the re-fetch.
	BroadcastConfigChange(flagsetName string, config ConfigIdentity) error
	// ServeHTTP handles incoming SSE client connections. The request must
	// carry a "stream" query parameter set to the target flagset name.
	ServeHTTP(w http.ResponseWriter, r *http.Request)
	// ServeKeyed is ServeHTTP for a client that authenticated with an API key: the service keeps an identifier of the
	// key (never the key), so that [SSEService.DisconnectRevoked] can end the stream when the key does not route to the
	// flag set anymore. The stream is still shared by every client of the flag set: a message is published once.
	// stillValid is called once the client is registered: a key that was revoked while the request was being set up is
	// then refused (a revalidation that ran before the registration could not see the client).
	ServeKeyed(w http.ResponseWriter, r *http.Request, apiKey string, stillValid func() bool)
	// DisconnectRevoked ends the streams of the clients whose key is not in routes anymore or routes to another flag
	// set than the one of the stream. It cancels their requests and never waits for a client.
	DisconnectRevoked(routes KeyRoutes)
	// DisconnectAnonymous closes the streams of the clients without a key whose flag set is not keep; an empty keep
	// closes all of them. A client with a key is never touched.
	DisconnectAnonymous(keep string)
	// Connections is the number of connected clients.
	Connections() int
	// SetOnSubscribe registers a callback invoked when a client subscribes to
	// any stream. The streamID matches the flagset name used in BroadcastFlagChanges.
	SetOnSubscribe(fn func(streamID string))
	// Close shuts down the SSE server and disconnects all clients.
	Close()
}

// AnonymousStreamPrefix starts the stream ID of the clients that have no API key: they have a stream of their own
// next to the one of the flag set, so that the relay can close them when the anonymous access ends.
const AnonymousStreamPrefix = "anonymous:"

// SSEServiceImpl is the implementation of SSEService backed by r3labs/sse.
type SSEServiceImpl struct {
	server          *sse.Server
	activeStreams   map[string]int
	anonymous       map[*anonymousClient]struct{}
	keyed           map[*keyedClient]struct{}
	done            chan struct{}
	now             func() time.Time
	heartbeats      sync.WaitGroup
	hasher          keyHasher
	writeTimeout    time.Duration
	closeOnce       sync.Once
	muActiveStreams sync.Mutex
}

// NewSSEService creates a new SSEService backed by r3labs/sse.
func NewSSEService(opts ...Option) *SSEServiceImpl {
	server := sse.New()
	server.AutoReplay = false
	server.AutoStream = true

	svc := &SSEServiceImpl{
		server:        server,
		activeStreams: map[string]int{},
		anonymous:     map[*anonymousClient]struct{}{},
		keyed:         map[*keyedClient]struct{}{},
		hasher:        newKeyHasher(),
		writeTimeout:  newOptions(opts...).writeTimeout,
		now:           newOptions(opts...).now,
		done:          make(chan struct{}),
	}
	if interval := newOptions(opts...).sseHeartbeatInterval; interval > 0 {
		svc.heartbeats.Go(func() { svc.sendHeartbeats(interval) })
	}

	return svc
}

// BroadcastFlagChanges only notifies that the flags changed, the diff is not sent:
// providers re-fetch their evaluations when they receive the event.
func (s *SSEServiceImpl) BroadcastFlagChanges(flagsetName string, _ notifier.DiffCache) error {
	return s.BroadcastConfigChange(flagsetName, ConfigIdentity{ModifiedAt: clock.Now()})
}

// ConfigIdentity names the configuration of a flag set that a change leads to: the ETag the OFREP bulk evaluation
// compares with the one a replica serves, and the date the configuration was built.
type ConfigIdentity struct {
	ModifiedAt time.Time
	ETag       string
}

// BroadcastConfigChange sends the refetchEvaluation event with the identity of the configuration.
func (s *SSEServiceImpl) BroadcastConfigChange(flagsetName string, config ConfigIdentity) error {
	now := clock.Now()
	if config.ModifiedAt.IsZero() {
		config.ModifiedAt = now
	}

	data, err := json.Marshal(model.OFREPSSEEvent{
		Type:         model.OFREPSSEEventTypeRefetchEvaluation,
		ETag:         config.ETag,
		LastModified: config.ModifiedAt.Unix(),
	})
	if err != nil {
		return fmt.Errorf("sse: failed to marshal OFREP event for stream %q: %w", flagsetName, err)
	}

	for _, streamID := range []string{flagsetName, AnonymousStreamPrefix + flagsetName} {
		s.server.Publish(streamID, &sse.Event{
			ID:    []byte(strconv.FormatInt(now.UnixNano(), 10)),
			Event: []byte("message"),
			Data:  data,
		})
	}

	return nil
}

// Connections is the number of connected clients.
func (s *SSEServiceImpl) Connections() int {
	s.muActiveStreams.Lock()
	defer s.muActiveStreams.Unlock()

	total := 0
	for _, count := range s.activeStreams {
		total += count
	}

	return total
}

// DisconnectAnonymous ends the requests of the clients without a key whose flag set is not keep. It cancels their
// request contexts, so the library ends each subscription itself: removing the stream under them would leave one
// goroutine of the library blocked per client.
func (s *SSEServiceImpl) DisconnectAnonymous(keep string) {
	s.muActiveStreams.Lock()

	var revoked []context.CancelFunc

	for client := range s.anonymous {
		name := strings.TrimPrefix(client.streamID, AnonymousStreamPrefix)
		if name != keep || keep == "" {
			revoked = append(revoked, client.cancel)
		}
	}

	s.muActiveStreams.Unlock()

	for _, cancel := range revoked {
		cancel()
	}
}

// anonymousClient is a connected client without a key: the way to end its request.
type anonymousClient struct {
	cancel   context.CancelFunc
	streamID string
}

// keyedClient is a connected client with an API key: the identifier of the key, the flag set of its stream, and the
// way to end its request.
type keyedClient struct {
	cancel   context.CancelFunc
	rc       *http.ResponseController
	streamID string
	keyID    uint64
	revoked  atomic.Bool
	// mu guards done: the handler sets it before it returns, and a revocation touches the response only while it is
	// not set (the server clears the state of a response when its request ends).
	mu   sync.Mutex
	done bool
}

// streamWriter is the writer of one stream: it sets the write deadline before every write and clears it after, and
// drops what is written to a client whose key was revoked. A deadline left armed would reset a quiet stream (HTTP/2)
// between two heartbeats.
type streamWriter struct {
	http.ResponseWriter

	rc      *http.ResponseController
	revoked *atomic.Bool
	now     func() time.Time
	timeout time.Duration
}

// revokedDeadline is the write deadline of a revoked stream: in the past, so a blocked write ends at once.
func revokedDeadline() time.Time { return time.Unix(1, 0) }

func (w *streamWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *streamWriter) Write(data []byte) (int, error) {
	if !w.arm() {
		return 0, io.ErrClosedPipe
	}

	defer w.disarm()

	return w.ResponseWriter.Write(data) //nolint:wrapcheck // the writer of the response, errors are the client's
}

func (w *streamWriter) Flush() {
	if !w.arm() {
		return
	}

	defer w.disarm()

	_ = w.rc.Flush()
}

// arm sets the deadline of the next write; it reports false when the client was revoked.
func (w *streamWriter) arm() bool {
	if w.revoked != nil && w.revoked.Load() {
		return false
	}

	// real time on purpose: a network deadline is enforced by the runtime poller, not by clock.Now. A writer that does
	// not support deadlines (a test recorder) is written to as before.
	_ = w.rc.SetWriteDeadline(w.now().Add(w.timeout))

	if w.revoked != nil && w.revoked.Load() {
		// revoked while the deadline was set: the deadline of the revocation must be the last one
		_ = w.rc.SetWriteDeadline(revokedDeadline())

		return false
	}

	return true
}

// teardown sets the deadline of the write that the server makes when the handler returns (the last chunk): always a
// deadline, the one of the revocation when the stream was revoked, so that a client that does not read never holds the
// connection.
func (w *streamWriter) teardown() {
	if w.revoked == nil || !w.revoked.Load() {
		_ = w.rc.SetWriteDeadline(w.now().Add(w.timeout))
	}

	// checked after the set: a revocation that comes in between must have the last word
	if w.revoked != nil && w.revoked.Load() {
		_ = w.rc.SetWriteDeadline(revokedDeadline())
	}
}

// disarm clears the deadline after a write, unless the stream was revoked meanwhile.
func (w *streamWriter) disarm() {
	if w.revoked != nil && w.revoked.Load() {
		return
	}

	_ = w.rc.SetWriteDeadline(time.Time{})

	if w.revoked != nil && w.revoked.Load() {
		_ = w.rc.SetWriteDeadline(revokedDeadline())
	}
}

// DisconnectRevoked ends the requests of the clients whose key does not route to their flag set anymore. The requests
// are cancelled after the lock is released, and a cancel never waits for the client.
func (s *SSEServiceImpl) DisconnectRevoked(routes KeyRoutes) {
	s.muActiveStreams.Lock()
	keyed := len(s.keyed) > 0
	s.muActiveStreams.Unlock()

	if !keyed {
		return
	}

	live := s.hasher.liveIDs(routes)

	s.muActiveStreams.Lock()

	var revoked []*keyedClient

	for client := range s.keyed {
		if set, ok := live[client.keyID]; !ok || set != client.streamID {
			client.revoked.Store(true)

			revoked = append(revoked, client)

			delete(s.keyed, client)
		}
	}

	s.muActiveStreams.Unlock()

	for _, client := range revoked {
		// a write that is blocked on the network ends now, and the request is cancelled; the response of a request that
		// has returned is not touched
		client.mu.Lock()

		if !client.done {
			_ = client.rc.SetWriteDeadline(revokedDeadline())
		}

		client.mu.Unlock()

		client.cancel()
	}
}

// ServeKeyed serves a client that authenticated with an API key.
func (s *SSEServiceImpl) ServeKeyed(w http.ResponseWriter, r *http.Request, apiKey string, stillValid func() bool) {
	ctx, cancel := context.WithCancel(r.Context())
	client := &keyedClient{
		cancel: cancel, streamID: r.URL.Query().Get("stream"), keyID: s.hasher.id(apiKey),
		rc: http.NewResponseController(w),
	}

	s.muActiveStreams.Lock()
	s.keyed[client] = struct{}{}
	s.muActiveStreams.Unlock()

	defer func() {
		client.mu.Lock()
		client.done = true
		client.mu.Unlock()

		s.muActiveStreams.Lock()
		delete(s.keyed, client)
		s.muActiveStreams.Unlock()

		cancel()
	}()

	if !stillValid() {
		return
	}

	s.serve(w, r.WithContext(ctx), &client.revoked)
}

// ServeHTTP serves a client without a key.
func (s *SSEServiceImpl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.serve(w, r, nil)
}

// SetOnSubscribe sets the callback called when a client subscribes to a stream.
func (s *SSEServiceImpl) SetOnSubscribe(fn func(streamID string)) {
	s.server.OnSubscribe = func(streamID string, _ *sse.Subscriber) {
		fn(strings.TrimPrefix(streamID, AnonymousStreamPrefix))
	}
}

// Close closes all the SSE streams.
func (s *SSEServiceImpl) Close() {
	s.closeOnce.Do(func() { close(s.done) })
	s.heartbeats.Wait()
	s.server.Close()
}

func (s *SSEServiceImpl) serve(w http.ResponseWriter, r *http.Request, revoked *atomic.Bool) {
	streamID := r.URL.Query().Get("stream")

	s.trackStream(streamID, 1)
	defer s.trackStream(streamID, -1)

	if strings.HasPrefix(streamID, AnonymousStreamPrefix) {
		ctx, cancel := context.WithCancel(r.Context())
		client := &anonymousClient{cancel: cancel, streamID: streamID}

		s.muActiveStreams.Lock()
		s.anonymous[client] = struct{}{}
		s.muActiveStreams.Unlock()

		defer func() {
			s.muActiveStreams.Lock()
			delete(s.anonymous, client)
			s.muActiveStreams.Unlock()

			cancel()
		}()

		r = r.WithContext(ctx)
	}

	writer := &streamWriter{
		ResponseWriter: w,
		rc:             http.NewResponseController(w),
		revoked:        revoked,
		timeout:        s.writeTimeout,
		now:            s.now,
	}
	w = writer

	// the server writes the last chunk of the response when the handler returns, after the last write cleared its
	// deadline: a client that stopped reading would hold the connection. The teardown keeps a deadline (the one of the
	// revocation when there is one).
	defer writer.teardown()

	if flusher, ok := w.(http.Flusher); ok {
		w = &connectedCommentWriter{ResponseWriter: w, flusher: flusher}
	}

	s.server.ServeHTTP(w, r)
}

func (s *SSEServiceImpl) trackStream(streamID string, delta int) {
	s.muActiveStreams.Lock()
	defer s.muActiveStreams.Unlock()

	s.activeStreams[streamID] += delta
	if s.activeStreams[streamID] <= 0 {
		delete(s.activeStreams, streamID)
	}
}

// sendHeartbeats periodically sends an SSE comment on every stream with connected clients,
// so that proxies and load balancers do not close the connections while no flag changes.
func (s *SSEServiceImpl) sendHeartbeats(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.muActiveStreams.Lock()

			streamIDs := make([]string, 0, len(s.activeStreams))
			for streamID := range s.activeStreams {
				streamIDs = append(streamIDs, streamID)
			}
			s.muActiveStreams.Unlock()

			for _, streamID := range streamIDs {
				s.server.TryPublish(streamID, &sse.Event{Comment: []byte("heartbeat")})
			}
		}
	}
}

// connectedCommentWriter sends an SSE comment right after the response headers.
// Some browsers (e.g. Firefox) fire the EventSource "open" event only once the first
// bytes of the body are received: without it, clients stay "connecting" until the first event.
type connectedCommentWriter struct {
	http.ResponseWriter

	flusher http.Flusher
	status  int
	sent    bool
}

func (w *connectedCommentWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *connectedCommentWriter) Flush() {
	if !w.sent && w.status == http.StatusOK {
		w.sent = true
		_, _ = io.WriteString(w.ResponseWriter, ": connected\n\n")
	}

	w.flusher.Flush()
}
