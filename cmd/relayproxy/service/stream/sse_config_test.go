package stream_test

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/model"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/service/stream"
	"github.com/blackbirdworks/semaphore/modules/core/testhelpers/clocktest"
)

// A change that names its configuration sends the identity and the date of that configuration, a change that does
// not sends the date of the broadcast.
func TestSSEService_BroadcastConfigChange(t *testing.T) {
	t.Parallel()

	built := time.Unix(1771622898, 0)

	tests := []struct {
		name   string
		config stream.ConfigIdentity
		want   model.OFREPSSEEvent
	}{
		{
			name:   "the identity and the date of the configuration",
			config: stream.ConfigIdentity{ETag: "3f9a1c", ModifiedAt: built},
			want: model.OFREPSSEEvent{
				Type: model.OFREPSSEEventTypeRefetchEvaluation, ETag: "3f9a1c", LastModified: built.Unix(),
			},
		},
		{
			name:   "no date: the date of the broadcast",
			config: stream.ConfigIdentity{ETag: "abc"},
			want: model.OFREPSSEEvent{
				Type: model.OFREPSSEEventTypeRefetchEvaluation, ETag: "abc", LastModified: clocktest.Instant().Unix(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sseService := stream.NewSSEService()
			defer sseService.Close()

			srv := httptest.NewServer(http.HandlerFunc(sseService.ServeHTTP))
			defer srv.Close()

			subscribed := make(chan struct{}, 1)

			sseService.SetOnSubscribe(func(string) { subscribed <- struct{}{} })

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+streamFlagsetA, http.NoBody)
			require.NoError(t, err)

			resp, err := (&http.Client{}).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			<-subscribed

			require.NoError(t, sseService.BroadcastConfigChange("flagsetA", tt.config))

			var data string

			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				if line, ok := strings.CutPrefix(scanner.Text(), "data: "); ok {
					data = line

					break
				}
			}

			var got model.OFREPSSEEvent

			require.NoError(t, json.Unmarshal([]byte(data), &got))
			assert.Equal(t, tt.want, got)
		})
	}
}
