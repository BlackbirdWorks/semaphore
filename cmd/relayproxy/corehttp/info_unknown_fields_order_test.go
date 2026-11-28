package corehttp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/controlsource"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/corehttp"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/flagruntime"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/model"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/relaycore"
	"github.com/blackbirdworks/semaphore/controlplane"
	"github.com/blackbirdworks/semaphore/internal/manifestreader"
)

// unknownFlagFile is a flag file with one flag that carries count unknown fields, named by prefix.
func unknownFlagFile(flag, prefix string, count int) []byte {
	var doc strings.Builder

	doc.WriteString(
		flag + ":\n  variations: {enabled: true, disabled: false}\n  defaultRule: {name: default, variation: enabled}\n",
	)

	for i := range count {
		fmt.Fprintf(&doc, "  %s%02d: 1\n", prefix, i)
	}

	return []byte(doc.String())
}

func configInfoOf(t *testing.T, base string) *model.InfoResponse {
	t.Helper()

	code, _, body := do(t, http.MethodGet, base+"/info", "", nil)
	require.Equal(t, http.StatusOK, code)

	var info model.InfoResponse
	require.NoError(t, json.Unmarshal(body, &info))
	require.NotNil(t, info.Config)

	return &info
}

// The fields of /info are sorted as strings, prefix included, and the limit of ten applies to that order: object
// "a-b" sorts before object "a" ("-" is before "/"), although "a" is the shorter name.
func TestInfo_UnknownFieldsAreSortedAfterThePrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantFields []string
		countA     int
		countAB    int
		wantCount  int
	}{
		{
			name: "one field each", countA: 1, countAB: 1, wantCount: 2,
			wantFields: []string{"a-b/flag-ab.f00", "a/flag-a.f00"},
		},
		{
			name: "the limit cuts the sorted list", countA: 8, countAB: 8, wantCount: 16,
			wantFields: []string{
				"a-b/flag-ab.f00", "a-b/flag-ab.f01", "a-b/flag-ab.f02", "a-b/flag-ab.f03", "a-b/flag-ab.f04",
				"a-b/flag-ab.f05", "a-b/flag-ab.f06", "a-b/flag-ab.f07", "a/flag-a.f00", "a/flag-a.f01",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rig := newV2With(t, &v2Options{cold: true})
			rig.flags = map[string][]byte{
				"a":   unknownFlagFile("flag-a", "f", tt.countA),
				"a-b": unknownFlagFile("flag-ab", "f", tt.countAB),
			}
			rig.sets = []controlplane.FlagSet{
				{Name: setName, Sources: []string{"a", "a-b"}, APIKeys: []string{tenantKey}},
			}
			rig.publish(t)
			rig.poll(t)

			info := configInfoOf(t, rig.url)
			assert.Equal(t, []model.UnknownFieldsInfo{
				{Kind: controlplane.KindFlags, Fields: tt.wantFields, Count: tt.wantCount},
			}, info.Config.UnknownFields)
		})
	}
}

// After a restart from the state directory the relay serves the restored documents, and /info describes them: the
// unknown fields of the manifest and of its objects are there before any poll of the store.
func TestInfo_UnknownFieldsSurviveARestartFromTheStateDirectory(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "state")
	first := newV2With(t, &v2Options{stateDir: stateDir, cold: true})

	first.flags[flagsObject] = unknownFlagFile("bool-flag", "future", 1)
	raw := nextManifestBytes(t, first)
	publishManifest(first, append(raw, []byte("extraManifest: 1\n")...))
	first.poll(t)

	want := []model.UnknownFieldsInfo{
		{Kind: controlplane.KindFlags, Fields: []string{flagsObject + "/bool-flag.future00"}, Count: 1},
		{Kind: strings.ToLower(controlplane.DocumentKindManifest), Fields: []string{"extraManifest"}, Count: 1},
	}
	require.Equal(t, want, configInfoOf(t, first.url).Config.UnknownFields)

	ctx, cancel := context.WithTimeout(t.Context(), waitBound)
	defer cancel()

	require.NoError(t, first.core.Shutdown(ctx))

	// the store is empty for the second start: only the state directory has the documents
	second := newV2With(t, &v2Options{stateDir: stateDir, cold: true, dir: filepath.Join(t.TempDir(), "empty")})
	second.awaitGeneration(t, 1)

	require.Equal(t, manifestreader.RestoreRestored, second.core.Status().Source.Reader.State.Restore)
	assert.Equal(t, want, configInfoOf(t, second.url).Config.UnknownFields)
}

// objectsWithUnknownFields builds the status of count flag objects, each with unknown fields, with names that sort
// in both orders (digits, dashes and letters).
func objectsWithUnknownFields(count int) map[string]*flagruntime.ObjectStatus {
	objects := make(map[string]*flagruntime.ObjectStatus, count)

	for i := range count {
		name := fmt.Sprintf("o%04d", i)
		if i%3 == 0 {
			name = fmt.Sprintf("o%04d-x", i/2)
		}

		objects[name] = &flagruntime.ObjectStatus{
			Name: name, Kind: controlplane.KindFlags, UnknownFields: 2,
			UnknownFieldPaths: []string{fmt.Sprintf("flag-%d.b", i%7), fmt.Sprintf("flag-%d.a", i%5)},
		}
	}

	return objects
}

func statusOf(objects map[string]*flagruntime.ObjectStatus) *relaycore.Status {
	return &relaycore.Status{Source: controlsource.Status{Runtime: &flagruntime.Status{Objects: objects}}}
}

// The ten paths are the ten smallest of all of them, in order: the same as a sort of every path, with an
// allocation count that does not grow with the number of objects.
//
// The test is serial: AllocsPerRun counts the allocations of the whole process, so no parallel test may run during it.
// t.Chdir makes the test serial for the runner and for paralleltest alike.
func TestUnknownFieldsInfoKeepsTheTenSmallestPaths(t *testing.T) {
	t.Chdir(t.TempDir())

	reference := func(objects map[string]*flagruntime.ObjectStatus) ([]string, int) {
		var all []string

		count := 0

		for _, obj := range objects {
			count += obj.UnknownFields

			for _, field := range obj.UnknownFieldPaths {
				all = append(all, obj.Name+"/"+field)
			}
		}

		slices.Sort(all)

		return all[:min(10, len(all))], count
	}

	tests := []struct {
		name    string
		objects int
	}{
		{name: "few", objects: 4},
		{name: "five thousand", objects: 5000},
	}

	allocs := make([]float64, len(tests))

	for i, tt := range tests {
		objects := objectsWithUnknownFields(tt.objects)
		status := statusOf(objects)
		wantFields, wantCount := reference(objects)

		got := corehttp.UnknownFieldsInfo(status)
		require.Len(t, got, 1, tt.name)
		assert.Equal(t, wantFields, got[0].Fields, tt.name)
		assert.Equal(t, wantCount, got[0].Count, tt.name)

		allocs[i] = testing.AllocsPerRun(5, func() { _ = corehttp.UnknownFieldsInfo(status) })
	}

	assert.InDelta(t, allocs[0], allocs[1], 8, "allocations do not grow with the number of objects: %v", allocs)
}
