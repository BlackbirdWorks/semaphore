package manifestreader_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/controlplane"
	"github.com/blackbirdworks/semaphore/internal/manifestreader"
)

// scaleObjects returns n distinct objects.
func scaleObjects(n int) []obj {
	objs := make([]obj, n)
	for i := range objs {
		objs[i] = obj{
			name:    fmt.Sprintf("set-%05d", i),
			content: fmt.Sprintf(`{"flag-%d":{"variations":{"a":1,"b":2},"defaultRule":{"variation":"a"}}}`, i),
		}
	}

	return objs
}

// scaleWait is the failure bound of a round of the largest manifest: the cold round hashes and decodes every object.
const scaleWait = 2 * time.Minute

func TestReader_TwentyThousandObjects(t *testing.T) {
	t.Parallel()

	const count = controlplane.DefaultMaxObjects

	store := newFakeStore()
	objs := scaleObjects(count)
	publishJSON(t, store, 1, objs...)

	rdr := startBoundedRig(
		t,
		store,
		&manifestreader.Config{FetchConcurrency: manifestreader.MaxFetchConcurrency},
		scaleWait,
	)

	// cold start: the manifest and one GET per object
	assert.Equal(t, coldCalls(t, objs...), store.takeCalls())

	snap := rdr.snapshot()
	require.NotNil(t, snap)
	assert.Len(t, snap.Objects, count)
	assert.Equal(t, manifestreader.Counts{OK: count}, rdr.reader.Status().Counts)

	// unchanged: one request per round, however many objects are listed
	const rounds = 3

	for range rounds {
		assert.Equal(t, []call{manifestCall(manifestETag(store))}, rdr.round())
	}

	assert.Nil(t, rdr.snapshot())

	// one object changes: one manifest GET and one object GET
	changed := append([]obj(nil), objs...)
	changed[count/2].content = `{"flag-changed":{}}`

	held := manifestETag(store)
	publishJSON(t, store, 2, changed...)

	assert.Equal(t, []call{manifestCall(held), objCall(t, changed[count/2])}, rdr.round())

	snap = rdr.snapshot()
	require.NotNil(t, snap)
	assert.Equal(t, uint64(2), snap.Generation)
	assert.JSONEq(t, `{"flag-changed":{}}`, string(snap.Objects[changed[count/2].name].Bytes))
	assert.Equal(t, manifestreader.Counts{OK: count}, rdr.reader.Status().Counts)
}

// publishJSON is publish with a JSON manifest, which is cheaper to write at this size.
func publishJSON(tb testing.TB, store *fakeStore, gen uint64, objs ...obj) {
	tb.Helper()

	for _, o := range objs {
		store.put(keyOf(tb, o), []byte(o.content))
	}

	data, err := controlplane.MarshalManifestJSON(manifestOf(tb, gen, objs...))
	require.NoError(tb, err)

	store.put(manifestKey, data)
}
