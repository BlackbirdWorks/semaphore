package ofrepconformance_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/bootstrap"
)

const (
	storeDirMode  = 0o750
	storeFileMode = 0o600
)

// flagSetSpec is one flag set of a published store: its name, its API keys (none: the public flag set) and the
// name of the flag document it serves.
type flagSetSpec struct {
	Name     string   `json:"name"`
	Document string   `json:"document"`
	Keys     []string `json:"keys"`
}

// liveStore is a file store that tests publish new generations to, while relays follow it.
type liveStore struct {
	documents  map[string]string
	dir        string
	generation int
}

func newLiveStore(t *testing.T, documents map[string]string) *liveStore {
	t.Helper()

	return &liveStore{dir: t.TempDir(), documents: documents}
}

// object writes content addressed content and returns its key, hash and size.
func (s *liveStore) object(t *testing.T, content string) (string, string, int) {
	t.Helper()

	sum := sha256.Sum256([]byte(content))
	hexSum := hex.EncodeToString(sum[:])
	rel := filepath.Join("objects", "sha256", hexSum[:2], hexSum+".yaml")
	full := filepath.Join(s.dir, rel)

	require.NoError(t, os.MkdirAll(filepath.Dir(full), storeDirMode))
	require.NoError(t, os.WriteFile(full, []byte(content), storeFileMode))

	return filepath.ToSlash(rel), hexSum, len(content)
}

// publish writes the next generation: the flag sets and the documents they serve. The manifest is replaced last and
// atomically, so a relay never reads a half written generation.
func (s *liveStore) publish(t *testing.T, sets []flagSetSpec) {
	t.Helper()

	s.generation++

	var (
		flagSets  strings.Builder
		manifest  strings.Builder
		documents strings.Builder
	)

	flagSets.WriteString("apiVersion: semaphore/v1\nkind: FlagSet\nflagSets:\n")

	for _, set := range sets {
		flagSets.WriteString("  - name: " + set.Name + "\n    sources:\n      - flags-" + set.Name + "\n")

		if len(set.Keys) > 0 {
			flagSets.WriteString("    apiKeys:\n")

			for _, key := range set.Keys {
				flagSets.WriteString("      - " + key + "\n")
			}
		}

		content, known := s.documents[set.Document]
		require.True(t, known, "unknown flag document %q", set.Document)

		key, sum, size := s.object(t, content)
		documents.WriteString(objectEntry("flags-"+set.Name, "flags", key, sum, size))
	}

	key, sum, size := s.object(t, flagSets.String())

	manifest.WriteString("apiVersion: semaphore/v1\nkind: Manifest\nchannel: stable\ngeneration: " +
		strconv.Itoa(s.generation) + "\nobjects:\n")
	manifest.WriteString(objectEntry("flagsets", "flagSets", key, sum, size))
	manifest.WriteString(documents.String())

	manifestPath := filepath.Join(s.dir, "manifests", "stable.yaml")
	require.NoError(t, os.MkdirAll(filepath.Dir(manifestPath), storeDirMode))

	tmp := manifestPath + ".tmp"
	require.NoError(t, os.WriteFile(tmp, []byte(manifest.String()), storeFileMode))
	require.NoError(t, os.Rename(tmp, manifestPath))
}

func objectEntry(name, kind, key, sum string, size int) string {
	return "  - name: " + name + "\n    kind: " + kind + "\n    connection: control\n    key: " + key +
		"\n    format: yaml\n    checksum:\n      algorithm: sha256\n      value: " + strconv.Quote(sum) +
		"\n    size: " + strconv.Itoa(size) + "\n"
}

// relayOptions are the settings of a relay that follows a live store.
type relayOptions struct {
	Poll string `json:"poll"`
	// AdminKeys authenticate and are refused by the flag set endpoints.
	AdminKeys []string `json:"adminKeys"`
	// Stream makes the relay advertise its own flag change stream in the bulk evaluation.
	Stream bool `json:"stream"`
}

// startLiveRelay starts a relay that follows the store, from the generation the store is at.
func startLiveRelay(t *testing.T, store *liveStore, opts relayOptions) string {
	t.Helper()

	cfgPath := filepath.Join("..", "bootstrap.yaml")

	if len(opts.AdminKeys) > 0 {
		raw, err := os.ReadFile(cfgPath)
		require.NoError(t, err)

		cfgPath = filepath.Join(t.TempDir(), "bootstrap.yaml")
		require.NoError(t, os.WriteFile(cfgPath,
			append(raw, []byte("auth:\n  adminKeys: ["+strings.Join(opts.AdminKeys, ", ")+"]\n")...), storeFileMode))
	}

	load := bootstrap.LoadOptions{ReadFile: os.ReadFile, LookupEnv: noEnv}

	cfg, err := bootstrap.Load(&bootstrap.Overrides{Config: cfgPath}, load)
	require.NoError(t, err)

	conn := cfg.Connections["control"]
	require.NotNil(t, conn.File)
	conn.File.Path = store.dir

	poll := time.Second
	if opts.Poll != "" {
		poll, err = time.ParseDuration(opts.Poll)
		require.NoError(t, err)
	}

	interval := bootstrap.Duration(poll)
	cfg.ControlPlane.Manifest.PollInterval = &interval

	return startRelayFromConfig(t, cfg, load, opts.Stream, store.generation)
}
