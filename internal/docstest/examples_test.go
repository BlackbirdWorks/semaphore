package docstest_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/bootstrap"
	"github.com/blackbirdworks/semaphore/controlplane"
	"github.com/blackbirdworks/semaphore/testhelpers/gopherstack"
)

// loadOptions resolves every secret reference to a value, so that a documented file is checked for its shape.
func loadOptions() bootstrap.LoadOptions {
	return bootstrap.LoadOptions{
		LookupEnv: func(name string) (string, bool) {
			// the settings variables of the relay stay unset; the secrets of the examples get a value
			if strings.HasPrefix(name, "SEMAPHORE_") {
				return "", false
			}

			return "x", true
		},
		ReadFile: func(string) ([]byte, error) { return []byte("x"), nil },
	}
}

// Every bootstrap file of the documentation loads with the validation of the relay: a fenced yaml block that starts
// with `version: 2`, and a here-document of a shell block that does.
func TestDocumentedBootstrapFilesLoad(t *testing.T) {
	t.Parallel()

	found := 0

	for _, page := range pages(t) {
		for _, block := range fences(t, page) {
			var files []string

			switch block.lang {
			case langYAML, "yml":
				files = []string{block.body}
			case langShell, "bash", "sh":
				files = heredocs(block.body, "YAML")
			}

			for _, file := range files {
				if !strings.HasPrefix(strings.TrimSpace(stripComments(file)), "version: 2") {
					continue
				}

				found++

				t.Run(block.name(), func(t *testing.T) {
					t.Parallel()

					_, err := bootstrap.Parse([]byte(file), nil, loadOptions())
					require.NoError(t, err)
				})
			}
		}
	}

	assert.Greater(t, found, 20, "the pages show bootstrap files")
}

// stripComments drops the comment lines at the head of a block.
func stripComments(text string) string {
	for {
		trimmed := strings.TrimSpace(text)
		if !strings.HasPrefix(trimmed, "#") {
			return trimmed
		}

		_, after, _ := strings.Cut(trimmed, "\n")
		text = after
	}
}

// The flag-set document of the getting-started page is a valid document that the flag set `web` names its API key in.
func TestGettingStartedFlagSetDocument(t *testing.T) {
	t.Parallel()

	docs := make([]string, 0, 4)

	for _, block := range fences(t, gettingPage) {
		docs = append(docs, heredocs(block.body, "YAML")...)
	}

	var found *controlplane.FlagSetDocument

	for _, doc := range docs {
		if !strings.Contains(doc, "kind: FlagSet") {
			continue
		}

		parsed, _, err := controlplane.UnmarshalFlagSet([]byte(doc))
		require.NoError(t, err)

		found = parsed
	}

	require.NotNil(t, found)
	require.Len(t, found.FlagSets, 1)
	assert.Equal(t, "web", found.FlagSets[0].Name)
	assert.Equal(t, []string{"checkout"}, found.FlagSets[0].Sources)
	assert.Equal(t, []string{"8d3b5f0e-6a4f-4c8e-9c3e-2f2f5d1a7b10"}, found.FlagSets[0].APIKeys)
}

// Every fenced block of the getting-started page is marked: a shell block is run, a text block is the output of the
// block before it, and no block of another language is left unchecked.
func TestGettingStartedBlocksAreMarked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lang       string
		wantMarker string
	}{
		{lang: langShell, wantMarker: markerRun},
		{lang: "text", wantMarker: markerOutput},
	}

	pagesToCheck := []string{gettingPage, gettingFilePage}
	blocks := make([]fence, 0, 2*len(pagesToCheck))

	for _, page := range pagesToCheck {
		blocks = append(blocks, fences(t, page)...)
	}

	require.NotEmpty(t, blocks)

	seen := map[string]int{}

	for _, block := range blocks {
		seen[block.lang]++

		var want string

		for _, tt := range tests {
			if tt.lang == block.lang {
				want = tt.wantMarker
			}
		}

		assert.Equal(t, want, block.marker, "block %s (%s)", block.name(), block.lang)
	}

	assert.Len(t, seen, len(tests), "the page has only the languages of the table: %v", seen)
}

// The emulator image of the page is the one the tests of the repository run.
func TestGettingStartedUsesThePinnedEmulator(t *testing.T) {
	t.Parallel()

	page, err := os.ReadFile(gettingPage)
	require.NoError(t, err)

	assert.Contains(t, string(page), gopherstack.Image)
}
