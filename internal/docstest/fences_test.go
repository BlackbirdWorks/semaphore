package docstest_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	docsRoot    = "../../docs"
	gettingPage = docsRoot + "/relay-proxy/getting-started.md"
	// gettingFilePage is the variant of the page that needs no emulator.
	gettingFilePage = docsRoot + "/relay-proxy/getting-started-file-store.md"

	markerPrefix = "<!-- docs-test: "
	markerRun    = "run"
	langYAML     = "yaml"
	langShell    = "shell"
	markerOutput = "output"
)

// fence is a fenced code block of a Markdown page.
type fence struct {
	page   string
	lang   string
	marker string
	body   string
	line   int
}

func (f fence) name() string { return f.page + ":" + strconv.Itoa(f.line) }

// fences returns the fenced blocks of a page, with the docs-test marker on the line before each.
func fences(t *testing.T, path string) []fence {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	var out []fence

	lines := strings.Split(string(raw), "\n")
	marker := ""

	for i := 0; i < len(lines); i++ {
		line := lines[i]

		if rest, found := strings.CutPrefix(line, markerPrefix); found {
			marker = strings.TrimSuffix(strings.TrimSpace(rest), " -->")

			continue
		}

		lang, isFence := strings.CutPrefix(line, "```")
		if !isFence || lang == "" {
			if strings.TrimSpace(line) != "" {
				marker = ""
			}

			continue
		}

		start := i + 1

		var body []string

		for i++; i < len(lines) && !strings.HasPrefix(lines[i], "```"); i++ {
			body = append(body, lines[i])
		}

		out = append(out, fence{
			page: path, lang: strings.TrimSpace(lang), marker: marker, body: strings.Join(body, "\n"), line: start,
		})
		marker = ""
	}

	return out
}

// pages lists the Markdown pages of the documentation, without the installed packages.
func pages(t *testing.T) []string {
	t.Helper()

	var out []string

	err := filepath.WalkDir(docsRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() && entry.Name() == "node_modules" {
			return filepath.SkipDir
		}

		if !entry.IsDir() && strings.HasSuffix(path, ".md") {
			out = append(out, path)
		}

		return nil
	})
	require.NoError(t, err)

	return out
}

// heredocs returns the bodies of the here-documents of a shell block that end with the given delimiter.
func heredocs(script, delimiter string) []string {
	var out []string

	lines := strings.Split(script, "\n")

	for i := 0; i < len(lines); i++ {
		if !strings.Contains(lines[i], "<<'"+delimiter+"'") {
			continue
		}

		var body []string

		for i++; i < len(lines) && lines[i] != delimiter; i++ {
			body = append(body, lines[i])
		}

		out = append(out, strings.Join(body, "\n")+"\n")
	}

	return out
}
