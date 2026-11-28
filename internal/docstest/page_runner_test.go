package docstest_test

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// pipeWait is how long Run waits for the output pipes after the context ended.
	pipeWait   = 10 * time.Second
	e2eTimeout = 5 * time.Minute
	sentinel   = "@@docs-test-block@@"
)

// build compiles a command of the repository into dir.
func build(ctx context.Context, t *testing.T, dir, name, pkg string) {
	t.Helper()

	cmd := exec.CommandContext(ctx, "go", "build", "-o", filepath.Join(dir, name), pkg)
	cmd.Dir = "../.."

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}

// killRelay stops the relay process that the page started, if it wrote relay.pid.
func killRelay(work string) {
	pid, err := strconv.Atoi(readTrimmed(filepath.Join(work, "relay.pid")))
	if err != nil {
		return
	}

	process, err := os.FindProcess(pid)
	if err == nil {
		_ = process.Signal(syscall.SIGTERM)
	}
}

func readTrimmed(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(raw))
}

// runPage follows a page literally: every shell block of the page runs, in order, in one shell, against the binaries
// built from this tree. The port that the page shows (examplePort) is replaced by a free port of this run, so that
// pages and shards that run together cannot collide. The text block after a shell block is what that block prints.
// cleanup removes what a failed run leaves behind besides the relay process.
func runPage(t *testing.T, page, examplePort string, cleanup func(ctx context.Context)) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	t.Cleanup(cancel)

	bin, work := t.TempDir(), t.TempDir()

	build(ctx, t, bin, "semaphore", "./cmd/relayproxy")
	build(ctx, t, bin, "semaphore-cli", "./cmd/cli")

	var script strings.Builder

	script.WriteString("set -euo pipefail\n")

	var wantOutputs []string // the expected output of each run block, empty when the page shows none

	blocks := fences(t, page)
	for i, block := range blocks {
		if block.marker != markerRun {
			continue
		}

		script.WriteString(block.body + "\necho " + sentinel + "\n")

		want := ""
		if i+1 < len(blocks) && blocks[i+1].marker == markerOutput {
			want = strings.TrimSpace(blocks[i+1].body)
		}

		wantOutputs = append(wantOutputs, want)
	}

	require.NotEmpty(t, wantOutputs)

	runnable := strings.ReplaceAll(script.String(), examplePort, freePort(t))

	t.Cleanup(func() {
		killRelay(work)
		cleanup(context.WithoutCancel(ctx))
	})

	var stdout, stderr bytes.Buffer

	cmd := exec.CommandContext(ctx, "bash", "-c", runnable)
	cmd.Dir = work

	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	// a command that holds the output pipes open after the deadline cannot keep the test from its cleanup
	cmd.WaitDelay = pipeWait

	err := cmd.Run()
	require.NoError(t, err, "stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())

	got := strings.Split(stdout.String(), sentinel+"\n")
	require.Len(t, got, len(wantOutputs)+1, "one output per run block")

	for i, want := range wantOutputs {
		if want == "" {
			continue
		}

		t.Run("block "+strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, want, strings.TrimSpace(got[i]))
		})
	}
}

// freePort returns a port that nothing listens on now.
func freePort(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig

	listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	require.NoError(t, listener.Close())

	return port
}
