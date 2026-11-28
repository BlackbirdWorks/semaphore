//go:build docker

package docstest_test

import (
	"context"
	"os/exec"
	"testing"
)

const emulator = "semaphore-s3"

// The getting-started page is followed literally, with a gopherstack emulator that the page starts itself.
func TestGettingStartedRunsAsWritten(t *testing.T) {
	t.Parallel()

	runPage(t, gettingPage, "1031", func(ctx context.Context) {
		_ = exec.CommandContext(ctx, "docker", "rm", "-f", emulator).Run()
	})
}
