package docstest_test

import (
	"context"
	"testing"
)

// The file-store page needs no emulator: it runs everywhere, in the normal test run.
func TestGettingStartedFileStoreRunsAsWritten(t *testing.T) {
	t.Parallel()

	runPage(t, gettingFilePage, "1041", func(context.Context) {})
}
