package filepublish_test

import (
	"testing"

	"github.com/blackbirdworks/semaphore/modules/core/testhelpers/clocktest"
)

// TestMain pins the clock once for the package.
func TestMain(m *testing.M) {
	clocktest.Freeze()
	m.Run()
}
