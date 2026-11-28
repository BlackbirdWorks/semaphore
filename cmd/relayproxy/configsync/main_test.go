package configsync_test

import (
	"testing"

	"github.com/blackbirdworks/semaphore/modules/core/testhelpers/clocktest"
)

func TestMain(m *testing.M) {
	clocktest.Freeze()
	m.Run()
}
