//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly || windows)

package filepublish

import (
	"fmt"
	"os"
	"runtime"
)

// tryLock is not available: there is no advisory lock without a dependency, and a lock file that survives a crash
// would block every later publish.
func tryLock(*os.File) (bool, error) {
	return false, fmt.Errorf("%w: publishing to a file store on %s", ErrUnsupported, runtime.GOOS)
}

func releaseLock(file *os.File) { _ = file.Close() }
