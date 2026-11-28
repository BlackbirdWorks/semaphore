//go:build windows

package filepublish

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// tryLock asks for an exclusive lock of the first byte of the lock file without waiting: false when another holder
// has it (ERROR_LOCK_VIOLATION).
func tryLock(file *os.File) (bool, error) {
	overlapped := new(windows.Overlapped)

	err := windows.LockFileEx(
		windows.Handle(
			file.Fd(),
		),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		overlapped,
	)

	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, windows.ERROR_LOCK_VIOLATION):
		return false, nil
	}

	return false, fmt.Errorf("LockFileEx: %w", err)
}

// releaseLock unlocks the byte and closes the file; the system also drops the lock when the process dies.
func releaseLock(file *os.File) {
	_ = windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, new(windows.Overlapped))
	_ = file.Close()
}
