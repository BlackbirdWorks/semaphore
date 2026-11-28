//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package filepublish

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// tryLock asks for the exclusive lock without waiting: false when another holder has it.
func tryLock(file *os.File) (bool, error) {
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)

		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, syscall.EINTR):
		case errors.Is(err, syscall.EWOULDBLOCK):
			return false, nil
		default:
			return false, fmt.Errorf("flock: %w", err)
		}
	}
}

// releaseLock gives the lock back: the system drops it when the descriptor closes, or when the process dies.
func releaseLock(file *os.File) { _ = file.Close() }
