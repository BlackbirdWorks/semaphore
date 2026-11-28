//go:build !windows

package filepublish

import (
	"errors"
	"fmt"
	"os"
)

// syncDir flushes a directory so that a rename or a new entry in it survives a crash.
func syncDir(root *os.Root, dir string) error {
	handle, err := root.Open(dir)
	if err != nil {
		return fmt.Errorf("open directory: %w", err)
	}

	err = handle.Sync()

	err = errors.Join(err, handle.Close())
	if err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}

	return nil
}
