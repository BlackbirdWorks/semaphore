//go:build windows

package filepublish

import "os"

// syncDir does nothing: Windows has no flush of a directory (the rename itself is journalled by the file system).
func syncDir(*os.Root, string) error { return nil }
