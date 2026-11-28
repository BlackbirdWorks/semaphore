package filepublish

import (
	"fmt"
	"strings"
)

const illegalChars = `<>:"|?*`

// portableKey refuses a key that could not be written on every platform or that would touch what the store keeps
// for itself: a component that is the lock file or starts with the prefix of the files written aside, a Windows device
// name (CON, NUL, COM1, ...), a name that ends with a dot or a space, and the characters < > : " | ? * and controls.
// The rule is the writer's: the reader serves any key that is a clean relative path.
func portableKey(key string) error {
	for comp := range strings.SplitSeq(key, "/") {
		reason := badComponent(comp)
		if reason != "" {
			return fmt.Errorf("%w: %q: %s", ErrKeyRefused, key, reason)
		}
	}

	return nil
}

func badComponent(comp string) string {
	lower := strings.ToLower(comp)

	switch {
	case lower == lockName || strings.HasPrefix(lower, tmpPrefix):
		return "the name is reserved by the store"
	case strings.ContainsAny(comp, illegalChars) || strings.IndexFunc(comp, func(r rune) bool { return r < ' ' }) >= 0:
		return "the name has a character that is not portable"
	case strings.HasSuffix(comp, ".") || strings.HasSuffix(comp, " "):
		return "the name ends with a dot or a space"
	case isDevice(comp):
		return "the name is a Windows device name"
	}

	return ""
}

func isDevice(comp string) bool {
	base, _, _ := strings.Cut(comp, ".")
	base = strings.ToUpper(strings.TrimRight(base, " "))

	switch base {
	case "CON", "PRN", "AUX", "NUL":
		return true
	}

	if len(base) == len("COM1") && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) {
		return base[3] >= '0' && base[3] <= '9'
	}

	return false
}
