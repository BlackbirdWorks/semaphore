package manifestreader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
)

// Causes carried by the [*FetchError] of a [FileStore]. They are for [errors.Is]; the reader reports only the reason.
var (
	// ErrKeyRefused is the cause of a key that is not a clean relative slash path: empty, absolute, with a ".",
	// ".." or empty element, a trailing slash, a backslash or a NUL byte.
	ErrKeyRefused = errors.New("manifestreader: file key refused")
	// ErrOutsideRoot is the cause of a path that leaves the root directory through a symbolic link.
	ErrOutsideRoot = errors.New("manifestreader: path leaves the root directory")
	// ErrNotRegular is the cause of a key that names a directory, a device, a socket or a FIFO.
	ErrNotRegular = errors.New("manifestreader: not a regular file")
	// ErrRootUnavailable is the cause of a root directory that cannot be opened.
	ErrRootUnavailable = errors.New("manifestreader: root directory unavailable")
)

const (
	// fileChunk is the size of one read: the context is looked at between two reads, and it is the buffer of the
	// hashing pass, which therefore allocates nothing that grows with the file.
	fileChunk = 32 << 10
	// maxPrealloc bounds the buffer reserved from the size the file had when it was opened.
	maxPrealloc = 64 << 20
	// escapeMessage is the text of the error [os.Root] returns for a path that leaves the root. The standard
	// library has no exported value for it; a test pins this text so a change of it is noticed.
	escapeMessage = "path escapes from parent"
)

// FileStore is an [ObjectStore] over a directory of the local file system (a development directory, a volume
// mount, a Kubernetes ConfigMap or Secret mount).
//
// Keys are relative slash paths under the root. A key that is empty, absolute, or has a ".", ".." or empty
// element is refused ([ErrKeyRefused], reason denied). Every path is resolved with [os.Root], so it cannot leave
// the root: a symbolic link inside the root that points to somewhere inside the root is followed (a ConfigMap
// mount is a tree of such links), one that points outside, and any absolute link, is refused
// ([ErrOutsideRoot], reason denied). Only regular files are served; a directory, device, socket or FIFO is
// refused without being read ([ErrNotRegular]), and opening never blocks.
//
// The ETag is the quoted sha256 of the bytes. A conditional Get reads the file to decide: an unchanged file is
// answered with [ErrNotModified] after one streaming pass that allocates a fixed buffer and returns no body, a
// changed one costs a second pass. The answer never relies on the size or the modification time of the file, so
// a rewrite that keeps both is still seen. See docs/concepts/control-plane-file-store.md.
//
// VersionID is not supported: a file has no versions, so a request for one is answered with [ErrNotFound]. The
// store holds no state and is safe for concurrent use; the root is opened on each call, so a root that is
// replaced (a swapped mount) is followed.
type FileStore struct {
	// hooks is the seam of the tests of this package (nil in production): a fault or a concurrent writer injected
	// at an exact point of a read.
	hooks *fileHooks
	root  string
}

// The passes of a conditional read, as the hooks name them.
const (
	passHash = 1
	passRead = 2
)

// fileHooks are the points where a test can step into a Get.
type fileHooks struct {
	// wrap wraps the file as one pass reads it (passHash or passRead).
	wrap func(pass int, src io.Reader) io.Reader
	// between runs after the hashing pass said "changed" and before the read pass starts.
	between func()
	// stat replaces the stat of the open file.
	stat func(file *os.File) (os.FileInfo, error)
}

func (h *fileHooks) reader(pass int, src io.Reader) io.Reader {
	if h == nil || h.wrap == nil {
		return src
	}

	return h.wrap(pass, src)
}

func (h *fileHooks) betweenPasses() {
	if h != nil && h.between != nil {
		h.between()
	}
}

func (h *fileHooks) stater(file *os.File) (os.FileInfo, error) {
	if h != nil && h.stat != nil {
		return h.stat(file)
	}

	return file.Stat() //nolint:wrapcheck // open wraps it
}

var _ ObjectStore = (*FileStore)(nil)

// NewFileStore returns the store of the directory root. It checks that root is a directory now; a root that
// disappears later (an unmounted volume) makes Get fail with a retryable [*FetchError] whose causes include
// [ErrRootUnavailable], so the reader backs off and recovers when the root is back.
func NewFileStore(root string) (*FileStore, error) {
	if root == "" {
		return nil, fmt.Errorf("%w: the root directory is empty", ErrInvalidConfig)
	}

	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("%w: root directory: %w", ErrInvalidConfig, err)
	}

	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("%w: root directory: %w", ErrInvalidConfig, err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("%w: root directory %q is not a directory", ErrInvalidConfig, root)
	}

	return &FileStore{root: abs}, nil
}

// Get implements [ObjectStore]. See [FileStore] for the rules.
func (s *FileStore) Get(ctx context.Context, req GetRequest) (Object, error) {
	err := ctx.Err()
	if err != nil {
		return Object{}, interrupted(err)
	}

	if !ValidFileKey(req.Key) {
		return Object{}, &FetchError{Err: ErrKeyRefused, Reason: ReasonDenied}
	}

	if req.VersionID != "" {
		return Object{}, ErrNotFound
	}

	limit := max(req.Limit, 0)

	file, size, err := s.open(req.Key)
	if err != nil {
		return Object{}, fileFailure(ctx, err)
	}

	defer func() { _ = file.Close() }()

	if size > limit {
		return Object{}, ErrTooLarge
	}

	if req.ETag != "" {
		sum, hashErr := hashFile(ctx, s.hooks.reader(passHash, file), limit)
		if hashErr != nil {
			return Object{}, fileFailure(ctx, hashErr)
		}

		if etagOfSum(sum) == req.ETag {
			return Object{}, ErrNotModified
		}

		s.hooks.betweenPasses()

		_, err = file.Seek(0, io.SeekStart)
		if err != nil {
			return Object{}, fileFailure(ctx, err)
		}
	}

	data, err := readBounded(ctx, s.hooks.reader(passRead, file), size, limit)
	if err != nil {
		return Object{}, fileFailure(ctx, err)
	}

	return Object{Bytes: data, ETag: etagOfSum(sha256.Sum256(data))}, nil
}

// open opens the key under the root without blocking, and returns the file with the size it had.
func (s *FileStore) open(key string) (*os.File, int64, error) {
	root, err := OpenFileRoot(s.root)
	if err != nil {
		return nil, 0, err
	}

	defer func() { _ = root.Close() }()

	file, err := root.OpenFile(FileKeyPath(key), openFlags, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("open: %w", err)
	}

	// the type is read from the open file, not from an earlier lookup: a path swapped between the two cannot
	// make the store read a file it did not look at
	info, err := s.hooks.stater(file)
	if err != nil {
		_ = file.Close()

		return nil, 0, fmt.Errorf("stat: %w", err)
	}

	if !info.Mode().IsRegular() {
		_ = file.Close()

		return nil, 0, fmt.Errorf("%w: %s", ErrNotRegular, info.Mode().Type())
	}

	return file, info.Size(), nil
}

// OpenFileRoot opens the root directory of a file store the way the reader does: the lookup requires a directory,
// so a FIFO in its place is refused before it is opened. The error is [ErrRootUnavailable]. The publisher of a
// file store opens its root with this function, so reader and writer resolve a key in the same way.
func OpenFileRoot(dir string) (*os.Root, error) {
	// "/." makes the lookup itself require a directory (ENOTDIR otherwise): a FIFO that took the place of the root
	// is refused before it is opened, where os.OpenRoot would open it, and wait for a writer.
	root, err := os.OpenRoot(dir + string(filepath.Separator) + ".")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRootUnavailable, err)
	}

	return root, nil
}

// FileOpenFlags are the flags with which a store opens a key to read it: read-only, and on Unix without blocking, so
// that a FIFO that took the place of a file is refused (after the type is read from the open handle) instead of
// waiting for a writer. The publisher opens keys with the same flags as the reader.
const FileOpenFlags = openFlags

// FileKeyPath maps a valid key (see [ValidFileKey]) to the path below the root of the store. It is the only
// key-to-path mapping of reader and publisher.
func FileKeyPath(key string) string { return filepath.FromSlash(key) }

// ValidFileKey reports whether key is a clean relative slash path.
func ValidFileKey(key string) bool {
	return fs.ValidPath(key) && key != "." && !strings.ContainsAny(key, "\\\x00")
}

func etagOfSum(sum [sha256.Size]byte) string {
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// hashFile hashes the file in fixed chunks, reading at most limit+1 bytes.
func hashFile(ctx context.Context, file io.Reader, limit int64) ([sha256.Size]byte, error) {
	var sum [sha256.Size]byte

	hasher := sha256.New()
	chunk := make([]byte, fileChunk)

	var total int64

	for {
		ctxErr := ctx.Err()
		if ctxErr != nil {
			return sum, ctxErr //nolint:wrapcheck // the caller classifies it with the context
		}

		want := int64(len(chunk))
		if limit < math.MaxInt64 {
			want = min(want, limit+1-total)
		}

		count, err := file.Read(chunk[:want])
		total += int64(count)
		_, _ = hasher.Write(chunk[:count]) // a hash.Hash never fails

		if total > limit {
			return sum, ErrTooLarge
		}

		if errors.Is(err, io.EOF) {
			hasher.Sum(sum[:0])

			return sum, nil
		}

		if err != nil {
			return sum, err //nolint:wrapcheck // the caller classifies it
		}
	}
}

// readBounded reads the whole of r into one buffer: of the size the file had, and never more than limit+1
// bytes are requested from r. A body longer than limit is [ErrTooLarge]. ctx is looked at between the reads.
func readBounded(ctx context.Context, src io.Reader, size, limit int64) ([]byte, error) {
	buf := make([]byte, 0, min(size+1, maxPrealloc))

	for {
		ctxErr := ctx.Err()
		if ctxErr != nil {
			return nil, ctxErr //nolint:wrapcheck // the caller classifies it with the context
		}

		if len(buf) == cap(buf) {
			buf = slices.Grow(buf, fileChunk)
		}

		room := buf[len(buf):cap(buf)]
		if len(room) > fileChunk {
			room = room[:fileChunk]
		}

		if limit < math.MaxInt64 {
			room = room[:min(int64(len(room)), limit+1-int64(len(buf)))]
		}

		count, err := src.Read(room)
		buf = buf[:len(buf)+count]

		if int64(len(buf)) > limit {
			return nil, ErrTooLarge
		}

		if errors.Is(err, io.EOF) {
			return buf, nil
		}

		if err != nil {
			return nil, err //nolint:wrapcheck // the caller classifies it
		}
	}
}

// interrupted is the result of a Get whose context ended: the same shape as an S3 request that was cancelled.
func interrupted(err error) error {
	return &FetchError{Err: err, Reason: ReasonError, Retryable: true}
}

// fileFailure maps the error of a file operation to the results of [ObjectStore].
func fileFailure(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, ErrTooLarge):
		return err
	case ctx.Err() != nil && errors.Is(err, ctx.Err()):
		return interrupted(err)
	}

	return classifyFileError(err)
}

// classifyFileError maps an error of the operating system. A missing file (or a missing directory on its path)
// is [ErrNotFound]; what a retry cannot fix without a change of the setup (permissions, a path that leaves the
// root, a name that is too long, a symbolic link loop, a file that is not regular) is denied; everything else
// (I/O errors, exhausted descriptors, a stale handle) is a retryable error. An error of the root directory goes
// through [classifyRoot]: it is never "not found", because the object is not what is missing.
func classifyFileError(err error) error {
	if errors.Is(err, ErrRootUnavailable) {
		return classifyRoot(err)
	}

	switch {
	case errors.Is(err, ErrNotRegular):
		return &FetchError{Err: err, Reason: ReasonDenied}
	case errors.Is(err, fs.ErrNotExist), errors.Is(err, syscall.ENOTDIR):
		return ErrNotFound
	case errors.Is(err, syscall.ENAMETOOLONG):
		return &FetchError{Err: fmt.Errorf("%w: %w", ErrKeyRefused, err), Reason: ReasonDenied}
	case errors.Is(err, syscall.ENODEV), isIrregularOpen(err):
		return &FetchError{Err: fmt.Errorf("%w: %w", ErrNotRegular, err), Reason: ReasonDenied}
	case errors.Is(err, fs.ErrPermission), errors.Is(err, syscall.ELOOP):
		return &FetchError{Err: err, Reason: ReasonDenied}
	case isEscape(err):
		// fail closed: if a Go release words the message differently the escape is still refused (os.Root
		// refuses it), only as a retryable error instead of a denied one. TestFileStore_ContainmentAndSymlinks
		// pins the text against the real os.Root.
		return &FetchError{Err: fmt.Errorf("%w: %w", ErrOutsideRoot, err), Reason: ReasonDenied}
	}

	return &FetchError{Err: err, Reason: ReasonError, Retryable: true}
}

// isEscape reports the error [os.Root] returns for a path that leaves the root. It compares the text of each error
// of the chain exactly, and not the text of the whole error: that one names the path, and a file called
// "path escapes from parent.json" must not be taken for an escape.
func isEscape(err error) bool {
	for current := err; current != nil; current = errors.Unwrap(current) {
		if current.Error() == escapeMessage {
			return true
		}
	}

	return false
}

// classifyRoot maps the error of opening the root directory. Only a permission error is final: a root that does
// not exist yet (a volume mounted late), is out of descriptors, or answers with an I/O error may be there at the
// next poll, so the reader backs off and retries as for any transient failure.
func classifyRoot(err error) error {
	if errors.Is(err, fs.ErrPermission) {
		return &FetchError{Err: err, Reason: ReasonDenied}
	}

	return &FetchError{Err: err, Reason: ReasonError, Retryable: true}
}
