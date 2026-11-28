package filepublish

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/blackbirdworks/semaphore/controlplane/s3publish"
	"github.com/blackbirdworks/semaphore/internal/manifestreader"
)

const (
	lockName = ".semaphore-publish.lock"
	// lockMode and fileMode: owner writes, nobody else does. The umask can only remove bits.
	lockMode  = 0o600
	fileMode  = 0o644
	dirMode   = 0o755
	tmpPrefix = ".tmp-"
	anyETag   = "*"
)

// Errors of the store. Test them with [errors.Is].
var (
	// ErrKeyRefused is the cause of a key that is not a clean relative slash path or that leaves the root.
	ErrKeyRefused = errors.New("filepublish: key refused")
	// ErrNotRegular is the cause of a key that names a directory, a device, a socket, a FIFO or a symbolic link
	// that a write would replace.
	ErrNotRegular = errors.New("filepublish: not a regular file")
	// ErrUnsupported is the cause of a write on a platform without an advisory file lock.
	ErrUnsupported = errors.New("filepublish: writing to a file store is not supported on this platform")
	// ErrChecksum is the cause of a body that does not have the checksum the caller announced.
	ErrChecksum = errors.New("filepublish: body does not match its checksum")
	// ErrLockWait is the cause of a wait for the lock that ended because the context ended; the error also wraps the
	// error of the context.
	ErrLockWait = errors.New("filepublish: wait for the publish lock ended")
	// ErrTooLarge is the cause of a body over the limit of the store (see [WithMaxBodyBytes]).
	ErrTooLarge = errors.New("filepublish: body is over the size limit")
)

// Store is an [s3publish.API] over a directory. It holds no state besides the path of the root: it is safe for
// concurrent use, and the root is opened on each call.
type Store struct {
	root    string
	set     settings
	noticed atomic.Bool
}

var _ s3publish.API = (*Store)(nil)

// New returns the store of the directory root, which must exist and be a directory (the relay does not create it
// either). A relative path is made absolute against the working directory, as the reader does.
func New(root string, opts ...Option) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("%w: the root directory is empty", ErrKeyRefused)
	}

	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("file store root: %w", err)
	}

	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("file store root: %w", err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("%w: %q is not a directory", ErrNotRegular, root)
	}

	return &Store{root: abs, set: newSettings(opts)}, nil
}

func apiError(code, message string) error {
	return &smithy.GenericAPIError{Code: code, Message: message}
}

// checkKey refuses a key that is not a clean relative slash path, and one that is not portable (see portableKey).
func checkKey(key *string) (string, error) {
	if key == nil || !manifestreader.ValidFileKey(*key) {
		return "", fmt.Errorf("%w: %q", ErrKeyRefused, aws.ToString(key))
	}

	err := portableKey(*key)
	if err != nil {
		return "", err
	}

	return *key, nil
}

func mapOpenError(key string, err error) error {
	switch {
	case errors.Is(err, fs.ErrNotExist), errors.Is(err, syscall.ENOTDIR):
		return apiError("NoSuchKey", key)
	case errors.Is(err, syscall.ELOOP), errors.Is(err, syscall.ENAMETOOLONG):
		return fmt.Errorf("%w: %s: %w", ErrKeyRefused, key, err)
	}

	return fmt.Errorf("%s: %w", key, err)
}

func etagOf(sum [sha256.Size]byte) string { return `"` + hex.EncodeToString(sum[:]) + `"` }

func sumOf(file io.Reader) ([sha256.Size]byte, error) {
	var sum [sha256.Size]byte

	hasher := sha256.New()

	_, err := io.Copy(hasher, file)
	if err != nil {
		return sum, fmt.Errorf("hash: %w", err)
	}

	hasher.Sum(sum[:0])

	return sum, nil
}

// GetObject implements [s3publish.API]. The ETag is the quoted sha256 of the bytes, like the reader's. A version
// is never found: a file has none. The caller closes the body.
func (s *Store) GetObject(
	ctx context.Context,
	input *s3.GetObjectInput,
	_ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}

	key, err := checkKey(input.Key)
	if err != nil {
		return nil, err
	}

	if input.VersionId != nil {
		return nil, apiError("NoSuchVersion", key)
	}

	file, size, err := s.open(key)
	if err != nil {
		return nil, err
	}

	sum, err := sumOf(file)
	if err == nil {
		_, err = file.Seek(0, io.SeekStart)
	}

	if err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("%s: %w", key, err)
	}

	return &s3.GetObjectOutput{Body: file, ContentLength: aws.Int64(size), ETag: aws.String(etagOf(sum))}, nil
}

// HeadObject implements [s3publish.API]. It reports the size and the full sha256 of the file.
func (s *Store) HeadObject(
	ctx context.Context,
	input *s3.HeadObjectInput,
	_ ...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("head: %w", err)
	}

	key, err := checkKey(input.Key)
	if err != nil {
		return nil, err
	}

	file, size, err := s.open(key)
	if err != nil {
		return nil, err
	}

	defer func() { _ = file.Close() }()

	sum, err := sumOf(file)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}

	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(size), ETag: aws.String(etagOf(sum)),
		ChecksumSHA256: aws.String(
			base64.StdEncoding.EncodeToString(sum[:]),
		), ChecksumType: types.ChecksumTypeFullObject,
	}, nil
}

// PutObject implements [s3publish.API] with the conditions of the publisher: IfNoneMatch "*" creates the file only
// if it is absent, IfMatch replaces it only if its ETag is the one given. The check and the rename run under the
// lock of the store; a failed condition is the S3 error PreconditionFailed, a missing file under IfMatch is
// NoSuchKey. See the package comment for the write.
func (s *Store) PutObject(
	ctx context.Context,
	input *s3.PutObjectInput,
	_ ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("put: %w", err)
	}

	key, err := checkKey(input.Key)
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(io.LimitReader(input.Body, s.set.maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("put %s: read body: %w", key, err)
	}

	if int64(len(body)) > s.set.maxBody {
		return nil, fmt.Errorf("%w: %s is over %d bytes", ErrTooLarge, key, s.set.maxBody)
	}

	sum := sha256.Sum256(body)

	if want := aws.ToString(input.ChecksumSHA256); want != "" && want != base64.StdEncoding.EncodeToString(sum[:]) {
		return nil, fmt.Errorf("%w: %s", ErrChecksum, key)
	}

	root, err := manifestreader.OpenFileRoot(s.root)
	if err != nil {
		return nil, fmt.Errorf("put %s: %w", key, err)
	}

	defer func() { _ = root.Close() }()

	unlock, err := s.acquire(ctx, root)
	if err != nil {
		return nil, err
	}

	defer unlock()

	err = s.checkCondition(key, input)
	if err != nil {
		return nil, err
	}

	err = writeAside(root, key, body)
	if err != nil {
		return nil, fmt.Errorf("put %s: %w", key, err)
	}

	return &s3.PutObjectOutput{ETag: aws.String(etagOf(sum))}, nil
}

// open opens a key for reading and returns the regular file with its size.
func (s *Store) open(key string) (*os.File, int64, error) {
	root, err := manifestreader.OpenFileRoot(s.root)
	if err != nil {
		return nil, 0, fmt.Errorf("open %s: %w", key, err)
	}

	defer func() { _ = root.Close() }()

	file, err := root.Open(manifestreader.FileKeyPath(key))
	if err != nil {
		return nil, 0, mapOpenError(key, err)
	}

	info, err := file.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = fmt.Errorf("%w: %s", ErrNotRegular, info.Mode().Type())
	}

	if err != nil {
		_ = file.Close()

		return nil, 0, fmt.Errorf("%s: %w", key, err)
	}

	return file, info.Size(), nil
}

// checkCondition evaluates If-None-Match and If-Match against the file as it is now.
func (s *Store) checkCondition(key string, input *s3.PutObjectInput) error {
	file, _, err := s.open(key)

	switch {
	case err == nil:
		defer func() { _ = file.Close() }()
	case !isNoSuchKey(err):
		return err
	}

	switch {
	case aws.ToString(input.IfNoneMatch) == anyETag && err == nil:
		return apiError("PreconditionFailed", key)
	case input.IfMatch != nil && err != nil:
		return err
	case input.IfMatch != nil:
		sum, sumErr := sumOf(file)
		if sumErr != nil {
			return fmt.Errorf("%s: %w", key, sumErr)
		}

		if etagOf(sum) != aws.ToString(input.IfMatch) {
			return apiError("PreconditionFailed", key)
		}
	}

	return nil
}

func isNoSuchKey(err error) bool {
	apiErr, ok := errors.AsType[smithy.APIError](err)

	return ok && apiErr.ErrorCode() == "NoSuchKey"
}

// writeAside writes body to a new file of a unique name in the directory of key, flushes it, renames it over key
// and flushes the directory. The temporary file is removed when anything fails before the rename.
func writeAside(root *os.Root, key string, body []byte) error {
	dir := path.Dir(key)
	rel := manifestreader.FileKeyPath(key)

	if dir != "." {
		err := ensureDirs(root, dir)
		if err != nil {
			return err
		}
	}

	info, err := root.Lstat(rel)
	if err == nil && !info.Mode().IsRegular() {
		return fmt.Errorf("%w: %s: %s", ErrNotRegular, key, info.Mode().Type())
	}

	tmp := manifestreader.FileKeyPath(path.Join(dir, tmpPrefix+rand.Text()))

	err = writeSynced(root, tmp, body)
	if err == nil {
		err = root.Rename(tmp, rel)
		if err != nil {
			err = fmt.Errorf("rename: %w", err)
		}
	}

	if err != nil {
		_ = root.Remove(tmp)

		return err
	}

	return syncDir(root, manifestreader.FileKeyPath(dir))
}

func writeSynced(root *os.Root, name string, body []byte) error {
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode)
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}

	_, err = file.Write(body)
	if err == nil {
		err = file.Sync()
	}

	err = errors.Join(err, file.Close())
	if err != nil {
		return fmt.Errorf("write temporary file: %w", err)
	}

	return nil
}

// ensureDirs creates the missing directories of dir (a slash path) one by one, and flushes each new one and the
// directory that holds it, so that the new entries survive a crash like the renamed file does.
func ensureDirs(root *os.Root, dir string) error {
	current := ""

	for part := range strings.SplitSeq(dir, "/") {
		parent := current
		current = path.Join(current, part)
		rel := manifestreader.FileKeyPath(current)

		_, err := root.Lstat(rel)
		if err == nil {
			continue
		}

		err = root.Mkdir(rel, dirMode)
		if err != nil {
			return fmt.Errorf("create directory: %w", err)
		}

		err = errors.Join(syncDir(root, rel), syncDir(root, manifestreader.FileKeyPath(path.Join(".", parent))))
		if err != nil {
			return err
		}
	}

	return nil
}
