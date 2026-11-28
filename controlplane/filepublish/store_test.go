package filepublish_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/controlplane"
	"github.com/blackbirdworks/semaphore/controlplane/filepublish"
	"github.com/blackbirdworks/semaphore/controlplane/s3publish"
)

const (
	manifestKey = "manifests/stable.yaml"
	wantNoKey   = "NoSuchKey"
	wantPrecond = "PreconditionFailed"
	bodyOld     = "old"
	bodyNew     = "new"
)

func newStore(t *testing.T) (*filepublish.Store, string) {
	t.Helper()

	dir := t.TempDir()
	store, err := filepublish.New(dir)
	require.NoError(t, err)

	return store, dir
}

func put(store *filepublish.Store, key, body string, mutate func(*s3.PutObjectInput)) error {
	in := &s3.PutObjectInput{Key: aws.String(key), Body: strings.NewReader(body)}
	if mutate != nil {
		mutate(in)
	}

	_, err := store.PutObject(context.Background(), in)

	return err
}

func codeOf(err error) string {
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		return apiErr.ErrorCode()
	}

	return ""
}

func etagOf(body string) string {
	sum := sha256.Sum256([]byte(body))

	return `"` + strings.ToLower(hexOf(sum[:])) + `"`
}

func hexOf(b []byte) string {
	const digits = "0123456789abcdef"

	out := make([]byte, 0, len(b)*2)
	for _, v := range b {
		out = append(out, digits[v>>4], digits[v&0xf])
	}

	return string(out)
}

func TestNewRefusesAMissingRootAndAFile(t *testing.T) {
	t.Parallel()

	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))

	tests := []struct {
		wantErr error
		name    string
		root    string
	}{
		{name: "empty", root: "", wantErr: filepublish.ErrKeyRefused},
		{name: "a file", root: file, wantErr: filepublish.ErrNotRegular},
		{name: "missing", root: filepath.Join(t.TempDir(), "missing"), wantErr: os.ErrNotExist},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := filepublish.New(tt.root)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestKeysAreRefused(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
	}{
		{name: "empty", key: ""},
		{name: "absolute", key: "/etc/passwd"},
		{name: "parent", key: "../outside"},
		{name: "parent in the middle", key: "a/../../outside"},
		{name: "dot element", key: "a/./b"},
		{name: "empty element", key: "a//b"},
		{name: "trailing slash", key: "a/"},
		{name: "backslash", key: `a\b`},
		{name: "nul", key: "a\x00b"},
		{name: "the lock file", key: ".semaphore-publish.lock"},
		{name: "the lock file in a directory", key: "manifests/.semaphore-publish.lock"},
		{name: "the lock file in other case", key: ".SEMAPHORE-Publish.Lock"},
		{name: "a file written aside", key: "manifests/.tmp-1234"},
		{name: "a directory written aside", key: ".TMP-x/stable.yaml"},
		{name: "device CON", key: "CON"},
		{name: "device with an extension", key: "dir/nul.txt"},
		{name: "device in lower case", key: "aux"},
		{name: "device COM1", key: "COM1"},
		{name: "device LPT9 with an extension", key: "a/LPT9.log"},
		{name: "device with a trailing space", key: "con /x"},
		{name: "trailing dot", key: "a/b."},
		{name: "trailing space", key: "a/b "},
		{name: "colon", key: "a:b"},
		{name: "colon stream", key: "stable.yaml:stream"},
		{name: "angle bracket", key: "a<b"},
		{name: "question mark", key: "a?b"},
		{name: "control character", key: "a\x01b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, dir := newStore(t)

			_, err := store.GetObject(t.Context(), &s3.GetObjectInput{Key: aws.String(tt.key)})
			require.ErrorIs(t, err, filepublish.ErrKeyRefused)

			_, err = store.HeadObject(t.Context(), &s3.HeadObjectInput{Key: aws.String(tt.key)})
			require.ErrorIs(t, err, filepublish.ErrKeyRefused)

			require.ErrorIs(t, put(store, tt.key, "x", nil), filepublish.ErrKeyRefused)

			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			assert.Empty(t, entries, "nothing is written for a refused key")
		})
	}
}

func TestSymlinksCannotLeaveTheRoot(t *testing.T) {
	t.Parallel()

	store, dir := newStore(t)
	outside := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(outside, "secret"), []byte("secret"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, "dirlink")))
	require.NoError(t, os.Symlink(filepath.Join(outside, "secret"), filepath.Join(dir, "filelink")))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "real"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "real", "inside"), []byte("inside"), 0o600))
	require.NoError(t, os.Symlink("real/inside", filepath.Join(dir, "insidelink")))

	tests := []struct {
		name     string
		key      string
		wantRead bool
	}{
		{name: "directory link out", key: "dirlink/secret"},
		{name: "file link out", key: "filelink"},
		{name: "link that stays inside", key: "insidelink", wantRead: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := store.GetObject(t.Context(), &s3.GetObjectInput{Key: aws.String(tt.key)})
			if tt.wantRead {
				require.NoError(t, err)

				body, readErr := io.ReadAll(out.Body)
				require.NoError(t, readErr)
				require.NoError(t, out.Body.Close())
				assert.Equal(t, "inside", string(body))

				return
			}

			require.Error(t, err)
			assert.NotEqual(t, wantNoKey, codeOf(err))
		})
	}

	t.Run("a write does not follow a link out", func(t *testing.T) {
		t.Parallel()

		require.Error(t, put(store, "dirlink/new", "x", nil))

		_, err := os.Stat(filepath.Join(outside, bodyNew))
		assert.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("a write does not replace a link", func(t *testing.T) {
		t.Parallel()

		require.ErrorIs(t, put(store, "insidelink", "x", nil), filepublish.ErrNotRegular)

		got, err := os.ReadFile(filepath.Join(dir, "real", "inside"))
		require.NoError(t, err)
		assert.Equal(t, "inside", string(got))
	})
}

func TestPutWritesAtomically(t *testing.T) {
	t.Parallel()

	store, dir := newStore(t)

	require.NoError(t, put(store, "objects/sha256/ab/cd.yaml", "one", nil))
	require.NoError(t, put(store, "objects/sha256/ab/cd.yaml", "two", func(input *s3.PutObjectInput) {
		input.IfMatch = aws.String(etagOf("one"))
	}))

	got, err := os.ReadFile(filepath.Join(dir, "objects", "sha256", "ab", "cd.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "two", string(got))

	var names, loose []string

	require.NoError(t, filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		require.NoError(t, err)

		if !entry.IsDir() {
			names = append(names, entry.Name())
		}

		if strings.HasPrefix(entry.Name(), ".tmp-") {
			loose = append(loose, path)
		}

		return nil
	}))

	assert.ElementsMatch(t, []string{"cd.yaml", ".semaphore-publish.lock"}, names)
	assert.Empty(t, loose, "no file written aside is left")
}

func TestPutRefusesADirectoryAndLeavesNoLeftover(t *testing.T) {
	t.Parallel()

	store, dir := newStore(t)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "a", "dir"), 0o750))

	require.ErrorIs(t, put(store, "a/dir", "x", nil), filepublish.ErrNotRegular)

	entries, err := os.ReadDir(filepath.Join(dir, "a"))
	require.NoError(t, err)
	assert.Len(t, entries, 1)

	require.Error(
		t,
		put(store, "a/dir/x/y", "x", func(input *s3.PutObjectInput) { input.ChecksumSHA256 = aws.String("AAAA") }),
	)
}

func TestPutConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		existing string
		mutate   func(*s3.PutObjectInput)
		wantCode string
		wantBody string
	}{
		{
			name:     "create only on a new key",
			mutate:   func(input *s3.PutObjectInput) { input.IfNoneMatch = aws.String("*") },
			wantBody: bodyNew,
		},
		{
			name: "create only on an existing key", existing: bodyOld,
			mutate:   func(input *s3.PutObjectInput) { input.IfNoneMatch = aws.String("*") },
			wantCode: wantPrecond, wantBody: bodyOld,
		},
		{
			name: "match of the current etag", existing: bodyOld,
			mutate:   func(input *s3.PutObjectInput) { input.IfMatch = aws.String(etagOf(bodyOld)) },
			wantBody: bodyNew,
		},
		{
			name: "match of another etag", existing: bodyOld,
			mutate:   func(input *s3.PutObjectInput) { input.IfMatch = aws.String(etagOf("other")) },
			wantCode: wantPrecond, wantBody: bodyOld,
		},
		{
			name:     "match on a missing key",
			mutate:   func(input *s3.PutObjectInput) { input.IfMatch = aws.String(etagOf(bodyOld)) },
			wantCode: wantNoKey,
		},
		{name: "unconditional", existing: bodyOld, wantBody: bodyNew},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, dir := newStore(t)

			if tt.existing != "" {
				require.NoError(t, put(store, manifestKey, tt.existing, nil))
			}

			err := put(store, manifestKey, bodyNew, tt.mutate)
			assert.Equal(t, tt.wantCode, codeOf(err))

			got, readErr := os.ReadFile(filepath.Join(dir, filepath.FromSlash(manifestKey)))
			if tt.wantBody == "" {
				require.ErrorIs(t, readErr, os.ErrNotExist)

				return
			}

			require.NoError(t, readErr)
			assert.Equal(t, tt.wantBody, string(got))
		})
	}
}

func TestPutRefusesABodyThatIsNotItsChecksum(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t)
	sum := sha256.Sum256([]byte("body"))

	err := put(store, "a", "other", func(input *s3.PutObjectInput) {
		input.ChecksumSHA256 = aws.String(base64.StdEncoding.EncodeToString(sum[:]))
	})
	require.ErrorIs(t, err, filepublish.ErrChecksum)

	require.NoError(t, put(store, "a", "body", func(input *s3.PutObjectInput) {
		input.ChecksumSHA256 = aws.String(base64.StdEncoding.EncodeToString(sum[:]))
	}))
}

func TestGetAndHead(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t)
	require.NoError(t, put(store, "k/v", "hello", nil))

	sum := sha256.Sum256([]byte("hello"))

	head, err := store.HeadObject(t.Context(), &s3.HeadObjectInput{Key: aws.String("k/v")})
	require.NoError(t, err)
	assert.Equal(t, int64(5), aws.ToInt64(head.ContentLength))
	assert.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), aws.ToString(head.ChecksumSHA256))
	assert.Equal(t, etagOf("hello"), aws.ToString(head.ETag))

	got, err := store.GetObject(t.Context(), &s3.GetObjectInput{Key: aws.String("k/v")})
	require.NoError(t, err)

	body, err := io.ReadAll(got.Body)
	require.NoError(t, err)
	require.NoError(t, got.Body.Close())
	assert.Equal(t, "hello", string(body))
	assert.Equal(t, etagOf("hello"), aws.ToString(got.ETag))

	_, err = store.GetObject(t.Context(), &s3.GetObjectInput{Key: aws.String("k/v"), VersionId: aws.String("1")})
	assert.Equal(t, "NoSuchVersion", codeOf(err))

	_, err = store.GetObject(t.Context(), &s3.GetObjectInput{Key: aws.String("k/missing")})
	assert.Equal(t, wantNoKey, codeOf(err))

	_, err = store.HeadObject(t.Context(), &s3.HeadObjectInput{Key: aws.String("k/v/below")})
	assert.Equal(t, wantNoKey, codeOf(err), "a file in the place of a directory is a missing key")

	_, err = store.GetObject(t.Context(), &s3.GetObjectInput{Key: aws.String("k")})
	require.ErrorIs(t, err, filepublish.ErrNotRegular)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = store.GetObject(ctx, &s3.GetObjectInput{Key: aws.String("k/v")})
	require.ErrorIs(t, err, context.Canceled)

	_, err = store.HeadObject(ctx, &s3.HeadObjectInput{Key: aws.String("k/v")})
	require.ErrorIs(t, err, context.Canceled)

	_, err = store.PutObject(ctx, &s3.PutObjectInput{Key: aws.String("k/w"), Body: strings.NewReader("x")})
	require.ErrorIs(t, err, context.Canceled)
}

func TestConcurrentCreatesHaveOneWinner(t *testing.T) {
	t.Parallel()

	store, dir := newStore(t)

	const writers = 16

	var (
		wg      sync.WaitGroup
		winners atomic.Int32
		lost    atomic.Int32
	)

	for i := range writers {
		wg.Go(func() {
			err := put(store, manifestKey, strings.Repeat("x", i+1), func(input *s3.PutObjectInput) {
				input.IfNoneMatch = aws.String("*")
			})

			switch codeOf(err) {
			case "":
				winners.Add(1)
			case wantPrecond:
				lost.Add(1)
			}
		})
	}

	wg.Wait()

	assert.Equal(t, int32(1), winners.Load())
	assert.Equal(t, int32(writers-1), lost.Load())

	got, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(manifestKey)))
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("x", len(got)), string(got))
}

// gate holds the write of the manifest until every publisher is at it, so that all of them have read the manifest
// that they replace.
type gate struct {
	s3publish.API

	arrived chan struct{}
	release chan struct{}
}

func (g *gate) PutObject(
	ctx context.Context,
	input *s3.PutObjectInput,
	opts ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	if aws.ToString(input.Key) == manifestKey {
		g.arrived <- struct{}{}

		<-g.release
	}

	out, err := g.API.PutObject(ctx, input, opts...)
	if err != nil {
		return nil, fmt.Errorf("put: %w", err)
	}

	return out, nil
}

func objectsFor(t *testing.T, label string) []s3publish.Object {
	t.Helper()

	body, err := controlplane.MarshalFlags(controlplane.Flags{
		"flag-" + label: {
			Variations:  map[string]any{"on": true, "off": false},
			DefaultRule: &controlplane.Rule{Variation: new("off")},
		},
	})
	require.NoError(t, err)

	return []s3publish.Object{
		{Name: "flags", Kind: controlplane.KindFlags, Format: controlplane.FormatYAML, Body: body},
	}
}

func publisher(t *testing.T, api s3publish.API) *s3publish.Publisher {
	t.Helper()

	pub, err := s3publish.New(api, &s3publish.Config{Bucket: "store", Channel: "stable", Connection: "control"})
	require.NoError(t, err)

	return pub
}

func TestPublishAndVerify(t *testing.T) {
	t.Parallel()

	store, dir := newStore(t)
	pub := publisher(t, store)

	first, err := pub.Publish(t.Context(), s3publish.Request{Objects: objectsFor(t, "a")})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), first.Generation)
	assert.True(t, first.Written)

	again, err := pub.Publish(t.Context(), s3publish.Request{Objects: objectsFor(t, "a")})
	require.NoError(t, err)
	assert.False(t, again.Written)
	assert.Equal(t, 1, again.Present)

	dry, err := pub.Publish(t.Context(), s3publish.Request{Objects: objectsFor(t, "b"), DryRun: true})
	require.NoError(t, err)
	assert.False(t, dry.Written)
	assert.Equal(t, uint64(2), dry.Generation)

	second, err := pub.Publish(t.Context(), s3publish.Request{Objects: objectsFor(t, "b")})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), second.Generation)

	rep, err := pub.Verify(t.Context())
	require.NoError(t, err)
	assert.True(t, rep.OK())

	// damage an object: verify reports it
	var damaged string

	require.NoError(
		t,
		filepath.WalkDir(filepath.Join(dir, "objects"), func(path string, entry os.DirEntry, err error) error {
			if err == nil && !entry.IsDir() && damaged == "" {
				damaged = path
			}

			return err
		}),
	)
	require.NoError(t, os.WriteFile(damaged, bytes.Repeat([]byte("z"), 5), 0o600))

	rep, err = pub.Verify(t.Context())
	require.NoError(t, err)
	assert.False(t, rep.OK())
}

func TestTwoPublishersOverOneManifestHaveOneWinner(t *testing.T) {
	t.Parallel()

	store, dir := newStore(t)

	_, err := publisher(t, store).Publish(t.Context(), s3publish.Request{Objects: objectsFor(t, "base")})
	require.NoError(t, err)

	gated := &gate{API: store, arrived: make(chan struct{}, 2), release: make(chan struct{})}

	labels := []string{"left", "right"}
	results := make([]*s3publish.Result, 2)
	errs := make([]error, 2)

	var wg sync.WaitGroup

	for i, label := range labels {
		wg.Go(func() {
			results[i], errs[i] = publisher(
				t,
				gated,
			).Publish(context.Background(), s3publish.Request{Objects: objectsFor(t, label)})
		})
	}

	<-gated.arrived
	<-gated.arrived
	close(gated.release)
	wg.Wait()

	winner := -1

	for i, publishErr := range errs {
		if publishErr != nil {
			require.ErrorIs(t, publishErr, s3publish.ErrConflict)

			continue
		}

		require.Equal(t, -1, winner, "only one publisher wins")

		winner = i
	}

	require.NotEqual(t, -1, winner, "one publisher wins")

	raw, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(manifestKey)))
	require.NoError(t, err)

	manifest, _, err := controlplane.UnmarshalManifest(raw)
	require.NoError(t, err)

	assert.Equal(t, results[winner].Generation, manifest.Generation)
	require.Len(t, manifest.Objects, len(results[winner].Objects))

	for i := range manifest.Objects {
		obj := &manifest.Objects[i]
		assert.Equal(t, results[winner].Objects[i].Key, obj.Key, "the manifest lists the objects of the winner")

		body, readErr := os.ReadFile(filepath.Join(dir, filepath.FromSlash(obj.Key)))
		require.NoError(t, readErr)
		require.NoError(t, controlplane.VerifyChecksum(body, obj.Checksum))
		assert.Equal(t, objectsFor(t, labels[winner])[0].Body, body, "the flags are the ones of the winner")
	}

	rep, err := publisher(t, store).Verify(t.Context())
	require.NoError(t, err)
	assert.True(t, rep.OK())
}

func TestFailuresOfTheFileSystemAreErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantIs  error
		prepare func(t *testing.T, dir string)
		run     func(store *filepublish.Store) error
		name    string
	}{
		{
			name: "a key longer than the file system allows",
			run: func(store *filepublish.Store) error {
				_, err := store.GetObject(
					context.Background(),
					&s3.GetObjectInput{Key: aws.String(strings.Repeat("a", 300))},
				)

				return err
			},
			wantIs: filepublish.ErrKeyRefused,
		},
		{
			name: "a symbolic link loop",
			prepare: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Symlink("loop", filepath.Join(dir, "loop")))
			},
			run: func(store *filepublish.Store) error {
				_, err := store.GetObject(context.Background(), &s3.GetObjectInput{Key: aws.String("loop")})

				return err
			},
			wantIs: filepublish.ErrKeyRefused,
		},
		{
			name: "a root that went away, on get",
			prepare: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Remove(dir))
			},
			run: func(store *filepublish.Store) error {
				_, err := store.GetObject(context.Background(), &s3.GetObjectInput{Key: aws.String("a")})

				return err
			},
			wantIs: os.ErrNotExist,
		},
		{
			name: "a root that went away, on put",
			prepare: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Remove(dir))
			},
			run:    func(store *filepublish.Store) error { return put(store, "a", "x", nil) },
			wantIs: os.ErrNotExist,
		},
		{
			name: "the lock file is a directory",
			prepare: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Mkdir(filepath.Join(dir, ".semaphore-publish.lock"), 0o750))
			},
			run:    func(store *filepublish.Store) error { return put(store, "a", "x", nil) },
			wantIs: syscall.EISDIR,
		},
		{
			name: "a parent that is a file",
			prepare: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o600))
			},
			run:    func(store *filepublish.Store) error { return put(store, "f/x", "x", nil) },
			wantIs: syscall.ENOTDIR,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, dir := newStore(t)

			if tt.prepare != nil {
				tt.prepare(t, dir)
			}

			require.ErrorIs(t, tt.run(store), tt.wantIs)
		})
	}
}

func TestPortableKeysAreAccepted(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t)

	keys := []string{
		"manifests/stable.yaml", "objects/sha256/ab/cd.json", "..data/manifest.yaml", "console.yaml", "com10",
		"a.tmp-b",
	}

	for _, key := range keys {
		require.NoError(t, put(store, key, "x", nil), key)
	}
}

func TestBodyOverTheLimitIsRefused(t *testing.T) {
	t.Parallel()

	store, err := filepublish.New(t.TempDir(), filepublish.WithMaxBodyBytes(4))
	require.NoError(t, err)

	require.NoError(t, put(store, "a", "1234", nil))
	require.ErrorIs(t, put(store, "b", "12345", nil), filepublish.ErrTooLarge)
}

// lockClock is the timer of a lock wait that the test drives: every wait is announced and ended by the test.
type lockClock struct {
	waits chan chan time.Time
}

func (lc *lockClock) after(time.Duration) <-chan time.Time {
	tick := make(chan time.Time, 1)
	lc.waits <- tick

	return tick
}

// advance ends the current wait and returns the next one, announced by the loop when it waits again.
func (lc *lockClock) advance(t *testing.T, current chan time.Time) chan time.Time {
	t.Helper()

	current <- time.Time{}

	select {
	case next := <-lc.waits:
		return next
	case <-t.Context().Done():
		require.FailNow(t, "the lock wait did not go on")
	}

	return nil
}

// lockRig is a store whose lock is held, and a store that waits for it with a clock that the test drives.
type lockRig struct {
	waiter  *filepublish.Store
	release func()
	clock   *lockClock
	notices chan string
	dir     string
}

func heldLock(t *testing.T) *lockRig {
	t.Helper()

	dir := t.TempDir()

	holder, err := filepublish.New(dir)
	require.NoError(t, err)

	release, err := holder.Lock(t.Context())
	require.NoError(t, err)

	t.Cleanup(release)

	clock := &lockClock{waits: make(chan chan time.Time)}
	notices := make(chan string, 8)

	waiter, err := filepublish.New(dir, filepublish.WithTimer(clock.after), filepublish.WithPollInterval(time.Second),
		filepublish.WithNoticeAfter(2*time.Second), filepublish.WithNotice(func(msg string) { notices <- msg }))
	require.NoError(t, err)

	return &lockRig{waiter: waiter, release: release, clock: clock, notices: notices, dir: dir}
}

func TestLockWaitGivesOneNoticeAndEndsWithTheContext(t *testing.T) {
	t.Parallel()

	rig := heldLock(t)
	waiter, clock, notices, dir := rig.waiter, rig.clock, rig.notices, rig.dir

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { _, lockErr := waiter.Lock(ctx); done <- lockErr }()

	wait := <-clock.waits

	assert.Empty(t, notices, "no notice before the wait is long")

	wait = clock.advance(t, wait)

	assert.Empty(t, notices, "no notice after one second")

	wait = clock.advance(t, wait)

	require.Len(t, notices, 1, "one notice after two seconds")
	assert.Equal(t, "waiting for the publish lock "+filepath.Join(dir, ".semaphore-publish.lock"), <-notices)

	_ = clock.advance(t, wait)

	assert.Empty(t, notices, "the notice is given once")

	cancel()

	lockErr := <-done
	require.ErrorIs(t, lockErr, context.Canceled)
	require.ErrorIs(t, lockErr, filepublish.ErrLockWait)
}

func TestLockWaitProceedsWhenTheLockIsReleased(t *testing.T) {
	t.Parallel()

	rig := heldLock(t)
	waiter, release, clock, dir := rig.waiter, rig.release, rig.clock, rig.dir

	done := make(chan error, 1)

	go func() {
		_, putErr := waiter.PutObject(
			t.Context(),
			&s3.PutObjectInput{Key: aws.String("a"), Body: strings.NewReader("x")},
		)
		done <- putErr
	}()

	wait := <-clock.waits

	release()

	wait <- time.Time{}

	require.NoError(t, <-done)

	got, err := os.ReadFile(filepath.Join(dir, "a"))
	require.NoError(t, err)
	assert.Equal(t, "x", string(got))
}

func TestLockOfACancelledContextIsNeverTaken(t *testing.T) {
	t.Parallel()

	store, dir := newStore(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := store.Lock(ctx)
	require.ErrorIs(t, err, filepublish.ErrLockWait)
	require.ErrorIs(t, err, context.Canceled)

	// the lock is free: a live context takes it at once
	release, err := store.Lock(t.Context())
	require.NoError(t, err)
	release()

	assert.FileExists(t, filepath.Join(dir, ".semaphore-publish.lock"))
}

func TestTheLargestBodyLimitStillWritesTheWholeBody(t *testing.T) {
	t.Parallel()

	store, err := filepublish.New(t.TempDir(), filepublish.WithMaxBodyBytes(math.MaxInt64))
	require.NoError(t, err)

	require.NoError(t, put(store, "a", "the whole body", nil))

	got, err := store.GetObject(t.Context(), &s3.GetObjectInput{Key: aws.String("a")})
	require.NoError(t, err)

	body, err := io.ReadAll(got.Body)
	require.NoError(t, err)
	require.NoError(t, got.Body.Close())
	assert.Equal(t, "the whole body", string(body))
}
