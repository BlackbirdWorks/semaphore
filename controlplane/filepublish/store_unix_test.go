//go:build unix

package filepublish_test

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/semaphore/controlplane/filepublish"
)

func TestPutCreatesFilesAndDirectoriesThatOthersCannotWrite(t *testing.T) {
	t.Parallel()

	store, dir := newStore(t)

	require.NoError(t, put(store, "objects/sha256/ab/cd.yaml", "one", nil))

	require.NoError(t, filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		require.NoError(t, err)

		if path == dir {
			return nil
		}

		info, infoErr := entry.Info()
		require.NoError(t, infoErr)
		assert.Zero(t, info.Mode().Perm()&0o022, "%s is writable by group or others", path)

		return nil
	}))
}

// A FIFO with no writer must never make a read or a write wait: the open does not block, the type is read from the
// handle, and the lock is free afterwards.
func TestAFifoAtAKeyIsRefusedWithoutBlocking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
	}{
		{name: "an object key", key: "objects/sha256/ab/cd.yaml"},
		{name: "the manifest key", key: manifestKey},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, dir := newStore(t)

			require.NoError(t, os.MkdirAll(filepath.Join(dir, filepath.Dir(filepath.FromSlash(tt.key))), 0o750))
			require.NoError(t, syscall.Mkfifo(filepath.Join(dir, filepath.FromSlash(tt.key)), 0o600))

			require.ErrorIs(t, put(store, tt.key, "x", nil), filepublish.ErrNotRegular)
			require.ErrorIs(t, put(store, tt.key, "x", func(in *s3.PutObjectInput) { in.IfMatch = aws.String(`"x"`) }),
				filepublish.ErrNotRegular)

			_, err := store.GetObject(context.Background(), &s3.GetObjectInput{Key: aws.String(tt.key)})
			require.ErrorIs(t, err, filepublish.ErrNotRegular)

			_, err = store.HeadObject(context.Background(), &s3.HeadObjectInput{Key: aws.String(tt.key)})
			require.ErrorIs(t, err, filepublish.ErrNotRegular)

			// the lock was released by the refused writes
			require.NoError(t, put(store, "other", "x", nil))
		})
	}
}
