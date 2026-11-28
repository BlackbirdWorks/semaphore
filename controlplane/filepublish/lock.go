package filepublish

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/blackbirdworks/semaphore/internal/manifestreader"
)

// Lock takes the exclusive advisory lock of the store and returns the function that releases it. Every write of a
// [Store] holds it for its read-compare-rename section; a caller that holds it keeps all writers out. It waits for a
// busy lock with the timer of the store, gives the notice once, and ends with the error of ctx when ctx ends.
func (s *Store) Lock(ctx context.Context) (func(), error) {
	root, err := manifestreader.OpenFileRoot(s.root)
	if err != nil {
		return nil, fmt.Errorf("lock: %w", err)
	}

	defer func() { _ = root.Close() }()

	return s.acquire(ctx, root)
}

func (s *Store) acquire(ctx context.Context, root *os.Root) (func(), error) {
	file, err := root.OpenFile(lockName, os.O_RDWR|os.O_CREATE, lockMode)
	if err != nil {
		return nil, fmt.Errorf("lock file: %w", err)
	}

	var waited time.Duration

	where := filepath.Join(s.root, lockName)

	for {
		held, tryErr := tryLock(file)
		if tryErr != nil {
			_ = file.Close()

			return nil, fmt.Errorf("lock %s: %w", where, tryErr)
		}

		if held {
			return func() { releaseLock(file) }, nil
		}

		select {
		case <-ctx.Done():
			_ = file.Close()

			return nil, fmt.Errorf("%w %s: %w", ErrLockWait, where, ctx.Err())
		case <-s.set.after(s.set.poll):
		}

		waited += s.set.poll
		if waited >= s.set.noticeAfter && s.set.notice != nil && s.noticed.CompareAndSwap(false, true) {
			s.set.notice("waiting for the publish lock " + where)
		}
	}
}
