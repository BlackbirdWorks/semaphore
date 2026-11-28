package publish

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/semaphore/controlplane/filepublish"
	"github.com/blackbirdworks/semaphore/controlplane/s3publish"
	"github.com/blackbirdworks/semaphore/internal/connection"
	"github.com/blackbirdworks/semaphore/modules/core/clock"
)

// Deps are the parts of the outside world that the commands use. Production code uses [DefaultDeps]; tests and
// the end-to-end tests replace what they need.
type Deps struct {
	// LookupEnv reads an environment variable (secret references of the bootstrap file).
	LookupEnv func(string) (string, bool)
	// ReadFile reads a file (the bootstrap file and its secret files).
	ReadFile func(string) ([]byte, error)
	// OpenDir opens the source directory.
	OpenDir func(dir string) fs.FS
	// NewAPI builds the API of the target (the spec is nil for a file store). Nil means the connection factory of the
	// relay for S3 (it honours ConnectionOptions) and a [filepublish.Store] for a file store.
	NewAPI func(ctx context.Context, spec *connection.S3Spec, name string) (s3publish.API, error)
	// Now stamps createdAt of the manifest.
	Now func() time.Time
	// FileOptions returns the options of the file store of a file target, applied after the options of the command (a
	// test drives the lock wait with a timer of its own). Nil means none.
	FileOptions func() []filepublish.Option
	// ConnectionOptions are passed to the connection factory (a round tripper that counts requests, for example).
	ConnectionOptions []connection.Option
}

// DefaultDeps returns the dependencies of the real command.
func DefaultDeps() Deps {
	return Deps{
		LookupEnv: os.LookupEnv,
		ReadFile:  os.ReadFile,
		OpenDir:   os.DirFS,
		Now:       func() time.Time { return clock.Now() },
	}
}

// newClient builds the S3 client of the target with the connection factory of the relay.
func newClient(ctx context.Context, opts []connection.Option, tgt *target) (*s3.Client, error) {
	factory := connection.New(opts...)

	err := factory.Set(tgt.name, tgt.spec)
	if err != nil {
		return nil, fmt.Errorf("connection %s: %w", tgt.name, err)
	}

	client, err := factory.S3(ctx, tgt.name)
	if err != nil {
		return nil, fmt.Errorf("connection %s: %w", tgt.name, err)
	}

	return client, nil
}
