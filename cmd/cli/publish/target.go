package publish

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/semaphore/cmd/cli/helper"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/bootstrap"
	"github.com/blackbirdworks/semaphore/controlplane/s3publish"
	"github.com/blackbirdworks/semaphore/internal/connection"
)

// Defaults of the explicit target flags.
const (
	defaultChannel    = "stable"
	defaultConnection = "control"
)

// Errors of the target resolution. Test them with [errors.Is].
var (
	// ErrNoControlPlane is returned when the bootstrap file has no controlPlane section.
	ErrNoControlPlane = errors.New("the bootstrap file has no controlPlane section")
	// ErrNotS3 is returned when the control-plane connection is neither an S3 nor a file connection.
	ErrNotS3 = errors.New("the controlPlane connection is neither an s3 nor a file connection")
)

// targetFlags are the flags that say where to publish.
type targetFlags struct {
	config      string
	bucket      string
	prefix      string
	region      string
	endpoint    string
	channel     string
	connection  string
	manifestKey string
}

// explicit lists the flags that only make sense without --config.
func (flags *targetFlags) explicit() []string {
	var set []string

	for _, p := range []struct{ name, value string }{
		{"--bucket", flags.bucket}, {"--region", flags.region}, {"--endpoint", flags.endpoint}, {"--channel", flags.channel},
		{"--connection", flags.connection}, {"--manifest-key", flags.manifestKey},
	} {
		if p.value != "" {
			set = append(set, p.name)
		}
	}

	return set
}

// target is where to publish and how to reach it.
type target struct {
	spec *connection.S3Spec
	// root is the directory of a file store; empty for S3.
	root string
	name string
	cfg  s3publish.Config
}

// sourceLimits returns the limits that the loader applies while it reads the tree: the ones of the publisher.
func (t *target) sourceLimits() SourceLimits {
	return SourceLimits{
		MaxObjects: t.cfg.MaxObjects, MaxObjectBytes: t.cfg.MaxObjectBytes, MaxTotalBytes: t.cfg.MaxTotalBytes,
	}
}

// resolveTarget builds the target from the bootstrap file (--config) or from the explicit flags.
func resolveTarget(flags *targetFlags, deps *Deps) (*target, error) {
	if flags.config != "" {
		if set := flags.explicit(); len(set) > 0 {
			return nil, fmt.Errorf("%w: %s cannot be combined with --config (the file says where to publish)",
				helper.ErrUsage, strings.Join(set, ", "))
		}

		return targetFromConfig(flags, deps)
	}

	return targetFromFlags(flags)
}

func targetFromFlags(flags *targetFlags) (*target, error) {
	if flags.bucket == "" || flags.region == "" {
		return nil, fmt.Errorf("%w: give --config, or --bucket and --region", helper.ErrUsage)
	}

	channel := orDefault(flags.channel, defaultChannel)
	name := orDefault(flags.connection, defaultConnection)
	spec := &connection.S3Spec{
		Region:      flags.region,
		Endpoint:    flags.endpoint,
		Credentials: connection.Credentials{Type: connection.CredentialsChain},
		Bucket:      flags.bucket,
		// A custom endpoint (an emulator, an S3 compatible store) rarely serves virtual-hosted bucket names.
		PathStyle: flags.endpoint != "",
	}

	return &target{
		spec: spec, name: name,
		cfg: s3publish.Config{
			Bucket:      flags.bucket,
			Prefix:      flags.prefix,
			Channel:     channel,
			Connection:  name,
			ManifestKey: flags.manifestKey,
		},
	}, nil
}

func targetFromConfig(flags *targetFlags, deps *Deps) (*target, error) {
	opts := bootstrap.LoadOptions{LookupEnv: deps.LookupEnv, ReadFile: deps.ReadFile, SkipRelayChecks: true}

	cfg, err := bootstrap.Load(&bootstrap.Overrides{Config: flags.config}, opts)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", flags.config, err)
	}

	plane := cfg.ControlPlane
	if plane == nil {
		return nil, fmt.Errorf("%s: %w", flags.config, ErrNoControlPlane)
	}

	conn := cfg.Connections[plane.Connection]
	if conn.File != nil {
		return fileTarget(flags, plane, conn.File), nil
	}

	if conn.S3 == nil {
		return nil, fmt.Errorf("%s: connection %q: %w", flags.config, plane.Connection, ErrNotS3)
	}

	spec, err := bootstrap.S3Spec(conn.S3, opts)
	if err != nil {
		return nil, fmt.Errorf("%s: connection %q: %w", flags.config, plane.Connection, err)
	}

	out := s3publish.Config{
		Bucket: conn.S3.Bucket, Prefix: flags.prefix, Channel: plane.Manifest.Channel, Connection: plane.Connection,
		ManifestKey: plane.Manifest.Key,
	}
	if plane.Manifest.MaxObjects != nil {
		out.MaxObjects = *plane.Manifest.MaxObjects
	}

	return &target{spec: spec, name: plane.Connection, cfg: out}, nil
}

// fileTarget is the target of a file connection: the root directory stands for the bucket.
func fileTarget(flags *targetFlags, plane *bootstrap.ControlPlane, conn *bootstrap.FileConnection) *target {
	out := s3publish.Config{
		Bucket: conn.Path, Prefix: flags.prefix, Channel: plane.Manifest.Channel, Connection: plane.Connection,
		ManifestKey: plane.Manifest.Key,
	}
	if plane.Manifest.MaxObjects != nil {
		out.MaxObjects = *plane.Manifest.MaxObjects
	}

	return &target{root: conn.Path, name: plane.Connection, cfg: out}
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}

	return v
}
