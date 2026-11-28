package publish

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/alecthomas/kong"

	"github.com/blackbirdworks/semaphore/cmd/cli/helper"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/bootstrap"
	"github.com/blackbirdworks/semaphore/controlplane"
	"github.com/blackbirdworks/semaphore/controlplane/filepublish"
	"github.com/blackbirdworks/semaphore/controlplane/flagcheck"
	"github.com/blackbirdworks/semaphore/controlplane/s3publish"
	"github.com/blackbirdworks/semaphore/internal/connection"
)

// HelpVars returns the kong interpolation variables used by [Cmd] help tags.
func HelpVars() kong.Vars {
	return kong.Vars{
		"helpPubConfig":   "bootstrap file whose controlPlane connection is the target",
		"helpPubBucket":   "bucket (without --config)",
		"helpPubPrefix":   "key prefix of the objects and of the default manifest key",
		"helpPubRegion":   "AWS region (without --config)",
		"helpPubEndpoint": "S3 endpoint, for emulators and S3 compatible stores (without --config)",
		"helpPubChannel":  "channel of the manifest (without --config; default stable)",
		"helpPubConn":     "connection name written in the manifest (without --config; default control)",
		"helpPubMKey":     "manifest key (without --config; default <prefix>manifests/<channel>.yaml)",
		"helpOut":         "output format: text or json",
		"helpPubDryRun":   "plan the publication: the same reads, no write",
		"helpPubGen":      "generation to write (default: the one in the bucket plus one)",
		"helpPubReplace":  "replace a manifest in the bucket that is invalid or over the limits (needs --generation)",
		"helpPubVerify":   "Check the manifest and every object it lists in the bucket",
	}
}

// Cmd publishes flag files and flag-set documents to S3 behind a manifest, and verifies the bucket.
//
//nolint:govet // fieldalignment: field order sets kong help flag and command order
type Cmd struct {
	Config      string `help:"${helpPubConfig}"   name:"config"                   short:"c"`
	Bucket      string `help:"${helpPubBucket}"   name:"bucket"`
	Prefix      string `help:"${helpPubPrefix}"   name:"prefix"`
	Region      string `help:"${helpPubRegion}"   name:"region"`
	Endpoint    string `help:"${helpPubEndpoint}" name:"endpoint"`
	Channel     string `help:"${helpPubChannel}"  name:"channel"`
	Connection  string `help:"${helpPubConn}"     name:"connection"`
	ManifestKey string `help:"${helpPubMKey}"     name:"manifest-key"`
	Output      string `default:"text"            help:"${helpOut}"               name:"output"     short:"o"`
	Generation  uint64 `default:"0"               help:"${helpPubGen}"            name:"generation"`
	DryRun      bool   `help:"${helpPubDryRun}"   name:"dry-run"`
	Replace     bool   `help:"${helpPubReplace}"  name:"replace-invalid-manifest"`

	dashVerifyDir bool

	Dir    ApplyCmd  `arg:"" help:"Source directory containing flags/, flagsets/ and segments/."`
	Verify VerifyCmd `cmd:"" help:"${helpPubVerify}"`
}

// SetDashVerifyDir records whether a bare "verify" directory appeared after "--" so it is not confused with
// the "verify" subcommand.
func (c *Cmd) SetDashVerifyDir(enabled bool) { c.dashVerifyDir = enabled }

// RewriteDashVerify rewrites a bare "verify" immediately after the first "--" to "./verify" so Kong dispatches it to
// the <dir> branch rather than the "verify" subcommand.
func RewriteDashVerify(args []string) ([]string, bool) {
	for idx, arg := range args {
		if arg != "--" {
			continue
		}

		if idx+1 < len(args) && args[idx+1] == "verify" {
			rewritten := slices.Clone(args)
			rewritten[idx+1] = "./verify"

			return rewritten, true
		}

		break
	}

	return args, false
}

// Help returns the detailed description of publish for kong's help output.
func (*Cmd) Help() string {
	return `Reads the documents of <dir> (flags/, flagsets/ and segments/, each *.yaml|yml|json), validates them,
uploads the objects that are absent under their content-addressed keys, and writes the manifest last with a
conditional write, so a reader never sees a half-written state.

Where to publish comes from the control-plane connection of a bootstrap file (--config), or from --bucket and
--region. Exit codes: 0 success, 1 failure or bad usage, 2 invalid documents or configuration, 3 conflict (another
publisher won; nothing visible changed), 4 store call failed (safe to repeat), 5 the bucket holds a bad state.`
}

func (c *Cmd) targetFlags() targetFlags {
	return targetFlags{
		config:      c.Config,
		bucket:      c.Bucket,
		prefix:      c.Prefix,
		region:      c.Region,
		endpoint:    c.Endpoint,
		channel:     c.Channel,
		connection:  c.Connection,
		manifestKey: c.ManifestKey,
	}
}

// ApplyCmd uploads source documents from a directory and writes the manifest.
type ApplyCmd struct {
	Dir string `arg:"" help:"Source directory containing flags/, flagsets/ and segments/." name:"dir"`
}

// Run executes the publish operation against ApplyCmd.Dir.
func (a ApplyCmd) Run(ctx context.Context, parent *Cmd, streams *helper.Streams, deps *Deps) error {
	dir := a.Dir
	if parent.dashVerifyDir && dir == "./verify" {
		dir = "verify"
	}

	err := helper.ReportJSONError(
		streams.Stdout,
		parent.Output == outputJSON,
		errorDetails,
		runPublish(ctx, streams, dir, parent, deps),
	)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	return nil
}

// VerifyCmd checks the manifest and every listed object in the bucket without writing.
type VerifyCmd struct{}

// Help returns the detailed description of publish verify for kong's help output.
func (VerifyCmd) Help() string {
	return `Reads the manifest and every listed object of the connection, and checks size, checksum, the content of
each document, and that every flag-set source resolves. It never writes. Exit code 5 when a problem is found.`
}

// Run executes the bucket verification check.
func (VerifyCmd) Run(ctx context.Context, parent *Cmd, streams *helper.Streams, deps *Deps) error {
	var err error
	if parent.DryRun || parent.Generation != 0 || parent.Replace {
		err = fmt.Errorf("%w: --dry-run, --generation, and --replace-invalid-manifest do not apply to verify",
			helper.ErrUsage)
	} else {
		err = runVerify(ctx, streams, parent, deps)
	}

	repErr := helper.ReportJSONError(streams.Stdout, parent.Output == outputJSON, errorDetails, err)
	if repErr != nil {
		return fmt.Errorf("%w", repErr)
	}

	return nil
}

// Run parses args against [Cmd] and executes publish or publish verify with injected context, streams, and deps.
func Run(ctx context.Context, args []string, streams *helper.Streams, deps Deps) error {
	var runner struct {
		Cmd

		Help helper.HelpFlag `help:"Show context-sensitive help." name:"help" short:"h"`
	}

	parser, err := kong.New(&runner,
		kong.Name("publish"),
		kong.Description("Publish flag files and flag-set documents to S3 behind a manifest"),
		kong.NoDefaultHelp(),
		kong.KindMapper(reflect.String, kong.MapperFunc(helper.DecodeString)),
		kong.Writers(streams.Stdout, streams.Stderr),
		kong.ConfigureHelp(kong.HelpOptions{WrapUpperBound: helper.HelpWidth}),
		HelpVars(),
	)
	if err != nil {
		return fmt.Errorf("build publish parser: %w", err)
	}

	parseArgs, dashVerifyDir := RewriteDashVerify(args)
	runner.Cmd.SetDashVerifyDir(dashVerifyDir)

	kctx, exited, err := helper.Parse(parser, parseArgs)
	if exited {
		return nil
	}

	if err != nil {
		if parseErr, ok := errors.AsType[*kong.ParseError](err); ok {
			val, found := helper.ScanOutputFlag(parseErr.Context.Flags(), args)
			repErr := helper.ReportJSONError(
				streams.Stdout,
				found && val == outputJSON,
				errorDetails,
				fmt.Errorf("%w: %w", helper.ErrUsage, err),
			)

			return fmt.Errorf("%w", repErr)
		}

		return fmt.Errorf("%w: %w", helper.ErrUsage, err)
	}

	kctx.BindTo(ctx, (*context.Context)(nil))

	//nolint:wrapcheck // subcommand Run already wraps errors and reports JSON
	return kctx.Run(&runner.Cmd, streams, &deps)
}

func checkOutput(format string) error {
	if format != outputText && format != outputJSON {
		return fmt.Errorf("%w: --output must be text or json, not %q", helper.ErrUsage, format)
	}

	return nil
}

func runPublish(ctx context.Context, streams *helper.Streams, dir string, opts *Cmd, deps *Deps) error {
	err := checkOutput(opts.Output)
	if err != nil {
		return err
	}

	tf := opts.targetFlags()

	tgt, err := resolveTarget(&tf, deps)
	if err != nil {
		return withCode(err)
	}

	objects, err := LoadSource(deps.OpenDir(dir), dir, tgt.sourceLimits())
	if err != nil {
		return withCode(err)
	}

	tgt.cfg.Check = checkObject
	tgt.cfg.CheckAll = checkReferences

	pub, err := newPublisher(ctx, tgt, deps, func(msg string) { _, _ = fmt.Fprintln(streams.Stderr, msg) })
	if err != nil {
		return withCode(err)
	}

	created := deps.Now().UTC().Truncate(time.Second)

	res, err := pub.Publish(ctx, s3publish.Request{
		Objects: objects, Generation: opts.Generation, CreatedAt: &created, DryRun: opts.DryRun,
		ReplaceInvalidManifest: opts.Replace,
	})
	if err != nil {
		return withCode(err)
	}

	return printResult(streams.Stdout, opts.Output, res)
}

// checkObject validates a flag file or a segments document with the engine of the relay (the flagcheck package).
func checkObject(obj *s3publish.Object) error {
	if obj.Kind == controlplane.KindSegments {
		return checkSegments(obj)
	}

	if obj.Kind != controlplane.KindFlags {
		return nil
	}

	err := flagcheck.Check(obj.Body, obj.Format)
	if err != nil {
		return fmt.Errorf("engine check: %w", err)
	}

	return nil
}

// errorDetails lists the individual problems behind an error: the field errors of a validation and the flags
// that the engine rejects, in the order reported.
func errorDetails(err error) []string {
	var details []string

	if verr, ok := errors.AsType[*controlplane.ValidationError](err); ok {
		for i := range verr.Problems {
			details = append(details, verr.Problems[i].Error())
		}
	}

	if rerr, ok := errors.AsType[*flagcheck.ReferenceError](err); ok {
		details = append(details, rerr.Error())
	}

	for _, leaf := range leaves(err) {
		if perr, ok := errors.AsType[*flagcheck.PublicError](leaf); ok {
			for i := range perr.Problems {
				details = append(details, perr.Problems[i].Message())
			}
		}
	}

	if ferr, ok := errors.AsType[*flagcheck.Error](err); ok {
		for i := range ferr.Problems {
			details = append(details, ferr.Problems[i].Message())
		}
	}

	return details
}

// leaves lists the errors of a joined error (errors.Join, or an error with an Unwrap() []error), in order, looking
// through the wrappers that hold one error; any other error is its own leaf.
func leaves(err error) []error {
	if multi, ok := err.(interface{ Unwrap() []error }); ok {
		out := make([]error, 0, len(multi.Unwrap()))

		for _, each := range multi.Unwrap() {
			out = append(out, leaves(each)...)
		}

		return out
	}

	if single, ok := err.(interface{ Unwrap() error }); ok && single.Unwrap() != nil {
		return leaves(single.Unwrap())
	}

	return []error{err}
}

// newPublisher builds the publisher of the target; notice receives the message of a lock that stays busy.
func newPublisher(ctx context.Context, tgt *target, deps *Deps, notice func(string)) (*s3publish.Publisher, error) {
	var api s3publish.API

	switch {
	case tgt.root != "" && deps.NewAPI == nil:
		opts := []filepublish.Option{
			filepublish.WithNotice(notice),
			filepublish.WithMaxBodyBytes(max(tgt.cfg.MaxObjectBytes, tgt.cfg.MaxManifestBytes)),
		}

		if deps.FileOptions != nil {
			opts = append(opts, deps.FileOptions()...)
		}

		store, err := filepublish.New(tgt.root, opts...)
		if err != nil {
			return nil, fmt.Errorf("connection %s: %w", tgt.name, err)
		}

		api = store
	case deps.NewAPI != nil:
		custom, err := deps.NewAPI(ctx, tgt.spec, tgt.name)
		if err != nil {
			return nil, err
		}

		api = custom
	default:
		client, err := newClient(ctx, deps.ConnectionOptions, tgt)
		if err != nil {
			return nil, err
		}

		api = client
	}

	pub, err := s3publish.New(api, &tgt.cfg)
	if err != nil {
		return nil, fmt.Errorf("publisher: %w", err)
	}

	return pub, nil
}

func runVerify(ctx context.Context, streams *helper.Streams, opts *Cmd, deps *Deps) error {
	err := checkOutput(opts.Output)
	if err != nil {
		return err
	}

	tf := opts.targetFlags()

	tgt, err := resolveTarget(&tf, deps)
	if err != nil {
		return withCode(err)
	}

	tgt.cfg.Check = checkObject
	tgt.cfg.CheckAll = checkReferences

	pub, err := newPublisher(ctx, tgt, deps, nil)
	if err != nil {
		return withCode(err)
	}

	rep, err := pub.Verify(ctx)
	if err != nil {
		return withCode(err)
	}

	err = printReport(streams.Stdout, opts.Output, rep)
	if err != nil {
		return err
	}

	if !rep.OK() {
		return &helper.ExitError{
			Err:  helper.MarkReported(fmt.Errorf("%w: %d problems", errVerify, len(rep.Problems))),
			Code: helper.ExitVerify,
		}
	}

	return nil
}

var errVerify = errors.New("verification failed")

// withCode attaches the exit code that the kind of error calls for: 2 invalid input, 3 conflict, 4 store call
// failed, 5 bad state in the bucket, 1 anything else.
func withCode(err error) error {
	if errors.As(err, new(*helper.ExitError)) {
		return err
	}

	return &helper.ExitError{Err: err, Code: codeOf(err)}
}

func codeOf(err error) int {
	switch {
	case errors.Is(err, s3publish.ErrConflict):
		return helper.ExitConflict
	case errors.Is(err, filepublish.ErrLockWait):
		return helper.ExitFailure
	case errors.Is(err, filepublish.ErrKeyRefused), errors.Is(err, filepublish.ErrTooLarge),
		errors.Is(err, filepublish.ErrUnsupported):
		return helper.ExitInvalid
	case errors.As(err, new(*s3publish.OperationError)):
		return helper.ExitTransport
	case errors.Is(err, s3publish.ErrPreviousManifest), errors.Is(err, s3publish.ErrObjectMismatch),
		errors.Is(err, s3publish.ErrNoManifest):
		return helper.ExitVerify
	case errors.Is(err, helper.ErrUsage):
		return helper.ExitFailure
	case isInvalidInput(err):
		return helper.ExitInvalid
	}

	return helper.ExitFailure
}

// isInvalidInput reports the errors that the caller fixes by changing documents or configuration.
func isInvalidInput(err error) bool {
	for _, target := range []error{
		s3publish.ErrInvalid, s3publish.ErrConfig, s3publish.ErrGeneration, s3publish.ErrChannelMismatch,
		ErrUnknownFile, ErrDuplicateObject, ErrNoDocuments, ErrNotRegular, ErrFileTooLarge, ErrSourceTooLarge,
		ErrNoControlPlane, ErrNotS3, bootstrap.ErrSecretUnresolved,
	} {
		if errors.Is(err, target) {
			return true
		}
	}

	return errors.As(err, new(*bootstrap.Error)) || errors.As(err, new(*connection.SpecError))
}
