package corehttp

import (
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/semaphore/cmd/relayproxy/flagruntime"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/model"
	"github.com/blackbirdworks/semaphore/cmd/relayproxy/relaycore"
	"github.com/blackbirdworks/semaphore/internal/manifestreader"
	"github.com/blackbirdworks/semaphore/modules/core/clock"
)

const (
	kindManifest         = "manifest"
	maxUnknownFieldPaths = 10
)

// monitoring answers the health, the info and the probe endpoints from a core.
type monitoring struct {
	*Readiness

	holder *flagruntime.Holder
}

// Health reports that the server is initialized. It is liveness: it never fails.
func (monitoring) Health() model.HealthResponse { return model.HealthResponse{Initialized: true} }

// Info lists the flag sets with the date of their last change (the fields of v1) and the state of the runtime. It
// works with zero flag sets.
func (m monitoring) Info() (model.InfoResponse, error) {
	current := m.holder.Load()
	dates := make(map[string]time.Time, len(current.FlagSets))

	var latest time.Time

	for name, set := range current.FlagSets {
		dates[name] = set.ModifiedAt
		if set.ModifiedAt.After(latest) {
			latest = set.ModifiedAt
		}
	}

	status := m.src.Status()
	cfg := configInfo(&status, clock.Now())

	addSegmentInfo(&cfg, &status.Segments)

	return model.InfoResponse{Flagsets: dates, LatestCacheRefresh: &latest, Config: &cfg}, nil
}

func configInfo(status *relaycore.Status, now time.Time) model.ConfigInfo {
	info := model.ConfigInfo{
		Reloads: map[string]uint64{
			string(flagruntime.OutcomeApplied):  status.Reloads.Applied,
			string(flagruntime.OutcomeRejected): status.Reloads.Rejected,
			string(flagruntime.OutcomeIgnored):  status.Reloads.Ignored,
		},
		FlagSets: []model.FlagSetInfo{}, Objects: []model.ObjectInfo{}, Withdrawn: []model.WithdrawnInfo{},
		PendingRestart: []string{},
		Public:         model.PublicInfo{Sets: []string{}, Flags: 0, Present: false},
		Staleness:      staleness(status, now),
		State:          stateInfo(&status.Source.Reader.State),
	}

	info.Reader = readerInfo(&status.Source.Reader.Manifest)
	info.LastReload = roundInfo(status.Reloads.LastRound)
	info.LastApplied = roundInfo(status.Reloads.LastApplied)
	info.UnknownFields = UnknownFieldsInfo(status)

	if status.Reloads.LastRejected != nil {
		rejected := roundInfo(status.Reloads.LastRejected)
		info.LastRejected = &rejected
	}

	current := status.Source.Runtime
	if current == nil {
		return info
	}

	info.Generation, info.Snapshot = current.Generation, current.Snapshot
	info.DisabledAdminKeys = current.DisabledAdminKeys
	info.PendingRestart = append(info.PendingRestart, current.PendingRestart...)

	if public := current.Public; public != nil {
		info.Public = model.PublicInfo{Present: public.Present, Flags: public.Flags, Sets: slices.Clone(public.Sets)}
	}

	failures := map[string]manifestreader.ObjectFailure{}
	for _, failure := range status.Source.Reader.Failures {
		failures[failure.Name] = failure
	}

	referenced := map[string]struct{}{}

	for _, set := range current.FlagSets {
		info.FlagSets = append(info.FlagSets, flagSetInfo(set))

		for _, source := range set.Sources {
			referenced[source] = struct{}{}
		}
	}

	for _, obj := range current.Objects {
		info.Objects = append(info.Objects, objectInfo(obj, failures))

		// only the objects that a flag set serves from count: a restart loses nothing of the others
		if _, used := referenced[obj.Name]; used {
			info.State.KeptFlags += keptFlags(obj)
		}
	}

	for _, w := range current.Withdrawn {
		info.Withdrawn = append(info.Withdrawn, model.WithdrawnInfo{Name: w.Name, Generation: w.Generation})
	}

	slices.SortFunc(info.FlagSets, func(a, b model.FlagSetInfo) int { return strings.Compare(a.Name, b.Name) })
	slices.SortFunc(info.Objects, func(a, b model.ObjectInfo) int { return strings.Compare(a.Name, b.Name) })

	return info
}

func readerInfo(manifest *manifestreader.ManifestStatus) *model.ReaderInfo {
	if manifest.Outcome == "" || manifest.Outcome == manifestreader.OutcomeNone {
		return nil
	}

	out := &model.ReaderInfo{
		Outcome: manifest.Outcome,
		Reason:  manifestreader.RejectionReason(manifest.Outcome, manifest.Reason),
	}
	if !manifest.At.IsZero() {
		at := manifest.At
		out.At = &at
	}

	return out
}

// UnknownFieldsInfo summarises the unknown fields of the served documents by kind: the count and at most ten paths.
func UnknownFieldsInfo(status *relaycore.Status) []model.UnknownFieldsInfo {
	byKind := map[string]*model.UnknownFieldsInfo{}

	manifest := status.Source.Reader.Manifest
	if manifest.UnknownFields > 0 {
		var fields []string

		if len(manifest.UnknownFieldPaths) > 0 {
			limit := min(len(manifest.UnknownFieldPaths), maxUnknownFieldPaths)
			fields = slices.Clone(manifest.UnknownFieldPaths[:limit])
		}

		byKind[kindManifest] = &model.UnknownFieldsInfo{
			Kind: kindManifest, Fields: fields, Count: manifest.UnknownFields,
		}
	}

	if status.Source.Runtime != nil {
		collectObjectUnknownFields(byKind, status.Source.Runtime.Objects)
	}

	if len(byKind) == 0 {
		return nil
	}

	out := make([]model.UnknownFieldsInfo, 0, len(byKind))
	for _, kind := range slices.Sorted(maps.Keys(byKind)) {
		out = append(out, *byKind[kind])
	}

	return out
}

// path is the unknown field path "object/path" of an object, kept in two parts so that no string is built for it.
type path struct{ object, field string }

func (p path) String() string { return p.object + "/" + p.field }

// byteAt returns the byte i of object+"/"+field and whether there is one.
func (p path) byteAt(i int) (byte, bool) {
	switch {
	case i < len(p.object):
		return p.object[i], true
	case i == len(p.object):
		return '/', true
	case i-len(p.object) <= len(p.field):
		return p.field[i-len(p.object)-1], true
	}

	return 0, false
}

// compare orders two paths as the strings they stand for.
func (p path) compare(other path) int {
	for i := 0; ; i++ {
		mine, haveMine := p.byteAt(i)
		theirs, haveTheirs := other.byteAt(i)

		switch {
		case !haveMine && !haveTheirs:
			return 0
		case !haveMine:
			return -1
		case !haveTheirs:
			return 1
		case mine != theirs:
			return int(mine) - int(theirs)
		}
	}
}

// collectObjectUnknownFields adds the unknown fields of the objects to byKind. The paths are "object/path": the entry
// keeps the ten smallest of them as strings, sorted (so object "a-b" is before object "a": "-" sorts before "/").
// It never holds more than ten paths per kind and builds no string before the end, so the work and the allocations
// do not grow with the number of objects. The result does not depend on the order of the map.
func collectObjectUnknownFields(
	byKind map[string]*model.UnknownFieldsInfo, objects map[string]*flagruntime.ObjectStatus,
) {
	smallest := map[string][]path{}

	for _, obj := range objects {
		if obj.UnknownFields == 0 {
			continue
		}

		entry := byKind[obj.Kind]
		if entry == nil {
			entry = &model.UnknownFieldsInfo{Kind: obj.Kind}
			byKind[obj.Kind] = entry
			smallest[obj.Kind] = make([]path, 0, maxUnknownFieldPaths)
		}

		entry.Count += obj.UnknownFields

		for _, field := range obj.UnknownFieldPaths {
			smallest[obj.Kind] = keepSmallest(smallest[obj.Kind], path{object: obj.Name, field: field})
		}
	}

	for kind, paths := range smallest {
		fields := byKind[kind].Fields

		for _, p := range paths {
			fields = append(fields, p.String())
		}

		slices.Sort(fields)

		byKind[kind].Fields = fields[:min(len(fields), maxUnknownFieldPaths)]
	}
}

// keepSmallest inserts entry into the sorted list of at most ten paths when it is among the ten smallest.
func keepSmallest(paths []path, entry path) []path {
	full := len(paths) >= maxUnknownFieldPaths
	if full && entry.compare(paths[len(paths)-1]) >= 0 {
		return paths
	}

	pos, _ := slices.BinarySearchFunc(paths, entry, path.compare)
	if full {
		paths = paths[:len(paths)-1]
	}

	return slices.Insert(paths, pos, entry)
}

func stateInfo(state *manifestreader.StateStatus) model.StateInfo {
	return model.StateInfo{
		Enabled: state.Enabled, LastSaveError: state.LastSaveError, Saves: state.Saves, SaveErrors: state.SaveErrors,
	}
}

// keptFlags counts the rejected flags of the object that keep serving an earlier definition.
func keptFlags(obj *flagruntime.ObjectStatus) int {
	kept := 0

	for _, rejection := range obj.Rejected {
		if rejection.Kept {
			kept++
		}
	}

	return kept
}

func roundInfo(round *relaycore.RoundSummary) model.ReloadInfo {
	if round == nil {
		return model.ReloadInfo{At: nil, Outcome: "", Code: "", Generation: 0}
	}

	at := round.At

	return model.ReloadInfo{At: &at, Outcome: round.Outcome, Code: round.Code, Generation: round.Generation}
}

func staleness(status *relaycore.Status, now time.Time) model.StalenessInfo {
	var out model.StalenessInfo

	if sync := status.Source.Reader.LastSync; !sync.IsZero() {
		out.LastConfirmed = &sync
		out.AgeSeconds = max(0, now.Sub(sync).Seconds())
	}

	if ok := status.Reloads.LastSuccess; !ok.IsZero() {
		out.LastSuccessfulReload = &ok
	}

	return out
}

func flagSetInfo(set *flagruntime.FlagSetStatus) model.FlagSetInfo {
	out := model.FlagSetInfo{
		Name: set.Name, State: setLabel(set.State), Problems: []model.ProblemInfo{},
		Flags: set.Flags, APIKeys: set.APIKeys, Rejected: set.Rejected,
	}

	if set.Segments != nil {
		out.Segments = segmentCountsInfo(set.Segments)
	}

	for _, p := range set.Problems {
		out.Problems = append(out.Problems, model.ProblemInfo{Code: string(p.Code), Subject: p.Subject})
	}

	return out
}

func segmentCountsInfo(counts *flagruntime.SegmentCounts) *model.SegmentCountsInfo {
	out := &model.SegmentCountsInfo{
		Static: counts.Static, Dynamic: counts.Dynamic, Unresolved: counts.Unresolved,
		UnresolvedNames: counts.UnresolvedNames,
	}

	if counts.Dynamic > 0 {
		out.DynamicStates = &model.DynamicStatesInfo{
			Valid: counts.Dynamic - counts.RejectedDefinitions, Rejected: counts.RejectedDefinitions,
		}
	}

	return out
}

func objectInfo(obj *flagruntime.ObjectStatus, failures map[string]manifestreader.ObjectFailure) model.ObjectInfo {
	// the listed checksum is known for an object that failed to load, and equals the held one when it is ok; for
	// the other states (invalid, pending) the status does not carry it
	failure, failed := failures[obj.Name]
	want, reason := failure.Checksum, failure.Reason

	if !failed && obj.State == flagruntime.ObjectOK {
		want = obj.Checksum
	}

	return model.ObjectInfo{
		Name: obj.Name, Kind: obj.Kind, State: string(obj.State), Reason: reason,
		ChecksumHeld: obj.Checksum, ChecksumListed: want, Flags: obj.Flags, Rejected: len(obj.Rejected),
		Kept: keptFlags(obj),
	}
}
