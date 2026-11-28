package flagruntime

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/semaphore/modules/core/flagdef"
	"github.com/blackbirdworks/semaphore/modules/core/segment"
)

// SetState is the state of a flag set.
type SetState string

// The states of a flag set (issue 6).
const (
	// StateServing: every source is current and every flag is valid.
	StateServing SetState = "serving"
	// StateDegraded: the flag set serves, but a source is stale or missing, a flag was rejected, an entry
	// problem was reported, or a source does not resolve.
	StateDegraded SetState = "degraded"
	// StatePending: no source of the flag set was ever loaded, or its entry is invalid and it never had a valid
	// definition; it serves zero flags (its keys still route to it).
	StatePending SetState = "pending"
	// StateWithdrawn: the publisher removed the flag set. It is the state of the entries of Status.Withdrawn and
	// of the Removed change event; a withdrawn flag set is not in FlagSets and its keys are not routed.
	StateWithdrawn SetState = "withdrawn"
)

// Runtime is one immutable generation of what the relay serves. Nothing may mutate a Runtime or anything it
// reaches (the maps, the flag sets, the flags, the variation values, the status): they are shared between
// generations and read without a lock.
type Runtime struct {
	// FlagSets are the flag sets by name. A flag set that did not change is the same pointer as in the
	// previous Runtime.
	FlagSets map[string]*FlagSet
	// ByKey routes an API key to its flag set. It is rebuilt for every generation that changes a flag set
	// or a key: O(keys), not O(flags).
	ByKey map[string]*FlagSet
	// AdminKeys are the admin keys of the bootstrap file.
	AdminKeys map[string]struct{}
	// Public is the flag set that serves the requests without an API key: the flag sets published without a key,
	// merged into one namespace. Nil when the generation has none: such a request is refused.
	Public *FlagSet
	// Status is the status tree of this runtime.
	Status *Status
	// Generation is the manifest generation of the content.
	Generation uint64
	// Snapshot is a local counter, strictly increasing with every successful swap.
	Snapshot uint64
}

// FlagSetByName returns the flag set with a name. It does not allocate.
func (r *Runtime) FlagSetByName(name string) (*FlagSet, bool) {
	fs, ok := r.FlagSets[name]

	return fs, ok
}

// FlagSet is the immutable flag table of one flag set. It is reused by pointer across generations while its
// definition and the content of its sources are unchanged.
type FlagSet struct {
	ModifiedAt time.Time
	change     *changeMemo
	Flags      map[string]*flagdef.InternalFlag
	// Segments is what the rules of the flag set can use as segments, nil when the flag set has none.
	Segments *SetSegments
	// flagCatalogs is, for the merged public flag set of several sources, the catalog of the flag set that each flag
	// comes from: a segment name is looked up in the namespace of the flag set of the flag, never in a merged one.
	flagCatalogs map[string]*segment.Catalog
	// flagSigs is, for each flag of the set that uses segments, a hash of the segments it uses (members included). Two
	// builds of a flag set that hold the same flag pointer differ for a flag when its signature differs.
	flagSigs map[string]uint64
	// dynTerms is, for each flag that uses a served dynamic segment, the terms that the view serves: what an
	// evaluation fetches in one batch before the rules run.
	dynTerms map[string][]segment.Term
	// uses is, for each flag of the set that uses segments, the segments it uses and whether one is dynamic.
	uses       map[string]segUse
	Name       string
	State      SetState
	Hash       uint64
	APIKeys    int
	Rejected   int
	changeOnce sync.Once
}

// ConfigETag is the identity of the configuration the flag set serves: the hash of its flags (and of the segments
// they use), in hexadecimal. It depends on the content only, so every replica that serves the same content has the
// same value, and a rebuild that changes nothing keeps it.
func (f *FlagSet) ConfigETag() string { return strconv.FormatUint(f.Hash, hexBase) }

// hexBase is the base of the text form of [FlagSet.ConfigETag].
const hexBase = 16

// Flag returns the flag with a name. It does not allocate.
func (f *FlagSet) Flag(name string) (*flagdef.InternalFlag, bool) {
	if f == nil {
		return nil, false
	}

	fl, ok := f.Flags[name]

	return fl, ok
}

// SegmentCatalogFor is the catalog that the flag evaluates against: the one of the flag set it comes from. A flag set
// of one source has one catalog; the merged public flag set keeps the catalog of the source of each flag.
func (f *FlagSet) SegmentCatalogFor(flagKey string) *segment.Catalog {
	if f != nil && f.flagCatalogs != nil {
		return f.flagCatalogs[flagKey]
	}

	return f.SegmentCatalog()
}

// SegmentCatalog is the catalog of the segments of the flag set, nil when it has none. It does not allocate.
func (f *FlagSet) SegmentCatalog() *segment.Catalog {
	if f == nil || f.Segments == nil {
		return nil
	}

	return f.Segments.Catalog
}

// Holder is the one place readers get the current Runtime from. The zero value is usable: the first Load stores
// one empty Runtime, so Load does not allocate afterwards. [NewHolder] stores it up front.
type Holder struct {
	p atomic.Pointer[Runtime]
}

// NewHolder returns a Holder that holds a valid empty Runtime.
func NewHolder() *Holder {
	h := &Holder{}
	h.p.Store(newEmptyRuntime())

	return h
}

// Load returns the current Runtime. It never returns nil and does not allocate once the Holder holds a Runtime.
func (h *Holder) Load() *Runtime {
	if r := h.p.Load(); r != nil {
		return r
	}

	h.p.CompareAndSwap(nil, newEmptyRuntime())

	return h.p.Load()
}

// store publishes a Runtime: the only write of the Holder, made by the engine goroutine.
func (h *Holder) store(r *Runtime) {
	h.p.Store(r)
}

func newEmptyRuntime() *Runtime {
	return &Runtime{
		FlagSets:  map[string]*FlagSet{},
		ByKey:     map[string]*FlagSet{},
		AdminKeys: map[string]struct{}{},
		Status: &Status{
			FlagSets: map[string]*FlagSetStatus{},
			Objects:  map[string]*ObjectStatus{},
		},
	}
}

// SegmentUse tells which segments the rules of a flag use (sorted names, scheduled steps included; nil for a flag
// that uses none) and whether the flag needs the server to be evaluated: one of them is dynamic (its members live in a
// store) or cannot be resolved by the flag set (no source defines it). It does not allocate.
func (f *FlagSet) SegmentUse(flagKey string) ([]string, bool) {
	if f == nil {
		return nil, false
	}

	use := f.uses[flagKey]

	return use.names, use.dynamic || use.unresolved
}

// usesDynamicSegment tells whether the rules of a flag use a dynamic segment. It does not allocate, and costs nothing
// for a flag set that has no flag that uses segments.
func (f *FlagSet) usesDynamicSegment(flagKey string) bool {
	if f == nil || f.uses == nil {
		return false
	}

	return f.uses[flagKey].dynamic
}
