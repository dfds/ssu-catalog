package reachability

import (
	"sort"
	"sync/atomic"

	"go.dfds.cloud/ssu-catalog/internal/model"
)

// Store holds the latest reachability verdicts as an immutable snapshot behind an
// atomic pointer: lock-free reads, atomic full-snapshot swap each tick. Because
// the worker rebuilds the whole map every cycle, stale hosts self-prune — a host
// that stops being exposed (or opts out) simply drops out of the next snapshot.
type Store struct {
	snapshot atomic.Pointer[map[string]model.ReachabilityResult]
}

// NewStore returns an empty Store.
func NewStore() *Store {
	s := &Store{}
	empty := map[string]model.ReachabilityResult{}
	s.snapshot.Store(&empty)
	return s
}

// Store atomically replaces the current snapshot.
func (s *Store) Store(results map[string]model.ReachabilityResult) {
	if results == nil {
		results = map[string]model.ReachabilityResult{}
	}
	s.snapshot.Store(&results)
}

// Lookup returns the verdict for a "namespace/service/host" key, if present.
func (s *Store) Lookup(key string) (model.ReachabilityResult, bool) {
	m := s.snapshot.Load()
	if m == nil {
		return model.ReachabilityResult{}, false
	}
	res, ok := (*m)[key]
	return res, ok
}

// All returns every verdict in the current snapshot, sorted by host for a stable
// response ordering.
func (s *Store) All() []model.ReachabilityResult {
	m := s.snapshot.Load()
	if m == nil {
		return []model.ReachabilityResult{}
	}
	out := make([]model.ReachabilityResult, 0, len(*m))
	for _, r := range *m {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Host != out[j].Host {
			return out[i].Host < out[j].Host
		}
		return out[i].URL < out[j].URL
	})
	return out
}
