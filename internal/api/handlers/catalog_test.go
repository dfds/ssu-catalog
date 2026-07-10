package handlers

import (
	"sync/atomic"
	"testing"

	"go.dfds.cloud/ssu-catalog/internal/model"
	"go.dfds.cloud/ssu-catalog/internal/reachability"
)

func TestOverlayApp_FillsReachabilityWithoutMutatingSnapshot(t *testing.T) {
	app := model.ApplicationEntry{
		Namespace: "cap-a",
		Name:      "api",
		Services: []model.ServiceRef{{
			Name:          "api",
			ExternalHosts: []string{"api.example.com"},
		}},
	}
	cat := &model.Catalog{Applications: []model.ApplicationEntry{app}}
	ptr := &atomic.Pointer[model.Catalog]{}
	ptr.Store(cat)

	store := reachability.NewStore()
	store.Store(map[string]model.ReachabilityResult{
		"cap-a/api/api.example.com": {Host: "api.example.com", Status: "reachable", StatusCode: 200},
	})

	h := NewCatalog(ptr, store, "hellman")

	got := h.overlayApp(cat.Applications[0])
	if len(got.Services[0].Reachability) != 1 {
		t.Fatalf("overlay produced %d verdicts, want 1", len(got.Services[0].Reachability))
	}
	if got.Services[0].Reachability[0].Status != "reachable" {
		t.Errorf("status = %q, want reachable", got.Services[0].Reachability[0].Status)
	}

	// The shared snapshot must be untouched.
	if cat.Applications[0].Services[0].Reachability != nil {
		t.Error("overlayApp mutated the shared catalog snapshot")
	}
}

func TestOverlayApp_NoVerdictsLeavesAppUnchanged(t *testing.T) {
	app := model.ApplicationEntry{
		Namespace: "cap-a",
		Name:      "api",
		Services: []model.ServiceRef{{
			Name:          "api",
			ExternalHosts: []string{"api.example.com"},
		}},
	}
	store := reachability.NewStore() // empty
	h := NewCatalog(&atomic.Pointer[model.Catalog]{}, store, "hellman")

	got := h.overlayApp(app)
	if got.Services[0].Reachability != nil {
		t.Error("expected no reachability when store has no matching verdict")
	}
}

func TestOverlayApp_NilStoreIsSafe(t *testing.T) {
	app := model.ApplicationEntry{
		Namespace: "cap-a",
		Name:      "api",
		Services:  []model.ServiceRef{{Name: "api", ExternalHosts: []string{"api.example.com"}}},
	}
	h := NewCatalog(&atomic.Pointer[model.Catalog]{}, nil, "hellman")
	got := h.overlayApp(app)
	if got.Services[0].Reachability != nil {
		t.Error("nil store must not populate reachability")
	}
}
