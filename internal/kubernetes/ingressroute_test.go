package kubernetes

import (
	"context"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

// ctxAwareDynamic wraps a dynamic client so List honors context cancellation.
// The dynamic fake ignores ctx, which hid a bug where the errgroup-derived
// (cancelled-on-Wait) context was reused for the post-scan dynamic listing.
type ctxAwareDynamic struct{ dynamic.Interface }

func (d ctxAwareDynamic) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return ctxAwareNRI{d.Interface.Resource(gvr)}
}

type ctxAwareNRI struct {
	dynamic.NamespaceableResourceInterface
}

func (n ctxAwareNRI) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return n.NamespaceableResourceInterface.List(ctx, opts)
}

func TestParseMatchHosts(t *testing.T) {
	tests := []struct {
		name  string
		match string
		want  []string
	}{
		{"single", "Host(`api.example.com`)", []string{"api.example.com"}},
		{"or", "Host(`a.example.com`) || Host(`b.example.com`)", []string{"a.example.com", "b.example.com"}},
		{"multi-arg", "Host(`a.example.com`, `b.example.com`)", []string{"a.example.com", "b.example.com"}},
		{"host-and-path", "Host(`api.example.com`) && PathPrefix(`/v1`)", []string{"api.example.com"}},
		{"hostsni", "HostSNI(`db.example.com`)", []string{"db.example.com"}},
		{"none", "PathPrefix(`/v1`)", nil},
		{"dedup", "Host(`a.example.com`) || Host(`a.example.com`)", []string{"a.example.com"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseMatchHosts(tt.match); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseMatchHosts(%q) = %v, want %v", tt.match, got, tt.want)
			}
		})
	}
}

func TestParseMatchPaths(t *testing.T) {
	tests := []struct {
		name  string
		match string
		want  []string
	}{
		{"prefix", "Host(`a`) && PathPrefix(`/api`)", []string{"/api"}},
		{"exact-path", "Path(`/healthz`)", []string{"/healthz"}},
		{"both", "PathPrefix(`/api`) || Path(`/healthz`)", []string{"/api", "/healthz"}},
		{"none", "Host(`a`)", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseMatchPaths(tt.match); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseMatchPaths(%q) = %v, want %v", tt.match, got, tt.want)
			}
		})
	}
}

func ingressRouteListKinds() map[schema.GroupVersionResource]string {
	return map[schema.GroupVersionResource]string{
		ingressRouteResources[0]: "IngressRouteList",
		ingressRouteResources[1]: "IngressRouteList",
	}
}

func TestListIngressRoutes_ParsesAndLinks(t *testing.T) {
	scheme := runtime.NewScheme()
	ir := newUnstructured("traefik.io/v1alpha1", "IngressRoute", "cap-a", "api-route", map[string]interface{}{
		"spec": map[string]interface{}{
			"entryPoints": []interface{}{"websecure"},
			"tls":         map[string]interface{}{},
			"routes": []interface{}{
				map[string]interface{}{
					"match": "Host(`api.example.com`) && PathPrefix(`/v1`)",
					"services": []interface{}{
						map[string]interface{}{"name": "api", "port": int64(80)},
					},
				},
				map[string]interface{}{
					"match": "Host(`web.example.com`)",
					"services": []interface{}{
						map[string]interface{}{"name": "web"},
					},
				},
			},
		},
	})
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, ingressRouteListKinds(), ir)

	c := NewClientFromInterfaces(fake.NewSimpleClientset(newNamespace("cap-a", nil)), dynClient, Options{Concurrency: 2})
	snap, err := c.ListResources(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(snap.IngressRoutes) != 1 {
		t.Fatalf("expected 1 ingressroute, got %d", len(snap.IngressRoutes))
	}
	got := snap.IngressRoutes[0]
	if got.Namespace != "cap-a" || got.Name != "api-route" {
		t.Errorf("metadata wrong: %+v", got)
	}
	if !got.TLS {
		t.Error("expected TLS true (spec.tls present)")
	}
	if len(got.EntryPoints) != 1 || got.EntryPoints[0] != "websecure" {
		t.Errorf("entrypoints wrong: %+v", got.EntryPoints)
	}
	if len(got.Routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(got.Routes))
	}
	r0 := got.Routes[0]
	if len(r0.Hosts) != 1 || r0.Hosts[0] != "api.example.com" {
		t.Errorf("route0 hosts wrong: %+v", r0.Hosts)
	}
	if len(r0.PathPrefixes) != 1 || r0.PathPrefixes[0] != "/v1" {
		t.Errorf("route0 paths wrong: %+v", r0.PathPrefixes)
	}
	if len(r0.Services) != 1 || r0.Services[0] != "api" {
		t.Errorf("route0 services wrong: %+v", r0.Services)
	}
}

func TestListIngressRoutes_NamespaceFiltered(t *testing.T) {
	scheme := runtime.NewScheme()
	ir := newUnstructured("traefik.io/v1alpha1", "IngressRoute", "kube-system", "ignored", map[string]interface{}{
		"spec": map[string]interface{}{
			"routes": []interface{}{
				map[string]interface{}{
					"match":    "Host(`x`)",
					"services": []interface{}{map[string]interface{}{"name": "x"}},
				},
			},
		},
	})
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, ingressRouteListKinds(), ir)

	c := NewClientFromInterfaces(
		fake.NewSimpleClientset(newNamespace("cap-a", nil), newNamespace("kube-system", nil)),
		dynClient,
		Options{Concurrency: 2, NamespaceExclude: []string{"kube-system"}},
	)
	snap, err := c.ListResources(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snap.IngressRoutes) != 0 {
		t.Errorf("expected excluded-namespace ingressroute to be filtered, got %+v", snap.IngressRoutes)
	}
}

// TestListIngressRoutes_LiveContext guards against reusing the errgroup's
// cancelled context for the post-scan dynamic listing: with a context-honoring
// dynamic client, the IngressRoute must still be discovered after the namespace
// scan + Wait have completed.
func TestListIngressRoutes_LiveContext(t *testing.T) {
	scheme := runtime.NewScheme()
	ir := newUnstructured("traefik.io/v1alpha1", "IngressRoute", "cap-a", "api-route", map[string]interface{}{
		"spec": map[string]interface{}{
			"routes": []interface{}{
				map[string]interface{}{
					"match":    "Host(`api.example.com`)",
					"services": []interface{}{map[string]interface{}{"name": "api"}},
				},
			},
		},
	})
	dynClient := ctxAwareDynamic{dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, ingressRouteListKinds(), ir)}

	c := NewClientFromInterfaces(fake.NewSimpleClientset(newNamespace("cap-a", nil)), dynClient, Options{Concurrency: 2})
	snap, err := c.ListResources(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snap.IngressRoutes) != 1 {
		t.Fatalf("expected 1 ingressroute with live context, got %d (cancelled-context regression?)", len(snap.IngressRoutes))
	}
}

func TestListIngressRoutes_NilDynamicClient(t *testing.T) {
	c := NewClientFromInterfaces(fake.NewSimpleClientset(newNamespace("cap-a", nil)), nil, Options{Concurrency: 2})
	snap, err := c.ListResources(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.IngressRoutes != nil {
		t.Errorf("expected nil ingressroutes with nil dynamic client, got %+v", snap.IngressRoutes)
	}
}
