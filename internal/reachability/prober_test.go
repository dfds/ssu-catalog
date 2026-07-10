package reachability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.dfds.cloud/ssu-catalog/internal/model"
)

func testProber() *Prober {
	return NewProber(2*time.Second, 4, nil, Counters{})
}

// mkTarget builds a probe target for url with the given expectation.
func mkTarget(url, method, expect string) target {
	m, resolved, _ := parseExpect(expect)
	if method == "" {
		method = http.MethodGet
	}
	return target{key: "ns/svc/host", host: "host", url: url, method: method, expect: m, expectStr: resolved}
}

func TestProbeOne_Reachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	res := testProber().probeOne(context.Background(), mkTarget(srv.URL, "GET", "200"))
	if res.Status != statusReachable {
		t.Fatalf("status = %q, want reachable (reason=%q)", res.Status, res.Reason)
	}
	if res.StatusCode != 200 {
		t.Errorf("statusCode = %d, want 200", res.StatusCode)
	}
	if res.CheckedAt.IsZero() {
		t.Error("CheckedAt not stamped")
	}
}

func TestProbeOne_WrongCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	res := testProber().probeOne(context.Background(), mkTarget(srv.URL, "GET", "200"))
	if res.Status != statusUnreachable {
		t.Fatalf("status = %q, want unreachable", res.Status)
	}
	if res.StatusCode != 500 {
		t.Errorf("statusCode = %d, want 500", res.StatusCode)
	}
	if res.Reason == "" {
		t.Error("expected a reason describing the mismatch")
	}
}

func TestProbeOne_ExpectRangeMatches(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent) // 204
	}))
	defer srv.Close()

	res := testProber().probeOne(context.Background(), mkTarget(srv.URL, "GET", "2xx"))
	if res.Status != statusReachable {
		t.Fatalf("status = %q, want reachable", res.Status)
	}
}

func TestProbeOne_Refused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing is listening now → connection refused

	res := testProber().probeOne(context.Background(), mkTarget(url, "GET", "200"))
	if res.Status != statusUnknown {
		t.Fatalf("status = %q, want unknown", res.Status)
	}
	if res.StatusCode != 0 {
		t.Errorf("statusCode = %d, want 0 on transport error", res.StatusCode)
	}
	if res.Reason == "" {
		t.Error("expected a transport-error reason")
	}
}

func TestProbeOne_RedirectFollowedToFinalStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/final", http.StatusFound)
	})
	mux.HandleFunc("/final", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent) // 204
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Expect 204: only satisfied if redirects are followed to the final response.
	res := testProber().probeOne(context.Background(), mkTarget(srv.URL, "GET", "204"))
	if res.Status != statusReachable {
		t.Fatalf("status = %q, want reachable (final 204 after redirect)", res.Status)
	}
	if res.StatusCode != 204 {
		t.Errorf("statusCode = %d, want 204 (final)", res.StatusCode)
	}
}

func TestProbeOne_TLSFailureIsUnknown(t *testing.T) {
	// TLS server with a self-signed cert the prober's verifying client rejects.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	res := testProber().probeOne(context.Background(), mkTarget(srv.URL, "GET", "200"))
	if res.Status != statusUnknown {
		t.Fatalf("status = %q, want unknown on TLS verify failure", res.Status)
	}
	if res.Reason == "" {
		t.Error("expected a TLS reason")
	}
}

func TestProbeOne_MethodHonoured(t *testing.T) {
	var gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_ = testProber().probeOne(context.Background(), mkTarget(srv.URL, "HEAD", "200"))
	if gotMethod != http.MethodHead {
		t.Errorf("server saw method %q, want HEAD", gotMethod)
	}
}

// catalogWith builds a single-service catalog with the given routes.
func catalogWith(routes []model.RouteRef) *model.Catalog {
	return &model.Catalog{
		Applications: []model.ApplicationEntry{{
			Namespace: "cap-a",
			Name:      "api",
			Services: []model.ServiceRef{{
				Name:   "api",
				Routes: routes,
			}},
		}},
	}
}

func targetsByHost(ts []target) map[string]target {
	m := make(map[string]target, len(ts))
	for _, t := range ts {
		m[t.host] = t
	}
	return m
}

func TestBuildTargets_ShortestPrefixWinsAndDefaults(t *testing.T) {
	cat := catalogWith([]model.RouteRef{
		{Kind: "IngressRoute", Hosts: []string{"api.example.com"}, PathPrefixes: []string{"/dev/addon/metrics"}},
		{Kind: "IngressRoute", Hosts: []string{"api.example.com"}, PathPrefixes: []string{"/dev/addon"}},
	})
	ts := testProber().buildTargets(cat)
	if len(ts) != 1 {
		t.Fatalf("targets = %d, want 1", len(ts))
	}
	got := ts[0]
	if got.url != "https://api.example.com/dev/addon" {
		t.Errorf("url = %q, want shortest-prefix mount", got.url)
	}
	if got.method != http.MethodGet {
		t.Errorf("method = %q, want default GET", got.method)
	}
	if got.expectStr != "200" {
		t.Errorf("expect = %q, want default 200", got.expectStr)
	}
	if got.key != "cap-a/api/api.example.com" {
		t.Errorf("key = %q, want namespace/service/host", got.key)
	}
}

func TestBuildTargets_NoPrefixMountsAtRoot(t *testing.T) {
	cat := catalogWith([]model.RouteRef{
		{Kind: "Ingress", Hosts: []string{"root.example.com"}},
	})
	ts := testProber().buildTargets(cat)
	if len(ts) != 1 || ts[0].url != "https://root.example.com/" {
		t.Fatalf("targets = %+v, want single root mount", ts)
	}
}

func TestBuildTargets_OptOutSkipsHost(t *testing.T) {
	cat := catalogWith([]model.RouteRef{
		{Kind: "Ingress", Hosts: []string{"a.example.com"}, Annotations: map[string]string{annoProbeOptOut: "false"}},
		{Kind: "Ingress", Hosts: []string{"b.example.com"}},
	})
	ts := targetsByHost(testProber().buildTargets(cat))
	if _, ok := ts["a.example.com"]; ok {
		t.Error("opted-out host a.example.com should have no target")
	}
	if _, ok := ts["b.example.com"]; !ok {
		t.Error("host b.example.com should be probed")
	}
}

func TestBuildTargets_AnnotationOverrides(t *testing.T) {
	cat := catalogWith([]model.RouteRef{
		{
			Kind:         "IngressRoute",
			Hosts:        []string{"api.example.com"},
			PathPrefixes: []string{"/base"},
			Annotations: map[string]string{
				annoProbePath:   "/healthz",
				annoProbeMethod: "head",
				annoProbeExpect: "204",
			},
		},
	})
	ts := testProber().buildTargets(cat)
	if len(ts) != 1 {
		t.Fatalf("targets = %d, want 1", len(ts))
	}
	got := ts[0]
	if got.url != "https://api.example.com/healthz" {
		t.Errorf("url = %q, want path override applied", got.url)
	}
	if got.method != http.MethodHead {
		t.Errorf("method = %q, want HEAD (uppercased)", got.method)
	}
	if got.expectStr != "204" {
		t.Errorf("expect = %q, want 204", got.expectStr)
	}
	if !got.expect.matches(204) || got.expect.matches(200) {
		t.Error("expectation matcher not resolved from annotation")
	}
}

func TestProbe_DoesNotMutateCatalog(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cat := catalogWith([]model.RouteRef{
		{Kind: "Ingress", Hosts: []string{"a.example.com"}},
	})
	out := testProber().Probe(context.Background(), cat)
	if len(out) != 1 {
		t.Fatalf("results = %d, want 1", len(out))
	}
	// The live snapshot must be untouched — reachability is a serve-time overlay.
	if cat.Applications[0].Services[0].Reachability != nil {
		t.Error("Probe mutated the catalog snapshot")
	}
}
