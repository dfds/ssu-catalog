package swagger

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.dfds.cloud/ssu-catalog/internal/model"
)

// roundTripFunc lets a test stub HTTP responses without a real network. The
// cluster-local hostnames the prober builds are never resolved.
type roundTripFunc func(*http.Request) *http.Response

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func resp(status int, contentType, body string) *http.Response {
	h := http.Header{}
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	return &http.Response{
		StatusCode: status,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func newTestProber(rt roundTripFunc) *Prober {
	p := NewProber(time.Second, 4, nil, nil)
	p.client = &http.Client{Transport: rt}
	return p
}

func appWithService(ports ...int32) []model.ApplicationEntry {
	svcPorts := make([]model.ServicePort, 0, len(ports))
	for _, p := range ports {
		svcPorts = append(svcPorts, model.ServicePort{Port: p})
	}
	return []model.ApplicationEntry{{
		Namespace: "cap-a", Name: "api", Kind: "Deployment",
		Services: []model.ServiceRef{{
			Name:    "api",
			Ports:   svcPorts,
			APIDocs: []model.APIDocInfo{},
		}},
	}}
}

func TestProbe_RecordsHitsNoShortCircuit(t *testing.T) {
	// Return a JSON 200 for two distinct doc paths; 404 for everything else.
	hitPaths := map[string]bool{
		"/swagger/v1/swagger.json": true,
		"/openapi.json":            true,
	}
	rt := roundTripFunc(func(req *http.Request) *http.Response {
		if hitPaths[req.URL.Path] {
			return resp(200, "application/json", `{"openapi":"3.0.0"}`)
		}
		return resp(404, "text/plain", "not found")
	})

	apps := appWithService(80)
	probes, hits := newTestProber(rt).Probe(context.Background(), apps)

	if probes != len(defaultPaths) {
		t.Errorf("probes = %d, want %d", probes, len(defaultPaths))
	}
	if hits != 2 {
		t.Fatalf("hits = %d, want 2", hits)
	}
	docs := apps[0].Services[0].APIDocs
	if len(docs) != 2 {
		t.Fatalf("expected 2 recorded docs, got %d: %+v", len(docs), docs)
	}
	// Sorted by (port, path): /openapi.json before /swagger/v1/swagger.json.
	if docs[0].Path != "/openapi.json" || docs[1].Path != "/swagger/v1/swagger.json" {
		t.Errorf("docs not sorted: %+v", docs)
	}
	if docs[0].URL != "http://api.cap-a.svc.cluster.local:80/openapi.json" {
		t.Errorf("url wrong: %q", docs[0].URL)
	}
}

func TestProbe_HitRequiresJSONorHTML(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) *http.Response {
		// 200 but a non-doc content type → not a hit.
		return resp(200, "text/plain", "ok")
	})
	apps := appWithService(80)
	_, hits := newTestProber(rt).Probe(context.Background(), apps)
	if hits != 0 {
		t.Errorf("plain-text 200 should not be a hit, got %d", hits)
	}
}

func TestProbe_HTMLContentTypeIsHit(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) *http.Response {
		if req.URL.Path == "/swagger" {
			return resp(200, "text/html; charset=utf-8", `<html><body><div id="swagger-ui"></div></body></html>`)
		}
		return resp(404, "", "")
	})
	apps := appWithService(8080)
	_, hits := newTestProber(rt).Probe(context.Background(), apps)
	if hits != 1 {
		t.Errorf("swagger-ui html 200 should be a hit, got %d", hits)
	}
	if apps[0].Services[0].APIDocs[0].Port != 8080 {
		t.Errorf("port not recorded: %+v", apps[0].Services[0].APIDocs)
	}
}

func TestProbe_SPACatchAllIsNotHit(t *testing.T) {
	// A SPA wildcard catch-all returns its index.html shell with 200 text/html
	// for every path. None of these are API docs.
	rt := roundTripFunc(func(*http.Request) *http.Response {
		return resp(200, "text/html; charset=utf-8",
			`<html><head><title>Self Service</title></head><body><div id="root"></div></body></html>`)
	})
	apps := appWithService(80)
	_, hits := newTestProber(rt).Probe(context.Background(), apps)
	if hits != 0 {
		t.Errorf("SPA catch-all html should not be a hit, got %d", hits)
	}
}

func TestProbe_JSONWithoutSpecKeyIsNotHit(t *testing.T) {
	rt := roundTripFunc(func(*http.Request) *http.Response {
		// Valid JSON, but not an OpenAPI/Swagger document.
		return resp(200, "application/json", `{"message":"ok"}`)
	})
	apps := appWithService(80)
	_, hits := newTestProber(rt).Probe(context.Background(), apps)
	if hits != 0 {
		t.Errorf("non-spec json 200 should not be a hit, got %d", hits)
	}
}

func TestProbe_OptOutAnnotation(t *testing.T) {
	rt := roundTripFunc(func(*http.Request) *http.Response {
		t.Error("opted-out application should not be probed")
		return resp(200, "application/json", "{}")
	})
	apps := appWithService(80)
	apps[0].Annotations = map[string]string{annoProbeOptOut: "false"}
	probes, hits := newTestProber(rt).Probe(context.Background(), apps)
	if probes != 0 || hits != 0 {
		t.Errorf("expected no probing for opt-out, got probes=%d hits=%d", probes, hits)
	}
}

func TestProbe_PathOverride(t *testing.T) {
	var probedPaths []string
	rt := roundTripFunc(func(req *http.Request) *http.Response {
		probedPaths = append(probedPaths, req.URL.Path)
		return resp(200, "application/json", `{"openapi":"3.0.0"}`)
	})
	apps := appWithService(80)
	apps[0].Annotations = map[string]string{annoProbePath: "/custom/openapi.json"}
	probes, hits := newTestProber(rt).Probe(context.Background(), apps)
	if probes != 1 || hits != 1 {
		t.Fatalf("override should probe exactly one path, got probes=%d hits=%d", probes, hits)
	}
	if len(probedPaths) != 1 || probedPaths[0] != "/custom/openapi.json" {
		t.Errorf("override path not honoured: %+v", probedPaths)
	}
}

func TestProbe_NoServicesNoJobs(t *testing.T) {
	rt := roundTripFunc(func(*http.Request) *http.Response {
		t.Error("application without services should not be probed")
		return resp(200, "application/json", "{}")
	})
	apps := []model.ApplicationEntry{{Namespace: "cap-a", Name: "worker", Kind: "Deployment"}}
	probes, hits := newTestProber(rt).Probe(context.Background(), apps)
	if probes != 0 || hits != 0 {
		t.Errorf("expected no probing, got probes=%d hits=%d", probes, hits)
	}
}

func TestProbe_DeduplicatesPorts(t *testing.T) {
	rt := roundTripFunc(func(*http.Request) *http.Response { return resp(404, "", "") })
	apps := appWithService(80, 80, 443) // 80 repeated
	probes, _ := newTestProber(rt).Probe(context.Background(), apps)
	if probes != 2*len(defaultPaths) {
		t.Errorf("expected dedup to 2 ports, got %d probes (want %d)", probes, 2*len(defaultPaths))
	}
}

func TestExternalPathForMount(t *testing.T) {
	cases := []struct {
		name    string
		prefix  string
		docPath string
		want    string
	}{
		{"whole-host mount serves doc path as-is", "", "/swagger", "/swagger"},
		{"root prefix serves doc path as-is", "/", "/swagger", "/swagger"},
		{"strip-prefix mount prepends the prefix", "/dev/addon", "/swagger", "/dev/addon/swagger"},
		{"trailing slash on prefix is normalised", "/dev/addon/", "/swagger", "/dev/addon/swagger"},
		{"doc already under prefix is served directly", "/swagger", "/swagger/v1/swagger.json", "/swagger/v1/swagger.json"},
		{"exact prefix equals doc path", "/openapi.json", "/openapi.json", "/openapi.json"},
		{"non-boundary overlap is treated as a mount", "/api", "/apidocs", "/api/apidocs"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := externalPathForMount(tc.prefix, tc.docPath); got != tc.want {
				t.Errorf("externalPathForMount(%q, %q) = %q, want %q", tc.prefix, tc.docPath, got, tc.want)
			}
		})
	}
}

func TestAnnotateExternalAvailability(t *testing.T) {
	t.Run("host-less route is not external", func(t *testing.T) {
		routes := []model.RouteRef{{Name: "internal", PathPrefixes: []string{"/"}}}
		docs := []model.APIDocInfo{{Path: "/swagger", URL: "http://internal/swagger"}}
		annotateExternalAvailability(routes, docs)
		if docs[0].ExternallyAvailable || docs[0].ExternalURL != "" {
			t.Errorf("expected internal-only, got %+v", docs[0])
		}
	})

	t.Run("doc already under the route prefix is served directly", func(t *testing.T) {
		routes := []model.RouteRef{{
			Name:         "api",
			Hosts:        []string{"api.example.com"},
			PathPrefixes: []string{"/swagger"},
			TLS:          true,
		}}
		docs := []model.APIDocInfo{{Path: "/swagger/v1/swagger.json"}}
		annotateExternalAvailability(routes, docs)
		if !docs[0].ExternallyAvailable {
			t.Fatalf("expected externally available, got %+v", docs[0])
		}
		if docs[0].ExternalURL != "https://api.example.com/swagger/v1/swagger.json" {
			t.Errorf("external url wrong: %q", docs[0].ExternalURL)
		}
	})

	t.Run("service mounted under a path prefix prepends it to the doc path", func(t *testing.T) {
		// Mirrors the real DFDS pattern: the service is mounted at /dev/addon
		// (StripPrefix), so an in-cluster doc at /swagger is externally reachable
		// at /dev/addon/swagger. A more specific /dev/addon/metrics sub-route must
		// not win — the base mount produces the shorter external path.
		routes := []model.RouteRef{
			{Name: "base", Hosts: []string{"api.example.com"}, PathPrefixes: []string{"/dev/addon"}, TLS: true},
			{Name: "metrics", Hosts: []string{"api.example.com"}, PathPrefixes: []string{"/dev/addon/metrics"}, TLS: true},
			{Name: "internal-metrics", Hosts: []string{"internal.example.com"}, PathPrefixes: []string{"/dev/addon/metrics"}, TLS: true},
		}
		docs := []model.APIDocInfo{{Path: "/swagger"}, {Path: "/swagger/index.html"}}
		annotateExternalAvailability(routes, docs)
		if !docs[0].ExternallyAvailable || docs[0].ExternalURL != "https://api.example.com/dev/addon/swagger" {
			t.Errorf("doc[0] wrong: %+v", docs[0])
		}
		if !docs[1].ExternallyAvailable || docs[1].ExternalURL != "https://api.example.com/dev/addon/swagger/index.html" {
			t.Errorf("doc[1] wrong: %+v", docs[1])
		}
	})
}
