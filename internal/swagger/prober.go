// Package swagger probes applications for OpenAPI/Swagger documentation by
// issuing HTTP GETs to well-known paths on every declared service port. It is
// the only feature that actively connects to other workloads; partial coverage
// (default-deny NetworkPolicies, non-HTTP ports) is expected and fine.
package swagger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"go.dfds.cloud/ssu-catalog/internal/model"
)

// Workload annotations controlling probe behaviour.
const (
	annoProbeOptOut = "dfds.cloud/openapi-probe" // "false" → skip the application
	annoProbePath   = "dfds.cloud/openapi-path"  // override → probe only this path
)

// maxProbeBodyBytes caps how much of a probe response we read for validation.
const maxProbeBodyBytes = 2 << 20 // 2 MiB

// defaultPaths are the well-known documentation locations probed on each port.
var defaultPaths = []string{
	"/swagger",
	"/swagger/index.html",
	"/swagger.json",
	"/swagger/v1/swagger.json",
	"/api-docs",
	"/openapi",
	"/openapi.json",
	"/v3/api-docs",
	"/.well-known/openapi",
}

// Prober issues concurrent HTTP probes against application service ports.
type Prober struct {
	client      *http.Client
	concurrency int
	paths       []string

	probes prometheus.Counter // total requests issued (nil-safe)
	hits   prometheus.Counter // total hits recorded (nil-safe)
}

// NewProber builds a Prober. The probe counters may be nil (e.g. in tests).
func NewProber(timeout time.Duration, concurrency int, probes, hits prometheus.Counter) *Prober {
	if concurrency <= 0 {
		concurrency = 20
	}
	return &Prober{
		client:      &http.Client{Timeout: timeout},
		concurrency: concurrency,
		paths:       defaultPaths,
		probes:      probes,
		hits:        hits,
	}
}

// job is a single probe request for one (application, service, port, path).
type job struct {
	appIdx int
	svcIdx int
	host   string
	port   int32
	path   string
}

// Probe fills ServiceRef.APIDocs on each application in place, recording every
// hit (no short-circuit). It returns the number of requests issued and hits
// found.
func (p *Prober) Probe(ctx context.Context, apps []model.ApplicationEntry) (int, int) {
	jobs := buildJobs(apps)
	if len(jobs) == 0 {
		return 0, 0
	}

	var (
		mu      sync.Mutex
		results = make(map[int]map[int][]model.APIDocInfo) // appIdx -> svcIdx -> docs
	)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(p.concurrency)

	for _, j := range jobs {
		j := j
		g.Go(func() error {
			doc, ok := p.probeOne(ctx, j)
			if !ok {
				return nil
			}
			mu.Lock()
			if results[j.appIdx] == nil {
				results[j.appIdx] = map[int][]model.APIDocInfo{}
			}
			results[j.appIdx][j.svcIdx] = append(results[j.appIdx][j.svcIdx], doc)
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait() // probeOne never returns an error; failures are non-fatal.

	p.addProbes(len(jobs))

	hits := 0
	for appIdx, bySvc := range results {
		for svcIdx, docs := range bySvc {
			sortDocs(docs)
			svc := &apps[appIdx].Services[svcIdx]
			annotateExternalAvailability(svc.Routes, docs)
			svc.APIDocs = docs
			hits += len(docs)
		}
	}
	p.addHits(hits)

	return len(jobs), hits
}

// buildJobs enumerates one probe per (service port × path) for every probeable
// application, skipping opt-outs and honouring path overrides.
func buildJobs(apps []model.ApplicationEntry) []job {
	var jobs []job
	for appIdx := range apps {
		app := &apps[appIdx]
		if app.Annotations[annoProbeOptOut] == "false" {
			continue
		}
		paths := defaultPaths
		if override := app.Annotations[annoProbePath]; override != "" {
			paths = []string{override}
		}
		for svcIdx := range app.Services {
			svc := &app.Services[svcIdx]
			host := fmt.Sprintf("%s.%s.svc.cluster.local", svc.Name, app.Namespace)
			seen := map[int32]struct{}{}
			for _, port := range svc.Ports {
				if _, dup := seen[port.Port]; dup {
					continue
				}
				seen[port.Port] = struct{}{}
				for _, path := range paths {
					jobs = append(jobs, job{appIdx: appIdx, svcIdx: svcIdx, host: host, port: port.Port, path: path})
				}
			}
		}
	}
	return jobs
}

// probeOne issues a single GET. A hit is HTTP 200 whose body validates as an
// OpenAPI/Swagger spec or a Swagger-UI/Redoc page — a 200 alone is not enough,
// since SPA wildcard catch-alls return 200 for every path.
func (p *Prober) probeOne(ctx context.Context, j job) (model.APIDocInfo, bool) {
	url := fmt.Sprintf("http://%s:%d%s", j.host, j.port, j.path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return model.APIDocInfo{}, false
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return model.APIDocInfo{}, false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return model.APIDocInfo{}, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProbeBodyBytes))
	if err != nil {
		return model.APIDocInfo{}, false
	}
	if !bodyLooksLikeAPIDoc(resp.Header.Get("Content-Type"), body) {
		return model.APIDocInfo{}, false
	}
	return model.APIDocInfo{Port: j.port, Path: j.path, URL: url}, true
}

// bodyLooksLikeAPIDoc reports whether the response body is an OpenAPI/Swagger
// document or a Swagger-UI/Redoc HTML page — not just any 200 response.
func bodyLooksLikeAPIDoc(contentType string, body []byte) bool {
	if jsonIsOpenAPISpec(body) {
		return true
	}
	if strings.Contains(strings.ToLower(contentType), "html") && htmlIsAPIDocUI(body) {
		return true
	}
	return false
}

// jsonIsOpenAPISpec parses body as a JSON object and checks for the mandatory
// top-level version key: "openapi" (3.x) or "swagger" (2.0). It runs regardless
// of Content-Type, so specs mis-labelled as text/plain still validate.
func jsonIsOpenAPISpec(body []byte) bool {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(body, &doc); err != nil {
		return false
	}
	_, hasOpenAPI := doc["openapi"]
	_, hasSwagger := doc["swagger"]
	return hasOpenAPI || hasSwagger
}

// htmlIsAPIDocUI looks for markers a Swagger-UI or Redoc shell carries that a
// generic SPA index.html does not.
func htmlIsAPIDocUI(body []byte) bool {
	b := strings.ToLower(string(body))
	for _, marker := range []string{"swagger-ui", "swaggeruibundle", "redoc"} {
		if strings.Contains(b, marker) {
			return true
		}
	}
	return false
}

func (p *Prober) addProbes(n int) {
	if p.probes != nil && n > 0 {
		p.probes.Add(float64(n))
	}
}

func (p *Prober) addHits(n int) {
	if p.hits != nil && n > 0 {
		p.hits.Add(float64(n))
	}
}

// annotateExternalAvailability marks each doc externally available when a Traefik
// IngressRoute on an external host exposes this service, filling ExternalURL with
// the reachable https URL. DFDS services are typically mounted under a path prefix
// with a StripPrefix middleware, so the external path is the route's mount prefix
// joined with the doc's own in-cluster path — e.g. a doc probed at /swagger behind
// a route on host api.example.com PathPrefix(`/dev/addon`) is reachable at
// https://api.example.com/dev/addon/swagger. The scheme is always https, mirroring
// the portal's serviceUrlsFor derivation of external URLs.
func annotateExternalAvailability(routes []model.RouteRef, docs []model.APIDocInfo) {
	for i := range docs {
		if url, ok := externalURLForPath(routes, docs[i].Path); ok {
			docs[i].ExternallyAvailable = true
			docs[i].ExternalURL = url
		}
	}
}

// externalURLForPath returns the reachable https URL for a doc at the given
// in-cluster path, and whether any external host exposes it. Every route in the
// list already forwards to this service, so any host-bearing route makes the
// service externally reachable. Among all (host, prefix) mount points, the one
// producing the shortest external path wins — this prefers a service's base mount
// (e.g. /dev/addon) over a more specific sub-route (e.g. /dev/addon/metrics).
func externalURLForPath(routes []model.RouteRef, path string) (string, bool) {
	bestHost, bestPath := "", ""
	for _, r := range routes {
		if len(r.Hosts) == 0 {
			continue
		}
		prefixes := r.PathPrefixes
		if len(prefixes) == 0 {
			prefixes = []string{""} // whole-host route mounts the service at root
		}
		for _, p := range prefixes {
			ext := externalPathForMount(p, path)
			if bestHost == "" || len(ext) < len(bestPath) {
				bestHost, bestPath = r.Hosts[0], ext
			}
		}
	}
	if bestHost == "" {
		return "", false
	}
	return "https://" + bestHost + bestPath, true
}

// externalPathForMount computes the external request path that reaches a doc at
// in-cluster path docPath through a route whose PathPrefix is `prefix`. When the
// doc path already falls under the prefix the route serves it directly (no strip);
// otherwise the prefix is a StripPrefix mount point and is prepended.
func externalPathForMount(prefix, docPath string) string {
	p := "/" + strings.Trim(prefix, "/")
	if p == "/" { // empty / "/" — whole-host mount
		return docPath
	}
	if docPath == p || strings.HasPrefix(docPath, p+"/") {
		return docPath // already served under this prefix
	}
	return p + docPath
}

// sortDocs gives APIDocs a stable order (port, then path).
func sortDocs(docs []model.APIDocInfo) {
	for i := 1; i < len(docs); i++ {
		for j := i; j > 0; j-- {
			a, b := docs[j-1], docs[j]
			if a.Port < b.Port || (a.Port == b.Port && a.Path <= b.Path) {
				break
			}
			docs[j-1], docs[j] = b, a
		}
	}
}
