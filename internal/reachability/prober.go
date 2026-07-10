// Package reachability actively probes the external ingress hosts a workload is
// exposed at and records whether each responds as expected. It runs on its own
// goroutine and interval, decoupled from the main collection cycle: it reads the
// live catalog snapshot read-only, probes, and writes verdicts into a separate
// store that the API layer overlays at serve time. Full rebuild each tick makes
// the store self-pruning — stale hosts simply stop being produced.
package reachability

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"go.uber.org/zap"

	"go.dfds.cloud/ssu-catalog/internal/model"
)

// Reachability probe config, resolved from the annotations of the ingress that
// owns the winning (host, path) route for a given host.
const (
	annoProbeOptOut = "dfds.cloud/reachability-probe"  // "false" → skip the host (no store entry)
	annoProbePath   = "dfds.cloud/reachability-path"   // override the probed path
	annoProbeMethod = "dfds.cloud/reachability-method" // override the HTTP method
	annoProbeExpect = "dfds.cloud/reachability-expect" // expected status (see expect.go)
)

// userAgent identifies this service on every outbound HTTP request.
const userAgent = "ssu-catalog - https://github.com/dfds/ssu-catalog"

// probeAttempts is the number of in-probe attempts for transient transport
// failures; probeBackoff is the wait between them. No cross-cycle state is kept.
const (
	probeAttempts = 2
	probeBackoff  = 250 * time.Millisecond
)

// Verdict statuses.
const (
	statusReachable   = "reachable"
	statusUnreachable = "unreachable"
	statusUnknown     = "unknown"
)

// Prober issues concurrent reachability probes against the external hosts a
// catalog exposes.
type Prober struct {
	client      *http.Client
	concurrency int
	logger      *zap.Logger

	probes      prometheus.Counter // total probes issued (nil-safe)
	reachable   prometheus.Counter // verdict counters (nil-safe)
	unreachable prometheus.Counter
	unknown     prometheus.Counter
	duration    prometheus.Histogram // cycle duration (nil-safe)
}

// Counters bundles the nil-safe Prometheus collectors a Prober updates. Any may
// be nil (e.g. in tests).
type Counters struct {
	Probes      prometheus.Counter
	Reachable   prometheus.Counter
	Unreachable prometheus.Counter
	Unknown     prometheus.Counter
	Duration    prometheus.Histogram
}

// NewProber builds a Prober. It verifies TLS and follows redirects (Go
// defaults); the timeout bounds each individual attempt.
func NewProber(timeout time.Duration, concurrency int, logger *zap.Logger, c Counters) *Prober {
	if concurrency <= 0 {
		concurrency = 20
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Prober{
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
			},
		},
		concurrency: concurrency,
		logger:      logger,
		probes:      c.Probes,
		reachable:   c.Reachable,
		unreachable: c.Unreachable,
		unknown:     c.Unknown,
		duration:    c.Duration,
	}
}

// target is one probe: a distinct external host, the URL/method/expectation
// resolved for it, and the store key it maps to.
type target struct {
	key       string // namespace/service/host
	host      string
	url       string
	method    string
	expect    statusMatcher
	expectStr string
}

// Probe builds targets from the catalog snapshot, probes each distinct host, and
// returns a fresh verdict map keyed by "namespace/service/host". It never mutates
// cat — cat is the live served snapshot.
func (p *Prober) Probe(ctx context.Context, cat *model.Catalog) map[string]model.ReachabilityResult {
	start := time.Now()
	defer func() {
		if p.duration != nil {
			p.duration.Observe(time.Since(start).Seconds())
		}
	}()

	targets := p.buildTargets(cat)
	if len(targets) == 0 {
		return map[string]model.ReachabilityResult{}
	}

	var (
		mu      sync.Mutex
		results = make(map[string]model.ReachabilityResult, len(targets))
	)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(p.concurrency)
	for _, t := range targets {
		t := t
		g.Go(func() error {
			res := p.probeOne(ctx, t)
			mu.Lock()
			results[t.key] = res
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait() // probeOne never returns an error; failures are captured as verdicts.

	p.recordMetrics(results)
	return results
}

// buildTargets enumerates one probe per distinct exposed host per service. For
// each host it picks the route with the shortest known path prefix; that winning
// route's annotations govern the probe config. Opted-out hosts are skipped.
func (p *Prober) buildTargets(cat *model.Catalog) []target {
	if cat == nil {
		return nil
	}
	var targets []target
	for _, app := range cat.Applications {
		for _, svc := range app.Services {
			for host, win := range winningRoutesByHost(svc.Routes) {
				anno := win.annotations
				if anno[annoProbeOptOut] == "false" {
					continue
				}
				path := win.path
				if override := strings.TrimSpace(anno[annoProbePath]); override != "" {
					path = normalizePath(override)
				}
				method := http.MethodGet
				if m := strings.TrimSpace(anno[annoProbeMethod]); m != "" {
					method = strings.ToUpper(m)
				}
				matcher, expectStr, ok := parseExpect(anno[annoProbeExpect])
				if !ok {
					p.logger.Warn("unparseable reachability-expect annotation; defaulting to 200",
						zap.String("namespace", app.Namespace),
						zap.String("service", svc.Name),
						zap.String("host", host),
						zap.String("value", anno[annoProbeExpect]),
					)
				}
				targets = append(targets, target{
					key:       app.Namespace + "/" + svc.Name + "/" + host,
					host:      host,
					url:       "https://" + host + path,
					method:    method,
					expect:    matcher,
					expectStr: expectStr,
				})
			}
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].key < targets[j].key })
	return targets
}

// hostWinner is the winning route for a host: the shortest path prefix seen and
// the annotations of the route that carried it.
type hostWinner struct {
	path        string
	annotations map[string]string
}

// winningRoutesByHost picks, per host across the routes, the route with the
// shortest path prefix (preferring a base mount over a deeper sub-route). The
// path is "/" when a route has no prefix (whole-host mount).
func winningRoutesByHost(routes []model.RouteRef) map[string]hostWinner {
	byHost := make(map[string]hostWinner)
	for _, r := range routes {
		path := shortestPrefix(r.PathPrefixes)
		for _, h := range r.Hosts {
			if h == "" {
				continue
			}
			cur, seen := byHost[h]
			if !seen || len(path) < len(cur.path) {
				byHost[h] = hostWinner{path: path, annotations: r.Annotations}
			}
		}
	}
	return byHost
}

// shortestPrefix returns the normalized shortest prefix among prefixes, or "/"
// when there are none.
func shortestPrefix(prefixes []string) string {
	if len(prefixes) == 0 {
		return "/"
	}
	best := prefixes[0]
	for _, p := range prefixes[1:] {
		if len(p) < len(best) {
			best = p
		}
	}
	return normalizePath(best)
}

// normalizePath ensures a leading slash and no trailing slash (except root).
func normalizePath(p string) string {
	p = "/" + strings.Trim(p, "/")
	return p
}

// probeOne issues the target's method+URL, following redirects and verifying
// TLS, retrying transient transport failures. The verdict is reachable when the
// final status matches the expectation, unreachable when it responded but
// mismatched, and unknown on a transport error (DNS/timeout/refused/TLS).
func (p *Prober) probeOne(ctx context.Context, t target) model.ReachabilityResult {
	res := model.ReachabilityResult{
		Host:     t.host,
		URL:      t.url,
		Expected: t.expectStr,
	}

	var (
		code    int
		lastErr error
	)
	for attempt := 0; attempt < probeAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				lastErr = ctx.Err()
			case <-time.After(probeBackoff):
			}
		}
		code, lastErr = p.doRequest(ctx, t)
		if lastErr == nil {
			break
		}
	}

	res.CheckedAt = time.Now()
	if lastErr != nil {
		res.Status = statusUnknown
		res.Reason = shortReason(lastErr)
		return res
	}
	res.StatusCode = code
	if t.expect.matches(code) {
		res.Status = statusReachable
	} else {
		res.Status = statusUnreachable
		res.Reason = fmt.Sprintf("expected %s, got %d", t.expectStr, code)
	}
	return res
}

// doRequest issues a single request and returns the final status code (after
// redirects). The body is not read — the verdict is status-only.
func (p *Prober) doRequest(ctx context.Context, t target) (int, error) {
	req, err := http.NewRequestWithContext(ctx, t.method, t.url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// shortReason condenses a transport error into a compact human-readable reason.
func shortReason(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "no such host"):
		return "dns lookup failed"
	case strings.Contains(lower, "connection refused"):
		return "connection refused"
	case strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline exceeded"):
		return "timeout"
	case strings.Contains(lower, "x509") || strings.Contains(lower, "certificate") || strings.Contains(lower, "tls"):
		return "tls error: " + afterLastColon(msg)
	default:
		return afterLastColon(msg)
	}
}

// afterLastColon trims the "Get \"url\": " prefix the net/http error wraps around
// the underlying cause, returning just the cause.
func afterLastColon(msg string) string {
	if i := strings.LastIndex(msg, ": "); i != -1 {
		return strings.TrimSpace(msg[i+2:])
	}
	return msg
}

func (p *Prober) recordMetrics(results map[string]model.ReachabilityResult) {
	add(p.probes, len(results))
	var reachable, unreachable, unknown int
	for _, r := range results {
		switch r.Status {
		case statusReachable:
			reachable++
		case statusUnreachable:
			unreachable++
		case statusUnknown:
			unknown++
		}
	}
	add(p.reachable, reachable)
	add(p.unreachable, unreachable)
	add(p.unknown, unknown)
}

func add(c prometheus.Counter, n int) {
	if c != nil && n > 0 {
		c.Add(float64(n))
	}
}
