package telemetry

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"go.dfds.cloud/ssu-catalog/internal/model"
)

// QueryClient is the subset of the Mimir client the overlay depends on
// (satisfied by *Client; stubbed in tests).
type QueryClient interface {
	InstantQuery(ctx context.Context, query string) ([]Sample, error)
}

// Origins tagged on every overlay-derived item.
const (
	originServiceGraph = "otel-servicegraph"
	originMetrics      = "otel-metrics"
)

// serviceGraphMetric is the service-graph counter (emitted by Beyla/OBI and, for
// OTel-instrumented apps, Tempo's metrics generator) stored in Mimir.
const serviceGraphMetric = "traces_service_graph_request_total"

// httpClientMetric is the OTel/Beyla HTTP client-request counter. Its
// server_address label carries the concrete destination host (an AWS API, a
// third-party endpoint, an in-cluster FQDN) that the service graph otherwise
// collapses into a single "outgoing" bucket — so it is how we resolve real
// external egress by name.
const httpClientMetric = "http_client_request_body_size_bytes_count"

// Overlayer queries Mimir and overlays a best-effort runtime dependency graph
// onto the (already complete) K8s/GitOps/swagger catalog. Failures degrade
// gracefully — the catalog is never invalidated by a missing/failed query.
type Overlayer struct {
	client      QueryClient
	cluster     string
	lookback    time.Duration
	queryErrors prometheus.Counter // nil-safe
	logger      *zap.Logger

	// lookupAddr resolves an IP to its PTR hostnames; nil defaults to a
	// time-bounded DNS lookup. Overridable in tests to avoid real DNS.
	lookupAddr func(ctx context.Context, ip string) ([]string, error)
	ptrMu      sync.Mutex
	ptrCache   map[string]string

	// lookupHost forward-resolves a hostname to its IPs; nil defaults to a
	// time-bounded DNS lookup. Overridable in tests to avoid real DNS. Used to
	// join HTTP peers (reported by hostname) against db_client peers (reported by
	// IP) so a phantom DB on an HTTP host's IP can be dropped.
	lookupHost func(ctx context.Context, host string) ([]string, error)
	hostMu     sync.Mutex
	hostCache  map[string][]string
}

// NewOverlayer builds an Overlayer. queryErrors may be nil (e.g. in tests).
func NewOverlayer(client QueryClient, cluster string, lookback time.Duration, queryErrors prometheus.Counter, logger *zap.Logger) *Overlayer {
	if logger == nil {
		logger = zap.NewNop()
	}
	if lookback <= 0 {
		lookback = time.Hour
	}
	return &Overlayer{
		client:      client,
		cluster:     cluster,
		lookback:    lookback,
		queryErrors: queryErrors,
		logger:      logger,
	}
}

// Apply runs the overlay queries and returns the dependency edges, mutating
// applications in place to attach observed Databases/KafkaTopics.
func (o *Overlayer) Apply(ctx context.Context, apps []model.ApplicationEntry) []model.DependencyEdge {
	res := newResolver(o.cluster, apps)
	appByKey := indexApps(apps)

	edges := newEdgeSet()
	httpPeers := newClientPeers()
	o.applyServiceGraph(ctx, res, appByKey, edges)
	o.applyHTTPClient(ctx, res, appByKey, edges, httpPeers)
	o.applyDatabaseMetrics(ctx, res, appByKey, edges, httpPeers)
	o.applyMessagingMetrics(ctx, res, appByKey, edges)
	o.applyRuntimeMetrics(ctx, res, appByKey)
	o.applyTrafficMetrics(ctx, res, appByKey)

	return edges.list
}

// serviceGraphQuery aggregates edges while KEEPING the k8s namespace labels
// Beyla attaches to each side, so resolveEndpoint can join on (namespace, name)
// instead of parsing the bare service.name. Cluster-scoped: without it, same-name
// workloads collide across the fleet (namespace+name is only unique per cluster).
//
// Windowed over the lookback via count_over_time, NOT an instant snapshot. The
// service-graph counter is scraped per-series, and a plain instant query only
// returns series with a sample inside Prometheus's ~5m staleness window. Edges
// from CONTINUOUS callers (scrapers, meshes) always survive that, but edges from
// PERIODIC callers do not: ssu-catalog probes each workload on an interval, so its
// service-graph series appear in bursts with multi-minute gaps and are stale —
// hence dropped — whenever a scan lands between bursts. count_over_time(...[window])
// instead includes every series active at ANY point in the lookback, so a periodic
// prober's edges show as reliably as a continuous caller's. This mirrors the
// windowing the db_client/http_client/messaging overlays already use. The value is
// irrelevant here (applyServiceGraph reads only the label set), so a bare
// count_over_time is enough.
func (o *Overlayer) serviceGraphQuery() string {
	return fmt.Sprintf(`count by (client, server, client_k8s_namespace_name, server_k8s_namespace_name) (count_over_time(%s%s[%s]))`,
		serviceGraphMetric, o.clusterMatcher(), o.window())
}

// clusterMatcher renders the PromQL label matcher scoping a query to this
// Overlayer's cluster, or "" when no cluster is configured.
func (o *Overlayer) clusterMatcher() string {
	if o.cluster == "" {
		return ""
	}
	return fmt.Sprintf(`{cluster=%q}`, o.cluster)
}

// applyServiceGraph processes the service-graph edges (service→service, →DB,
// →kafka), attaching DB/Kafka refs to the client application when in-cluster.
func (o *Overlayer) applyServiceGraph(ctx context.Context, res *resolver, appByKey map[string]*model.ApplicationEntry, edges *edgeSet) {
	samples, err := o.client.InstantQuery(ctx, o.serviceGraphQuery())
	if err != nil {
		o.queryFailed("service_graph", err)
		return
	}
	for _, s := range samples {
		client, server := s.Metric["client"], s.Metric["server"]
		if client == "" || server == "" {
			continue
		}
		// Beyla buckets un-attributed egress/ingress under the synthetic names
		// "outgoing"/"incoming" — no real destination, and now superseded by the
		// server_address the HTTP-client overlay resolves. Drop them.
		if beylaEgressBucket(client) || beylaEgressBucket(server) {
			continue
		}
		src := res.resolveEndpointNode(client, s.Metric["client_k8s_namespace_name"])
		dst, kind := res.resolveEndpoint(server, s.Metric["server_k8s_namespace_name"])

		edgeType := kind
		if kind == kindExternal {
			edgeType = kindService
		}
		edges.add(model.DependencyEdge{
			Source:  src,
			Target:  dst,
			Type:    edgeType,
			Origin:  originServiceGraph,
			Details: fmt.Sprintf("%s → %s", client, server),
		})

		if src.External {
			continue
		}
		app := appByKey[src.Namespace+"/"+src.Service]
		if app == nil {
			continue
		}
		switch kind {
		case kindDatabase:
			attachDatabase(app, dst.Service, originServiceGraph)
		case kindKafka:
			attachKafka(app, dst.Service, "", originServiceGraph)
		}
	}
}

// applyHTTPClient resolves EXTERNAL egress the service graph can't attribute.
// Beyla buckets un-resolvable egress under server="outgoing"; the HTTP client
// metric instead carries the concrete server_address, so real dependencies on
// AWS APIs, third-party HTTP services, etc. surface by name. In-cluster
// destinations are left to the (richer) service-graph overlay; private/infra IPs
// are dropped as mesh noise, while public IPs are kept and reverse-resolved to a
// hostname when DNS has a PTR record.
func (o *Overlayer) applyHTTPClient(ctx context.Context, res *resolver, appByKey map[string]*model.ApplicationEntry, edges *edgeSet, httpPeers *clientPeers) {
	query := fmt.Sprintf(
		`count by (k8s_namespace_name, k8s_deployment_name, k8s_statefulset_name, service_name, service, server_address, server_port) (rate(%s%s[%s]))`,
		httpClientMetric, o.clusterMatcher(), o.window(),
	)
	samples, err := o.client.InstantQuery(ctx, query)
	if err != nil {
		o.queryFailed("http_client", err)
		return
	}
	for _, s := range samples {
		addr := s.Metric["server_address"]
		if addr == "" {
			continue
		}
		src := res.resolveClient(s.Metric)
		if src.External {
			continue // can't attribute the caller to a known workload
		}
		host, keep := o.externalHost(ctx, stripPort(addr))
		// Remember every host this workload talks to over HTTP — including the
		// private and in-cluster peers we don't draw an egress edge for. The DB
		// overlay consults this set to reject Beyla's phantom databases: a peer this
		// workload reaches over HTTP/gRPC is not its database, however confidently
		// eBPF stamped a db_system on the same TLS-on-443 stream. Recorded before the
		// keep filter so a private-IP HTTP peer still shadows a phantom DB.
		appKey := src.Namespace + "/" + src.Service
		peerHost := firstNonEmpty(host, stripPort(addr))
		httpPeers.add(appKey, peerHost)
		// Beyla reports HTTP peers by HOSTNAME (Host header / SNI) but db_client
		// peers by IP, so also index the hostname's resolved IPs — otherwise the DB
		// overlay's IP peer never matches this HTTP host and the phantom survives.
		if !isBareIP(peerHost) {
			for _, ip := range o.forwardLookup(ctx, peerHost) {
				httpPeers.add(appKey, ip)
			}
		}
		if !keep {
			continue // private/infra IP (kube API server, node/pod IPs)
		}
		// Beyla emits the destination port in a SEPARATE server_port label (once
		// revealed via attributes.select), not appended to server_address. Re-attach
		// it so resolve can classify egress by well-known port (5432→postgresql,
		// 9092→kafka, …); fall back to a port embedded in the address for older data.
		dst, kind := res.resolve(joinHostPort(host, peerPort(s.Metric, addr)))
		if !dst.External {
			continue // in-cluster: the service graph already draws this, better
		}
		edgeType := kind
		if kind == kindExternal {
			edgeType = kindService
		}
		edges.add(model.DependencyEdge{
			Source:  src,
			Target:  dst,
			Type:    edgeType,
			Origin:  originMetrics,
			Details: fmt.Sprintf("%s → %s", src.Service, host),
		})
		if app := appByKey[src.Namespace+"/"+src.Service]; app != nil {
			switch kind {
			case kindDatabase:
				// Classify the engine from the peer PORT (5432→postgresql,
				// 3306→mysql, …), never from Beyla's db.system label: its eBPF
				// protocol autodetection fabricates that engine — it has stamped
				// the very same peer as both mysql and postgresql. The port is the
				// only trustworthy signal, so record it as the DB type and keep the
				// host as the instance identity.
				engine := wellKnownPorts[peerPort(s.Metric, addr)]
				if engine == "" {
					engine = dst.Service // resolve matched a bare engine name, not a port
				}
				attachDatabaseNamed(app, engine, host, originMetrics)
			case kindKafka:
				attachKafka(app, dst.Service, "", originMetrics)
			}
		}
	}
}

// applyDatabaseMetrics attaches DB usage from Beyla's per-service db_client metric.
// Beyla is eBPF-only: it has the peer server_address (an RDS endpoint, a VPC IP, a
// public IP) and a db_system engine label, but exposes NO server_port for these
// series. Most of these databases are EXTERNAL (RDS/managed), so we do NOT gate on
// in-cluster-ness; and Beyla's eBPF SQL autodetection can misfire on opaque TLS
// egress (it has stamped one public peer as BOTH mysql and postgresql), so the
// engine label is imperfect. We attach broadly here — any series with an engine or
// a resolvable host — and keep phantom-filtering as a separate, evidence-based
// concern rather than dropping real RDS databases with an over-eager gate.
func (o *Overlayer) applyDatabaseMetrics(ctx context.Context, res *resolver, appByKey map[string]*model.ApplicationEntry, edges *edgeSet, httpPeers *clientPeers) {
	query := fmt.Sprintf(
		`count by (k8s_namespace_name, k8s_deployment_name, k8s_statefulset_name, service_name, service, db_system_name, db_system, db_name, server_address, server_port) (rate(db_client_operation_duration_seconds_count%s[%s]))`,
		o.clusterMatcher(), o.window(),
	)
	samples, err := o.client.InstantQuery(ctx, query)
	if err != nil {
		o.queryFailed("db", err)
		return
	}
	// Reconcile intra-peer engine conflicts before attaching. A single peer (one
	// server_address = one server = one engine) that Beyla stamps with more than
	// one db_system is eBPF misdetection: the real engine carries continuous query
	// traffic (a high summed operation rate) while the fabricated one shows only
	// sparse bursts, so we trust the dominant engine per (workload, peer) and drop
	// the rest. See engineWinners for the exact rule (ties keep both).
	winners := engineWinners(res, samples)
	for _, s := range samples {
		// db.system.name is the current OTel semconv label; db_system is the legacy
		// name. Read both so a semconv rename doesn't silently blank the engine.
		system := firstNonEmpty(s.Metric["db_system_name"], s.Metric["db_system"])
		dbName := s.Metric["db_name"]
		rawHost := stripPort(s.Metric["server_address"])
		host := rawHost
		// Drop a fabricated engine label when this same (workload, peer) also carries
		// the true, dominant engine. Only fires on a genuine multi-engine conflict;
		// single-engine peers are never in the map, so ordinary DBs are untouched.
		if system != "" {
			if keep, conflicted := winners[engineConflictKey(res.resolveClient(s.Metric), rawHost)]; conflicted && !keep[system] {
				continue
			}
		}
		// Reject OpenTelemetry collector peers. Beyla stamps a (fabricated, always
		// postgresql) db_system on the gRPC/HTTP-2 stream a workload uses to EXPORT OTLP
		// to its otel-collector-service, surfacing the collector as a phantom database
		// fleet-wide. No real database is named this, so this is a zero-risk drop —
		// unlike a blanket in-cluster gate, which would delete genuine in-cluster DBs.
		if isOTLPCollectorPeer(rawHost) {
			continue
		}
		port := peerPort(s.Metric, s.Metric["server_address"])
		// Infer the engine from a well-known DB peer port (5432→postgresql, …) on the
		// rare series that carries one, when the engine label is absent.
		if system == "" {
			if sys, ok := wellKnownPorts[port]; ok {
				if _, isDB := databaseSystems[sys]; isDB {
					system = sys
				}
			}
		}
		// Make a public-IP peer readable via reverse DNS (e.g. an RDS endpoint). A
		// private/infra peer with no logical name and no detected engine is
		// unattributable noise — drop only that, not real DBs behind private VPC IPs.
		resolved, keep := o.externalHost(ctx, host)
		if keep {
			host = resolved
		} else if dbName == "" && system == "" {
			continue
		}
		src := res.resolveClient(s.Metric)
		// Reject Beyla's phantom databases. If this same workload reaches this peer
		// over HTTP (recorded by applyHTTPClient), the db_client series is eBPF
		// misdetecting an HTTP/gRPC-on-443 stream as a DB call — Beyla even stamps a
		// bogus db_system on it. The check is PER WORKLOAD, so a host that is a real
		// database for one workload still attaches there while being dropped as a
		// mere HTTP peer for another (e.g. an API hostname sharing an IP with an RDS
		// box: real postgres for booking-auto-approval, phantom for a workload that
		// only calls its HTTP API). Match on the raw peer (db_client reports the IP)
		// AND the reverse-resolved host, since the HTTP index carries both the
		// hostname and its forward-resolved IPs.
		if !src.External {
			appKey := src.Namespace + "/" + src.Service
			if httpPeers.has(appKey, rawHost) || httpPeers.has(appKey, host) {
				continue
			}
		}
		// Identify the instance by its logical db_name when instrumented, else by the
		// peer host; point the edge at the concrete host when we have one.
		name := firstNonEmpty(dbName, host)
		target := firstNonEmpty(host, system)
		if target == "" {
			continue
		}
		edges.add(model.DependencyEdge{
			Source:  src,
			Target:  externalNode(target),
			Type:    kindDatabase,
			Origin:  originMetrics,
			Details: dbName,
		})
		if src.External {
			continue
		}
		if app := appByKey[src.Namespace+"/"+src.Service]; app != nil {
			attachDatabaseNamed(app, system, name, originMetrics)
		}
	}
}

// engineConflictKey identifies a (client workload, peer host) pair for
// engine-conflict reconciliation. The host is the raw db_client server_address
// (pre-reverse-DNS): Beyla reports every engine sample for one peer under the
// same address, so grouping on it collects the conflicting engines together.
func engineConflictKey(src model.DependencyNode, rawHost string) string {
	return src.Namespace + "/" + src.Service + "\x00" + rawHost
}

// engineWinners resolves Beyla's intra-peer engine conflicts. A single peer is one
// server (one address → one engine), yet Beyla's eBPF SQL autodetection fabricates
// a second engine on the opaque TLS/gRPC stream to a genuine database — stamping,
// say, both mysql and postgresql on the same RDS box. The real engine carries
// continuous query traffic and so a high summed db_client operation rate, while the
// fabricated one appears only as sparse, bursty misdetections. So per (client
// workload, peer host) we keep the engine(s) with the maximum summed Sample.Value
// and drop the strictly-dominated ones. Exact ties keep every tied engine: with
// equal evidence there is no basis to choose, and showing both beats guessing
// wrong. Peers carrying a single engine are omitted entirely, so ordinary
// one-engine databases are never reconciled. Returns, per conflicted key, the set
// of engines to keep.
func engineWinners(res *resolver, samples []Sample) map[string]map[string]bool {
	totals := map[string]map[string]float64{}
	for _, s := range samples {
		system := firstNonEmpty(s.Metric["db_system_name"], s.Metric["db_system"])
		if system == "" {
			continue
		}
		key := engineConflictKey(res.resolveClient(s.Metric), stripPort(s.Metric["server_address"]))
		byEngine := totals[key]
		if byEngine == nil {
			byEngine = map[string]float64{}
			totals[key] = byEngine
		}
		byEngine[system] += s.Value
	}
	winners := map[string]map[string]bool{}
	for key, byEngine := range totals {
		if len(byEngine) < 2 {
			continue // single engine — no conflict to reconcile
		}
		max := 0.0
		for _, v := range byEngine {
			if v > max {
				max = v
			}
		}
		keep := map[string]bool{}
		for eng, v := range byEngine {
			if v >= max {
				keep[eng] = true // dominant engine, or tied for the lead
			}
		}
		winners[key] = keep
	}
	return winners
}

// messagingProcessMetric is Beyla's Kafka CONSUME counter. OTel semconv split
// messaging into messaging.publish.duration (produce) and
// messaging.process.duration (consume); in this Beyla build only the process
// (consume) side carries data — the publish series are empty fleet-wide — so every
// edge derived here is a consume edge, and a produce edge is underivable from this
// source. Beyla labels it with messaging_system (kafka), messaging_destination_name
// (the topic) and its usual k8s decoration (k8s_deployment_name, k8s_namespace_name).
const messagingProcessMetric = "messaging_process_duration_seconds_count"

// applyMessagingMetrics supplements Kafka usage from Beyla's per-workload consume
// metric. Best-effort SUPPLEMENT only: authoritative topics come from SSU's
// registry, and Beyla sees a workload's Kafka traffic only when it can hook that
// client library and decrypt the stream, so this covers a subset of consumers and
// never yields a complete Kafka graph. Joins on the k8s labels via resolveClient
// (the workload, not the bare service.name). Direction is always "consume": Beyla
// emits no publish series here.
func (o *Overlayer) applyMessagingMetrics(ctx context.Context, res *resolver, appByKey map[string]*model.ApplicationEntry, edges *edgeSet) {
	query := fmt.Sprintf(
		`count by (k8s_namespace_name, k8s_deployment_name, k8s_statefulset_name, service_name, service, messaging_destination_name) (rate(%s%s[%s]))`,
		messagingProcessMetric, o.messagingSelector(), o.window(),
	)
	samples, err := o.client.InstantQuery(ctx, query)
	if err != nil {
		o.queryFailed("messaging", err)
		return
	}
	for _, s := range samples {
		dest := s.Metric["messaging_destination_name"]
		if dest == "" {
			continue
		}
		src := res.resolveClient(s.Metric)
		edges.add(model.DependencyEdge{
			Source:  src,
			Target:  externalNode(dest),
			Type:    kindKafka,
			Origin:  originMetrics,
			Details: "consume",
		})
		if src.External {
			continue
		}
		if app := appByKey[src.Namespace+"/"+src.Service]; app != nil {
			attachKafka(app, dest, "consume", originMetrics)
		}
	}
}

// messagingSelector scopes the messaging query to Kafka — the only system Beyla
// emits process metrics for here — and to this cluster when one is configured.
func (o *Overlayer) messagingSelector() string {
	if o.cluster == "" {
		return `{messaging_system="kafka"}`
	}
	return fmt.Sprintf(`{messaging_system="kafka",cluster=%q}`, o.cluster)
}

// targetInfoMetric is the OTel resource metric Beyla emits per instrumented
// workload. Its telemetry_sdk_language label carries the eBPF-detected runtime;
// "generic" means Beyla could not fingerprint a language and is treated as "not
// detected".
const targetInfoMetric = "target_info"

// applyRuntimeMetrics attaches Beyla's detected runtime/language to each workload
// from target_info's telemetry_sdk_language. Best-effort: only workloads Beyla
// instruments appear, and unfingerprintable ones report "generic" (dropped here).
// No dependency edge is emitted — runtime is a workload attribute, not a relation.
//
// Windowed via count_over_time, NOT rate: target_info is a constant-1 info gauge
// (rate would be 0), and windowing over the lookback includes every series active
// at any point, avoiding the per-pod staleness gaps documented on serviceGraphQuery.
// count by (...) collapses target_info's high-cardinality pod labels to one row per
// (workload, language). Joins on the k8s labels via resolveClient, same as the
// db/messaging overlays — the scrape `service` label (="beyla") is never consulted
// because k8s_deployment_name/service_name are always populated on target_info.
func (o *Overlayer) applyRuntimeMetrics(ctx context.Context, res *resolver, appByKey map[string]*model.ApplicationEntry) {
	query := fmt.Sprintf(
		`count by (k8s_namespace_name, k8s_deployment_name, k8s_statefulset_name, service_name, service, telemetry_sdk_language) (count_over_time(%s%s[%s]))`,
		targetInfoMetric, o.clusterMatcher(), o.window(),
	)
	samples, err := o.client.InstantQuery(ctx, query)
	if err != nil {
		o.queryFailed("runtime", err)
		return
	}
	for _, s := range samples {
		lang := s.Metric["telemetry_sdk_language"]
		if lang == "" || lang == "generic" {
			continue // Beyla couldn't fingerprint a runtime — treat as undetected
		}
		src := res.resolveClient(s.Metric)
		if src.External {
			continue
		}
		if app := appByKey[src.Namespace+"/"+src.Service]; app != nil && app.Runtime == "" {
			app.Runtime = lang // first detected wins
		}
	}
}

// httpServerMetric is Beyla's inbound HTTP request counter. The `application`
// feature emits it server-side with the workload's k8s labels and an
// http_response_status_code label; the _count survives scrape (buckets are
// dropped), so it yields request rate (activity) and the 5xx share (health) — but
// no latency percentiles.
const httpServerMetric = "http_server_request_duration_seconds_count"

// applyTrafficMetrics attaches inbound HTTP throughput + error ratio to each
// workload from Beyla's http_server_request_duration_seconds_count. Best-effort:
// only Beyla-instrumented HTTP servers report it. Unlike the other overlays this
// one AGGREGATES across the http_response_status_code series per workload before
// assigning, so it accumulates into a temp map keyed by *ApplicationEntry. Uses
// rate() (a counter, like the db/messaging overlays) and joins via resolveClient —
// the server metric carries the same k8s workload labels. No dependency edge is
// emitted: request rate/health is a workload attribute, not a relation.
func (o *Overlayer) applyTrafficMetrics(ctx context.Context, res *resolver, appByKey map[string]*model.ApplicationEntry) {
	query := fmt.Sprintf(
		`sum by (k8s_namespace_name, k8s_deployment_name, k8s_statefulset_name, service_name, service, http_response_status_code) (rate(%s%s[%s]))`,
		httpServerMetric, o.clusterMatcher(), o.window(),
	)
	samples, err := o.client.InstantQuery(ctx, query)
	if err != nil {
		o.queryFailed("traffic", err)
		return
	}
	type acc struct{ total, errors float64 }
	byApp := map[*model.ApplicationEntry]*acc{}
	for _, s := range samples {
		src := res.resolveClient(s.Metric) // server metric: k8s labels identify the owning workload
		if src.External {
			continue
		}
		app := appByKey[src.Namespace+"/"+src.Service]
		if app == nil {
			continue
		}
		a := byApp[app]
		if a == nil {
			a = &acc{}
			byApp[app] = a
		}
		a.total += s.Value
		if code := s.Metric["http_response_status_code"]; strings.HasPrefix(code, "5") {
			a.errors += s.Value
		}
	}
	for app, a := range byApp {
		if a.total <= 0 {
			continue
		}
		app.RequestRate = a.total
		app.ErrorRate = a.errors / a.total
	}
}

func (o *Overlayer) queryFailed(name string, err error) {
	o.logger.Warn("telemetry query failed; continuing without overlay",
		zap.String("query", name), zap.Error(err))
	if o.queryErrors != nil {
		o.queryErrors.Inc()
	}
}

// window renders the lookback as a PromQL range literal (e.g. "60m").
func (o *Overlayer) window() string {
	minutes := int(o.lookback.Minutes())
	if minutes < 1 {
		minutes = 1
	}
	return fmt.Sprintf("%dm", minutes)
}

// externalHost normalises a peer host for display. A hostname is returned as-is.
// A bare IP is dropped (keep=false) when it is private/infra space, and otherwise
// reverse-resolved to its PTR hostname — falling back to the literal public IP
// when no PTR exists (so genuine external egress still surfaces, by name when we
// can, by address when we can't).
func (o *Overlayer) externalHost(ctx context.Context, host string) (string, bool) {
	if host == "" || !isBareIP(host) {
		return host, host != ""
	}
	if isPrivateIP(host) {
		return "", false
	}
	if name := o.reverseLookup(ctx, host); name != "" {
		return name, true
	}
	return host, true
}

// reverseLookup returns a PTR hostname for a public IP, or "" if none resolves.
// Results (misses included) are cached for the Overlayer's lifetime and each
// lookup is time-bounded, so a slow or blocked resolver can't stall a scan.
func (o *Overlayer) reverseLookup(ctx context.Context, ip string) string {
	o.ptrMu.Lock()
	if o.ptrCache == nil {
		o.ptrCache = map[string]string{}
	} else if name, ok := o.ptrCache[ip]; ok {
		o.ptrMu.Unlock()
		return name
	}
	o.ptrMu.Unlock()

	lookup := o.lookupAddr
	if lookup == nil {
		lookup = func(ctx context.Context, ip string) ([]string, error) {
			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			return net.DefaultResolver.LookupAddr(ctx, ip)
		}
	}
	names, _ := lookup(ctx, ip)
	name := ""
	for _, n := range names {
		if n = strings.TrimSuffix(strings.TrimSpace(n), "."); n != "" {
			name = n
			break
		}
	}
	o.ptrMu.Lock()
	o.ptrCache[ip] = name
	o.ptrMu.Unlock()
	return name
}

// forwardLookup returns the IPs a hostname resolves to, or nil. Results (misses
// included) are cached for the Overlayer's lifetime and each lookup is
// time-bounded, so a slow or blocked resolver can't stall a scan. Used to align
// HTTP peers (hostnames) with db_client peers (IPs) for phantom-DB rejection.
func (o *Overlayer) forwardLookup(ctx context.Context, host string) []string {
	o.hostMu.Lock()
	if o.hostCache == nil {
		o.hostCache = map[string][]string{}
	} else if ips, ok := o.hostCache[host]; ok {
		o.hostMu.Unlock()
		return ips
	}
	o.hostMu.Unlock()

	lookup := o.lookupHost
	if lookup == nil {
		lookup = func(ctx context.Context, host string) ([]string, error) {
			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			return net.DefaultResolver.LookupHost(ctx, host)
		}
	}
	addrs, _ := lookup(ctx, host)
	ips := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if ip := strings.TrimSpace(a); ip != "" && isBareIP(ip) {
			ips = append(ips, ip)
		}
	}
	o.hostMu.Lock()
	o.hostCache[host] = ips
	o.hostMu.Unlock()
	return ips
}

// --- application attachment --------------------------------------------------

func indexApps(apps []model.ApplicationEntry) map[string]*model.ApplicationEntry {
	idx := make(map[string]*model.ApplicationEntry, len(apps))
	for i := range apps {
		idx[apps[i].Namespace+"/"+apps[i].Name] = &apps[i]
	}
	return idx
}

func attachDatabase(app *model.ApplicationEntry, system, source string) {
	attachDatabaseNamed(app, system, "", source)
}

func attachDatabaseNamed(app *model.ApplicationEntry, system, name, source string) {
	// Need at least one identifier: the logical system (OTel) or the peer host
	// (Beyla eBPF, which sees the address but not the db_system).
	if system == "" && name == "" {
		return
	}
	for _, db := range app.Databases {
		if db.System == system && db.Name == name {
			return
		}
	}
	app.Databases = append(app.Databases, model.DatabaseRef{System: system, Name: name, Source: source})
}

func attachKafka(app *model.ApplicationEntry, name, direction, source string) {
	if name == "" {
		return
	}
	for _, t := range app.KafkaTopics {
		if t.Name == name && t.Direction == direction {
			return
		}
	}
	app.KafkaTopics = append(app.KafkaTopics, model.KafkaTopicRef{Name: name, Direction: direction, Source: source})
}

// --- HTTP peer index ---------------------------------------------------------

// clientPeers records, per client workload ("namespace/service"), the set of
// hosts it was observed talking to over HTTP. The DB overlay uses it to drop
// Beyla's phantom databases — a peer reached over HTTP is not a database, so a
// db_client series for the same (workload, host) is eBPF protocol misdetection.
type clientPeers struct {
	byApp map[string]map[string]struct{}
}

func newClientPeers() *clientPeers {
	return &clientPeers{byApp: map[string]map[string]struct{}{}}
}

func (c *clientPeers) add(appKey, host string) {
	if host == "" {
		return
	}
	set := c.byApp[appKey]
	if set == nil {
		set = map[string]struct{}{}
		c.byApp[appKey] = set
	}
	set[host] = struct{}{}
}

func (c *clientPeers) has(appKey, host string) bool {
	if host == "" {
		return false
	}
	_, ok := c.byApp[appKey][host]
	return ok
}

// --- edge dedup --------------------------------------------------------------

type edgeSet struct {
	seen map[string]struct{}
	list []model.DependencyEdge
}

func newEdgeSet() *edgeSet {
	return &edgeSet{seen: map[string]struct{}{}, list: []model.DependencyEdge{}}
}

func (e *edgeSet) add(edge model.DependencyEdge) {
	key := fmt.Sprintf("%s/%s/%s|%s/%s/%s|%s|%s",
		edge.Source.Namespace, edge.Source.Service, boolStr(edge.Source.External),
		edge.Target.Namespace, edge.Target.Service, boolStr(edge.Target.External),
		edge.Type, edge.Origin)
	if _, dup := e.seen[key]; dup {
		return
	}
	e.seen[key] = struct{}{}
	e.list = append(e.list, edge)
}

func boolStr(b bool) string {
	if b {
		return "ext"
	}
	return "int"
}
