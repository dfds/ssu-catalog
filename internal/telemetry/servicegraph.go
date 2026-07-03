package telemetry

import (
	"context"
	"fmt"
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
	o.applyServiceGraph(ctx, res, appByKey, edges)
	o.applyHTTPClient(ctx, res, appByKey, edges)
	o.applyDatabaseMetrics(ctx, res, appByKey, edges)
	o.applyMessagingMetrics(ctx, res, appByKey, edges)

	return edges.list
}

// serviceGraphQuery aggregates edges while KEEPING the k8s namespace labels
// Beyla attaches to each side, so resolveEndpoint can join on (namespace, name)
// instead of parsing the bare service.name. Cluster-scoped: without it, same-name
// workloads collide across the fleet (namespace+name is only unique per cluster).
func (o *Overlayer) serviceGraphQuery() string {
	return fmt.Sprintf(`count by (client, server, client_k8s_namespace_name, server_k8s_namespace_name) (%s%s)`,
		serviceGraphMetric, o.clusterMatcher())
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
// destinations are left to the (richer) service-graph overlay, and bare IPs are
// dropped as infra/mesh noise — DB hosts arrive via the DB metric instead.
func (o *Overlayer) applyHTTPClient(ctx context.Context, res *resolver, appByKey map[string]*model.ApplicationEntry, edges *edgeSet) {
	query := fmt.Sprintf(
		`count by (k8s_namespace_name, k8s_deployment_name, k8s_statefulset_name, service_name, service, server_address) (rate(%s%s[%s]))`,
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
		host := stripPort(addr)
		if isBareIP(host) {
			continue // infra/mesh noise (kube API server, node IPs)
		}
		dst, kind := res.resolve(host)
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
				attachDatabase(app, dst.Service, originMetrics)
			case kindKafka:
				attachKafka(app, dst.Service, "", originMetrics)
			}
		}
	}
}

// applyDatabaseMetrics supplements DB usage from per-service DB metrics. Beyla is
// eBPF-only, so it rarely knows the logical db_system/db_name — but it always has
// the peer server_address (e.g. an RDS endpoint). Attribute the caller via its
// k8s labels (the `service` label is the Beyla collector, not the app) and fall
// back to the address as the database identity when the system is unknown.
func (o *Overlayer) applyDatabaseMetrics(ctx context.Context, res *resolver, appByKey map[string]*model.ApplicationEntry, edges *edgeSet) {
	query := fmt.Sprintf(
		`count by (k8s_namespace_name, k8s_deployment_name, k8s_statefulset_name, service_name, service, db_system, db_name, server_address) (rate(db_client_operation_duration_seconds_count%s[%s]))`,
		o.clusterMatcher(), o.window(),
	)
	samples, err := o.client.InstantQuery(ctx, query)
	if err != nil {
		o.queryFailed("db", err)
		return
	}
	for _, s := range samples {
		system, dbName := s.Metric["db_system"], s.Metric["db_name"]
		// DB identity: the logical system when instrumented, else the peer host.
		target := system
		attachName := dbName
		if target == "" {
			target = stripPort(s.Metric["server_address"])
			attachName = target
		}
		if target == "" {
			continue
		}
		src := res.resolveClient(s.Metric)
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
			attachDatabaseNamed(app, system, attachName, originMetrics)
		}
	}
}

// applyMessagingMetrics supplements Kafka/messaging usage from per-service
// messaging metrics (best-effort; authoritative topics come from SSU's registry).
func (o *Overlayer) applyMessagingMetrics(ctx context.Context, res *resolver, appByKey map[string]*model.ApplicationEntry, edges *edgeSet) {
	query := fmt.Sprintf(`count by (service, k8s_namespace_name, messaging_destination_name, messaging_operation) (rate(messaging_client_operation_duration_seconds_count%s[%s]))`, o.clusterMatcher(), o.window())
	samples, err := o.client.InstantQuery(ctx, query)
	if err != nil {
		o.queryFailed("messaging", err)
		return
	}
	for _, s := range samples {
		service, dest := s.Metric["service"], s.Metric["messaging_destination_name"]
		if service == "" || dest == "" {
			continue
		}
		direction := messagingDirection(s.Metric["messaging_operation"])
		src := res.resolveEndpointNode(service, s.Metric["k8s_namespace_name"])
		edges.add(model.DependencyEdge{
			Source:  src,
			Target:  externalNode(dest),
			Type:    kindKafka,
			Origin:  originMetrics,
			Details: direction,
		})
		if src.External {
			continue
		}
		if app := appByKey[src.Namespace+"/"+src.Service]; app != nil {
			attachKafka(app, dest, direction, originMetrics)
		}
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

// messagingDirection maps an OTel messaging.operation value to produce/consume.
func messagingDirection(op string) string {
	switch op {
	case "receive", "process", "deliver":
		return "consume"
	case "publish", "send", "create":
		return "produce"
	default:
		return ""
	}
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
