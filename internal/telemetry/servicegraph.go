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

// serviceGraphQuery is the primary, reliably-populated source: a ready-made
// dependency graph from Tempo's metrics generator stored in Mimir.
const serviceGraphQuery = `count by (client, server) (traces_service_graph_request_total)`

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
	o.applyDatabaseMetrics(ctx, res, appByKey, edges)
	o.applyMessagingMetrics(ctx, res, appByKey, edges)

	return edges.list
}

// applyServiceGraph processes the service-graph edges (service→service, →DB,
// →kafka), attaching DB/Kafka refs to the client application when in-cluster.
func (o *Overlayer) applyServiceGraph(ctx context.Context, res *resolver, appByKey map[string]*model.ApplicationEntry, edges *edgeSet) {
	samples, err := o.client.InstantQuery(ctx, serviceGraphQuery)
	if err != nil {
		o.queryFailed("service_graph", err)
		return
	}
	for _, s := range samples {
		client, server := s.Metric["client"], s.Metric["server"]
		if client == "" || server == "" {
			continue
		}
		src := res.resolveNode(client)
		dst, kind := res.resolve(server)

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

// applyDatabaseMetrics supplements DB usage from per-service DB metrics.
func (o *Overlayer) applyDatabaseMetrics(ctx context.Context, res *resolver, appByKey map[string]*model.ApplicationEntry, edges *edgeSet) {
	query := fmt.Sprintf(`count by (service, db_system, db_name) (rate(db_client_operation_duration_seconds_count[%s]))`, o.window())
	samples, err := o.client.InstantQuery(ctx, query)
	if err != nil {
		o.queryFailed("db", err)
		return
	}
	for _, s := range samples {
		service, system, dbName := s.Metric["service"], s.Metric["db_system"], s.Metric["db_name"]
		if service == "" || system == "" {
			continue
		}
		src := res.resolveNode(service)
		edges.add(model.DependencyEdge{
			Source:  src,
			Target:  externalNode(system),
			Type:    kindDatabase,
			Origin:  originMetrics,
			Details: dbName,
		})
		if src.External {
			continue
		}
		if app := appByKey[src.Namespace+"/"+src.Service]; app != nil {
			attachDatabaseNamed(app, system, dbName, originMetrics)
		}
	}
}

// applyMessagingMetrics supplements Kafka/messaging usage from per-service
// messaging metrics (best-effort; authoritative topics come from SSU's registry).
func (o *Overlayer) applyMessagingMetrics(ctx context.Context, res *resolver, appByKey map[string]*model.ApplicationEntry, edges *edgeSet) {
	query := fmt.Sprintf(`count by (service, messaging_destination_name, messaging_operation) (rate(messaging_client_operation_duration_seconds_count[%s]))`, o.window())
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
		src := res.resolveNode(service)
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
	if system == "" {
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
