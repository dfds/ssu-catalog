package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.dfds.cloud/ssu-catalog/internal/model"
)

func TestInstantQuery_ParsesVector(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if u, p, ok := r.BasicAuth(); !ok || u != "12345" || p != "secret" {
			t.Errorf("basic auth wrong: %q/%q ok=%v", u, p, ok)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"status":"success",
			"data":{"resultType":"vector","result":[
				{"metric":{"client":"a","server":"b"},"value":[1700000000,"3"]},
				{"metric":{"client":"c","server":"d"},"value":[1700000000,"7.5"]}
			]}
		}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "12345", "secret", time.Second)
	samples, err := c.InstantQuery(context.Background(), serviceGraphMetric)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(samples) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(samples))
	}
	if samples[0].Metric["client"] != "a" || samples[0].Value != 3 {
		t.Errorf("sample 0 wrong: %+v", samples[0])
	}
	if samples[1].Value != 7.5 {
		t.Errorf("sample 1 value wrong: %+v", samples[1])
	}
}

func TestInstantQuery_ErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"parse error"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", "", time.Second)
	if _, err := c.InstantQuery(context.Background(), "broken"); err == nil {
		t.Fatal("expected error for status=error response")
	}
}

func TestInstantQuery_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", "", time.Second)
	if _, err := c.InstantQuery(context.Background(), "q"); err == nil {
		t.Fatal("expected error for HTTP 500")
	}
}

// stubClient returns canned samples (or errors) per query substring.
type stubClient struct {
	byQuery map[string][]Sample
	errFor  map[string]bool
}

func (s *stubClient) InstantQuery(_ context.Context, query string) ([]Sample, error) {
	for frag, fail := range s.errFor {
		if fail && contains(query, frag) {
			return nil, errBoom
		}
	}
	for frag, samples := range s.byQuery {
		if contains(query, frag) {
			return samples, nil
		}
	}
	return nil, nil
}

var errBoom = &boomError{}

type boomError struct{}

func (*boomError) Error() string { return "boom" }

func contains(s, sub string) bool {
	return len(sub) > 0 && len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func sampleApps() []model.ApplicationEntry {
	return []model.ApplicationEntry{
		{
			Cluster: "hellman", Namespace: "cap-a", Name: "api", Kind: "Deployment",
			Services:    []model.ServiceRef{{Name: "api"}},
			KafkaTopics: []model.KafkaTopicRef{},
			Databases:   []model.DatabaseRef{},
		},
		{
			Cluster: "hellman", Namespace: "cap-b", Name: "worker", Kind: "Deployment",
			Services:    []model.ServiceRef{{Name: "worker"}},
			KafkaTopics: []model.KafkaTopicRef{},
			Databases:   []model.DatabaseRef{},
		},
	}
}

func overlayerWith(stub *stubClient) *Overlayer {
	return NewOverlayer(stub, "hellman", time.Hour, nil, nil)
}

func findApp(apps []model.ApplicationEntry, name string) *model.ApplicationEntry {
	for i := range apps {
		if apps[i].Name == name {
			return &apps[i]
		}
	}
	return nil
}

func TestApply_ServiceGraphResolution(t *testing.T) {
	stub := &stubClient{byQuery: map[string][]Sample{
		"traces_service_graph_request_total": {
			// in-cluster → in-cluster
			{Metric: map[string]string{"client": "api.cap-a.svc.cluster.local", "server": "worker.cap-b.svc.cluster.local"}},
			// in-cluster → database (generic system string)
			{Metric: map[string]string{"client": "api.cap-a.svc.cluster.local", "server": "postgresql"}},
			// in-cluster → bare DB port (redis) — distinct target from postgresql
			{Metric: map[string]string{"client": "api.cap-a.svc.cluster.local", "server": "6379"}},
			// in-cluster → kafka
			{Metric: map[string]string{"client": "worker.cap-b.svc.cluster.local", "server": "kafka"}},
			// unresolvable → external
			{Metric: map[string]string{"client": "api.cap-a.svc.cluster.local", "server": "api.public.example.com"}},
		},
	}}
	apps := sampleApps()
	edges := overlayerWith(stub).Apply(context.Background(), apps)

	if len(edges) != 5 {
		t.Fatalf("expected 5 edges, got %d: %+v", len(edges), edges)
	}

	byType := map[string]int{}
	for _, e := range edges {
		byType[e.Type]++
	}
	if byType[kindService] != 2 || byType[kindDatabase] != 2 || byType[kindKafka] != 1 {
		t.Errorf("edge type distribution wrong: %+v", byType)
	}

	// First edge: both endpoints resolved in-cluster.
	var svcEdge *model.DependencyEdge
	for i := range edges {
		if edges[i].Target.Service == "worker" {
			svcEdge = &edges[i]
		}
	}
	if svcEdge == nil || svcEdge.Source.External || svcEdge.Target.External {
		t.Errorf("expected fully-resolved service edge, got %+v", svcEdge)
	}
	if svcEdge.Source.Service != "api" || svcEdge.Target.Namespace != "cap-b" {
		t.Errorf("service edge resolution wrong: %+v", svcEdge)
	}

	// api should have postgresql (generic string) + redis (bare port) attached.
	api := findApp(apps, "api")
	dbSystems := map[string]string{}
	for _, db := range api.Databases {
		dbSystems[db.System] = db.Source
	}
	if len(api.Databases) != 2 || dbSystems["postgresql"] == "" || dbSystems["redis"] == "" {
		t.Errorf("api databases wrong: %+v", api.Databases)
	}
	if dbSystems["postgresql"] != originServiceGraph {
		t.Errorf("db source not tagged: %+v", api.Databases)
	}

	worker := findApp(apps, "worker")
	if len(worker.KafkaTopics) != 1 || worker.KafkaTopics[0].Name != "kafka" {
		t.Errorf("worker kafka topics wrong: %+v", worker.KafkaTopics)
	}
}

func TestApply_ServiceGraph_BeylaLabels(t *testing.T) {
	// Beyla emits the BARE service.name plus the k8s namespace as a separate
	// label — never the name.namespace DNS form. Resolution must join on the
	// namespace label, and a service.name that matches no workload/service in
	// its namespace (e.g. a .NET OTEL_SERVICE_NAME) stays honestly external.
	stub := &stubClient{byQuery: map[string][]Sample{
		"traces_service_graph_request_total": {
			{Metric: map[string]string{
				"client": "api", "client_k8s_namespace_name": "cap-a",
				"server": "worker", "server_k8s_namespace_name": "cap-b",
			}},
			{Metric: map[string]string{
				"client": "api", "client_k8s_namespace_name": "cap-a",
				"server": "Ferry.CustomsCompliance", "server_k8s_namespace_name": "customs-compliance-cvwbp",
			}},
			{Metric: map[string]string{
				"client": "worker", "client_k8s_namespace_name": "cap-b",
				"server": "postgresql",
			}},
		},
	}}
	apps := sampleApps()
	edges := overlayerWith(stub).Apply(context.Background(), apps)

	var internalEdge, externalEdge *model.DependencyEdge
	for i := range edges {
		switch edges[i].Target.Service {
		case "worker":
			internalEdge = &edges[i]
		case "Ferry.CustomsCompliance":
			externalEdge = &edges[i]
		}
	}
	if internalEdge == nil || internalEdge.Source.External || internalEdge.Target.External {
		t.Fatalf("expected api.cap-a → worker.cap-b fully resolved, got %+v", internalEdge)
	}
	if internalEdge.Source.Service != "api" || internalEdge.Source.Namespace != "cap-a" ||
		internalEdge.Target.Namespace != "cap-b" {
		t.Errorf("beyla-label resolution wrong: %+v", internalEdge)
	}
	// Unknown service.name in a real namespace → external, not mis-joined.
	if externalEdge == nil || !externalEdge.Target.External {
		t.Errorf("unmatched .NET service.name should be external, got %+v", externalEdge)
	}
	// worker → postgresql (no namespace label) still classifies as a database.
	worker := findApp(apps, "worker")
	if len(worker.Databases) != 1 || worker.Databases[0].System != "postgresql" {
		t.Errorf("worker postgresql attach wrong: %+v", worker.Databases)
	}
}

func TestServiceGraphQuery_ClusterScopedWithNamespaceLabels(t *testing.T) {
	o := NewOverlayer(nil, "hellman", time.Hour, nil, nil)
	q := o.serviceGraphQuery()
	for _, want := range []string{
		`cluster="hellman"`,
		"client_k8s_namespace_name",
		"server_k8s_namespace_name",
	} {
		if !contains(q, want) {
			t.Errorf("service-graph query missing %q: %s", want, q)
		}
	}
	// No cluster configured → no matcher (avoid an empty {} selector).
	if m := NewOverlayer(nil, "", time.Hour, nil, nil).clusterMatcher(); m != "" {
		t.Errorf("empty cluster should yield empty matcher, got %q", m)
	}
}

func TestApply_DatabaseMetricsSupplement(t *testing.T) {
	stub := &stubClient{byQuery: map[string][]Sample{
		"db_client_operation_duration_seconds_count": {
			{Metric: map[string]string{"service": "api.cap-a", "db_system": "redis", "db_name": "cache"}},
		},
	}}
	apps := sampleApps()
	edges := overlayerWith(stub).Apply(context.Background(), apps)

	api := findApp(apps, "api")
	if len(api.Databases) != 1 || api.Databases[0].System != "redis" || api.Databases[0].Name != "cache" {
		t.Fatalf("api db supplement wrong: %+v", api.Databases)
	}
	if api.Databases[0].Source != originMetrics {
		t.Errorf("expected otel-metrics source, got %+v", api.Databases[0])
	}
	if len(edges) != 1 || edges[0].Type != kindDatabase {
		t.Errorf("expected 1 database edge, got %+v", edges)
	}
}

func TestApply_MessagingMetricsSupplement(t *testing.T) {
	stub := &stubClient{byQuery: map[string][]Sample{
		"messaging_client_operation_duration_seconds_count": {
			{Metric: map[string]string{"service": "worker.cap-b", "messaging_destination_name": "orders", "messaging_operation": "receive"}},
		},
	}}
	apps := sampleApps()
	overlayerWith(stub).Apply(context.Background(), apps)

	worker := findApp(apps, "worker")
	if len(worker.KafkaTopics) != 1 {
		t.Fatalf("expected 1 kafka topic, got %+v", worker.KafkaTopics)
	}
	if worker.KafkaTopics[0].Name != "orders" || worker.KafkaTopics[0].Direction != "consume" {
		t.Errorf("messaging supplement wrong: %+v", worker.KafkaTopics[0])
	}
}

func TestApply_QueryFailureDegradesGracefully(t *testing.T) {
	stub := &stubClient{
		errFor: map[string]bool{"traces_service_graph_request_total": true},
		byQuery: map[string][]Sample{
			"db_client_operation_duration_seconds_count": {
				{Metric: map[string]string{"service": "api.cap-a", "db_system": "mysql"}},
			},
		},
	}
	apps := sampleApps()
	// Service-graph query fails, but the DB supplement still applies.
	edges := overlayerWith(stub).Apply(context.Background(), apps)
	if len(edges) != 1 || edges[0].Type != kindDatabase {
		t.Errorf("expected only the DB edge to survive, got %+v", edges)
	}
	if api := findApp(apps, "api"); len(api.Databases) != 1 {
		t.Errorf("expected mysql attached despite service-graph failure, got %+v", api.Databases)
	}
}

func TestResolve_BarePortAndExternal(t *testing.T) {
	r := newResolver("hellman", sampleApps())

	node, kind := r.resolve("5432")
	if kind != kindDatabase || !node.External || node.Service != "postgresql" {
		t.Errorf("bare port resolution wrong: %+v kind=%s", node, kind)
	}

	node, kind = r.resolve("9092")
	if kind != kindKafka || node.Service != "kafka" {
		t.Errorf("kafka port resolution wrong: %+v kind=%s", node, kind)
	}

	node, kind = r.resolve("totally-unknown-host")
	if kind != kindExternal || !node.External {
		t.Errorf("unknown host should be external: %+v kind=%s", node, kind)
	}

	node, kind = r.resolve("api.cap-a.svc.cluster.local:8080")
	if kind != kindService || node.External || node.Service != "api" {
		t.Errorf("in-cluster with port wrong: %+v kind=%s", node, kind)
	}
}

func TestWindowLiteral(t *testing.T) {
	o := NewOverlayer(nil, "c", 90*time.Minute, nil, nil)
	if w := o.window(); w != "90m" {
		t.Errorf("window = %q, want 90m", w)
	}
	o = NewOverlayer(nil, "c", 0, nil, nil)
	if w := o.window(); w != "60m" {
		t.Errorf("default lookback window = %q, want 60m", w)
	}
}
