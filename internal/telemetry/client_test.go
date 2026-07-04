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
	o := NewOverlayer(stub, "hellman", time.Hour, nil, nil)
	// Never touch real DNS in tests; individual cases override with a stub map.
	o.lookupAddr = func(context.Context, string) ([]string, error) { return nil, nil }
	return o
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
		// Windowed, not instant: a periodic prober's bursty edges must survive a
		// scan that lands between bursts (see serviceGraphQuery).
		"count_over_time(",
		"[60m]",
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
	// DB usage is supplemented from the http_client metric via the peer PORT: egress
	// to 6379 is classified redis from the port alone (Beyla's db.system label, which
	// its eBPF autodetection fabricates, is never consulted).
	stub := &stubClient{byQuery: map[string][]Sample{
		"http_client_request_body_size_bytes_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
				"server_address": "cache.example.com", "server_port": "6379",
			}},
		},
	}}
	apps := sampleApps()
	edges := overlayerWith(stub).Apply(context.Background(), apps)

	api := findApp(apps, "api")
	if len(api.Databases) != 1 || api.Databases[0].System != "redis" || api.Databases[0].Name != "cache.example.com" {
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
	// Beyla emits the CONSUME metric (messaging_process_duration_seconds_count)
	// with the bare workload labels + topic — never messaging.operation or a
	// name.namespace service; resolution joins on the k8s labels via resolveClient.
	stub := &stubClient{byQuery: map[string][]Sample{
		"messaging_process_duration_seconds_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-b", "k8s_deployment_name": "worker", "service_name": "worker",
				"messaging_system": "kafka", "messaging_destination_name": "orders",
			}},
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

func TestApply_RuntimeMetricsSupplement(t *testing.T) {
	// Beyla's target_info carries the detected runtime in telemetry_sdk_language
	// alongside the workload's k8s labels; resolution joins via resolveClient. A
	// "generic" value means Beyla could not fingerprint the language and must be
	// treated as undetected (left empty), never surfaced.
	stub := &stubClient{byQuery: map[string][]Sample{
		"target_info": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
				"telemetry_sdk_language": "dotnet",
			}},
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-b", "k8s_deployment_name": "worker", "service_name": "worker",
				"telemetry_sdk_language": "generic",
			}},
		},
	}}
	apps := sampleApps()
	overlayerWith(stub).Apply(context.Background(), apps)

	if api := findApp(apps, "api"); api.Runtime != "dotnet" {
		t.Errorf("expected api runtime dotnet, got %q", api.Runtime)
	}
	if worker := findApp(apps, "worker"); worker.Runtime != "" {
		t.Errorf("expected generic to be dropped (empty runtime), got %q", worker.Runtime)
	}
}

func TestApply_TrafficMetrics(t *testing.T) {
	// Beyla's http_server_request_duration_seconds_count carries the workload's k8s
	// labels + an http_response_status_code label; resolution joins via resolveClient.
	// The overlay aggregates across status codes per workload: total req/s = sum of
	// all series, error ratio = 5xx share. A workload with only 2xx traffic has a 0
	// error ratio; a workload with no series stays absent (RequestRate 0).
	stub := &stubClient{byQuery: map[string][]Sample{
		"http_server_request_duration_seconds_count": {
			// api: 9 req/s of 200 + 1 req/s of 500 → total 10, error ratio 0.1
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
				"http_response_status_code": "200",
			}, Value: 9},
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
				"http_response_status_code": "500",
			}, Value: 1},
			// worker: only 2xx → error ratio 0
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-b", "k8s_deployment_name": "worker", "service_name": "worker",
				"http_response_status_code": "204",
			}, Value: 4},
		},
	}}
	apps := sampleApps()
	overlayerWith(stub).Apply(context.Background(), apps)

	api := findApp(apps, "api")
	if api.RequestRate != 10 {
		t.Errorf("expected api RequestRate 10, got %v", api.RequestRate)
	}
	if api.ErrorRate != 0.1 {
		t.Errorf("expected api ErrorRate 0.1, got %v", api.ErrorRate)
	}
	worker := findApp(apps, "worker")
	if worker.RequestRate != 4 {
		t.Errorf("expected worker RequestRate 4, got %v", worker.RequestRate)
	}
	if worker.ErrorRate != 0 {
		t.Errorf("expected worker ErrorRate 0, got %v", worker.ErrorRate)
	}
}

func TestApply_QueryFailureDegradesGracefully(t *testing.T) {
	stub := &stubClient{
		errFor: map[string]bool{"traces_service_graph_request_total": true},
		byQuery: map[string][]Sample{
			"http_client_request_body_size_bytes_count": {
				{Metric: map[string]string{
					"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
					"server_address": "db.example.com", "server_port": "3306",
				}},
			},
		},
	}
	apps := sampleApps()
	// Service-graph query fails, but the port-based DB supplement still applies.
	edges := overlayerWith(stub).Apply(context.Background(), apps)
	if len(edges) != 1 || edges[0].Type != kindDatabase {
		t.Errorf("expected only the DB edge to survive, got %+v", edges)
	}
	if api := findApp(apps, "api"); len(api.Databases) != 1 {
		t.Errorf("expected mysql attached despite service-graph failure, got %+v", api.Databases)
	}
}

func TestApply_HTTPClientResolvesExternalEgress(t *testing.T) {
	// The HTTP client metric carries the concrete server_address the service
	// graph collapses into "outgoing". Named external hosts resolve; in-cluster
	// FQDNs are left to the service graph; private/infra IPs are dropped as noise.
	stub := &stubClient{byQuery: map[string][]Sample{
		"http_client_request_body_size_bytes_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
				"server_address": "api.external.example.com:443",
			}},
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
				"server_address": "worker.cap-b.svc.cluster.local:8080",
			}},
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
				"server_address": "10.0.0.53:443", // private → dropped as infra noise
			}},
		},
	}}
	apps := sampleApps()
	edges := overlayerWith(stub).Apply(context.Background(), apps)

	if len(edges) != 1 {
		t.Fatalf("expected exactly 1 external HTTP edge, got %d: %+v", len(edges), edges)
	}
	e := edges[0]
	if !e.Target.External || e.Target.Service != "api.external.example.com" {
		t.Errorf("expected external target, got %+v", e.Target)
	}
	if e.Source.External || e.Source.Service != "api" || e.Source.Namespace != "cap-a" {
		t.Errorf("expected caller resolved to api/cap-a, got %+v", e.Source)
	}
	if e.Origin != originMetrics || e.Type != kindService {
		t.Errorf("expected otel-metrics service edge, got %+v", e)
	}
}

func TestApply_HTTPClient_PublicIPReverseResolves(t *testing.T) {
	// Public egress IPs are kept (unlike private/infra IPs); a PTR record makes
	// them readable, and an unresolvable one still surfaces as the literal IP.
	stub := &stubClient{byQuery: map[string][]Sample{
		"http_client_request_body_size_bytes_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
				"server_address": "203.0.113.10:443", // has a PTR record below
			}},
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
				"server_address": "192.0.2.1:443", // public, but no PTR → kept as IP
			}},
		},
	}}
	o := overlayerWith(stub)
	o.lookupAddr = func(_ context.Context, ip string) ([]string, error) {
		if ip == "203.0.113.10" {
			return []string{"api.gateway.example.com."}, nil
		}
		return nil, nil
	}
	edges := o.Apply(context.Background(), sampleApps())

	targets := map[string]bool{}
	for _, e := range edges {
		if e.Target.External {
			targets[e.Target.Service] = true
		}
	}
	if !targets["api.gateway.example.com"] {
		t.Errorf("expected reverse-resolved PTR hostname, got edges %+v", edges)
	}
	if !targets["192.0.2.1"] {
		t.Errorf("expected unresolvable public IP kept as literal, got edges %+v", edges)
	}
}

func TestApply_DBClient_AttachesExternalDatabase(t *testing.T) {
	// Most databases here are EXTERNAL (RDS). Beyla's db_client metric gives the peer
	// address + engine but no port, so we attach broadly: a public RDS IP is reverse-
	// resolved to its endpoint name and the engine label is recorded as the type.
	stub := &stubClient{byQuery: map[string][]Sample{
		"db_client_operation_duration_seconds_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service": "beyla",
				"server_address": "203.0.113.55", "db_system_name": "postgresql",
			}},
		},
	}}
	o := overlayerWith(stub)
	o.lookupAddr = func(_ context.Context, ip string) ([]string, error) {
		if ip == "203.0.113.55" {
			return []string{"orders.abc123.eu-west-1.rds.example.com."}, nil
		}
		return nil, nil
	}
	apps := sampleApps()
	edges := o.Apply(context.Background(), apps)

	api := findApp(apps, "api")
	if len(api.Databases) != 1 || api.Databases[0].System != "postgresql" ||
		api.Databases[0].Name != "orders.abc123.eu-west-1.rds.example.com" {
		t.Fatalf("expected postgresql RDS database, got %+v", api.Databases)
	}
	if len(edges) != 1 || edges[0].Type != kindDatabase {
		t.Errorf("expected one database edge, got %+v", edges)
	}
}

func TestApply_DBClient_KeepsPrivateVPCDatabase(t *testing.T) {
	// A VPC-private RDS IP with an engine is a real database the cluster routes to;
	// it is kept (as the bare IP, since reverse DNS is skipped for private space).
	stub := &stubClient{byQuery: map[string][]Sample{
		"db_client_operation_duration_seconds_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service": "beyla",
				"server_address": "10.0.0.42", "db_system_name": "mysql",
			}},
		},
	}}
	apps := sampleApps()
	edges := overlayerWith(stub).Apply(context.Background(), apps)

	api := findApp(apps, "api")
	if len(api.Databases) != 1 || api.Databases[0].System != "mysql" || api.Databases[0].Name != "10.0.0.42" {
		t.Fatalf("expected mysql DB on the private VPC IP, got %+v", api.Databases)
	}
	if len(edges) != 1 || edges[0].Type != kindDatabase {
		t.Errorf("expected one database edge, got %+v", edges)
	}
}

func TestApply_DBClient_DropsUnidentifiableNoise(t *testing.T) {
	// A private/infra peer with NO engine and NO logical name is unattributable
	// noise (a resolver/proxy socket Beyla misread), not a database — drop it.
	stub := &stubClient{byQuery: map[string][]Sample{
		"db_client_operation_duration_seconds_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service": "beyla",
				"server_address": "10.0.0.7",
			}},
		},
	}}
	apps := sampleApps()
	edges := overlayerWith(stub).Apply(context.Background(), apps)

	if api := findApp(apps, "api"); len(api.Databases) != 0 {
		t.Fatalf("expected no database for unidentifiable noise, got %+v", api.Databases)
	}
	if len(edges) != 0 {
		t.Errorf("expected no edge for unidentifiable noise, got %+v", edges)
	}
}

func TestApply_DBClient_DropsPhantomWhenReachedOverHTTP(t *testing.T) {
	// Beyla's eBPF misdetects an HTTP/gRPC-on-443 stream as a db_client call and
	// stamps a bogus engine on it. Crucially, http_client reports the peer by
	// HOSTNAME while db_client reports the SAME peer by IP, so the join must bridge
	// hostname↔IP via forward DNS. The discriminator is PER WORKLOAD: `api` only
	// talks to the shared host over HTTP, so its db_client→postgresql is a phantom
	// and must be dropped — while `worker`, which reaches the SAME IP only over the
	// DB protocol, keeps its real postgres. (Mirrors an HTTP-API hostname that
	// shares a public IP with an unrelated RDS box.)
	const sharedHost = "logisticsapi.example.com"
	const sharedIP = "203.0.113.70" // documentation range; hostname resolves here
	stub := &stubClient{byQuery: map[string][]Sample{
		"http_client_request_body_size_bytes_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
				"server_address": sharedHost, "server_port": "443",
			}},
		},
		"db_client_operation_duration_seconds_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service": "beyla",
				"server_address": sharedIP, "db_system_name": "postgresql",
			}},
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-b", "k8s_deployment_name": "worker", "service": "beyla",
				"server_address": sharedIP, "db_system_name": "postgresql",
			}},
		},
	}}
	o := overlayerWith(stub)
	o.lookupHost = func(_ context.Context, host string) ([]string, error) {
		if host == sharedHost {
			return []string{sharedIP}, nil
		}
		return nil, nil
	}
	apps := sampleApps()
	edges := o.Apply(context.Background(), apps)

	if api := findApp(apps, "api"); len(api.Databases) != 0 {
		t.Fatalf("phantom DB should be dropped for the HTTP caller, got %+v", api.Databases)
	}
	worker := findApp(apps, "worker")
	if len(worker.Databases) != 1 || worker.Databases[0].System != "postgresql" ||
		worker.Databases[0].Name != sharedIP {
		t.Fatalf("real postgres should survive for the DB-only caller, got %+v", worker.Databases)
	}
	// One service edge (api's HTTP egress) + one database edge (worker's real DB);
	// the phantom draws no edge.
	byType := map[string]int{}
	for _, e := range edges {
		byType[e.Type]++
	}
	if byType[kindDatabase] != 1 || byType[kindService] != 1 || len(edges) != 2 {
		t.Errorf("expected 1 db + 1 service edge, got %d: %+v", len(edges), edges)
	}
}

func TestApply_DBClient_DropsOTLPCollectorPhantom(t *testing.T) {
	// Beyla's eBPF SQL autodetection stamps a (fabricated, always postgresql)
	// db_system on the gRPC/HTTP-2 stream a workload uses to EXPORT OTLP to its
	// otel-collector-service — surfacing the collector as a phantom database
	// fleet-wide. It must be dropped in EVERY address form Beyla emits (bare, and
	// name.namespace with or without .svc.cluster.local), while a genuine RDS in the
	// same batch survives. No real database is named this, so the drop is zero-risk.
	const rdsIP = "203.0.113.90" // documentation range; real DB for `worker`
	stub := &stubClient{byQuery: map[string][]Sample{
		"db_client_operation_duration_seconds_count": {
			// api → collector, three phantom forms:
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service": "beyla",
				"server_address": "otel-collector-service", "db_system_name": "postgresql",
			}},
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service": "beyla",
				"server_address": "otel-collector-service.monitoring-abc", "db_system_name": "postgresql",
			}},
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service": "beyla",
				"server_address": "otel-collector-service.monitoring-abc.svc.cluster.local", "db_system_name": "postgresql",
			}},
			// worker → a genuine RDS that must NOT be dropped.
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-b", "k8s_deployment_name": "worker", "service": "beyla",
				"server_address": rdsIP, "db_system_name": "postgresql",
			}},
		},
	}}
	o := overlayerWith(stub)
	o.lookupAddr = func(_ context.Context, ip string) ([]string, error) {
		if ip == rdsIP {
			return []string{"orders.abc123.eu-west-1.rds.example.com."}, nil
		}
		return nil, nil
	}
	apps := sampleApps()
	edges := o.Apply(context.Background(), apps)

	if api := findApp(apps, "api"); len(api.Databases) != 0 {
		t.Fatalf("all OTLP-collector phantoms should be dropped, got %+v", api.Databases)
	}
	worker := findApp(apps, "worker")
	if len(worker.Databases) != 1 || worker.Databases[0].System != "postgresql" ||
		worker.Databases[0].Name != "orders.abc123.eu-west-1.rds.example.com" {
		t.Fatalf("real RDS should survive, got %+v", worker.Databases)
	}
	// Only the real DB draws an edge; the three phantoms draw none.
	if len(edges) != 1 || edges[0].Type != kindDatabase {
		t.Errorf("expected exactly the real DB edge, got %d: %+v", len(edges), edges)
	}
}

func TestApply_DBClient_ResolvesEngineConflictByDominance(t *testing.T) {
	// A single peer is one server (one address → one engine), yet Beyla's eBPF SQL
	// autodetection fabricates a second engine on the opaque TLS stream to a genuine
	// database — here stamping the SAME RDS box as both postgresql and mysql. The real
	// engine carries continuous query traffic (high summed db_client rate); the
	// fabricated one appears only as sparse bursts. So the dominant engine wins per
	// (workload, peer) and the phantom is dropped — WITHOUT losing the peer. (Mirrors
	// ssu-mgmt → its Postgres RDS being intermittently mislabelled mysql.)
	const rdsIP = "203.0.113.55" // documentation range; one server, one real engine
	stub := &stubClient{byQuery: map[string][]Sample{
		"db_client_operation_duration_seconds_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service": "beyla",
				"server_address": rdsIP, "db_system_name": "postgresql",
			}, Value: 8}, // sustained real query traffic
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service": "beyla",
				"server_address": rdsIP, "db_system_name": "mysql",
			}, Value: 1}, // sparse fabricated burst
		},
	}}
	o := overlayerWith(stub)
	o.lookupAddr = func(_ context.Context, ip string) ([]string, error) {
		if ip == rdsIP {
			return []string{"orders.abc123.eu-west-1.rds.example.com."}, nil
		}
		return nil, nil
	}
	apps := sampleApps()
	edges := o.Apply(context.Background(), apps)

	api := findApp(apps, "api")
	if len(api.Databases) != 1 || api.Databases[0].System != "postgresql" ||
		api.Databases[0].Name != "orders.abc123.eu-west-1.rds.example.com" {
		t.Fatalf("dominant postgres should win, fabricated mysql dropped, got %+v", api.Databases)
	}
	// Both engine samples point at the same host, so they collapse to one DB edge.
	if len(edges) != 1 || edges[0].Type != kindDatabase {
		t.Errorf("expected exactly one database edge, got %d: %+v", len(edges), edges)
	}
}

func TestApply_DBClient_EngineConflictTieKeepsBoth(t *testing.T) {
	// When two engines on one peer carry EQUAL evidence (a genuine near-tie — e.g. a
	// low-traffic dev workload where both the real and the fabricated engine show a
	// single sparse sample), there is no basis to pick one, so both are kept.
	// Conservative on purpose: showing both beats silently guessing wrong.
	const peerIP = "203.0.113.60"
	stub := &stubClient{byQuery: map[string][]Sample{
		"db_client_operation_duration_seconds_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service": "beyla",
				"server_address": peerIP, "db_system_name": "postgresql",
			}, Value: 1},
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service": "beyla",
				"server_address": peerIP, "db_system_name": "mysql",
			}, Value: 1},
		},
	}}
	o := overlayerWith(stub)
	o.lookupAddr = func(_ context.Context, ip string) ([]string, error) {
		if ip == peerIP {
			return []string{"ambiguous.abc123.eu-west-1.rds.example.com."}, nil
		}
		return nil, nil
	}
	apps := sampleApps()
	o.Apply(context.Background(), apps)

	api := findApp(apps, "api")
	engines := map[string]bool{}
	for _, db := range api.Databases {
		engines[db.System] = true
	}
	if len(api.Databases) != 2 || !engines["postgresql"] || !engines["mysql"] {
		t.Fatalf("tie should keep both engines, got %+v", api.Databases)
	}
}

func TestIsOTLPCollectorPeer(t *testing.T) {
	cases := map[string]bool{
		"otel-collector-service":                                  true,
		"otel-collector-service.monitoring-abc":                   true,
		"otel-collector-service.monitoring-abc.svc.cluster.local": true,
		"otel-collector":                                          true,
		"opentelemetry-collector":                                 true,
		"OTEL-COLLECTOR-SERVICE":                                  true, // case-insensitive
		"orders.abc123.eu-west-1.rds.example.com":                 false,
		"collector-of-tolls":                                      false, // not an OTLP collector
		"203.0.113.90":                                            false,
		"":                                                        false,
	}
	for host, want := range cases {
		if got := isOTLPCollectorPeer(host); got != want {
			t.Errorf("isOTLPCollectorPeer(%q) = %v, want %v", host, got, want)
		}
	}
}

func TestApply_HTTPClient_DBPortInfersEngine(t *testing.T) {
	// DB detection is now purely port-based, off the http_client metric (the one that
	// carries server_port). A peer on 5432 is classified postgresql from the PORT —
	// never an engine label — and a PTR record names the endpoint behind the IP.
	stub := &stubClient{byQuery: map[string][]Sample{
		"http_client_request_body_size_bytes_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
				"server_address": "198.51.100.20:5432",
			}},
		},
	}}
	o := overlayerWith(stub)
	o.lookupAddr = func(_ context.Context, ip string) ([]string, error) {
		if ip == "198.51.100.20" {
			return []string{"db.example.rds.example.com."}, nil
		}
		return nil, nil
	}
	apps := sampleApps()
	edges := o.Apply(context.Background(), apps)

	api := findApp(apps, "api")
	if len(api.Databases) != 1 {
		t.Fatalf("expected 1 database, got %+v", api.Databases)
	}
	db := api.Databases[0]
	if db.System != "postgresql" || db.Name != "db.example.rds.example.com" {
		t.Errorf("expected postgresql engine (from port) + PTR name, got %+v", db)
	}
	if len(edges) != 1 || edges[0].Type != kindDatabase || edges[0].Target.Service != "db.example.rds.example.com" {
		t.Errorf("expected edge to the resolved DB host, got %+v", edges)
	}
}

func TestApply_HTTPClient_DBPortFromServerPortLabel(t *testing.T) {
	// The DB port arrives in the dedicated server_port label (revealed via Beyla's
	// attributes.select). A well-known DB port is the sole positive signal, so the
	// edge is a database and the engine is inferred from the port.
	stub := &stubClient{byQuery: map[string][]Sample{
		"http_client_request_body_size_bytes_count": {
			{Metric: map[string]string{
				"k8s_namespace_name": "cap-a", "k8s_deployment_name": "api", "service_name": "api",
				"server_address": "db.example.com", "server_port": "5432",
			}},
		},
	}}
	apps := sampleApps()
	edges := overlayerWith(stub).Apply(context.Background(), apps)

	api := findApp(apps, "api")
	if len(api.Databases) != 1 || api.Databases[0].System != "postgresql" || api.Databases[0].Name != "db.example.com" {
		t.Fatalf("expected postgresql DB on db.example.com, got %+v", api.Databases)
	}
	if len(edges) != 1 || edges[0].Type != kindDatabase || edges[0].Target.Service != "db.example.com" {
		t.Errorf("expected 1 database edge to db.example.com, got %+v", edges)
	}
}

func TestIsPrivateIP(t *testing.T) {
	cases := map[string]bool{
		"10.0.0.53":     true,  // RFC1918
		"172.16.4.4":    true,  // RFC1918
		"192.168.1.1":   true,  // RFC1918
		"127.0.0.1":     true,  // loopback
		"169.254.10.10": true,  // link-local
		"100.100.0.1":   true,  // RFC6598 CGNAT
		"203.0.113.10":  false, // public (TEST-NET-3)
		"198.51.100.20": false, // public (TEST-NET-2)
		"not-an-ip":     false, // hostname, not an IP literal
	}
	for host, want := range cases {
		if got := isPrivateIP(host); got != want {
			t.Errorf("isPrivateIP(%q) = %v, want %v", host, got, want)
		}
	}
}

func TestPortOf(t *testing.T) {
	cases := map[string]string{
		"198.51.100.20:5432":         "5432",
		"api.example.com:443":        "443",
		"198.51.100.20":              "",
		"[2001:db8::1]:443":          "443",
		"host-without-numeric:https": "",
	}
	for addr, want := range cases {
		if got := portOf(addr); got != want {
			t.Errorf("portOf(%q) = %q, want %q", addr, got, want)
		}
	}
}

func TestApply_ServiceGraph_DropsEgressBucket(t *testing.T) {
	// Beyla's synthetic "outgoing"/"incoming" buckets carry no real destination
	// and are dropped — only the genuine in-cluster edge survives.
	stub := &stubClient{byQuery: map[string][]Sample{
		"traces_service_graph_request_total": {
			{Metric: map[string]string{"client": "api", "client_k8s_namespace_name": "cap-a", "server": "outgoing"}},
			{Metric: map[string]string{"client": "incoming", "server": "api", "server_k8s_namespace_name": "cap-a"}},
			{Metric: map[string]string{
				"client": "api", "client_k8s_namespace_name": "cap-a",
				"server": "worker", "server_k8s_namespace_name": "cap-b",
			}},
		},
	}}
	apps := sampleApps()
	edges := overlayerWith(stub).Apply(context.Background(), apps)

	if len(edges) != 1 {
		t.Fatalf("expected only the real service edge, got %d: %+v", len(edges), edges)
	}
	if edges[0].Target.Service != "worker" || edges[0].Source.Service != "api" {
		t.Errorf("expected api → worker edge, got %+v", edges[0])
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
