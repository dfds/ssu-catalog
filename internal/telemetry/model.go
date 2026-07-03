package telemetry

import (
	"net"
	"strconv"
	"strings"

	"go.dfds.cloud/ssu-catalog/internal/model"
)

// Sample is one resolved series from a Mimir instant query.
type Sample struct {
	Metric map[string]string
	Value  float64
}

// promResponse decodes the Prometheus/Mimir HTTP query API response.
type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
		} `json:"result"`
	} `json:"data"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
}

const svcSuffix = ".svc.cluster.local"

// Target kinds an identifier can resolve to.
const (
	kindService  = "service"
	kindDatabase = "database"
	kindKafka    = "kafka"
	kindExternal = "external"
)

// wellKnownPorts maps a bare port to a database/messaging system.
var wellKnownPorts = map[string]string{
	"5432":  "postgresql",
	"3306":  "mysql",
	"6379":  "redis",
	"27017": "mongodb",
	"1433":  "mssql",
	"9200":  "elasticsearch",
	"9092":  "kafka",
	"5672":  "rabbitmq",
}

// databaseSystems and messagingSystems are generic data-system strings that may
// appear unnormalized as a service-graph `server`.
var databaseSystems = map[string]struct{}{
	"postgresql": {}, "postgres": {}, "mysql": {}, "mariadb": {}, "redis": {},
	"mongodb": {}, "mssql": {}, "sqlserver": {}, "cassandra": {}, "elasticsearch": {},
	"dynamodb": {}, "cosmosdb": {}, "oracle": {},
}

var messagingSystems = map[string]struct{}{
	"kafka": {}, "rabbitmq": {}, "amqp": {}, "servicebus": {},
	"sqs": {}, "sns": {}, "eventhub": {}, "nservicebus": {},
}

// resolver maps unnormalized service-graph identifiers back to in-cluster
// applications, degrading gracefully to External nodes. It deliberately does no
// fuzzy suffix-stripping that would overstate confidence.
type resolver struct {
	cluster string
	// services indexes "namespace/serviceName" and "namespace/appName" → appName.
	services map[string]string
}

func newResolver(cluster string, apps []model.ApplicationEntry) *resolver {
	r := &resolver{cluster: cluster, services: map[string]string{}}
	for i := range apps {
		app := &apps[i]
		r.services[app.Namespace+"/"+app.Name] = app.Name
		for _, svc := range app.Services {
			r.services[app.Namespace+"/"+svc.Name] = app.Name
		}
	}
	return r
}

// resolveNode resolves an identifier to a dependency node (used for the client
// side of an edge, where DB/Kafka classification is irrelevant).
func (r *resolver) resolveNode(identifier string) model.DependencyNode {
	node, _ := r.resolve(identifier)
	return node
}

// resolveEndpoint resolves a telemetry endpoint given the raw OTel service.name
// (a service-graph client/server, or a db/messaging `service`) plus Beyla's
// explicit k8s namespace label when present.
//
// Beyla emits the BARE service.name and the k8s namespace as a SEPARATE label
// (k8s.namespace.name) — it never uses the name.namespace DNS form the plain
// splitClusterDNS path expects. So when we have the namespace label it is the
// authoritative join key: match (namespace, name) directly. We deliberately do
// NOT fall back to Beyla's service.namespace attribute — that carries the
// LOGICAL capability (e.g. "ssu") rather than the real k8s namespace (e.g.
// "selfservice"), so it would mis-join. Absent a namespace label (uninstrumented
// endpoints, or Tempo-sourced series that only give an FQDN), defer to resolve,
// which still handles genuine name.namespace[.svc.cluster.local] identifiers.
func (r *resolver) resolveEndpoint(name, k8sNamespace string) (model.DependencyNode, string) {
	if k8sNamespace != "" {
		if app, found := r.services[k8sNamespace+"/"+name]; found {
			return model.DependencyNode{
				Cluster:   r.cluster,
				Namespace: k8sNamespace,
				Service:   app,
			}, kindService
		}
	}
	return r.resolve(name)
}

// resolveEndpointNode is the node-only variant of resolveEndpoint.
func (r *resolver) resolveEndpointNode(name, k8sNamespace string) model.DependencyNode {
	node, _ := r.resolveEndpoint(name, k8sNamespace)
	return node
}

// resolveClient resolves the source workload of an OTel/Beyla *_client_* metric.
// Beyla tags the instrumented workload with k8s_deployment_name +
// k8s_namespace_name (and service_name = the OTel service.name); Tempo/OTLP
// series may instead carry only `service` in name.namespace form. Prefer the
// authoritative k8s labels, then fall back to `service`.
func (r *resolver) resolveClient(m map[string]string) model.DependencyNode {
	ns := m["k8s_namespace_name"]
	name := firstNonEmpty(
		m["k8s_deployment_name"],
		m["k8s_statefulset_name"],
		m["k8s_daemonset_name"],
		m["service_name"],
	)
	if name != "" {
		return r.resolveEndpointNode(name, ns)
	}
	return r.resolveEndpointNode(m["service"], ns)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// stripPort removes a trailing :port from a host[:port], leaving bare hosts (and
// values with a non-numeric suffix) untouched. It is IPv6-aware: a bracketed
// literal ("[::1]:5432" or "[::1]") yields the inner address, and a bare IPv6
// literal (two or more colons, unbracketed — e.g. "::1") has no separable port
// and is returned intact. Without this, LastIndex(":") on "::1" would strip the
// final octet and leave ":" — a garbage host that surfaces as a phantom node.
func stripPort(addr string) string {
	host, _ := splitHostPort(addr)
	return host
}

// isBareIP reports whether host is a raw IP literal (no hostname).
func isBareIP(host string) bool {
	return net.ParseIP(host) != nil
}

// isPrivateIP reports whether host is a non-routable IP — RFC1918 private space,
// loopback, link-local, the unspecified address, or RFC6598 shared CGNAT space
// (100.64.0.0/10, where EKS pod IPs commonly live). These are in-cluster/infra
// peers (kube API server, node/pod IPs) with no useful external identity, so the
// egress overlays drop them; public IPs are kept (and reverse-resolved) instead.
func isPrivateIP(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}
	if ip4 := ip.To4(); ip4 != nil {
		return ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127
	}
	return false
}

// portOf returns the trailing numeric :port of a host[:port], or "" when absent.
// Like stripPort it is IPv6-aware: a bare IPv6 literal has no port to return.
func portOf(addr string) string {
	_, port := splitHostPort(addr)
	return port
}

// splitHostPort separates a host[:port] into (host, port), returning an empty
// port when none is present. It handles three shapes:
//   - bracketed IPv6, with or without a port: "[::1]:5432" / "[::1]"
//   - bare IPv6 literals (2+ unbracketed colons, e.g. "::1"): no separable port
//   - host:port / IPv4:port with a trailing numeric port
//
// A non-numeric trailing segment (e.g. ".svc.cluster.local") is treated as part
// of the host, not a port.
func splitHostPort(addr string) (host, port string) {
	if addr == "" {
		return "", ""
	}
	if strings.HasPrefix(addr, "[") {
		if i := strings.LastIndex(addr, "]"); i != -1 {
			host = addr[1:i]
			if rest := addr[i+1:]; strings.HasPrefix(rest, ":") {
				if _, err := strconv.Atoi(rest[1:]); err == nil {
					port = rest[1:]
				}
			}
			return host, port
		}
		return addr, ""
	}
	// Bare IPv6 literal — two or more colons and no port delimiter to strip.
	if strings.Count(addr, ":") > 1 {
		return addr, ""
	}
	if i := strings.LastIndex(addr, ":"); i != -1 {
		if _, err := strconv.Atoi(addr[i+1:]); err == nil {
			return addr[:i], addr[i+1:]
		}
	}
	return addr, ""
}

// peerPort returns the destination port for a client-metric sample. Beyla emits
// it in the dedicated server_port label (once revealed via attributes.select);
// older data may instead have it embedded in server_address, so fall back to that.
func peerPort(m map[string]string, addr string) string {
	if p := m["server_port"]; p != "" {
		return p
	}
	return portOf(addr)
}

// joinHostPort re-attaches a port to a host for classification, returning the bare
// host when port is empty. IPv6 hosts are bracketed so splitHostPort can separate
// them again ("[::1]:5432" rather than an ambiguous "::1:5432").
func joinHostPort(host, port string) string {
	if port == "" {
		return host
	}
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	return host + ":" + port
}

// beylaEgressBucket reports whether a service-graph endpoint is Beyla's synthetic
// catch-all for un-attributed egress/ingress ("outgoing"/"incoming"). These carry
// no real destination, so they are dropped in favour of the resolved
// server_address the HTTP-client overlay provides.
func beylaEgressBucket(name string) bool {
	return name == "outgoing" || name == "incoming"
}

// resolveTarget resolves an identifier and classifies what kind of target it is.
func (r *resolver) resolve(identifier string) (model.DependencyNode, string) {
	id := strings.TrimSpace(identifier)
	if id == "" {
		return model.DependencyNode{External: true}, kindExternal
	}

	// Drop a trailing :port, remembering it for bare-host classification.
	host, port := splitHostPort(id)

	// 1. In-cluster DNS: {name}.{namespace}[.svc.cluster.local].
	if name, ns, ok := splitClusterDNS(host); ok {
		if app, found := r.services[ns+"/"+name]; found {
			return model.DependencyNode{
				Cluster:   r.cluster,
				Namespace: ns,
				Service:   app,
			}, kindService
		}
	}

	lower := strings.ToLower(host)

	// 2. Bare port (e.g. "5432") → infer a database/messaging system.
	if _, err := strconv.Atoi(host); err == nil {
		if sys, ok := wellKnownPorts[host]; ok {
			return externalNode(sys), systemKind(sys)
		}
		return model.DependencyNode{Service: host, External: true}, kindExternal
	}
	// A known host carrying a well-known port also classifies as that system.
	if port != "" {
		if sys, ok := wellKnownPorts[port]; ok {
			return externalNode(lower), systemKind(sys)
		}
	}

	// 3. Generic data-system strings.
	if _, ok := databaseSystems[lower]; ok {
		return externalNode(lower), kindDatabase
	}
	if _, ok := messagingSystems[lower]; ok {
		return externalNode(lower), kindKafka
	}

	// 4. Anything else → external, unclassified.
	return model.DependencyNode{Service: host, External: true}, kindExternal
}

func externalNode(service string) model.DependencyNode {
	return model.DependencyNode{Service: service, External: true}
}

// systemKind classifies a well-known system string as database or kafka.
func systemKind(sys string) string {
	if _, ok := messagingSystems[sys]; ok {
		return kindKafka
	}
	return kindDatabase
}

// splitClusterDNS extracts (name, namespace) from a cluster-local service name.
// It accepts both the FQDN form and the short {name}.{namespace} form.
func splitClusterDNS(host string) (string, string, bool) {
	host = strings.TrimSuffix(host, svcSuffix)
	parts := strings.Split(host, ".")
	if len(parts) < 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}
