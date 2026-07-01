package telemetry

import (
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

// resolveTarget resolves an identifier and classifies what kind of target it is.
func (r *resolver) resolve(identifier string) (model.DependencyNode, string) {
	id := strings.TrimSpace(identifier)
	if id == "" {
		return model.DependencyNode{External: true}, kindExternal
	}

	// Drop a trailing :port, remembering it for bare-host classification.
	host, port := id, ""
	if i := strings.LastIndex(id, ":"); i != -1 {
		if _, err := strconv.Atoi(id[i+1:]); err == nil {
			host, port = id[:i], id[i+1:]
		}
	}

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
