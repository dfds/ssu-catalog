package model

import "time"

// Catalog is the per-cluster catalog snapshot, rebuilt each collection cycle.
type Catalog struct {
	Cluster      string             `json:"cluster"`
	Applications []ApplicationEntry `json:"applications"` // primary, portal-facing entity
	Namespaces   []NamespaceEntry   `json:"namespaces"`
	Dependencies []DependencyEdge   `json:"dependencies"` // best-effort telemetry/NetworkPolicy overlay
	CollectedAt  time.Time          `json:"collectedAt"`
	Stats        CatalogStats       `json:"stats"`
}

type CatalogStats struct {
	TotalApplications            int   `json:"totalApplications"`
	CapabilityOwnedApplications  int   `json:"capabilityOwnedApplications"`
	ApplicationsWithDocs         int   `json:"applicationsWithDocs"`
	ApplicationsWithDeploySource int   `json:"applicationsWithDeploySource"`
	TotalDependencies            int   `json:"totalDependencies"`
	CollectionDurationMs         int64 `json:"collectionDurationMs"`
}

// NamespaceEntry maps a K8s namespace to a DFDS Capability (when labeled).
type NamespaceEntry struct {
	Cluster      string            `json:"cluster"`
	Name         string            `json:"name"`
	CapabilityID string            `json:"capabilityId"` // dfds.cloud/capability == namespace name; "" when unlabeled
	AWSAccountID string            `json:"awsAccountId"` // dfds.cloud/aws-account
	ContextID    string            `json:"contextId"`    // dfds.cloud/context-id
	CostCentre   string            `json:"costCentre"`   // dfds.cloud/cost-centre
	Labels       map[string]string `json:"labels"`
}

// ApplicationEntry is a workload (Deployment/StatefulSet/DaemonSet) plus its
// matched K8s Service(s). The portal-facing primary entity.
type ApplicationEntry struct {
	// Identity / join keys
	Cluster      string `json:"cluster"`
	Namespace    string `json:"namespace"`
	Name         string `json:"name"`         // workload name
	Kind         string `json:"kind"`         // Deployment | StatefulSet | DaemonSet | Service (orphan)
	CapabilityID string `json:"capabilityId"` // "" when namespace not capability-owned

	// Workload runtime
	Replicas      int32           `json:"replicas"`
	ReadyReplicas int32           `json:"readyReplicas"`
	Containers    []ContainerInfo `json:"containers"` // image + tag + ports + resources

	// Deployment / repository (GitOps-derived; see GitOps Source Discovery)
	RepoURL          string            `json:"repoUrl"`          // DeploymentSource.RepoURL, else label/annotation fallback
	DeploymentSource *DeploymentSource `json:"deploymentSource"` // nil when not GitOps-managed

	// Best-effort owner (authoritative values joined SSU-side; may be empty)
	Owner   string `json:"owner"`
	Contact string `json:"contact"`

	// Attached networking / API surface
	Services []ServiceRef `json:"services"` // matched K8s Services; empty for background consumers

	// Observed runtime overlay (best-effort; sparse until OTel adoption grows)
	KafkaTopics []KafkaTopicRef `json:"kafkaTopics"`
	Databases   []DatabaseRef   `json:"databases"`

	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

// ServiceRef is a K8s Service attached to an application.
type ServiceRef struct {
	Name          string        `json:"name"`
	Type          string        `json:"type"` // ClusterIP, NodePort, LoadBalancer, ExternalName
	ClusterIP     string        `json:"clusterIP"`
	Ports         []ServicePort `json:"ports"`
	ExternalHosts []string      `json:"externalHosts"` // from Ingress + Traefik IngressRoute rules
	Routes        []RouteRef    `json:"routes"`        // Traefik IngressRoute detail (name/paths/entrypoints/TLS)
	APIDocs       []APIDocInfo  `json:"apiDocs"`       // every hit (no short-circuit)
}

// RouteRef is a Traefik IngressRoute that routes external traffic to a Service.
type RouteRef struct {
	Name         string   `json:"name"`         // IngressRoute metadata.name
	Kind         string   `json:"kind"`         // "IngressRoute"
	Hosts        []string `json:"hosts"`        // literal Host(`…`) matchers
	PathPrefixes []string `json:"pathPrefixes"` // literal PathPrefix(`…`) / Path(`…`) matchers
	EntryPoints  []string `json:"entryPoints"`  // spec.entryPoints
	TLS          bool     `json:"tls"`          // spec.tls present
}

type DeploymentSource struct {
	Tool     string `json:"tool"`     // "argocd" | "flux-helm" | "flux-kustomize" | ""
	RepoURL  string `json:"repoUrl"`  // e.g. https://github.com/dfds/ssu-apps
	Path     string `json:"path"`     // manifest path within repo
	Revision string `json:"revision"` // targetRevision / branch
	AppName  string `json:"appName"`  // Argo Application / Flux HelmRelease / Kustomization name
}

type ContainerInfo struct {
	Name      string          `json:"name"`
	Image     string          `json:"image"`
	ImageTag  string          `json:"imageTag"`
	Ports     []ContainerPort `json:"ports"`
	Resources ResourceInfo    `json:"resources"`
}

type ContainerPort struct {
	Name          string `json:"name"`
	ContainerPort int32  `json:"containerPort"`
	Protocol      string `json:"protocol"`
}

type ResourceInfo struct {
	RequestsCPU    string `json:"requestsCpu"`
	RequestsMemory string `json:"requestsMemory"`
	LimitsCPU      string `json:"limitsCpu"`
	LimitsMemory   string `json:"limitsMemory"`
}

type ServicePort struct {
	Name       string `json:"name"`
	Port       int32  `json:"port"`
	TargetPort string `json:"targetPort"`
	Protocol   string `json:"protocol"`
}

type APIDocInfo struct {
	Port                int32  `json:"port"` // which port the doc was found on
	Path                string `json:"path"` // e.g. "/swagger/v1/swagger.json"
	URL                 string `json:"url"`  // full in-cluster URL
	ExternallyAvailable bool   `json:"externallyAvailable"`
	ExternalURL         string `json:"externalUrl"` // reachable https URL, "" when internal-only
}

type KafkaTopicRef struct {
	Name      string `json:"name"`      // messaging.destination.name
	Direction string `json:"direction"` // "produce" | "consume"
	Source    string `json:"source"`    // "otel-servicegraph" | "otel-metrics"
}

type DatabaseRef struct {
	System string `json:"system"` // postgresql, redis, mysql...
	Name   string `json:"name"`   // db.name (or "" when only a port was observed)
	Source string `json:"source"` // "otel-servicegraph" | "otel-metrics"
}

// DependencyEdge is a directed runtime dependency (best-effort overlay).
type DependencyEdge struct {
	Source  DependencyNode `json:"source"`
	Target  DependencyNode `json:"target"`
	Type    string         `json:"type"`   // "service" | "database" | "kafka" | "network_policy"
	Origin  string         `json:"origin"` // "otel-servicegraph" | "otel-metrics" | "network_policy"
	Details string         `json:"details"`
}

type DependencyNode struct {
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
	Service   string `json:"service"`  // resolved app/service name, or raw identifier when unresolved
	External  bool   `json:"external"` // true when it couldn't be resolved to an in-cluster app
}
