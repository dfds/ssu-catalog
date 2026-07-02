package collector

import (
	"context"
	"sort"
	"strings"
	"time"

	"go.dfds.cloud/ssu-catalog/internal/gitops"
	"go.dfds.cloud/ssu-catalog/internal/kubernetes"
	"go.dfds.cloud/ssu-catalog/internal/model"
	"go.dfds.cloud/ssu-catalog/internal/swagger"
	"go.dfds.cloud/ssu-catalog/internal/telemetry"
	"go.uber.org/zap"
)

// Collector orchestrates the configured sources into a model.Catalog.
type Collector struct {
	cluster   string
	k8sClient kubernetes.ResourceLister
	prober    *swagger.Prober      // nil when swagger probing is disabled
	overlayer *telemetry.Overlayer // nil when telemetry overlay is disabled
	logger    *zap.Logger
}

// NewCollector builds a Collector for a given cluster. A nil prober disables
// OpenAPI/Swagger probing; a nil overlayer disables the telemetry overlay.
func NewCollector(cluster string, k8sClient kubernetes.ResourceLister, prober *swagger.Prober, overlayer *telemetry.Overlayer, logger *zap.Logger) *Collector {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Collector{cluster: cluster, k8sClient: k8sClient, prober: prober, overlayer: overlayer, logger: logger}
}

// Collect runs one full collection cycle and returns the assembled catalog.
func (c *Collector) Collect(ctx context.Context) (*model.Catalog, error) {
	started := time.Now()

	snapshot, err := c.k8sClient.ListResources(ctx)
	if err != nil {
		return nil, err
	}

	c.logger.Info("scanned cluster",
		zap.String("cluster", c.cluster),
		zap.Int("namespaces", len(snapshot.Namespaces)),
		zap.Int("workloads", len(snapshot.Workloads)),
		zap.Int("services", len(snapshot.Services)),
	)

	namespaces := c.buildNamespaces(snapshot)
	capabilityByNamespace := make(map[string]string, len(namespaces))
	for _, ns := range namespaces {
		capabilityByNamespace[ns.Name] = ns.CapabilityID
	}

	resolver := gitops.NewResolver(snapshot.GitOps)
	applications := c.buildApplications(snapshot, capabilityByNamespace, resolver)

	// OpenAPI/Swagger probing — actively connects to service ports, filling
	// ServiceRef.APIDocs in place before stats count documented applications.
	if c.prober != nil {
		probes, hits := c.prober.Probe(ctx, applications)
		c.logger.Info("swagger probing complete",
			zap.Int("probes", probes),
			zap.Int("hits", hits),
		)
	}

	// Telemetry overlay — best-effort runtime dependency edges + observed
	// Databases/KafkaTopics. Never invalidates the catalog on query failure.
	dependencies := []model.DependencyEdge{}
	if c.overlayer != nil {
		dependencies = c.overlayer.Apply(ctx, applications)
		c.logger.Info("telemetry overlay complete", zap.Int("dependencies", len(dependencies)))
	}

	// NetworkPolicy egress — declared (not observed) dependency intent. Appended
	// as a distinct edge type/origin so it never collides with telemetry edges.
	netpolEdges := c.buildNetworkPolicyEdges(snapshot)
	if len(netpolEdges) > 0 {
		dependencies = append(dependencies, netpolEdges...)
		c.logger.Info("network policy edges added", zap.Int("edges", len(netpolEdges)))
	}

	catalog := &model.Catalog{
		Cluster:      c.cluster,
		Applications: applications,
		Namespaces:   namespaces,
		Dependencies: dependencies,
		CollectedAt:  started,
	}
	catalog.Stats = computeStats(catalog, time.Since(started))

	return catalog, nil
}

func (c *Collector) buildNamespaces(snapshot *kubernetes.K8sSnapshot) []model.NamespaceEntry {
	entries := make([]model.NamespaceEntry, 0, len(snapshot.Namespaces))
	for _, ns := range snapshot.Namespaces {
		entries = append(entries, model.NamespaceEntry{
			Cluster:      c.cluster,
			Name:         ns.Name,
			CapabilityID: ns.CapabilityID,
			AWSAccountID: ns.AWSAccountID,
			ContextID:    ns.ContextID,
			CostCentre:   ns.CostCentre,
			Labels:       ns.Labels,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries
}

// buildApplications turns workloads into application entries, attaches matching
// Services, and emits orphan Services (no backing workload) as Kind:"Service".
func (c *Collector) buildApplications(
	snapshot *kubernetes.K8sSnapshot,
	capabilityByNamespace map[string]string,
	resolver *gitops.Resolver,
) []model.ApplicationEntry {
	// Index ingress external hosts by (namespace, service).
	hostsByService := make(map[string][]string)
	for _, ing := range snapshot.Ingresses {
		for svc, hosts := range ing.ServiceHosts {
			key := ing.Namespace + "/" + svc
			for _, h := range hosts {
				hostsByService[key] = appendUnique(hostsByService[key], h)
			}
		}
	}

	// Index Traefik IngressRoute hosts and route detail by (namespace, service).
	// Hosts merge into the same ExternalHosts surface as Ingress; the richer
	// per-route detail is attached separately as ServiceRef.Routes.
	routesByService := make(map[string][]model.RouteRef)
	for _, ir := range snapshot.IngressRoutes {
		for _, rule := range ir.Routes {
			for _, svc := range rule.Services {
				key := ir.Namespace + "/" + svc
				for _, h := range rule.Hosts {
					hostsByService[key] = appendUnique(hostsByService[key], h)
				}
				routesByService[key] = appendRouteUnique(routesByService[key], model.RouteRef{
					Name:         ir.Name,
					Kind:         "IngressRoute",
					Hosts:        rule.Hosts,
					PathPrefixes: rule.PathPrefixes,
					EntryPoints:  ir.EntryPoints,
					TLS:          ir.TLS,
				})
			}
		}
	}

	// Index services by namespace for selector matching, tracking which are claimed.
	type svcRecord struct {
		info    kubernetes.ServiceInfo
		claimed bool
	}
	servicesByNamespace := make(map[string][]*svcRecord)
	for _, svc := range snapshot.Services {
		rec := &svcRecord{info: svc}
		servicesByNamespace[svc.Namespace] = append(servicesByNamespace[svc.Namespace], rec)
	}

	apps := make([]model.ApplicationEntry, 0, len(snapshot.Workloads))

	for _, w := range snapshot.Workloads {
		app := model.ApplicationEntry{
			Cluster:       c.cluster,
			Namespace:     w.Namespace,
			Name:          w.Name,
			Kind:          w.Kind,
			CapabilityID:  capabilityByNamespace[w.Namespace],
			Replicas:      w.Replicas,
			ReadyReplicas: w.ReadyReplicas,
			Containers:    containersFrom(w.Containers),
			Labels:        w.Labels,
			Annotations:   w.Annotations,
			KafkaTopics:   []model.KafkaTopicRef{},
			Databases:     []model.DatabaseRef{},
		}

		// Match services whose selector targets this workload's pod labels.
		podLabels := w.PodLabels
		if podLabels == nil {
			podLabels = w.Selector
		}
		var services []model.ServiceRef
		for _, rec := range servicesByNamespace[w.Namespace] {
			if len(rec.info.Selector) == 0 {
				continue
			}
			if selectorMatches(rec.info.Selector, podLabels) {
				rec.claimed = true
				services = append(services, serviceRefFrom(rec.info, hostsByService, routesByService))
			}
		}
		app.Services = services

		// GitOps attribution — repo + deployment source from tracking metadata.
		source, repoURL := resolver.Resolve(w.Namespace, w.Labels, w.Annotations)
		app.DeploymentSource = source
		app.RepoURL = repoURL

		apps = append(apps, app)
	}

	// Orphan services — no workload claimed them — become standalone entries so
	// background-exposed surfaces still appear in the catalog.
	for ns, recs := range servicesByNamespace {
		for _, rec := range recs {
			if rec.claimed {
				continue
			}
			apps = append(apps, model.ApplicationEntry{
				Cluster:      c.cluster,
				Namespace:    ns,
				Name:         rec.info.Name,
				Kind:         "Service",
				CapabilityID: capabilityByNamespace[ns],
				Services:     []model.ServiceRef{serviceRefFrom(rec.info, hostsByService, routesByService)},
				Containers:   []model.ContainerInfo{},
				KafkaTopics:  []model.KafkaTopicRef{},
				Databases:    []model.DatabaseRef{},
			})
		}
	}

	sort.Slice(apps, func(i, j int) bool {
		if apps[i].Namespace != apps[j].Namespace {
			return apps[i].Namespace < apps[j].Namespace
		}
		return apps[i].Name < apps[j].Name
	})
	return apps
}

func containersFrom(specs []kubernetes.ContainerSpec) []model.ContainerInfo {
	out := make([]model.ContainerInfo, 0, len(specs))
	for _, s := range specs {
		image, tag := splitImageTag(s.Image)
		ports := make([]model.ContainerPort, 0, len(s.Ports))
		for _, p := range s.Ports {
			ports = append(ports, model.ContainerPort{
				Name:          p.Name,
				ContainerPort: p.ContainerPort,
				Protocol:      p.Protocol,
			})
		}
		out = append(out, model.ContainerInfo{
			Name:     s.Name,
			Image:    image,
			ImageTag: tag,
			Ports:    ports,
			Resources: model.ResourceInfo{
				RequestsCPU:    s.RequestsCPU,
				RequestsMemory: s.RequestsMemory,
				LimitsCPU:      s.LimitsCPU,
				LimitsMemory:   s.LimitsMemory,
			},
		})
	}
	return out
}

func serviceRefFrom(svc kubernetes.ServiceInfo, hostsByService map[string][]string, routesByService map[string][]model.RouteRef) model.ServiceRef {
	ports := make([]model.ServicePort, 0, len(svc.Ports))
	for _, p := range svc.Ports {
		ports = append(ports, model.ServicePort{
			Name:       p.Name,
			Port:       p.Port,
			TargetPort: p.TargetPort,
			Protocol:   p.Protocol,
		})
	}
	routes := routesByService[svc.Namespace+"/"+svc.Name]
	if routes == nil {
		routes = []model.RouteRef{}
	}
	return model.ServiceRef{
		Name:          svc.Name,
		Type:          svc.Type,
		ClusterIP:     svc.ClusterIP,
		Ports:         ports,
		ExternalHosts: hostsByService[svc.Namespace+"/"+svc.Name],
		Routes:        routes,
		APIDocs:       []model.APIDocInfo{},
	}
}

// appendRouteUnique appends a RouteRef unless an equivalent one (same name +
// hosts + path prefixes) is already present.
func appendRouteUnique(routes []model.RouteRef, r model.RouteRef) []model.RouteRef {
	key := r.Name + "|" + strings.Join(r.Hosts, ",") + "|" + strings.Join(r.PathPrefixes, ",")
	for _, existing := range routes {
		ek := existing.Name + "|" + strings.Join(existing.Hosts, ",") + "|" + strings.Join(existing.PathPrefixes, ",")
		if ek == key {
			return routes
		}
	}
	return append(routes, r)
}

// buildNetworkPolicyEdges derives declared dependency edges from NetworkPolicy
// egress rules: source = each workload the policy's PodSelector matches in its
// namespace; target = each egress destination (best-effort, mostly external/raw
// identifiers). These are declared intent, not observed traffic, hence the
// dedicated "network_policy" type/origin.
func (c *Collector) buildNetworkPolicyEdges(snapshot *kubernetes.K8sSnapshot) []model.DependencyEdge {
	if len(snapshot.NetPolicies) == 0 {
		return nil
	}

	workloadsByNamespace := make(map[string][]kubernetes.WorkloadInfo)
	for _, w := range snapshot.Workloads {
		workloadsByNamespace[w.Namespace] = append(workloadsByNamespace[w.Namespace], w)
	}

	var edges []model.DependencyEdge
	seen := make(map[string]struct{})
	for _, np := range snapshot.NetPolicies {
		if len(np.EgressTargets) == 0 {
			continue
		}
		for _, w := range workloadsByNamespace[np.Namespace] {
			if !podSelectorMatches(np.PodSelector, w.PodLabels) {
				continue
			}
			source := model.DependencyNode{Cluster: c.cluster, Namespace: np.Namespace, Service: w.Name}
			for _, target := range np.EgressTargets {
				key := np.Namespace + "/" + w.Name + "|" + target
				if _, dup := seen[key]; dup {
					continue
				}
				seen[key] = struct{}{}
				edges = append(edges, model.DependencyEdge{
					Source:  source,
					Target:  model.DependencyNode{Service: target, External: true},
					Type:    "network_policy",
					Origin:  "network_policy",
					Details: np.Name,
				})
			}
		}
	}
	return edges
}

// podSelectorMatches applies NetworkPolicy podSelector semantics: an empty
// selector matches every pod in the namespace.
func podSelectorMatches(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return true
	}
	return selectorMatches(selector, labels)
}

// selectorMatches reports whether every key/value in selector is present in
// labels (standard K8s service-selector semantics).
func selectorMatches(selector, labels map[string]string) bool {
	if len(selector) == 0 || len(labels) == 0 {
		return false
	}
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// splitImageTag separates an image reference into repository and tag. A digest
// reference (image@sha256:...) keeps the digest as the tag.
func splitImageTag(image string) (string, string) {
	if image == "" {
		return "", ""
	}
	if at := strings.Index(image, "@"); at != -1 {
		return image[:at], image[at+1:]
	}
	// Only treat the last ":" as a tag separator when it isn't part of a
	// registry host:port (which always contains a "/" after the colon).
	if colon := strings.LastIndex(image, ":"); colon != -1 && !strings.Contains(image[colon:], "/") {
		return image[:colon], image[colon+1:]
	}
	return image, ""
}

func computeStats(catalog *model.Catalog, duration time.Duration) model.CatalogStats {
	stats := model.CatalogStats{
		TotalApplications:    len(catalog.Applications),
		TotalDependencies:    len(catalog.Dependencies),
		CollectionDurationMs: duration.Milliseconds(),
	}
	for _, app := range catalog.Applications {
		if app.CapabilityID != "" {
			stats.CapabilityOwnedApplications++
		}
		if app.DeploymentSource != nil {
			stats.ApplicationsWithDeploySource++
		}
		if applicationHasDocs(app) {
			stats.ApplicationsWithDocs++
		}
	}
	return stats
}

func applicationHasDocs(app model.ApplicationEntry) bool {
	for _, svc := range app.Services {
		if len(svc.APIDocs) > 0 {
			return true
		}
	}
	return false
}

func appendUnique(slice []string, value string) []string {
	for _, existing := range slice {
		if existing == value {
			return slice
		}
	}
	return append(slice, value)
}
