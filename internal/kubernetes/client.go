package kubernetes

import (
	"context"
	"fmt"
	"strings"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"golang.org/x/sync/errgroup"
)

// ResourceLister is the interface the collector depends on. Implemented by
// Client (live) and by fakes in tests.
type ResourceLister interface {
	ListResources(ctx context.Context) (*K8sSnapshot, error)
}

// K8sSnapshot is the raw, unassembled result of one cluster scan.
type K8sSnapshot struct {
	Namespaces    []NamespaceInfo
	Pods          []PodInfo
	Services      []ServiceInfo
	Workloads     []WorkloadInfo
	Ingresses     []IngressInfo
	IngressRoutes []IngressRouteInfo
	NetPolicies   []NetworkPolicyInfo
	GitOps        []GitOpsSourceInfo // Argo/Flux CRs (if enabled)
}

type NamespaceInfo struct {
	Name         string
	CapabilityID string // dfds.cloud/capability (== namespace name for SSU-managed); "" otherwise
	AWSAccountID string // dfds.cloud/aws-account
	ContextID    string // dfds.cloud/context-id
	CostCentre   string // dfds.cloud/cost-centre
	Labels       map[string]string
}

type PodInfo struct {
	Namespace string
	Name      string
	Labels    map[string]string
	Ports     []ContainerPortInfo
}

type ContainerPortInfo struct {
	Name          string
	ContainerPort int32
	Protocol      string
}

type ServiceInfo struct {
	Namespace string
	Name      string
	Type      string
	ClusterIP string
	Selector  map[string]string
	Ports     []ServicePortInfo
}

type ServicePortInfo struct {
	Name       string
	Port       int32
	TargetPort string
	Protocol   string
}

type WorkloadInfo struct {
	Namespace     string
	Name          string
	Kind          string // "Deployment" | "StatefulSet" | "DaemonSet"
	Replicas      int32
	ReadyReplicas int32
	Selector      map[string]string // matchLabels
	PodLabels     map[string]string // pod template labels
	Labels        map[string]string
	Annotations   map[string]string
	Containers    []ContainerSpec
}

type ContainerSpec struct {
	Name           string
	Image          string
	Ports          []ContainerPortInfo
	RequestsCPU    string
	RequestsMemory string
	LimitsCPU      string
	LimitsMemory   string
}

type IngressInfo struct {
	Namespace string
	Name      string
	// ServiceHosts maps a backing service name to the external hostnames routed to it.
	ServiceHosts map[string][]string
	// ServiceRoutes maps a backing service name to the (host, path) pairs routing
	// to it, capturing the standard-Ingress path so reachability probing can target
	// the shortest known route prefix per host.
	ServiceRoutes map[string][]IngressPath
	// Annotations are the Ingress object's metadata.annotations, used to resolve
	// reachability probe config for the routes it owns.
	Annotations map[string]string
}

// IngressPath is one (host, path) pair from a standard Ingress rule.
type IngressPath struct {
	Host string
	Path string
}

type NetworkPolicyInfo struct {
	Namespace   string
	Name        string
	PodSelector map[string]string
	// EgressTargets is a best-effort list of egress destinations (namespace/pod selectors, IP blocks).
	EgressTargets []string
}

// GitOpsSourceInfo is a raw GitOps CR captured by the dynamic client. The
// gitops package interprets these into DeploymentSource attributions.
type GitOpsSourceInfo struct {
	Kind      string // "Application" | "HelmRelease" | "GitRepository" | "Kustomization"
	Namespace string
	Name      string
	Object    map[string]interface{} // unstructured CR content
}

// Client talks to the cluster with a typed client (core/apps/networking) and a
// dynamic client (GitOps CRs).
type Client struct {
	clientset        kubernetes.Interface
	dynamicClient    dynamic.Interface
	concurrency      int
	namespaceExclude map[string]struct{}
	namespaceInclude map[string]struct{}
	gitopsEnabled    bool
}

// Options configures a Client.
type Options struct {
	Concurrency int
	// NamespaceExclude and NamespaceInclude are mutually exclusive namespace
	// filters. When NamespaceInclude is non-empty, ONLY those namespaces are
	// scanned and NamespaceExclude is ignored; otherwise every namespace except
	// those in NamespaceExclude is scanned. Mutual exclusivity is enforced at
	// config load time (see internal/config).
	NamespaceExclude []string
	NamespaceInclude []string
	GitOpsEnabled    bool

	// QPS and Burst tune client-go's client-side rate limiter on the REST
	// config. Left at zero they fall back to client-go's conservative defaults
	// (QPS 5 / Burst 10), which throttle the per-cycle request fan-out; bump
	// them to avoid "client-side throttling" waits on large clusters.
	QPS   float32
	Burst int
}

// newNamespaceSet builds a lookup set from a namespace list, trimming whitespace
// and dropping blank entries.
func newNamespaceSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, ns := range names {
		ns = strings.TrimSpace(ns)
		if ns != "" {
			set[ns] = struct{}{}
		}
	}
	return set
}

// NewClient builds a Client from the in-cluster config, falling back to the
// local kubeconfig for development.
func NewClient(opts Options) (*Client, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			clientcmd.NewDefaultClientConfigLoadingRules(),
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to create kubernetes config: %w", err)
		}
	}

	// Raise the client-side rate limiter above client-go's defaults so the
	// per-cycle request fan-out isn't throttled. Server-side API Priority &
	// Fairness still protects the apiserver.
	if opts.QPS > 0 {
		cfg.QPS = opts.QPS
	}
	if opts.Burst > 0 {
		cfg.Burst = opts.Burst
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return NewClientFromInterfaces(clientset, dynamicClient, opts), nil
}

// NewClientFromInterfaces builds a Client from pre-constructed interfaces
// (used in tests with fake clients).
func NewClientFromInterfaces(clientset kubernetes.Interface, dynamicClient dynamic.Interface, opts Options) *Client {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 10
	}
	return &Client{
		clientset:        clientset,
		dynamicClient:    dynamicClient,
		concurrency:      opts.Concurrency,
		namespaceExclude: newNamespaceSet(opts.NamespaceExclude),
		namespaceInclude: newNamespaceSet(opts.NamespaceInclude),
		gitopsEnabled:    opts.GitOpsEnabled,
	}
}

// ListResources scans all (non-excluded) namespaces concurrently and returns a
// snapshot of everything the collector needs.
func (c *Client) ListResources(ctx context.Context) (*K8sSnapshot, error) {
	namespaceList, err := c.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing namespaces: %w", err)
	}

	snapshot := &K8sSnapshot{}
	var (
		mu       sync.Mutex
		scanList []corev1.Namespace
	)

	for _, ns := range namespaceList.Items {
		if !c.shouldScanNamespace(ns.Name) {
			continue
		}
		scanList = append(scanList, ns)
		snapshot.Namespaces = append(snapshot.Namespaces, namespaceInfoFrom(ns))
	}

	// gctx is the errgroup-scoped context; errgroup.Wait cancels it on return, so
	// it must NOT be reused for the post-Wait dynamic listing below (doing so
	// makes every List fail with "context canceled"). The original ctx is used
	// for listGitOps/listIngressRoutes.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.concurrency)

	for _, ns := range scanList {
		nsName := ns.Name
		g.Go(func() error {
			local, err := c.scanNamespace(gctx, nsName)
			if err != nil {
				return err
			}
			mu.Lock()
			snapshot.Pods = append(snapshot.Pods, local.Pods...)
			snapshot.Services = append(snapshot.Services, local.Services...)
			snapshot.Workloads = append(snapshot.Workloads, local.Workloads...)
			snapshot.Ingresses = append(snapshot.Ingresses, local.Ingresses...)
			snapshot.NetPolicies = append(snapshot.NetPolicies, local.NetPolicies...)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if c.gitopsEnabled {
		snapshot.GitOps = c.listGitOps(ctx)
	}

	snapshot.IngressRoutes = c.listIngressRoutes(ctx)

	return snapshot, nil
}

// shouldScanNamespace decides whether a namespace is in scope. An include list
// takes precedence (allow-list: only listed namespaces are scanned); otherwise
// the exclude list is applied (deny-list). The two are mutually exclusive by
// configuration, so include is only non-empty when exclude is empty.
func (c *Client) shouldScanNamespace(name string) bool {
	if len(c.namespaceInclude) > 0 {
		_, included := c.namespaceInclude[name]
		return included
	}
	_, excluded := c.namespaceExclude[name]
	return !excluded
}

// scanNamespace lists all per-namespace resources for one namespace.
func (c *Client) scanNamespace(ctx context.Context, ns string) (*K8sSnapshot, error) {
	local := &K8sSnapshot{}

	pods, err := c.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing pods in namespace %s: %w", ns, err)
	}
	for i := range pods.Items {
		local.Pods = append(local.Pods, podInfoFrom(&pods.Items[i]))
	}

	services, err := c.clientset.CoreV1().Services(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing services in namespace %s: %w", ns, err)
	}
	for i := range services.Items {
		local.Services = append(local.Services, serviceInfoFrom(&services.Items[i]))
	}

	deployments, err := c.clientset.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing deployments in namespace %s: %w", ns, err)
	}
	for i := range deployments.Items {
		local.Workloads = append(local.Workloads, deploymentInfoFrom(&deployments.Items[i]))
	}

	statefulSets, err := c.clientset.AppsV1().StatefulSets(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing statefulsets in namespace %s: %w", ns, err)
	}
	for i := range statefulSets.Items {
		local.Workloads = append(local.Workloads, statefulSetInfoFrom(&statefulSets.Items[i]))
	}

	daemonSets, err := c.clientset.AppsV1().DaemonSets(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing daemonsets in namespace %s: %w", ns, err)
	}
	for i := range daemonSets.Items {
		local.Workloads = append(local.Workloads, daemonSetInfoFrom(&daemonSets.Items[i]))
	}

	ingresses, err := c.clientset.NetworkingV1().Ingresses(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing ingresses in namespace %s: %w", ns, err)
	}
	for i := range ingresses.Items {
		local.Ingresses = append(local.Ingresses, ingressInfoFrom(&ingresses.Items[i]))
	}

	netpols, err := c.clientset.NetworkingV1().NetworkPolicies(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing networkpolicies in namespace %s: %w", ns, err)
	}
	for i := range netpols.Items {
		local.NetPolicies = append(local.NetPolicies, networkPolicyInfoFrom(&netpols.Items[i]))
	}

	return local, nil
}

// gitOpsResources enumerates the GVRs we attempt to discover.
var gitOpsResources = []struct {
	Kind string
	GVR  schema.GroupVersionResource
}{
	{"Application", schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "applications"}},
	{"HelmRelease", schema.GroupVersionResource{Group: "helm.toolkit.fluxcd.io", Version: "v2", Resource: "helmreleases"}},
	{"GitRepository", schema.GroupVersionResource{Group: "source.toolkit.fluxcd.io", Version: "v1", Resource: "gitrepositories"}},
	{"Kustomization", schema.GroupVersionResource{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Resource: "kustomizations"}},
}

// listGitOps best-effort lists GitOps CRs cluster-wide via the dynamic client.
// Any error for a given resource (CRD absent, RBAC) is silently skipped — the
// catalog stays intact without GitOps attribution.
func (c *Client) listGitOps(ctx context.Context) []GitOpsSourceInfo {
	if c.dynamicClient == nil {
		return nil
	}
	var result []GitOpsSourceInfo
	for _, res := range gitOpsResources {
		list, err := c.dynamicClient.Resource(res.GVR).List(ctx, metav1.ListOptions{})
		if err != nil {
			// CRD not installed, RBAC denied, or discovery miss — skip silently.
			continue
		}
		for i := range list.Items {
			item := list.Items[i]
			result = append(result, GitOpsSourceInfo{
				Kind:      res.Kind,
				Namespace: item.GetNamespace(),
				Name:      item.GetName(),
				Object:    item.Object,
			})
		}
	}
	return result
}

// --- conversion helpers -----------------------------------------------------

func namespaceInfoFrom(ns corev1.Namespace) NamespaceInfo {
	return NamespaceInfo{
		Name:         ns.Name,
		CapabilityID: ns.Labels["dfds.cloud/capability"],
		AWSAccountID: ns.Labels["dfds.cloud/aws-account"],
		ContextID:    ns.Labels["dfds.cloud/context-id"],
		CostCentre:   ns.Labels["dfds.cloud/cost-centre"],
		Labels:       ns.Labels,
	}
}

func podInfoFrom(pod *corev1.Pod) PodInfo {
	var ports []ContainerPortInfo
	for _, ctr := range pod.Spec.Containers {
		for _, p := range ctr.Ports {
			ports = append(ports, ContainerPortInfo{
				Name:          p.Name,
				ContainerPort: p.ContainerPort,
				Protocol:      string(p.Protocol),
			})
		}
	}
	return PodInfo{
		Namespace: pod.Namespace,
		Name:      pod.Name,
		Labels:    pod.Labels,
		Ports:     ports,
	}
}

func serviceInfoFrom(svc *corev1.Service) ServiceInfo {
	var ports []ServicePortInfo
	for _, p := range svc.Spec.Ports {
		ports = append(ports, ServicePortInfo{
			Name:       p.Name,
			Port:       p.Port,
			TargetPort: p.TargetPort.String(),
			Protocol:   string(p.Protocol),
		})
	}
	return ServiceInfo{
		Namespace: svc.Namespace,
		Name:      svc.Name,
		Type:      string(svc.Spec.Type),
		ClusterIP: svc.Spec.ClusterIP,
		Selector:  svc.Spec.Selector,
		Ports:     ports,
	}
}

func containerSpecsFrom(containers []corev1.Container) []ContainerSpec {
	var specs []ContainerSpec
	for _, ctr := range containers {
		var ports []ContainerPortInfo
		for _, p := range ctr.Ports {
			ports = append(ports, ContainerPortInfo{
				Name:          p.Name,
				ContainerPort: p.ContainerPort,
				Protocol:      string(p.Protocol),
			})
		}
		spec := ContainerSpec{
			Name:  ctr.Name,
			Image: ctr.Image,
			Ports: ports,
		}
		if req := ctr.Resources.Requests; req != nil {
			spec.RequestsCPU = req.Cpu().String()
			spec.RequestsMemory = req.Memory().String()
		}
		if lim := ctr.Resources.Limits; lim != nil {
			spec.LimitsCPU = lim.Cpu().String()
			spec.LimitsMemory = lim.Memory().String()
		}
		specs = append(specs, spec)
	}
	return specs
}

func deploymentInfoFrom(d *appsv1.Deployment) WorkloadInfo {
	var replicas int32
	if d.Spec.Replicas != nil {
		replicas = *d.Spec.Replicas
	}
	return WorkloadInfo{
		Namespace:     d.Namespace,
		Name:          d.Name,
		Kind:          "Deployment",
		Replicas:      replicas,
		ReadyReplicas: d.Status.ReadyReplicas,
		Selector:      selectorMatchLabels(d.Spec.Selector),
		PodLabels:     d.Spec.Template.Labels,
		Labels:        d.Labels,
		Annotations:   d.Annotations,
		Containers:    containerSpecsFrom(d.Spec.Template.Spec.Containers),
	}
}

func statefulSetInfoFrom(s *appsv1.StatefulSet) WorkloadInfo {
	var replicas int32
	if s.Spec.Replicas != nil {
		replicas = *s.Spec.Replicas
	}
	return WorkloadInfo{
		Namespace:     s.Namespace,
		Name:          s.Name,
		Kind:          "StatefulSet",
		Replicas:      replicas,
		ReadyReplicas: s.Status.ReadyReplicas,
		Selector:      selectorMatchLabels(s.Spec.Selector),
		PodLabels:     s.Spec.Template.Labels,
		Labels:        s.Labels,
		Annotations:   s.Annotations,
		Containers:    containerSpecsFrom(s.Spec.Template.Spec.Containers),
	}
}

func daemonSetInfoFrom(d *appsv1.DaemonSet) WorkloadInfo {
	return WorkloadInfo{
		Namespace:     d.Namespace,
		Name:          d.Name,
		Kind:          "DaemonSet",
		Replicas:      d.Status.DesiredNumberScheduled,
		ReadyReplicas: d.Status.NumberReady,
		Selector:      selectorMatchLabels(d.Spec.Selector),
		PodLabels:     d.Spec.Template.Labels,
		Labels:        d.Labels,
		Annotations:   d.Annotations,
		Containers:    containerSpecsFrom(d.Spec.Template.Spec.Containers),
	}
}

func selectorMatchLabels(sel *metav1.LabelSelector) map[string]string {
	if sel == nil {
		return nil
	}
	return sel.MatchLabels
}

func ingressInfoFrom(ing *networkingv1.Ingress) IngressInfo {
	serviceHosts := make(map[string][]string)
	serviceRoutes := make(map[string][]IngressPath)
	for _, rule := range ing.Spec.Rules {
		host := rule.Host
		if host == "" || rule.HTTP == nil {
			continue
		}
		for _, path := range rule.HTTP.Paths {
			if path.Backend.Service == nil {
				continue
			}
			svc := path.Backend.Service.Name
			serviceHosts[svc] = appendUnique(serviceHosts[svc], host)
			serviceRoutes[svc] = appendIngressPathUnique(serviceRoutes[svc], IngressPath{Host: host, Path: path.Path})
		}
	}
	return IngressInfo{
		Namespace:     ing.Namespace,
		Name:          ing.Name,
		ServiceHosts:  serviceHosts,
		ServiceRoutes: serviceRoutes,
		Annotations:   ing.Annotations,
	}
}

// appendIngressPathUnique appends p unless an identical (host, path) pair is
// already present.
func appendIngressPathUnique(paths []IngressPath, p IngressPath) []IngressPath {
	for _, existing := range paths {
		if existing == p {
			return paths
		}
	}
	return append(paths, p)
}

func networkPolicyInfoFrom(np *networkingv1.NetworkPolicy) NetworkPolicyInfo {
	var targets []string
	for _, egress := range np.Spec.Egress {
		for _, to := range egress.To {
			switch {
			case to.IPBlock != nil:
				targets = appendUnique(targets, "ipblock:"+to.IPBlock.CIDR)
			case to.NamespaceSelector != nil:
				targets = appendUnique(targets, "namespaceSelector:"+labelSelectorString(to.NamespaceSelector))
			case to.PodSelector != nil:
				targets = appendUnique(targets, "podSelector:"+labelSelectorString(to.PodSelector))
			}
		}
	}
	return NetworkPolicyInfo{
		Namespace:     np.Namespace,
		Name:          np.Name,
		PodSelector:   np.Spec.PodSelector.MatchLabels,
		EgressTargets: targets,
	}
}

func labelSelectorString(sel *metav1.LabelSelector) string {
	if sel == nil {
		return ""
	}
	parts := make([]string, 0, len(sel.MatchLabels))
	for k, v := range sel.MatchLabels {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

func appendUnique(slice []string, value string) []string {
	for _, existing := range slice {
		if existing == value {
			return slice
		}
	}
	return append(slice, value)
}
