package collector

import (
	"context"
	"testing"

	"go.dfds.cloud/ssu-catalog/internal/kubernetes"
	"go.dfds.cloud/ssu-catalog/internal/model"
)

type mockLister struct {
	snap *kubernetes.K8sSnapshot
	err  error
}

func (m *mockLister) ListResources(_ context.Context) (*kubernetes.K8sSnapshot, error) {
	return m.snap, m.err
}

func baseSnapshot() *kubernetes.K8sSnapshot {
	return &kubernetes.K8sSnapshot{
		Namespaces: []kubernetes.NamespaceInfo{
			{Name: "cap-a", CapabilityID: "cap-a", CostCentre: "cc-1"},
			{Name: "infra", CapabilityID: ""},
		},
		Workloads: []kubernetes.WorkloadInfo{
			{
				Namespace: "cap-a", Name: "api", Kind: "Deployment",
				Replicas: 2, ReadyReplicas: 2,
				PodLabels:  map[string]string{"app": "api"},
				Containers: []kubernetes.ContainerSpec{{Name: "api", Image: "dfdsdk/api:1.2.3"}},
			},
			{
				Namespace: "infra", Name: "worker", Kind: "Deployment",
				PodLabels:  map[string]string{"app": "worker"},
				Containers: []kubernetes.ContainerSpec{{Name: "worker", Image: "busybox:latest"}},
			},
		},
		Services: []kubernetes.ServiceInfo{
			{Namespace: "cap-a", Name: "api", Type: "ClusterIP", ClusterIP: "10.0.0.1",
				Selector: map[string]string{"app": "api"},
				Ports:    []kubernetes.ServicePortInfo{{Name: "http", Port: 80, TargetPort: "8080"}}},
			{Namespace: "cap-a", Name: "orphan", Type: "ClusterIP",
				Selector: map[string]string{"app": "ghost"}},
		},
		Ingresses: []kubernetes.IngressInfo{
			{Namespace: "cap-a", Name: "api-ing", ServiceHosts: map[string][]string{"api": {"api.example.com"}}},
		},
	}
}

func collect(t *testing.T, snap *kubernetes.K8sSnapshot) *model.Catalog {
	t.Helper()
	coll := NewCollector("hellman", &mockLister{snap: snap}, nil, nil, nil)
	cat, err := coll.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	return cat
}

func TestCollect_ApplicationAssembly(t *testing.T) {
	cat := collect(t, baseSnapshot())

	if cat.Cluster != "hellman" {
		t.Errorf("expected cluster hellman, got %q", cat.Cluster)
	}

	// 2 workloads + 1 orphan service = 3 applications
	if len(cat.Applications) != 3 {
		t.Fatalf("expected 3 applications, got %d", len(cat.Applications))
	}

	byKey := func(ns, name string) *model.ApplicationEntry {
		for i := range cat.Applications {
			if cat.Applications[i].Namespace == ns && cat.Applications[i].Name == name {
				return &cat.Applications[i]
			}
		}
		t.Fatalf("application %s/%s not found", ns, name)
		return nil
	}

	api := byKey("cap-a", "api")
	if api.Kind != "Deployment" || api.CapabilityID != "cap-a" {
		t.Errorf("api mapping wrong: %+v", api)
	}
	if len(api.Services) != 1 {
		t.Fatalf("api should have 1 service, got %d", len(api.Services))
	}
	if hosts := api.Services[0].ExternalHosts; len(hosts) != 1 || hosts[0] != "api.example.com" {
		t.Errorf("api external hosts = %+v", hosts)
	}
	if len(api.Containers) != 1 || api.Containers[0].ImageTag != "1.2.3" || api.Containers[0].Image != "dfdsdk/api" {
		t.Errorf("api image split wrong: %+v", api.Containers)
	}

	orphan := byKey("cap-a", "orphan")
	if orphan.Kind != "Service" {
		t.Errorf("orphan should be Kind=Service, got %q", orphan.Kind)
	}
	if orphan.CapabilityID != "cap-a" {
		t.Errorf("orphan capabilityID = %q", orphan.CapabilityID)
	}

	worker := byKey("infra", "worker")
	if worker.CapabilityID != "" {
		t.Errorf("worker should have empty capabilityID, got %q", worker.CapabilityID)
	}
	if len(worker.Services) != 0 {
		t.Errorf("worker (background consumer) should have no services, got %d", len(worker.Services))
	}
}

func TestCollect_Stats(t *testing.T) {
	cat := collect(t, baseSnapshot())
	if cat.Stats.TotalApplications != 3 {
		t.Errorf("TotalApplications = %d, want 3", cat.Stats.TotalApplications)
	}
	// cap-a/api and cap-a/orphan are capability-owned; infra/worker is not.
	if cat.Stats.CapabilityOwnedApplications != 2 {
		t.Errorf("CapabilityOwnedApplications = %d, want 2", cat.Stats.CapabilityOwnedApplications)
	}
	if cat.Stats.ApplicationsWithDocs != 0 {
		t.Errorf("ApplicationsWithDocs = %d, want 0", cat.Stats.ApplicationsWithDocs)
	}
	if cat.Stats.ApplicationsWithDeploySource != 0 {
		t.Errorf("ApplicationsWithDeploySource = %d, want 0", cat.Stats.ApplicationsWithDeploySource)
	}
}

func TestCollect_GitOpsDeploymentSource(t *testing.T) {
	snap := baseSnapshot()
	// Tag the api workload as Argo-managed and provide the Application CR.
	for i := range snap.Workloads {
		if snap.Workloads[i].Name == "api" {
			snap.Workloads[i].Annotations = map[string]string{
				"argocd.argoproj.io/tracking-id": "api:apps/Deployment:cap-a/api",
			}
		}
	}
	snap.GitOps = []kubernetes.GitOpsSourceInfo{{
		Kind: "Application", Namespace: "argocd", Name: "api",
		Object: map[string]interface{}{
			"spec": map[string]interface{}{
				"source": map[string]interface{}{
					"repoURL":        "https://github.com/example/apps",
					"path":           "apps/api",
					"targetRevision": "main",
				},
			},
		},
	}}

	cat := collect(t, snap)

	var api *model.ApplicationEntry
	for i := range cat.Applications {
		if cat.Applications[i].Namespace == "cap-a" && cat.Applications[i].Name == "api" {
			api = &cat.Applications[i]
		}
	}
	if api == nil {
		t.Fatal("api application not found")
	}
	if api.DeploymentSource == nil {
		t.Fatal("expected api to have a deployment source")
	}
	if api.DeploymentSource.Tool != "argocd" || api.RepoURL != "https://github.com/example/apps" {
		t.Errorf("deployment source wrong: %+v repo=%q", api.DeploymentSource, api.RepoURL)
	}
	if cat.Stats.ApplicationsWithDeploySource != 1 {
		t.Errorf("ApplicationsWithDeploySource = %d, want 1", cat.Stats.ApplicationsWithDeploySource)
	}
}

func TestCollect_NetworkPolicyEdges(t *testing.T) {
	snap := baseSnapshot()
	snap.NetPolicies = []kubernetes.NetworkPolicyInfo{
		{
			Namespace:     "cap-a",
			Name:          "api-egress",
			PodSelector:   map[string]string{"app": "api"}, // matches the api workload
			EgressTargets: []string{"ipblock:10.0.0.0/8", "namespaceSelector:kubernetes.io/metadata.name=db"},
		},
		{
			Namespace:     "cap-a",
			Name:          "no-match",
			PodSelector:   map[string]string{"app": "ghost"}, // matches no workload
			EgressTargets: []string{"ipblock:0.0.0.0/0"},
		},
		{
			Namespace:     "cap-a",
			Name:          "ingress-only",
			PodSelector:   map[string]string{"app": "api"},
			EgressTargets: nil, // no egress → no edges
		},
	}

	cat := collect(t, snap)

	var npEdges []model.DependencyEdge
	for _, e := range cat.Dependencies {
		if e.Type == "network_policy" {
			npEdges = append(npEdges, e)
		}
	}
	if len(npEdges) != 2 {
		t.Fatalf("expected 2 network_policy edges, got %d: %+v", len(npEdges), npEdges)
	}
	for _, e := range npEdges {
		if e.Origin != "network_policy" || e.Details != "api-egress" {
			t.Errorf("edge metadata wrong: %+v", e)
		}
		if e.Source.Service != "api" || e.Source.Namespace != "cap-a" || e.Source.External {
			t.Errorf("edge source wrong: %+v", e.Source)
		}
		if !e.Target.External {
			t.Errorf("egress target should be external: %+v", e.Target)
		}
	}
	if cat.Stats.TotalDependencies != 2 {
		t.Errorf("TotalDependencies = %d, want 2", cat.Stats.TotalDependencies)
	}
}

func TestCollect_IngressRouteLinking(t *testing.T) {
	snap := baseSnapshot()
	snap.IngressRoutes = []kubernetes.IngressRouteInfo{
		{
			Namespace: "cap-a", Name: "api-route",
			EntryPoints: []string{"websecure"}, TLS: true,
			Routes: []kubernetes.IngressRouteRule{
				{Hosts: []string{"api.route.example.com"}, PathPrefixes: []string{"/v1"}, Services: []string{"api"}},
			},
		},
		{
			Namespace: "cap-a", Name: "orphan-route",
			Routes: []kubernetes.IngressRouteRule{
				{Hosts: []string{"orphan.example.com"}, Services: []string{"orphan"}},
			},
		},
	}

	cat := collect(t, snap)

	byKey := func(ns, name string) *model.ApplicationEntry {
		for i := range cat.Applications {
			if cat.Applications[i].Namespace == ns && cat.Applications[i].Name == name {
				return &cat.Applications[i]
			}
		}
		t.Fatalf("application %s/%s not found", ns, name)
		return nil
	}

	// IngressRoute → Service "api" → workload "api": hosts merge into the same
	// ExternalHosts surface as the existing Ingress, and Routes carries detail.
	api := byKey("cap-a", "api")
	svc := api.Services[0]
	if !containsStr(svc.ExternalHosts, "api.example.com") || !containsStr(svc.ExternalHosts, "api.route.example.com") {
		t.Errorf("expected both Ingress and IngressRoute hosts, got %+v", svc.ExternalHosts)
	}
	if len(svc.Routes) != 1 {
		t.Fatalf("expected 1 route on api service, got %d", len(svc.Routes))
	}
	r := svc.Routes[0]
	if r.Name != "api-route" || r.Kind != "IngressRoute" || !r.TLS {
		t.Errorf("route metadata wrong: %+v", r)
	}
	if !containsStr(r.Hosts, "api.route.example.com") || !containsStr(r.PathPrefixes, "/v1") || !containsStr(r.EntryPoints, "websecure") {
		t.Errorf("route detail wrong: %+v", r)
	}

	// IngressRoute targeting an orphan Service surfaces on that orphan entry.
	orphan := byKey("cap-a", "orphan")
	if len(orphan.Services) != 1 || len(orphan.Services[0].Routes) != 1 {
		t.Fatalf("orphan should carry its IngressRoute, got %+v", orphan.Services)
	}
	if !containsStr(orphan.Services[0].ExternalHosts, "orphan.example.com") {
		t.Errorf("orphan external hosts = %+v", orphan.Services[0].ExternalHosts)
	}
}

func containsStr(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func TestCollect_NamespacesSorted(t *testing.T) {
	cat := collect(t, baseSnapshot())
	if len(cat.Namespaces) != 2 {
		t.Fatalf("expected 2 namespaces, got %d", len(cat.Namespaces))
	}
	if cat.Namespaces[0].Name != "cap-a" || cat.Namespaces[1].Name != "infra" {
		t.Errorf("namespaces not sorted: %+v", cat.Namespaces)
	}
}

func TestSplitImageTag(t *testing.T) {
	cases := []struct{ in, repo, tag string }{
		{"dfdsdk/api:1.2.3", "dfdsdk/api", "1.2.3"},
		{"busybox", "busybox", ""},
		{"registry.io:5000/team/app:v2", "registry.io:5000/team/app", "v2"},
		{"registry.io:5000/team/app", "registry.io:5000/team/app", ""},
		{"app@sha256:abc123", "app", "sha256:abc123"},
		{"", "", ""},
	}
	for _, tc := range cases {
		repo, tag := splitImageTag(tc.in)
		if repo != tc.repo || tag != tc.tag {
			t.Errorf("splitImageTag(%q) = (%q,%q), want (%q,%q)", tc.in, repo, tag, tc.repo, tc.tag)
		}
	}
}
