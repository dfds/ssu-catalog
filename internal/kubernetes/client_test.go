package kubernetes

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func ptrInt32(v int32) *int32 { return &v }

func newUnstructured(apiVersion, kind, namespace, name string, fields map[string]interface{}) *unstructured.Unstructured {
	obj := map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   map[string]interface{}{"namespace": namespace, "name": name},
	}
	for k, v := range fields {
		obj[k] = v
	}
	return &unstructured.Unstructured{Object: obj}
}

func newNamespace(name string, labels map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func TestListResources_AllNamespacesAndCapabilityID(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		newNamespace("cap-a", map[string]string{
			"dfds.cloud/capability":  "cap-a",
			"dfds.cloud/aws-account": "111111111111",
			"dfds.cloud/context-id":  "ctx-1",
			"dfds.cloud/cost-centre": "cc-1",
		}),
		newNamespace("kube-system", nil),
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "cap-a"},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptrInt32(2),
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name:  "api",
						Image: "dfdsdk/api:1.2.3",
						Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP}},
					}}},
				},
			},
			Status: appsv1.DeploymentStatus{ReadyReplicas: 2},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "cap-a"},
			Spec: corev1.ServiceSpec{
				Type:      corev1.ServiceTypeClusterIP,
				ClusterIP: "10.0.0.1",
				Selector:  map[string]string{"app": "api"},
				Ports:     []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt(8080), Protocol: corev1.ProtocolTCP}},
			},
		},
	)

	c := NewClientFromInterfaces(clientset, nil, Options{Concurrency: 4})
	snap, err := c.ListResources(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(snap.Namespaces) != 2 {
		t.Fatalf("expected 2 namespaces, got %d", len(snap.Namespaces))
	}
	var capA *NamespaceInfo
	for i := range snap.Namespaces {
		if snap.Namespaces[i].Name == "cap-a" {
			capA = &snap.Namespaces[i]
		}
	}
	if capA == nil {
		t.Fatal("cap-a namespace not found")
	}
	if capA.CapabilityID != "cap-a" {
		t.Errorf("expected capabilityID cap-a, got %q", capA.CapabilityID)
	}
	if capA.AWSAccountID != "111111111111" || capA.ContextID != "ctx-1" || capA.CostCentre != "cc-1" {
		t.Errorf("dfds.cloud labels not mapped: %+v", capA)
	}

	if len(snap.Workloads) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(snap.Workloads))
	}
	w := snap.Workloads[0]
	if w.Kind != "Deployment" || w.Replicas != 2 || w.ReadyReplicas != 2 {
		t.Errorf("workload mapping wrong: %+v", w)
	}
	if len(w.Containers) != 1 || w.Containers[0].Image != "dfdsdk/api:1.2.3" {
		t.Errorf("container mapping wrong: %+v", w.Containers)
	}
	if len(snap.Services) != 1 {
		t.Errorf("expected 1 service, got %d", len(snap.Services))
	}
}

func TestListResources_NamespaceExclude(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		newNamespace("cap-a", map[string]string{"dfds.cloud/capability": "cap-a"}),
		newNamespace("kube-system", nil),
		newNamespace("kube-node-lease", nil),
	)

	c := NewClientFromInterfaces(clientset, nil, Options{
		Concurrency:      4,
		NamespaceExclude: []string{"kube-system", " kube-node-lease "},
	})
	snap, err := c.ListResources(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snap.Namespaces) != 1 || snap.Namespaces[0].Name != "cap-a" {
		t.Errorf("exclude list not honored: %+v", snap.Namespaces)
	}
}

func TestListResources_NamespaceInclude(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		newNamespace("cap-a", map[string]string{"dfds.cloud/capability": "cap-a"}),
		newNamespace("cap-b", map[string]string{"dfds.cloud/capability": "cap-b"}),
		newNamespace("kube-system", nil),
	)

	c := NewClientFromInterfaces(clientset, nil, Options{
		Concurrency:      4,
		NamespaceInclude: []string{"cap-a", " cap-b "},
	})
	snap, err := c.ListResources(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snap.Namespaces) != 2 {
		t.Fatalf("include list not honored: %+v", snap.Namespaces)
	}
	for _, ns := range snap.Namespaces {
		if ns.Name != "cap-a" && ns.Name != "cap-b" {
			t.Errorf("unexpected namespace scanned: %s", ns.Name)
		}
	}
}

func TestListResources_NamespaceIncludeTakesPrecedence(t *testing.T) {
	// When both filters are set on the client, include wins. (Config rejects
	// this combination before it reaches the client, but the client stays
	// deterministic regardless.)
	clientset := fake.NewSimpleClientset(
		newNamespace("cap-a", nil),
		newNamespace("cap-b", nil),
	)

	c := NewClientFromInterfaces(clientset, nil, Options{
		Concurrency:      4,
		NamespaceInclude: []string{"cap-a"},
		NamespaceExclude: []string{"cap-a"},
	})
	snap, err := c.ListResources(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snap.Namespaces) != 1 || snap.Namespaces[0].Name != "cap-a" {
		t.Errorf("expected include to win: %+v", snap.Namespaces)
	}
}

func TestListResources_IngressAndNetworkPolicy(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		newNamespace("cap-a", map[string]string{"dfds.cloud/capability": "cap-a"}),
		&networkingv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{Name: "api-ing", Namespace: "cap-a"},
			Spec: networkingv1.IngressSpec{Rules: []networkingv1.IngressRule{{
				Host: "api.example.com",
				IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
					Paths: []networkingv1.HTTPIngressPath{{
						Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{Name: "api"}},
					}},
				}},
			}}},
		},
		&networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "deny", Namespace: "cap-a"},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
				Egress: []networkingv1.NetworkPolicyEgressRule{{
					To: []networkingv1.NetworkPolicyPeer{{
						IPBlock: &networkingv1.IPBlock{CIDR: "10.0.0.0/8"},
					}},
				}},
			},
		},
	)

	c := NewClientFromInterfaces(clientset, nil, Options{Concurrency: 2})
	snap, err := c.ListResources(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snap.Ingresses) != 1 {
		t.Fatalf("expected 1 ingress, got %d", len(snap.Ingresses))
	}
	if hosts := snap.Ingresses[0].ServiceHosts["api"]; len(hosts) != 1 || hosts[0] != "api.example.com" {
		t.Errorf("ingress hosts wrong: %+v", snap.Ingresses[0].ServiceHosts)
	}
	if len(snap.NetPolicies) != 1 {
		t.Fatalf("expected 1 netpol, got %d", len(snap.NetPolicies))
	}
	if got := snap.NetPolicies[0].EgressTargets; len(got) != 1 || got[0] != "ipblock:10.0.0.0/8" {
		t.Errorf("egress targets wrong: %+v", got)
	}
}

func TestListGitOps_ParsesAndTolerates(t *testing.T) {
	scheme := runtime.NewScheme()
	// The dynamic fake panics on a List for any GVR without a registered list
	// kind, so register all of them. (A real discovery client instead returns
	// NoKindMatchError for absent CRDs, which listGitOps tolerates via continue.)
	gvrToKind := map[schema.GroupVersionResource]string{
		gitOpsResources[0].GVR:   "ApplicationList",
		gitOpsResources[1].GVR:   "HelmReleaseList",
		gitOpsResources[2].GVR:   "GitRepositoryList",
		gitOpsResources[3].GVR:   "KustomizationList",
		ingressRouteResources[0]: "IngressRouteList",
		ingressRouteResources[1]: "IngressRouteList",
	}
	app := newUnstructured("argoproj.io/v1alpha1", "Application", "argocd", "my-app", map[string]interface{}{
		"spec": map[string]interface{}{
			"source": map[string]interface{}{
				"repoURL":        "https://github.com/example/apps",
				"path":           "apps/my-app",
				"targetRevision": "main",
			},
		},
	})
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToKind, app)

	c := NewClientFromInterfaces(fake.NewSimpleClientset(newNamespace("default", nil)), dynClient, Options{
		Concurrency:   2,
		GitOpsEnabled: true,
	})
	snap, err := c.ListResources(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snap.GitOps) != 1 {
		t.Fatalf("expected 1 gitops CR, got %d", len(snap.GitOps))
	}
	if snap.GitOps[0].Kind != "Application" || snap.GitOps[0].Name != "my-app" {
		t.Errorf("gitops CR wrong: %+v", snap.GitOps[0])
	}
}

func TestListGitOps_DisabledSkips(t *testing.T) {
	c := NewClientFromInterfaces(fake.NewSimpleClientset(newNamespace("default", nil)), nil, Options{
		Concurrency:   2,
		GitOpsEnabled: false,
	})
	snap, err := c.ListResources(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.GitOps != nil {
		t.Errorf("expected no gitops scan, got %+v", snap.GitOps)
	}
}
