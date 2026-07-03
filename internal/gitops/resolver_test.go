package gitops

import (
	"testing"

	"go.dfds.cloud/ssu-catalog/internal/kubernetes"
)

func cr(kind, namespace, name string, object map[string]interface{}) kubernetes.GitOpsSourceInfo {
	object["apiVersion"] = "x"
	object["kind"] = kind
	object["metadata"] = map[string]interface{}{"namespace": namespace, "name": name}
	return kubernetes.GitOpsSourceInfo{Kind: kind, Namespace: namespace, Name: name, Object: object}
}

func argoApp(namespace, name, repo, path, rev string) kubernetes.GitOpsSourceInfo {
	return cr("Application", namespace, name, map[string]interface{}{
		"spec": map[string]interface{}{
			"source": map[string]interface{}{
				"repoURL":        repo,
				"path":           path,
				"targetRevision": rev,
			},
		},
	})
}

func gitRepoCR(namespace, name, url, branch string) kubernetes.GitOpsSourceInfo {
	return cr("GitRepository", namespace, name, map[string]interface{}{
		"spec": map[string]interface{}{
			"url": url,
			"ref": map[string]interface{}{"branch": branch},
		},
	})
}

func helmReleaseCR(namespace, name, srcKind, srcName, srcNS string) kubernetes.GitOpsSourceInfo {
	sourceRef := map[string]interface{}{"kind": srcKind, "name": srcName}
	if srcNS != "" {
		sourceRef["namespace"] = srcNS
	}
	return cr("HelmRelease", namespace, name, map[string]interface{}{
		"spec": map[string]interface{}{
			"chart": map[string]interface{}{
				"spec": map[string]interface{}{"sourceRef": sourceRef},
			},
		},
	})
}

func kustomizationCR(namespace, name, path, srcKind, srcName string) kubernetes.GitOpsSourceInfo {
	return cr("Kustomization", namespace, name, map[string]interface{}{
		"spec": map[string]interface{}{
			"path":      path,
			"sourceRef": map[string]interface{}{"kind": srcKind, "name": srcName},
		},
	})
}

func TestResolve_ArgoTrackingID(t *testing.T) {
	r := NewResolver([]kubernetes.GitOpsSourceInfo{
		argoApp("argocd", "my-app", "https://github.com/example/apps", "apps/my-app", "main"),
	})
	annotations := map[string]string{
		annoArgoTrackingID: "my-app:apps/Deployment:cap-a/api",
	}
	src, repo := r.Resolve("cap-a", nil, annotations)
	if src == nil {
		t.Fatal("expected a deployment source")
	}
	if src.Tool != "argocd" || src.AppName != "my-app" {
		t.Errorf("argo attribution wrong: %+v", src)
	}
	if src.RepoURL != "https://github.com/example/apps" || len(repo) != 1 || repo[0] != src.RepoURL {
		t.Errorf("repo wrong: src=%q repo=%v", src.RepoURL, repo)
	}
	if src.Path != "apps/my-app" || src.Revision != "main" {
		t.Errorf("path/revision wrong: %+v", src)
	}
}

func TestResolve_ArgoInstanceOnlyWhenApplicationExists(t *testing.T) {
	// instance label present, but no matching Application → not honored.
	r := NewResolver(nil)
	src, _ := r.Resolve("cap-a", map[string]string{labelArgoInstance: "ghost"}, nil)
	if src != nil {
		t.Fatalf("expected nil source when no Application matches, got %+v", src)
	}

	// instance label present with a matching Application → honored.
	r = NewResolver([]kubernetes.GitOpsSourceInfo{
		argoApp("argocd", "real", "https://github.com/example/repo", "p", "main"),
	})
	src, _ = r.Resolve("cap-a", map[string]string{labelArgoInstance: "real"}, nil)
	if src == nil || src.Tool != "argocd" || src.AppName != "real" {
		t.Fatalf("expected argo attribution via instance label, got %+v", src)
	}
}

func TestResolve_ArgoAppInAnyNamespace(t *testing.T) {
	r := NewResolver([]kubernetes.GitOpsSourceInfo{
		argoApp("team-ns", "my-app", "https://github.com/example/apps", "p", "main"),
	})
	annotations := map[string]string{
		annoArgoTrackingID: "team-ns/my-app:apps/Deployment:cap-a/api",
	}
	src, _ := r.Resolve("cap-a", nil, annotations)
	if src == nil || src.AppName != "my-app" {
		t.Fatalf("expected ns-qualified app lookup, got %+v", src)
	}
}

func TestResolve_ArgoAppInAnyNamespaceUnderscore(t *testing.T) {
	r := NewResolver([]kubernetes.GitOpsSourceInfo{
		argoApp("team-x-tenant", "team-x", "https://github.com/example/svc", "chart", "HEAD"),
	})
	annotations := map[string]string{
		annoArgoTrackingID: "team-x-tenant_team-x:apps/Deployment:team-x-tenant/team-x",
	}
	src, _ := r.Resolve("team-x-tenant", nil, annotations)
	if src == nil || src.Tool != "argocd" || src.AppName != "team-x" {
		t.Fatalf("expected argo attribution via underscore-encoded instance name, got %+v", src)
	}
}

func TestResolve_FluxHelm(t *testing.T) {
	r := NewResolver([]kubernetes.GitOpsSourceInfo{
		helmReleaseCR("cap-a", "api", "GitRepository", "flux-system", "flux-system"),
		gitRepoCR("flux-system", "flux-system", "https://github.com/example/manifests", "main"),
	})
	labels := map[string]string{
		labelFluxHelmName: "api",
		labelFluxHelmNS:   "cap-a",
	}
	src, repo := r.Resolve("cap-a", labels, nil)
	if src == nil || src.Tool != "flux-helm" || src.AppName != "api" {
		t.Fatalf("flux-helm attribution wrong: %+v", src)
	}
	if src.RepoURL != "https://github.com/example/manifests" || len(repo) != 1 || repo[0] != src.RepoURL {
		t.Errorf("repo wrong: %+v repo=%v", src, repo)
	}
	if src.Revision != "main" {
		t.Errorf("revision wrong: %+v", src)
	}
}

func TestResolve_FluxHelmDefaultsSourceNamespace(t *testing.T) {
	// sourceRef without an explicit namespace resolves in the HelmRelease's ns.
	r := NewResolver([]kubernetes.GitOpsSourceInfo{
		helmReleaseCR("cap-a", "api", "GitRepository", "repo", ""),
		gitRepoCR("cap-a", "repo", "https://github.com/example/repo", "release"),
	})
	labels := map[string]string{labelFluxHelmName: "api", labelFluxHelmNS: "cap-a"}
	src, _ := r.Resolve("cap-a", labels, nil)
	if src == nil || src.RepoURL != "https://github.com/example/repo" || src.Revision != "release" {
		t.Fatalf("default-namespace source resolution wrong: %+v", src)
	}
}

func TestResolve_FluxHelmLabelsButNoCR(t *testing.T) {
	// Labels present but HelmRelease not captured → still attributed (tool+name),
	// repo blank.
	r := NewResolver(nil)
	labels := map[string]string{labelFluxHelmName: "api", labelFluxHelmNS: "cap-a"}
	src, repo := r.Resolve("cap-a", labels, nil)
	if src == nil || src.Tool != "flux-helm" || src.AppName != "api" {
		t.Fatalf("expected attribution from labels alone, got %+v", src)
	}
	if src.RepoURL != "" || len(repo) != 0 {
		t.Errorf("expected blank repo, got %q / %v", src.RepoURL, repo)
	}
}

func TestResolve_FluxKustomize(t *testing.T) {
	r := NewResolver([]kubernetes.GitOpsSourceInfo{
		kustomizationCR("flux-system", "apps", "clusters/prod/apps", "GitRepository", "flux-system"),
		gitRepoCR("flux-system", "flux-system", "https://github.com/example/manifests", "main"),
	})
	labels := map[string]string{
		labelFluxKustomizeName: "apps",
		labelFluxKustomizeNS:   "flux-system",
	}
	src, _ := r.Resolve("cap-a", labels, nil)
	if src == nil || src.Tool != "flux-kustomize" {
		t.Fatalf("flux-kustomize attribution wrong: %+v", src)
	}
	if src.Path != "clusters/prod/apps" {
		t.Errorf("path wrong: %+v", src)
	}
	if src.RepoURL != "https://github.com/example/manifests" {
		t.Errorf("repo wrong: %+v", src)
	}
}

func TestResolve_FallbackRepoAnnotation(t *testing.T) {
	r := NewResolver(nil)
	annotations := map[string]string{"dfds.cloud/repo": "https://github.com/example/standalone"}
	labels := map[string]string{"app.kubernetes.io/part-of": "billing"}
	src, repo := r.Resolve("cap-a", labels, annotations)
	if src == nil || src.Tool != "" {
		t.Fatalf("expected tool-less fallback source, got %+v", src)
	}
	if src.RepoURL != "https://github.com/example/standalone" || len(repo) != 1 || repo[0] != src.RepoURL {
		t.Errorf("fallback repo wrong: %+v repo=%v", src, repo)
	}
	if src.AppName != "billing" {
		t.Errorf("expected part-of as appName, got %q", src.AppName)
	}
}

func TestResolve_None(t *testing.T) {
	r := NewResolver(nil)
	src, repo := r.Resolve("cap-a", map[string]string{"app": "api"}, nil)
	if src != nil {
		t.Errorf("expected nil source, got %+v", src)
	}
	if len(repo) != 0 {
		t.Errorf("expected empty repo list, got %v", repo)
	}
}

// A declared dfds.cloud/repo complements — does not override — the discovered
// repo: both appear, discovered-first, deduped.
func TestResolve_DeclaredRepoComplementsDiscovered(t *testing.T) {
	r := NewResolver([]kubernetes.GitOpsSourceInfo{
		argoApp("argocd", "my-app", "https://github.com/example/apps", "p", "main"),
	})
	annotations := map[string]string{
		annoArgoTrackingID: "my-app:apps/Deployment:cap-a/api",
		"dfds.cloud/repo":  "https://github.com/team/service",
	}
	src, repo := r.Resolve("cap-a", nil, annotations)
	if src == nil || src.RepoURL != "https://github.com/example/apps" {
		t.Fatalf("expected discovered argo source, got %+v", src)
	}
	want := []string{"https://github.com/example/apps", "https://github.com/team/service"}
	if len(repo) != len(want) {
		t.Fatalf("expected %v, got %v", want, repo)
	}
	for i := range want {
		if repo[i] != want[i] {
			t.Errorf("repo[%d] = %q, want %q (full: %v)", i, repo[i], want[i], repo)
		}
	}

	// A declared repo equal to the discovered one is not duplicated.
	annotations["dfds.cloud/repo"] = "https://github.com/example/apps"
	if _, repo = r.Resolve("cap-a", nil, annotations); len(repo) != 1 {
		t.Errorf("expected deduped single repo, got %v", repo)
	}
}

func TestResolve_PriorityArgoBeforeFlux(t *testing.T) {
	// A workload carrying both Argo and Flux metadata is attributed to Argo.
	r := NewResolver([]kubernetes.GitOpsSourceInfo{
		argoApp("argocd", "my-app", "https://github.com/example/argo-wins", "p", "main"),
	})
	annotations := map[string]string{annoArgoTrackingID: "my-app:apps/Deployment:cap-a/api"}
	labels := map[string]string{labelFluxHelmName: "api", labelFluxHelmNS: "cap-a"}
	src, _ := r.Resolve("cap-a", labels, annotations)
	if src == nil || src.Tool != "argocd" {
		t.Fatalf("expected argo to win, got %+v", src)
	}
}
