// Package gitops attributes a DeploymentSource (repo + CI/CD link) to each
// workload by interpreting the Argo/Flux custom resources captured by the
// dynamic client. Attribution is by per-workload tracking metadata
// (labels/annotations) — never by namespace, since many apps share one.
package gitops

import (
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"go.dfds.cloud/ssu-catalog/internal/kubernetes"
	"go.dfds.cloud/ssu-catalog/internal/model"
)

// Tracking metadata keys.
const (
	annoArgoTrackingID = "argocd.argoproj.io/tracking-id"
	labelArgoInstance  = "app.kubernetes.io/instance"

	labelFluxHelmName      = "helm.toolkit.fluxcd.io/name"
	labelFluxHelmNS        = "helm.toolkit.fluxcd.io/namespace"
	labelFluxKustomizeName = "kustomize.toolkit.fluxcd.io/name"
	labelFluxKustomizeNS   = "kustomize.toolkit.fluxcd.io/namespace"
)

// Fallback repo hints, in priority order.
var fallbackRepoKeys = []string{"dfds.cloud/repo", "git-origin"}

// Resolver indexes the GitOps CRs once and resolves per-workload attribution.
type Resolver struct {
	argoByName   map[string]*argoSource // Application name -> source
	argoByNsName map[string]*argoSource // "ns/name" -> source (app-in-any-namespace)

	helmReleases   map[string]*fluxRelease   // "ns/name" -> HelmRelease
	kustomizations map[string]*fluxKustomize // "ns/name" -> Kustomization
	gitRepos       map[string]*gitRepo       // "ns/name" -> GitRepository
}

type argoSource struct {
	name     string
	repoURL  string
	path     string
	revision string
}

type sourceRef struct {
	kind      string
	name      string
	namespace string
}

type fluxRelease struct {
	name   string
	source sourceRef
}

type fluxKustomize struct {
	name   string
	path   string
	source sourceRef
}

type gitRepo struct {
	url    string
	branch string
}

// NewResolver builds a Resolver from the captured GitOps CRs. A nil/empty slice
// yields a resolver that attributes nothing (every Resolve returns nil).
func NewResolver(crs []kubernetes.GitOpsSourceInfo) *Resolver {
	r := &Resolver{
		argoByName:     map[string]*argoSource{},
		argoByNsName:   map[string]*argoSource{},
		helmReleases:   map[string]*fluxRelease{},
		kustomizations: map[string]*fluxKustomize{},
		gitRepos:       map[string]*gitRepo{},
	}
	for i := range crs {
		cr := crs[i]
		switch cr.Kind {
		case "Application":
			if s := argoSourceFrom(cr); s != nil {
				r.argoByName[cr.Name] = s
				r.argoByNsName[cr.Namespace+"/"+cr.Name] = s
			}
		case "HelmRelease":
			r.helmReleases[cr.Namespace+"/"+cr.Name] = helmReleaseFrom(cr)
		case "Kustomization":
			r.kustomizations[cr.Namespace+"/"+cr.Name] = kustomizationFrom(cr)
		case "GitRepository":
			r.gitRepos[cr.Namespace+"/"+cr.Name] = gitRepoFrom(cr)
		}
	}
	return r
}

// Resolve attributes a deployment source to a workload from its tracking
// metadata. It returns the source (nil when neither Argo nor Flux nor a repo
// hint matches) and the best-effort repo URL for the application entry.
func (r *Resolver) Resolve(namespace string, labels, annotations map[string]string) (*model.DeploymentSource, string) {
	if src := r.resolveArgo(labels, annotations); src != nil {
		return src, src.RepoURL
	}
	if src := r.resolveFluxHelm(labels); src != nil {
		return src, src.RepoURL
	}
	if src := r.resolveFluxKustomize(labels); src != nil {
		return src, src.RepoURL
	}
	if src := fallbackSource(labels, annotations); src != nil {
		return src, src.RepoURL
	}
	return nil, fallbackRepoURL(labels, annotations)
}

func (r *Resolver) resolveArgo(labels, annotations map[string]string) *model.DeploymentSource {
	appRef := ""
	if tid := annotations[annoArgoTrackingID]; tid != "" {
		// tracking-id is "<app[/ns]>:<group>/<kind>:<ns>/<name>"; the app
		// reference is the segment before the first ":".
		appRef = tid
		if i := strings.Index(appRef, ":"); i != -1 {
			appRef = appRef[:i]
		}
	} else if inst := labels[labelArgoInstance]; inst != "" {
		// instance is overloaded (Helm sets it too) — only honor it when a
		// matching Application actually exists.
		appRef = inst
	}
	if appRef == "" {
		return nil
	}
	app := r.lookupArgo(appRef)
	if app == nil {
		return nil
	}
	return &model.DeploymentSource{
		Tool:     "argocd",
		RepoURL:  app.repoURL,
		Path:     app.path,
		Revision: app.revision,
		AppName:  app.name,
	}
}

func (r *Resolver) lookupArgo(appRef string) *argoSource {
	if app, ok := r.argoByNsName[appRef]; ok {
		return app
	}
	if i := strings.Index(appRef, "_"); i != -1 {
		ns, name := appRef[:i], appRef[i+1:]
		if app, ok := r.argoByNsName[ns+"/"+name]; ok {
			return app
		}
		if app, ok := r.argoByName[name]; ok {
			return app
		}
	}
	name := appRef
	if i := strings.LastIndex(appRef, "/"); i != -1 {
		name = appRef[i+1:]
	}
	if app, ok := r.argoByName[name]; ok {
		return app
	}
	return nil
}

func (r *Resolver) resolveFluxHelm(labels map[string]string) *model.DeploymentSource {
	name, ns := labels[labelFluxHelmName], labels[labelFluxHelmNS]
	if name == "" || ns == "" {
		return nil
	}
	src := &model.DeploymentSource{Tool: "flux-helm", AppName: name}
	if hr := r.helmReleases[ns+"/"+name]; hr != nil {
		r.applyGitSource(src, hr.source, ns)
	}
	return src
}

func (r *Resolver) resolveFluxKustomize(labels map[string]string) *model.DeploymentSource {
	name, ns := labels[labelFluxKustomizeName], labels[labelFluxKustomizeNS]
	if name == "" || ns == "" {
		return nil
	}
	src := &model.DeploymentSource{Tool: "flux-kustomize", AppName: name}
	if k := r.kustomizations[ns+"/"+name]; k != nil {
		src.Path = k.path
		r.applyGitSource(src, k.source, ns)
	}
	return src
}

// applyGitSource fills RepoURL/Revision from a Flux sourceRef pointing at a
// GitRepository. Non-Git sources (HelmRepository, OCIRepository) leave the repo
// blank — the source is still attributed via Tool/AppName.
func (r *Resolver) applyGitSource(dst *model.DeploymentSource, ref sourceRef, defaultNS string) {
	if ref.kind != "" && ref.kind != "GitRepository" {
		return
	}
	if ref.name == "" {
		return
	}
	ns := ref.namespace
	if ns == "" {
		ns = defaultNS
	}
	if gr := r.gitRepos[ns+"/"+ref.name]; gr != nil {
		dst.RepoURL = gr.url
		if dst.Revision == "" {
			dst.Revision = gr.branch
		}
	}
}

// fallbackSource produces a tool-less DeploymentSource when a concrete repo
// hint is present but no Argo/Flux CR matched.
func fallbackSource(labels, annotations map[string]string) *model.DeploymentSource {
	repo := fallbackRepoURL(labels, annotations)
	if repo == "" {
		return nil
	}
	return &model.DeploymentSource{
		Tool:    "",
		RepoURL: repo,
		AppName: labels["app.kubernetes.io/part-of"],
	}
}

func fallbackRepoURL(labels, annotations map[string]string) string {
	for _, k := range fallbackRepoKeys {
		if v := annotations[k]; v != "" {
			return v
		}
		if v := labels[k]; v != "" {
			return v
		}
	}
	return ""
}

// --- CR parsing -------------------------------------------------------------

func argoSourceFrom(cr kubernetes.GitOpsSourceInfo) *argoSource {
	s := &argoSource{name: cr.Name}
	repo, _, _ := unstructured.NestedString(cr.Object, "spec", "source", "repoURL")
	path, _, _ := unstructured.NestedString(cr.Object, "spec", "source", "path")
	rev, _, _ := unstructured.NestedString(cr.Object, "spec", "source", "targetRevision")
	if repo == "" {
		// Newer multi-source Applications use spec.sources[].
		if sources, ok, _ := unstructured.NestedSlice(cr.Object, "spec", "sources"); ok && len(sources) > 0 {
			if first, ok := sources[0].(map[string]interface{}); ok {
				repo, _, _ = unstructured.NestedString(first, "repoURL")
				path, _, _ = unstructured.NestedString(first, "path")
				rev, _, _ = unstructured.NestedString(first, "targetRevision")
			}
		}
	}
	s.repoURL, s.path, s.revision = repo, path, rev
	return s
}

func helmReleaseFrom(cr kubernetes.GitOpsSourceInfo) *fluxRelease {
	ref := sourceRefFrom(cr.Object, "spec", "chart", "spec", "sourceRef")
	if ref.name == "" {
		// chartRef form (HelmRelease v2 with a direct chart reference).
		ref = sourceRefFrom(cr.Object, "spec", "chartRef")
	}
	return &fluxRelease{name: cr.Name, source: ref}
}

func kustomizationFrom(cr kubernetes.GitOpsSourceInfo) *fluxKustomize {
	path, _, _ := unstructured.NestedString(cr.Object, "spec", "path")
	return &fluxKustomize{
		name:   cr.Name,
		path:   path,
		source: sourceRefFrom(cr.Object, "spec", "sourceRef"),
	}
}

func gitRepoFrom(cr kubernetes.GitOpsSourceInfo) *gitRepo {
	url, _, _ := unstructured.NestedString(cr.Object, "spec", "url")
	branch, _, _ := unstructured.NestedString(cr.Object, "spec", "ref", "branch")
	if branch == "" {
		if tag, _, _ := unstructured.NestedString(cr.Object, "spec", "ref", "tag"); tag != "" {
			branch = tag
		}
	}
	return &gitRepo{url: url, branch: branch}
}

func sourceRefFrom(obj map[string]interface{}, fields ...string) sourceRef {
	kind, _, _ := unstructured.NestedString(obj, append(fields, "kind")...)
	name, _, _ := unstructured.NestedString(obj, append(fields, "name")...)
	ns, _, _ := unstructured.NestedString(obj, append(fields, "namespace")...)
	return sourceRef{kind: kind, name: name, namespace: ns}
}
