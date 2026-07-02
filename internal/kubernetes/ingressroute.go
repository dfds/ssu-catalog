package kubernetes

import (
	"context"
	"regexp"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// IngressRouteInfo is a Traefik IngressRoute (CRD) reduced to what links it to a
// Service: the external hostnames and route detail per backing service. It is
// the IngressRoute analogue of IngressInfo — both feed ServiceRef.ExternalHosts.
type IngressRouteInfo struct {
	Namespace   string
	Name        string
	EntryPoints []string // spec.entryPoints (spec-level, applies to every route)
	TLS         bool     // spec.tls present
	Routes      []IngressRouteRule
}

// IngressRouteRule is one entry of spec.routes[]: the hosts/paths parsed from its
// match expression and the in-namespace Services it targets.
type IngressRouteRule struct {
	Hosts        []string // parsed from match Host(`…`) / HostSNI(`…`)
	PathPrefixes []string // parsed from match PathPrefix(`…`) / Path(`…`)
	Services     []string // route.services[].name (resolved in the IngressRoute's namespace)
}

var ingressRouteResources = []schema.GroupVersionResource{
	{Group: "traefik.io", Version: "v1alpha1", Resource: "ingressroutes"},
	{Group: "traefik.containo.us", Version: "v1alpha1", Resource: "ingressroutes"}, // legacy
}

// listIngressRoutes best-effort lists Traefik IngressRoutes cluster-wide via the
// dynamic client, mirroring listGitOps. Any error for a given GVR (CRD absent,
// RBAC denied) is silently skipped, and results are filtered to in-scope
// namespaces and deduped by namespace/name across the two API groups.
func (c *Client) listIngressRoutes(ctx context.Context) []IngressRouteInfo {
	if c.dynamicClient == nil {
		return nil
	}
	var result []IngressRouteInfo
	seen := make(map[string]struct{})
	for _, gvr := range ingressRouteResources {
		list, err := c.dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{})
		if err != nil {
			// CRD not installed, RBAC denied, or discovery miss — skip silently.
			continue
		}
		for i := range list.Items {
			item := list.Items[i]
			ns := item.GetNamespace()
			if !c.shouldScanNamespace(ns) {
				continue
			}
			key := ns + "/" + item.GetName()
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			result = append(result, ingressRouteInfoFrom(item.Object))
		}
	}
	return result
}

// ingressRouteInfoFrom reduces a raw IngressRoute object to an IngressRouteInfo.
func ingressRouteInfoFrom(obj map[string]interface{}) IngressRouteInfo {
	u := unstructured.Unstructured{Object: obj}
	info := IngressRouteInfo{
		Namespace: u.GetNamespace(),
		Name:      u.GetName(),
	}

	entryPoints, _, _ := unstructured.NestedStringSlice(obj, "spec", "entryPoints")
	info.EntryPoints = entryPoints

	// spec.tls present (even as an empty object) means TLS is enabled.
	if _, found, _ := unstructured.NestedMap(obj, "spec", "tls"); found {
		info.TLS = true
	}

	routes, _, _ := unstructured.NestedSlice(obj, "spec", "routes")
	for _, r := range routes {
		rm, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		match, _, _ := unstructured.NestedString(rm, "match")
		rule := IngressRouteRule{
			Hosts:        parseMatchHosts(match),
			PathPrefixes: parseMatchPaths(match),
		}
		svcs, _, _ := unstructured.NestedSlice(rm, "services")
		for _, s := range svcs {
			sm, ok := s.(map[string]interface{})
			if !ok {
				continue
			}
			if name, _, _ := unstructured.NestedString(sm, "name"); name != "" {
				rule.Services = appendUnique(rule.Services, name)
			}
		}
		info.Routes = append(info.Routes, rule)
	}
	return info
}

// Traefik match expressions quote literal arguments in backticks, e.g.
// Host(`a.example.com`) || Host(`b.example.com`) && PathPrefix(`/api`). These
// extract the literal hosts and path prefixes best-effort; regex-based matchers
// (HostRegexp/PathRegexp) are intentionally skipped — they carry patterns, not
// literal values.
var (
	hostMatcherRe = regexp.MustCompile("(?i)\\b(?:Host|HostSNI)\\(([^)]*)\\)")
	pathMatcherRe = regexp.MustCompile("(?i)\\b(?:PathPrefix|Path)\\(([^)]*)\\)")
	backtickArgRe = regexp.MustCompile("`([^`]*)`")
)

// parseMatchHosts extracts literal hostnames from a Traefik match expression.
func parseMatchHosts(match string) []string { return extractBacktickArgs(hostMatcherRe, match) }

// parseMatchPaths extracts literal path prefixes from a Traefik match expression.
func parseMatchPaths(match string) []string { return extractBacktickArgs(pathMatcherRe, match) }

func extractBacktickArgs(callRe *regexp.Regexp, match string) []string {
	var out []string
	for _, call := range callRe.FindAllStringSubmatch(match, -1) {
		for _, arg := range backtickArgRe.FindAllStringSubmatch(call[1], -1) {
			if v := strings.TrimSpace(arg[1]); v != "" {
				out = appendUnique(out, v)
			}
		}
	}
	return out
}
