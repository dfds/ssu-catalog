package handlers

import (
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"go.dfds.cloud/ssu-catalog/internal/model"
	"go.dfds.cloud/ssu-catalog/internal/reachability"
)

// Catalog serves the in-memory snapshot. All reads are lock-free via the atomic
// pointer; the worker swaps in a fresh *Catalog each cycle. Reachability verdicts
// live in a separate store and are overlaid onto served applications at read time.
type Catalog struct {
	catalog *atomic.Pointer[model.Catalog]
	reach   *reachability.Store
	cluster string
}

func NewCatalog(catalog *atomic.Pointer[model.Catalog], reach *reachability.Store, cluster string) *Catalog {
	return &Catalog{catalog: catalog, reach: reach, cluster: cluster}
}

// overlayApp returns a shallow copy of app with each ServiceRef's Reachability
// filled from the store, keyed by "namespace/service/host". It never mutates the
// shared catalog snapshot — the Services slice and the affected ServiceRef values
// are copied before Reachability is set. Apps with no matching verdicts are
// returned unchanged.
func (h *Catalog) overlayApp(app model.ApplicationEntry) model.ApplicationEntry {
	if h.reach == nil || len(app.Services) == 0 {
		return app
	}
	var services []model.ServiceRef
	changed := false
	for i, svc := range app.Services {
		var results []model.ReachabilityResult
		for _, host := range svc.ExternalHosts {
			if r, ok := h.reach.Lookup(app.Namespace + "/" + svc.Name + "/" + host); ok {
				results = append(results, r)
			}
		}
		if len(results) == 0 {
			continue
		}
		if !changed {
			services = make([]model.ServiceRef, len(app.Services))
			copy(services, app.Services)
			changed = true
		}
		services[i].Reachability = results
	}
	if !changed {
		return app
	}
	app.Services = services
	return app
}

// overlayApps maps overlayApp over a slice, returning a new slice.
func (h *Catalog) overlayApps(apps []model.ApplicationEntry) []model.ApplicationEntry {
	out := make([]model.ApplicationEntry, len(apps))
	for i, app := range apps {
		out[i] = h.overlayApp(app)
	}
	return out
}

func (h *Catalog) snapshot() *model.Catalog {
	if cat := h.catalog.Load(); cat != nil {
		return cat
	}
	return &model.Catalog{Cluster: h.cluster}
}

func (h *Catalog) meta(cat *model.Catalog) gin.H {
	return gin.H{"collectedAt": cat.CollectedAt, "publishedAt": cat.PublishedAt, "cluster": cat.Cluster}
}

func (h *Catalog) respond(c *gin.Context, cat *model.Catalog, data any) {
	c.JSON(http.StatusOK, gin.H{"data": data, "meta": h.meta(cat)})
}

// GetCatalog returns the full snapshot with reachability overlaid onto its
// applications. cat is shallow-copied so the shared snapshot is never mutated.
func (h *Catalog) GetCatalog(c *gin.Context) {
	cat := h.snapshot()
	view := *cat
	view.Applications = h.overlayApps(cat.Applications)
	c.JSON(http.StatusOK, gin.H{"data": &view, "meta": h.meta(cat)})
}

// GetStats returns just the stats block.
func (h *Catalog) GetStats(c *gin.Context) {
	cat := h.snapshot()
	h.respond(c, cat, cat.Stats)
}

// ListApplications returns applications with optional filters:
// ?namespace=&capabilityId=&kind=&hasDocs=&q=
func (h *Catalog) ListApplications(c *gin.Context) {
	cat := h.snapshot()

	namespace := c.Query("namespace")
	kind := c.Query("kind")
	query := strings.ToLower(c.Query("q"))
	capabilityID, capabilityFilter := c.GetQuery("capabilityId")
	hasDocsRaw, hasDocsFilter := c.GetQuery("hasDocs")
	hasDocsWant := hasDocsRaw == "true" || hasDocsRaw == "1"

	out := make([]model.ApplicationEntry, 0, len(cat.Applications))
	for _, app := range cat.Applications {
		if namespace != "" && app.Namespace != namespace {
			continue
		}
		if kind != "" && app.Kind != kind {
			continue
		}
		// capabilityId filter is exact-match; an empty value selects unowned apps.
		if capabilityFilter && app.CapabilityID != capabilityID {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(app.Name), query) {
			continue
		}
		if hasDocsFilter && applicationHasDocs(app) != hasDocsWant {
			continue
		}
		out = append(out, h.overlayApp(app))
	}

	h.respond(c, cat, out)
}

// GetApplication returns one application by namespace + name.
func (h *Catalog) GetApplication(c *gin.Context) {
	cat := h.snapshot()
	namespace := c.Param("namespace")
	name := c.Param("name")

	for _, app := range cat.Applications {
		if app.Namespace == namespace && app.Name == name {
			h.respond(c, cat, h.overlayApp(app))
			return
		}
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "application not found"})
}

// ListNamespaces returns namespaces with an optional ?capabilityId= filter.
func (h *Catalog) ListNamespaces(c *gin.Context) {
	cat := h.snapshot()
	capabilityID, capabilityFilter := c.GetQuery("capabilityId")

	out := make([]model.NamespaceEntry, 0, len(cat.Namespaces))
	for _, ns := range cat.Namespaces {
		if capabilityFilter && ns.CapabilityID != capabilityID {
			continue
		}
		out = append(out, ns)
	}
	h.respond(c, cat, out)
}

// GetNamespace returns a namespace plus the applications running in it.
func (h *Catalog) GetNamespace(c *gin.Context) {
	cat := h.snapshot()
	name := c.Param("namespace")

	var found *model.NamespaceEntry
	for i := range cat.Namespaces {
		if cat.Namespaces[i].Name == name {
			found = &cat.Namespaces[i]
			break
		}
	}
	if found == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "namespace not found"})
		return
	}

	apps := make([]model.ApplicationEntry, 0)
	for _, app := range cat.Applications {
		if app.Namespace == name {
			apps = append(apps, h.overlayApp(app))
		}
	}
	h.respond(c, cat, gin.H{"namespace": found, "applications": apps})
}

// ListDependencies returns the dependency overlay with ?namespace=&type= filters.
func (h *Catalog) ListDependencies(c *gin.Context) {
	cat := h.snapshot()
	namespace := c.Query("namespace")
	depType := c.Query("type")

	out := make([]model.DependencyEdge, 0, len(cat.Dependencies))
	for _, dep := range cat.Dependencies {
		if depType != "" && dep.Type != depType {
			continue
		}
		if namespace != "" && dep.Source.Namespace != namespace && dep.Target.Namespace != namespace {
			continue
		}
		out = append(out, dep)
	}
	h.respond(c, cat, out)
}

// GetApplicationDependencies returns inbound + outbound edges for one application.
func (h *Catalog) GetApplicationDependencies(c *gin.Context) {
	cat := h.snapshot()
	namespace := c.Param("namespace")
	name := c.Param("name")

	inbound := make([]model.DependencyEdge, 0)
	outbound := make([]model.DependencyEdge, 0)
	for _, dep := range cat.Dependencies {
		if dep.Source.Namespace == namespace && dep.Source.Service == name {
			outbound = append(outbound, dep)
		}
		if dep.Target.Namespace == namespace && dep.Target.Service == name {
			inbound = append(inbound, dep)
		}
	}
	h.respond(c, cat, gin.H{"inbound": inbound, "outbound": outbound})
}

// ListReachability returns the full reachability store — every current verdict,
// keyed store flattened to a sorted list — for consumers that want the verdicts
// directly rather than overlaid onto applications.
func (h *Catalog) ListReachability(c *gin.Context) {
	cat := h.snapshot()
	var results []model.ReachabilityResult
	if h.reach != nil {
		results = h.reach.All()
	} else {
		results = []model.ReachabilityResult{}
	}
	h.respond(c, cat, results)
}

func applicationHasDocs(app model.ApplicationEntry) bool {
	for _, svc := range app.Services {
		if len(svc.APIDocs) > 0 {
			return true
		}
	}
	return false
}
