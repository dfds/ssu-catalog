package api

import (
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"go.dfds.cloud/ssu-catalog/internal/api/handlers"
	"go.dfds.cloud/ssu-catalog/internal/model"
)

// Configure registers all routes on the given router. The /api/v1 group is
// wrapped by authMW (the OIDC middleware, or a pass-through when OIDC is
// disabled); /healthz and /readyz stay unauthenticated.
func Configure(
	router *gin.Engine,
	catalogPtr *atomic.Pointer[model.Catalog],
	cluster string,
	authMW gin.HandlerFunc,
) {
	health := handlers.NewHealth(catalogPtr)
	router.GET("/healthz", health.Healthz)
	router.GET("/readyz", health.Readyz)

	cat := handlers.NewCatalog(catalogPtr, cluster)

	v1 := router.Group("/api/v1")
	if authMW != nil {
		v1.Use(authMW)
	}

	v1.GET("/catalog", cat.GetCatalog)
	v1.GET("/catalog/stats", cat.GetStats)
	v1.GET("/applications", cat.ListApplications)
	v1.GET("/applications/:namespace/:name", cat.GetApplication)
	v1.GET("/namespaces", cat.ListNamespaces)
	v1.GET("/namespaces/:namespace", cat.GetNamespace)
	v1.GET("/dependencies", cat.ListDependencies)
	v1.GET("/dependencies/:namespace/:name", cat.GetApplicationDependencies)
}
