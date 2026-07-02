package handlers

import (
	"net/http"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"go.dfds.cloud/ssu-catalog/internal/model"
)

// Health exposes liveness/readiness. Readiness flips true after the first
// successful collection (catalog pointer is non-nil).
type Health struct {
	catalog *atomic.Pointer[model.Catalog]
}

func NewHealth(catalog *atomic.Pointer[model.Catalog]) *Health {
	return &Health{catalog: catalog}
}

// Healthz always returns 200 — the process is up.
func (h *Health) Healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Readyz returns 200 once a catalog snapshot is available, 503 otherwise.
func (h *Health) Readyz(c *gin.Context) {
	if h.catalog.Load() == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "collecting"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}
