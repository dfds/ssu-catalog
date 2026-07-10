package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus collectors for ssu-catalog. The const cluster
// label distinguishes per-cluster instances in a shared metrics backend.
type Metrics struct {
	ApplicationsTotal            prometheus.Gauge
	CapabilityOwnedApplications  prometheus.Gauge
	ApplicationsWithDocs         prometheus.Gauge
	ApplicationsWithDeploySource prometheus.Gauge
	DependenciesTotal            prometheus.Gauge

	ScrapeDuration prometheus.Histogram
	ScrapeErrors   prometheus.Counter

	SwaggerProbes prometheus.Counter
	SwaggerHits   prometheus.Counter

	ReachabilityProbes      prometheus.Counter
	ReachabilityReachable   prometheus.Counter
	ReachabilityUnreachable prometheus.Counter
	ReachabilityUnknown     prometheus.Counter
	ReachabilityDuration    prometheus.Histogram

	TelemetryQueryErrors prometheus.Counter
	AuthRejections       prometheus.Counter
}

// NewMetrics registers and returns the metric set. The cluster name is attached
// as a const label on every series.
func NewMetrics(clusterName string) *Metrics {
	constLabels := prometheus.Labels{"cluster_name": clusterName}

	gauge := func(name, help string) prometheus.Gauge {
		return promauto.NewGauge(prometheus.GaugeOpts{
			Name:        name,
			Help:        help,
			ConstLabels: constLabels,
		})
	}
	counter := func(name, help string) prometheus.Counter {
		return promauto.NewCounter(prometheus.CounterOpts{
			Name:        name,
			Help:        help,
			ConstLabels: constLabels,
		})
	}

	return &Metrics{
		ApplicationsTotal:            gauge("ssu_catalog_applications_total", "Total applications discovered in the cluster"),
		CapabilityOwnedApplications:  gauge("ssu_catalog_capability_owned_applications_total", "Applications in capability-owned namespaces"),
		ApplicationsWithDocs:         gauge("ssu_catalog_applications_with_docs_total", "Applications with detected OpenAPI/Swagger docs"),
		ApplicationsWithDeploySource: gauge("ssu_catalog_applications_with_deploy_source_total", "Applications with a resolved GitOps deployment source"),
		DependenciesTotal:            gauge("ssu_catalog_dependencies_total", "Total dependency edges in the overlay"),
		ScrapeDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:        "ssu_catalog_scrape_duration_seconds",
			Help:        "Duration of the catalog collection cycle",
			Buckets:     prometheus.DefBuckets,
			ConstLabels: constLabels,
		}),
		ScrapeErrors:         counter("ssu_catalog_scrape_errors_total", "Total failed collection cycles"),
		SwaggerProbes:           counter("ssu_catalog_swagger_probes_total", "Total OpenAPI/Swagger probe requests issued"),
		SwaggerHits:             counter("ssu_catalog_swagger_hits_total", "Total OpenAPI/Swagger probe hits"),
		ReachabilityProbes:      counter("ssu_catalog_reachability_probes_total", "Total ingress reachability probes issued"),
		ReachabilityReachable:   counter("ssu_catalog_reachability_reachable_total", "Total reachability verdicts: reachable"),
		ReachabilityUnreachable: counter("ssu_catalog_reachability_unreachable_total", "Total reachability verdicts: unreachable"),
		ReachabilityUnknown:     counter("ssu_catalog_reachability_unknown_total", "Total reachability verdicts: unknown (transport error)"),
		ReachabilityDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:        "ssu_catalog_reachability_duration_seconds",
			Help:        "Duration of a reachability probe cycle",
			Buckets:     prometheus.DefBuckets,
			ConstLabels: constLabels,
		}),
		TelemetryQueryErrors: counter("ssu_catalog_telemetry_query_errors_total", "Total failed telemetry (Mimir) queries"),
		AuthRejections:       counter("ssu_catalog_auth_rejections_total", "Total rejected inbound API requests"),
	}
}

// Publish sets the gauge values from a finished collection's stats.
func (m *Metrics) Publish(
	totalApplications,
	capabilityOwned,
	withDocs,
	withDeploySource,
	dependencies int,
) {
	m.ApplicationsTotal.Set(float64(totalApplications))
	m.CapabilityOwnedApplications.Set(float64(capabilityOwned))
	m.ApplicationsWithDocs.Set(float64(withDocs))
	m.ApplicationsWithDeploySource.Set(float64(withDeploySource))
	m.DependenciesTotal.Set(float64(dependencies))
}
