package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.dfds.cloud/bootstrap"
	"go.dfds.cloud/ssu-catalog/internal/api"
	"go.dfds.cloud/ssu-catalog/internal/auth"
	"go.dfds.cloud/ssu-catalog/internal/collector"
	"go.dfds.cloud/ssu-catalog/internal/config"
	"go.dfds.cloud/ssu-catalog/internal/kubernetes"
	"go.dfds.cloud/ssu-catalog/internal/metrics"
	"go.dfds.cloud/ssu-catalog/internal/model"
	"go.dfds.cloud/ssu-catalog/internal/reachability"
	"go.dfds.cloud/ssu-catalog/internal/swagger"
	"go.dfds.cloud/ssu-catalog/internal/telemetry"
	"go.uber.org/zap"
)

func main() {
	conf, err := config.Load()
	if err != nil {
		panic(err)
	}

	builder := bootstrap.Builder()
	builder.EnableLogging(conf.LogDebug, conf.LogLevel)
	builder.EnableHttpRouterWithAddr(conf.LogDebug, fmt.Sprintf(":%d", conf.APIPort))
	builder.EnableMetrics()
	manager := builder.Build()
	log := manager.Logger
	defer log.Sync() //nolint:errcheck

	log.Info("ssu-catalog launching", zap.String("cluster", conf.ClusterName))

	m := metrics.NewMetrics(conf.ClusterName)

	// Live catalog snapshot, swapped atomically by the worker each cycle.
	catalogPtr := &atomic.Pointer[model.Catalog]{}

	// Reachability verdicts, produced by a separate worker on its own interval and
	// overlaid onto the catalog at serve time.
	reachStore := reachability.NewStore()

	// OIDC middleware (or pass-through for local dev).
	authMW := auth.DisabledMiddleware()
	if conf.OIDC.Enabled {
		verifier, err := auth.NewVerifier(manager.Context, conf.OIDC.IssuerURL, conf.OIDC.Audience, conf.OIDC.RequiredRoles, log)
		if err != nil {
			log.Fatal("failed to initialise OIDC verifier", zap.Error(err))
		}
		go verifier.Run(manager.Context, time.Hour)
		authMW = verifier.Middleware(func() { m.AuthRejections.Inc() })
	} else {
		log.Warn("OIDC validation is DISABLED — local dev only")
	}

	api.Configure(manager.HttpRouter, catalogPtr, reachStore, conf.ClusterName, authMW)

	go worker(manager.Context, conf, m, catalogPtr, log)
	if conf.Reachability.Enabled {
		go reachabilityWorker(manager.Context, conf, m, catalogPtr, reachStore, log)
	} else {
		log.Info("ingress reachability probing is disabled")
	}

	<-manager.Context.Done()
	if err := manager.HttpServer.Shutdown(context.Background()); err != nil {
		log.Info("HTTP server did not shut down gracefully", zap.Error(err))
	}
	log.Info("ssu-catalog shutting down")
}

func worker(
	ctx context.Context,
	conf config.Config,
	m *metrics.Metrics,
	catalogPtr *atomic.Pointer[model.Catalog],
	log *zap.Logger,
) {
	k8sClient, err := kubernetes.NewClient(kubernetes.Options{
		Concurrency:      conf.Kubernetes.Concurrency,
		NamespaceExclude: conf.Kubernetes.NamespaceExclude,
		NamespaceInclude: conf.Kubernetes.NamespaceInclude,
		GitOpsEnabled:    conf.GitOps.Enabled,
		QPS:              conf.Kubernetes.QPS,
		Burst:            conf.Kubernetes.Burst,
	})
	if err != nil {
		log.Fatal("failed to create Kubernetes client", zap.Error(err))
	}

	var prober *swagger.Prober
	if conf.Swagger.Enabled {
		prober = swagger.NewProber(
			time.Duration(conf.Swagger.TimeoutMs)*time.Millisecond,
			conf.Swagger.Concurrency,
			m.SwaggerProbes,
			m.SwaggerHits,
		)
	} else {
		log.Info("OpenAPI/Swagger probing is disabled")
	}

	var overlayer *telemetry.Overlayer
	if conf.Telemetry.Enabled {
		tClient := telemetry.NewClient(
			conf.Telemetry.MimirURL,
			conf.Telemetry.BasicAuthUser,
			conf.Telemetry.BasicAuthToken,
			time.Duration(conf.Telemetry.TimeoutMs)*time.Millisecond,
		)
		overlayer = telemetry.NewOverlayer(
			tClient,
			conf.ClusterName,
			time.Duration(conf.Telemetry.LookbackMinutes)*time.Minute,
			m.TelemetryQueryErrors,
			log,
		)
	} else {
		log.Info("telemetry overlay is disabled")
	}

	coll := collector.NewCollector(conf.ClusterName, k8sClient, prober, overlayer, log)
	interval := time.Duration(conf.WorkerInterval) * time.Second

	for {
		log.Info("collecting catalog")
		timer := prometheus.NewTimer(m.ScrapeDuration)
		catalog, err := coll.Collect(ctx)
		timer.ObserveDuration()

		if err != nil {
			log.Error("collection failed", zap.Error(err))
			m.ScrapeErrors.Inc()
		} else {
			catalogPtr.Store(catalog)
			m.Publish(
				catalog.Stats.TotalApplications,
				catalog.Stats.CapabilityOwnedApplications,
				catalog.Stats.ApplicationsWithDocs,
				catalog.Stats.ApplicationsWithDeploySource,
				catalog.Stats.TotalDependencies,
			)
			log.Info("catalog published",
				zap.Int("applications", catalog.Stats.TotalApplications),
				zap.Int("capability_owned", catalog.Stats.CapabilityOwnedApplications),
				zap.Int64("duration_ms", catalog.Stats.CollectionDurationMs),
			)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// reachabilityWorker probes exposed ingress hosts on its own cadence, decoupled
// from the collection cycle. It reads the live catalog snapshot read-only and
// writes fresh verdicts into the store each tick (full rebuild → self-pruning).
// It skips cleanly until the first catalog exists.
func reachabilityWorker(
	ctx context.Context,
	conf config.Config,
	m *metrics.Metrics,
	catalogPtr *atomic.Pointer[model.Catalog],
	store *reachability.Store,
	log *zap.Logger,
) {
	prober := reachability.NewProber(
		time.Duration(conf.Reachability.TimeoutMs)*time.Millisecond,
		conf.Reachability.Concurrency,
		log,
		reachability.Counters{
			Probes:      m.ReachabilityProbes,
			Reachable:   m.ReachabilityReachable,
			Unreachable: m.ReachabilityUnreachable,
			Unknown:     m.ReachabilityUnknown,
			Duration:    m.ReachabilityDuration,
		},
	)
	interval := time.Duration(conf.Reachability.Interval) * time.Second

	for {
		if cat := catalogPtr.Load(); cat != nil {
			results := prober.Probe(ctx, cat)
			store.Store(results)
			log.Info("reachability probing complete", zap.Int("hosts", len(results)))
		} else {
			log.Debug("reachability probing skipped: no catalog yet")
		}

		// Jitter (up to 10% of the interval) spreads probe cycles so multiple
		// instances don't hammer shared ingress hosts in lockstep.
		jitter := time.Duration(rand.Int63n(int64(interval)/10 + 1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval + jitter):
		}
	}
}
