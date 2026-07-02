# ssu-catalog

A Go service that builds a live, **application-centric catalog** of Kubernetes workloads for a
single cluster, maps them to DFDS Capabilities, links each to its repository and deployment
source (Argo/Flux), detects API documentation, and overlays a best-effort runtime dependency
graph from Grafana Cloud telemetry. The catalog is served from an OIDC-protected REST API.

It runs **one instance per cluster**. `selfservice-api` is the sole caller: it holds a registry
of per-cluster endpoints, fans out, joins each catalog with its own authoritative capability /
owner / Kafka data, and serves the merged result to `selfservice-portal`. ssu-catalog itself
keeps everything in memory (latest-only, no database) and rebuilds it every cycle.

## Configuration

All configuration comes from the environment with the **`SSUC_`** prefix (via
`kelseyhightower/envconfig`). Defaults and validation are applied in `internal/config/config.go`.

| Variable | Required | Default | Description |
|---|---|---|---|
| `SSUC_CLUSTERNAME` | **Yes** | — | Cluster name; first-class on every entry and metric (`cluster_name` label) |
| `SSUC_LOGLEVEL` | No | `info` | Zap log level |
| `SSUC_LOGDEBUG` | No | `false` | Enable debug logging |
| `SSUC_WORKERINTERVAL` | No | `300` | Collection interval (seconds) |
| `SSUC_APIPORT` | No | `8080` | Port the OIDC-protected REST API listens on (metrics stay on `:9090`) |
| `SSUC_KUBERNETES_CONCURRENCY` | No | `10` | Max concurrent namespace scans (`errgroup` limit) |
| `SSUC_KUBERNETES_QPS` | No | `50` | client-go client-side rate limiter QPS (raises client-go's default of 5 to avoid throttling) |
| `SSUC_KUBERNETES_BURST` | No | `100` | client-go client-side rate limiter burst (raises client-go's default of 10) |
| `SSUC_KUBERNETES_NAMESPACEEXCLUDE` | No | — | Comma-separated namespaces to skip (deny-list). Mutually exclusive with `SSUC_KUBERNETES_NAMESPACEINCLUDE` |
| `SSUC_KUBERNETES_NAMESPACEINCLUDE` | No | — | Comma-separated namespaces to scan exclusively (allow-list). Mutually exclusive with `SSUC_KUBERNETES_NAMESPACEEXCLUDE`; setting both exits with an error |
| `SSUC_OIDC_ENABLED` | No | `true` | Validate inbound Bearer tokens (`false` for local dev only) |
| `SSUC_OIDC_ISSUERURL` | If OIDC on | — | Azure AD issuer (must match the caller's token version) |
| `SSUC_OIDC_AUDIENCE` | If OIDC on | — | ssu-catalog app-registration client ID (token `aud`) |
| `SSUC_OIDC_REQUIREDROLES` | No | `Catalog.Read` | Required app role(s); comma-separated |
| `SSUC_GITOPS_ENABLED` | No | `true` | Discover Argo/Flux CRs for repo & deployment links |
| `SSUC_SWAGGER_ENABLED` | No | `true` | OpenAPI/Swagger detection |
| `SSUC_SWAGGER_TIMEOUTMS` | No | `2000` | Per-probe HTTP timeout (ms) |
| `SSUC_SWAGGER_CONCURRENCY` | No | `20` | Max concurrent probes |
| `SSUC_TELEMETRY_ENABLED` | No | `false` | Service-graph dependency overlay |
| `SSUC_TELEMETRY_MIMIRURL` | If telemetry on | — | Grafana Cloud Mimir base URL (e.g. `https://<stack>.grafana.net/api/prom`) |
| `SSUC_TELEMETRY_BASICAUTHUSER` | No | — | Grafana Cloud user / instance ID |
| `SSUC_TELEMETRY_BASICAUTHTOKEN` | No | — | Grafana Cloud token (`metrics:read`) — **secret**, inject via `envFromSecret` |
| `SSUC_TELEMETRY_LOOKBACKMINUTES` | No | `60` | Metric query lookback window |

**Conditional validation** (enforced at startup in `Load()`):

- `SSUC_CLUSTERNAME` is **always** required.
- When `SSUC_OIDC_ENABLED=true`, both `SSUC_OIDC_ISSUERURL` and `SSUC_OIDC_AUDIENCE` are required.
- When `SSUC_TELEMETRY_ENABLED=true`, `SSUC_TELEMETRY_MIMIRURL` is required.

## REST API

Base path `/api/v1`; JSON. All `/api/v1/*` routes require a valid Bearer token with the
`Catalog.Read` role. `/healthz` and `/readyz` are unauthenticated.

| Method | Path | Description |
|---|---|---|
| `GET` | `/healthz` | Liveness (always 200) |
| `GET` | `/readyz` | Readiness (200 after the first collection) |
| `GET` | `/api/v1/catalog` | Full snapshot (applications + namespaces + dependencies + stats) |
| `GET` | `/api/v1/catalog/stats` | Stats only |
| `GET` | `/api/v1/applications` | Applications. Filters: `?namespace=&capabilityId=&kind=&hasDocs=&q=` (`q` = name substring) |
| `GET` | `/api/v1/applications/:namespace/:name` | One application (workload + services + docs + deploy source + deps + topics + dbs) |
| `GET` | `/api/v1/namespaces` | Namespaces (`?capabilityId=` filter) |
| `GET` | `/api/v1/namespaces/:namespace` | Namespace detail + its applications |
| `GET` | `/api/v1/dependencies` | Dependency overlay (`?namespace=&type=` filters) |
| `GET` | `/api/v1/dependencies/:namespace/:name` | Inbound + outbound edges for one application |

Every response uses the envelope below; each entry already carries `cluster`, so a merged
multi-cluster response from `selfservice-api` stays unambiguous:

```json
{
  "data": [ ... ],
  "meta": { "collectedAt": "2026-06-30T12:00:00Z", "cluster": "hellman" }
}
```

## Metrics

Exposed on `:9090/metrics`. Every series carries a `cluster_name` const label.

| Metric | Type | Description |
|---|---|---|
| `ssu_catalog_applications_total` | gauge | Total applications discovered |
| `ssu_catalog_capability_owned_applications_total` | gauge | Applications in capability-owned namespaces |
| `ssu_catalog_applications_with_docs_total` | gauge | Applications with detected OpenAPI/Swagger docs |
| `ssu_catalog_applications_with_deploy_source_total` | gauge | Applications with a resolved GitOps deployment source |
| `ssu_catalog_dependencies_total` | gauge | Total dependency edges in the overlay |
| `ssu_catalog_scrape_duration_seconds` | histogram | Duration of the collection cycle |
| `ssu_catalog_scrape_errors_total` | counter | Failed collection cycles |
| `ssu_catalog_swagger_probes_total` | counter | OpenAPI/Swagger probe requests issued |
| `ssu_catalog_swagger_hits_total` | counter | OpenAPI/Swagger probe hits |
| `ssu_catalog_telemetry_query_errors_total` | counter | Failed telemetry (Mimir) queries |
| `ssu_catalog_auth_rejections_total` | counter | Rejected inbound API requests |

## Building

```bash
make build          # → bin/ssu-catalog
make docker-build   # → dfdsdk/ssu-catalog:latest
```

## Running locally

OIDC off is **local development only** — never set `SSUC_OIDC_ENABLED=false` in a deployed
environment. With a valid `KUBECONFIG`:

```bash
SSUC_CLUSTERNAME=local SSUC_LOGDEBUG=true SSUC_WORKERINTERVAL=30 \
SSUC_OIDC_ENABLED=false SSUC_SWAGGER_ENABLED=true SSUC_TELEMETRY_ENABLED=false \
make run

curl -s http://localhost:8080/api/v1/catalog/stats | jq .
curl -s 'http://localhost:8080/api/v1/applications?capabilityId=' | jq '.data[].deploymentSource'
curl -s http://localhost:8080/api/v1/dependencies | jq .
curl -s http://localhost:9090/metrics | grep ssu_catalog_
```

## Deployment (Helm)

The chart under `chart/` ships everything needed to run one instance:

- **ClusterRole/Binding** — read-only `get`/`list` on namespaces, services, pods, the apps
  workloads, ingresses, networkpolicies, and the Argo/Flux CRDs (the dynamic client tolerates
  absent CRDs).
- **Traefik IngressRoute** — public exposure so `selfservice-api` can reach it across clusters;
  auth is enforced in-app via OIDC, not at the edge. Set `ingress.host`.
- **ServiceMonitor + VMServiceScrape** — both ship (each gated by a values toggle) to cover
  Prometheus-operator and VictoriaMetrics-operator clusters.
- **Secrets** — provide `SSUC_TELEMETRY_BASICAUTHTOKEN` via a Secret referenced by
  `envFromSecret`; non-secret config sits under `env` in `values.yaml`.

```bash
helm template chart/ | grep -E 'kind: (ServiceMonitor|VMServiceScrape|IngressRoute|ClusterRole)'
helm upgrade --install ssu-catalog chart/ \
  --set env.SSUC_CLUSTERNAME=hellman \
  --set ingress.host=ssu-catalog.hellman.oxygen.dfds.cloud
```

## How it works

One collection cycle (`internal/collector/collector.go`):

1. **Scan** the cluster (all namespaces, `errgroup`-bounded) → `K8sSnapshot` of namespaces,
   workloads, services, ingresses, network policies, and GitOps CRs.
2. **Assemble applications** — one per workload with its matched Services attached; unclaimed
   Services become `Kind:"Service"` entries. Capability is taken from the namespace label.
3. **Attribute deployment source** — per-workload Argo/Flux tracking metadata →
   `DeploymentSource` + `RepoURL`.
4. **Probe** declared ports for OpenAPI/Swagger docs (if enabled) → `APIDocs` on each Service.
5. **Overlay telemetry** (if enabled) → dependency edges + per-app Databases/Kafka topics,
   degrading gracefully on query failure.
6. **Add NetworkPolicy egress** as declared (`network_policy`) dependency edges.
7. **Compute stats** and atomically swap in the new snapshot, served live by the API.
