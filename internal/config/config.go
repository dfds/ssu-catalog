package config

import (
	"fmt"
	"strings"

	"github.com/kelseyhightower/envconfig"
)

// hasEntries reports whether the slice contains at least one non-blank value.
// Blank/whitespace-only entries are ignored so that an empty env var (which
// envconfig may surface as a single empty element) doesn't count as "set".
func hasEntries(values []string) bool {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return true
		}
	}
	return false
}

// Config holds the full runtime configuration. Populated from the environment
// with the SSUC_ prefix (e.g. SSUC_CLUSTERNAME, SSUC_OIDC_ISSUERURL).
type Config struct {
	LogLevel       string `json:"logLevel"`
	LogDebug       bool   `json:"logDebug"`
	WorkerInterval int    `json:"workerInterval"`
	ClusterName    string `json:"clusterName"`
	// APIPort is the port the OIDC-protected REST API listens on. The Prometheus
	// metrics server (bootstrap) stays on its own port (:9090).
	APIPort int `json:"apiPort"`

	Kubernetes struct {
		Concurrency int `json:"concurrency"`
		// NamespaceExclude and NamespaceInclude are mutually exclusive namespace
		// filters. With NamespaceInclude set, ONLY those namespaces are scanned;
		// with NamespaceExclude set, all namespaces except those are scanned.
		// Setting both is a configuration error (see Load).
		NamespaceExclude []string `json:"namespaceExclude"`
		NamespaceInclude []string `json:"namespaceInclude"`
		QPS              float32  `json:"qps"`
		Burst            int      `json:"burst"`
	} `json:"kubernetes"`

	OIDC struct {
		Enabled       bool     `json:"enabled"`
		IssuerURL     string   `json:"issuerUrl"`
		Audience      string   `json:"audience"`
		RequiredRoles []string `json:"requiredRoles"`
	} `json:"oidc"`

	GitOps struct {
		Enabled bool `json:"enabled"`
	} `json:"gitops"`

	Swagger struct {
		Enabled     bool `json:"enabled"`
		TimeoutMs   int  `json:"timeoutMs"`
		Concurrency int  `json:"concurrency"`
	} `json:"swagger"`

	Telemetry struct {
		Enabled         bool   `json:"enabled"`
		MimirURL        string `json:"mimirUrl"`
		BasicAuthUser   string `json:"basicAuthUser"`
		BasicAuthToken  string `json:"basicAuthToken"`
		LookbackMinutes int    `json:"lookbackMinutes"`
		// TimeoutMs bounds a single overlay query. The service-graph instant query
		// scans every traces_service_graph_request_total series for the cluster, so
		// on a busy Mimir/VictoriaMetrics it can be slow; the overlay degrades
		// gracefully if it's exceeded (that cycle just has no overlay).
		TimeoutMs int `json:"timeoutMs"`
	} `json:"telemetry"`
}

const appConfPrefix = "SSUC"

// Load reads the configuration from the environment, applies defaults, and
// validates required fields.
func Load() (Config, error) {
	var conf Config

	// Booleans default to true unless explicitly disabled, so seed them before
	// envconfig.Process (which only overrides keys present in the environment).
	conf.OIDC.Enabled = true
	conf.GitOps.Enabled = true
	conf.Swagger.Enabled = true

	if err := envconfig.Process(appConfPrefix, &conf); err != nil {
		return conf, err
	}

	if conf.LogLevel == "" {
		conf.LogLevel = "info"
	}
	if conf.WorkerInterval == 0 {
		conf.WorkerInterval = 300
	}
	if conf.APIPort == 0 {
		conf.APIPort = 8080
	}
	if conf.Kubernetes.Concurrency == 0 {
		conf.Kubernetes.Concurrency = 10
	}
	if conf.Kubernetes.QPS == 0 {
		conf.Kubernetes.QPS = 50
	}
	if conf.Kubernetes.Burst == 0 {
		conf.Kubernetes.Burst = 100
	}
	if conf.ClusterName == "" {
		return conf, fmt.Errorf("SSUC_CLUSTERNAME must be set")
	}

	if hasEntries(conf.Kubernetes.NamespaceExclude) && hasEntries(conf.Kubernetes.NamespaceInclude) {
		return conf, fmt.Errorf("SSUC_KUBERNETES_NAMESPACEEXCLUDE and SSUC_KUBERNETES_NAMESPACEINCLUDE are mutually exclusive; set at most one")
	}

	if len(conf.OIDC.RequiredRoles) == 0 {
		conf.OIDC.RequiredRoles = []string{"Catalog.Read"}
	}
	if conf.Swagger.TimeoutMs == 0 {
		conf.Swagger.TimeoutMs = 2000
	}
	if conf.Swagger.Concurrency == 0 {
		conf.Swagger.Concurrency = 20
	}
	if conf.Telemetry.LookbackMinutes == 0 {
		conf.Telemetry.LookbackMinutes = 60
	}
	if conf.Telemetry.TimeoutMs == 0 {
		conf.Telemetry.TimeoutMs = 60000
	}

	if conf.OIDC.Enabled {
		if conf.OIDC.IssuerURL == "" {
			return conf, fmt.Errorf("SSUC_OIDC_ISSUERURL must be set when OIDC is enabled")
		}
		if conf.OIDC.Audience == "" {
			return conf, fmt.Errorf("SSUC_OIDC_AUDIENCE must be set when OIDC is enabled")
		}
	}

	if conf.Telemetry.Enabled {
		if conf.Telemetry.MimirURL == "" {
			return conf, fmt.Errorf("SSUC_TELEMETRY_MIMIRURL must be set when telemetry is enabled")
		}
	}

	return conf, nil
}
