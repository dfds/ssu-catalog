package config

import (
	"strings"
	"testing"
)

// setMinimalEnv sets the minimum required environment for Load to succeed and
// disables subsystems with their own required-field validation.
func setMinimalEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SSUC_CLUSTERNAME", "test")
	t.Setenv("SSUC_OIDC_ENABLED", "false")
}

func TestLoad_APIPortDefaultAndOverride(t *testing.T) {
	setMinimalEnv(t)

	conf, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conf.APIPort != 8080 {
		t.Errorf("expected default APIPort 8080, got %d", conf.APIPort)
	}

	t.Setenv("SSUC_APIPORT", "8081")
	conf, err = Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conf.APIPort != 8081 {
		t.Errorf("expected APIPort 8081, got %d", conf.APIPort)
	}
}

func TestLoad_NamespaceExcludeOnly(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("SSUC_KUBERNETES_NAMESPACEEXCLUDE", "kube-system,kube-node-lease")

	conf, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(conf.Kubernetes.NamespaceExclude) != 2 {
		t.Errorf("exclude not parsed: %+v", conf.Kubernetes.NamespaceExclude)
	}
	if len(conf.Kubernetes.NamespaceInclude) != 0 {
		t.Errorf("include should be empty: %+v", conf.Kubernetes.NamespaceInclude)
	}
}

func TestLoad_NamespaceIncludeOnly(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("SSUC_KUBERNETES_NAMESPACEINCLUDE", "cap-a,cap-b")

	conf, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(conf.Kubernetes.NamespaceInclude) != 2 {
		t.Errorf("include not parsed: %+v", conf.Kubernetes.NamespaceInclude)
	}
}

func TestLoad_NamespaceIncludeAndExcludeMutuallyExclusive(t *testing.T) {
	setMinimalEnv(t)
	t.Setenv("SSUC_KUBERNETES_NAMESPACEEXCLUDE", "kube-system")
	t.Setenv("SSUC_KUBERNETES_NAMESPACEINCLUDE", "cap-a")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error when both include and exclude are set")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("unexpected error: %v", err)
	}
}
