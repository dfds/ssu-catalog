package telemetry

import "testing"

func TestSplitHostPort(t *testing.T) {
	cases := []struct {
		addr     string
		wantHost string
		wantPort string
	}{
		// IPv4 and hostnames with a port.
		{"10.0.0.5:5432", "10.0.0.5", "5432"},
		{"db.example.com:3306", "db.example.com", "3306"},
		// Bare hosts / no port.
		{"db.example.com", "db.example.com", ""},
		{"cache.svc.cluster.local", "cache.svc.cluster.local", ""},
		{"", "", ""},
		// A non-numeric trailing segment is part of the host, not a port.
		{"foo.svc.cluster.local:bar", "foo.svc.cluster.local:bar", ""},
		// Bare IPv6 literals must survive intact — the regression this guards:
		// LastIndex(":") used to strip the final octet and leave ":".
		{"::1", "::1", ""},
		{"fd00::1", "fd00::1", ""},
		{"2001:db8::1", "2001:db8::1", ""},
		// Bracketed IPv6, with and without a port.
		{"[::1]:5432", "::1", "5432"},
		{"[2001:db8::1]:9092", "2001:db8::1", "9092"},
		{"[::1]", "::1", ""},
	}
	for _, c := range cases {
		gotHost, gotPort := splitHostPort(c.addr)
		if gotHost != c.wantHost || gotPort != c.wantPort {
			t.Errorf("splitHostPort(%q) = (%q, %q); want (%q, %q)",
				c.addr, gotHost, gotPort, c.wantHost, c.wantPort)
		}
		if stripPort(c.addr) != c.wantHost {
			t.Errorf("stripPort(%q) = %q; want %q", c.addr, stripPort(c.addr), c.wantHost)
		}
		if portOf(c.addr) != c.wantPort {
			t.Errorf("portOf(%q) = %q; want %q", c.addr, portOf(c.addr), c.wantPort)
		}
	}
}

func TestPeerPortPrefersLabel(t *testing.T) {
	// The dedicated server_port label wins over a port embedded in the address.
	if got := peerPort(map[string]string{"server_port": "5432"}, "10.0.0.5"); got != "5432" {
		t.Errorf("peerPort(label) = %q; want 5432", got)
	}
	// Fall back to a port embedded in server_address for older data.
	if got := peerPort(map[string]string{}, "10.0.0.5:3306"); got != "3306" {
		t.Errorf("peerPort(fallback) = %q; want 3306", got)
	}
	if got := peerPort(map[string]string{}, "10.0.0.5"); got != "" {
		t.Errorf("peerPort(none) = %q; want empty", got)
	}
}

func TestJoinHostPort(t *testing.T) {
	cases := []struct {
		host, port, want string
	}{
		{"db.example.com", "5432", "db.example.com:5432"},
		{"db.example.com", "", "db.example.com"},
		{"10.0.0.5", "3306", "10.0.0.5:3306"},
		{"::1", "5432", "[::1]:5432"}, // IPv6 gets bracketed so it round-trips
		{"[::1]", "5432", "[::1]:5432"},
	}
	for _, c := range cases {
		if got := joinHostPort(c.host, c.port); got != c.want {
			t.Errorf("joinHostPort(%q, %q) = %q; want %q", c.host, c.port, got, c.want)
		}
	}
}
