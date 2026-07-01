package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// --- minimal JWT/JWKS test harness (RS256), no external libs -----------------

type testIDP struct {
	server   *httptest.Server
	key      *rsa.PrivateKey
	kid      string
	issuer   string
	audience string
}

func newTestIDP(t *testing.T, audience string) *testIDP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	idp := &testIDP{key: key, kid: "test-kid", audience: audience}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                idp.issuer,
			"jwks_uri":                              idp.issuer + "/jwks",
			"authorization_endpoint":                idp.issuer + "/auth",
			"token_endpoint":                        idp.issuer + "/token",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		pub := key.PublicKey
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]any{{
				"kty": "RSA",
				"alg": "RS256",
				"use": "sig",
				"kid": idp.kid,
				"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
			}},
		})
	})

	idp.server = httptest.NewServer(mux)
	idp.issuer = idp.server.URL
	return idp
}

func (idp *testIDP) close() { idp.server.Close() }

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// mintToken builds a signed RS256 JWT with the given claims overrides.
func (idp *testIDP) mintToken(t *testing.T, aud string, roles []string, exp time.Time) string {
	t.Helper()
	header := map[string]any{"alg": "RS256", "typ": "JWT", "kid": idp.kid}
	now := time.Now()
	payload := map[string]any{
		"iss":   idp.issuer,
		"aud":   aud,
		"sub":   "service-principal",
		"iat":   now.Unix(),
		"exp":   exp.Unix(),
		"roles": roles,
	}
	hb, _ := json.Marshal(header)
	pb, _ := json.Marshal(payload)
	signingInput := b64url(hb) + "." + b64url(pb)

	hashed := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, idp.key, crypto.SHA256, hashed[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signingInput + "." + b64url(sig)
}

func newVerifier(t *testing.T, idp *testIDP, roles []string) *Verifier {
	t.Helper()
	v, err := NewVerifier(context.Background(), idp.issuer, idp.audience, roles, nil)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	return v
}

func doRequest(t *testing.T, v *Verifier, authHeader string) (int, int) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	rejections := 0
	router.GET("/api/v1/ping", v.Middleware(func() { rejections++ }), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec.Code, rejections
}

func TestMiddleware_ValidTokenWithRole(t *testing.T) {
	idp := newTestIDP(t, "catalog-aud")
	defer idp.close()
	v := newVerifier(t, idp, []string{"Catalog.Read"})

	token := idp.mintToken(t, "catalog-aud", []string{"Catalog.Read"}, time.Now().Add(time.Hour))
	code, rejections := doRequest(t, v, "Bearer "+token)
	if code != http.StatusOK {
		t.Errorf("expected 200, got %d", code)
	}
	if rejections != 0 {
		t.Errorf("expected 0 rejections, got %d", rejections)
	}
}

func TestMiddleware_MissingRole(t *testing.T) {
	idp := newTestIDP(t, "catalog-aud")
	defer idp.close()
	v := newVerifier(t, idp, []string{"Catalog.Read"})

	token := idp.mintToken(t, "catalog-aud", []string{"Other.Role"}, time.Now().Add(time.Hour))
	code, rejections := doRequest(t, v, "Bearer "+token)
	if code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", code)
	}
	if rejections != 1 {
		t.Errorf("expected 1 rejection, got %d", rejections)
	}
}

func TestMiddleware_WrongAudience(t *testing.T) {
	idp := newTestIDP(t, "catalog-aud")
	defer idp.close()
	v := newVerifier(t, idp, []string{"Catalog.Read"})

	token := idp.mintToken(t, "some-other-aud", []string{"Catalog.Read"}, time.Now().Add(time.Hour))
	code, _ := doRequest(t, v, "Bearer "+token)
	if code != http.StatusUnauthorized {
		t.Errorf("expected 401 for wrong audience, got %d", code)
	}
}

func TestMiddleware_ExpiredToken(t *testing.T) {
	idp := newTestIDP(t, "catalog-aud")
	defer idp.close()
	v := newVerifier(t, idp, []string{"Catalog.Read"})

	token := idp.mintToken(t, "catalog-aud", []string{"Catalog.Read"}, time.Now().Add(-time.Hour))
	code, _ := doRequest(t, v, "Bearer "+token)
	if code != http.StatusUnauthorized {
		t.Errorf("expected 401 for expired token, got %d", code)
	}
}

func TestMiddleware_MalformedHeader(t *testing.T) {
	idp := newTestIDP(t, "catalog-aud")
	defer idp.close()
	v := newVerifier(t, idp, []string{"Catalog.Read"})

	for _, header := range []string{"", "Token abc", "Bearer", "Bearer "} {
		code, _ := doRequest(t, v, header)
		if code != http.StatusUnauthorized {
			t.Errorf("header %q: expected 401, got %d", header, code)
		}
	}
}

func TestDisabledMiddleware_PassesThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/api/v1/ping", DisabledMiddleware(), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 with auth disabled, got %d", rec.Code)
	}
}

func TestHasRequiredRole(t *testing.T) {
	cases := []struct {
		name     string
		token    []string
		required []string
		want     bool
	}{
		{"no requirement", []string{"x"}, nil, true},
		{"present", []string{"a", "Catalog.Read"}, []string{"Catalog.Read"}, true},
		{"absent", []string{"a"}, []string{"Catalog.Read"}, false},
		{"any-of", []string{"b"}, []string{"a", "b"}, true},
		{"empty token", nil, []string{"Catalog.Read"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasRequiredRole(tc.token, tc.required); got != tc.want {
				t.Errorf("hasRequiredRole(%v,%v)=%v want %v", tc.token, tc.required, got, tc.want)
			}
		})
	}
}
