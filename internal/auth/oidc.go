// Package auth provides OIDC Bearer-token validation as Gin middleware. It
// mirrors the ssu-oidc-broker pattern: a coreos/go-oidc provider obtained via
// OIDC discovery, refreshed periodically in the background. JWKS rotation is
// handled internally by go-oidc (it re-fetches keys on a kid miss).
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Verifier validates inbound access tokens against the configured issuer and
// audience, and authorizes on the required app role(s).
type Verifier struct {
	issuerURL     string
	audience      string
	requiredRoles []string
	log           *zap.Logger

	state atomic.Pointer[oidc.IDTokenVerifier]
}

// claims is the subset of token claims we authorize on. Azure AD places app
// roles in the `roles` claim.
type claims struct {
	Roles []string `json:"roles"`
}

// NewVerifier performs the initial discovery and returns a ready Verifier.
func NewVerifier(ctx context.Context, issuerURL, audience string, requiredRoles []string, log *zap.Logger) (*Verifier, error) {
	if log == nil {
		log = zap.NewNop()
	}
	v := &Verifier{
		issuerURL:     issuerURL,
		audience:      audience,
		requiredRoles: requiredRoles,
		log:           log,
	}
	if err := v.refresh(ctx); err != nil {
		return nil, err
	}
	return v, nil
}

// Run blocks until ctx is done, refreshing discovery every interval. Refresh
// failures are logged; the previously loaded verifier remains in effect.
func (v *Verifier) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Hour
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := v.refresh(ctx); err != nil {
				v.log.Warn("oidc discovery refresh failed", zap.Error(err))
			}
		}
	}
}

func (v *Verifier) refresh(ctx context.Context) error {
	prov, err := oidc.NewProvider(ctx, v.issuerURL)
	if err != nil {
		return fmt.Errorf("oidc discovery: %w", err)
	}
	v.state.Store(prov.Verifier(&oidc.Config{ClientID: v.audience}))
	return nil
}

// verify validates a raw access token and checks the required role(s).
func (v *Verifier) verify(ctx context.Context, rawToken string) error {
	verifier := v.state.Load()
	if verifier == nil {
		return errors.New("oidc verifier not initialised")
	}
	tok, err := verifier.Verify(ctx, rawToken)
	if err != nil {
		return fmt.Errorf("verify token: %w", err)
	}

	var c claims
	if err := tok.Claims(&c); err != nil {
		return fmt.Errorf("decode claims: %w", err)
	}
	if !hasRequiredRole(c.Roles, v.requiredRoles) {
		return fmt.Errorf("missing required role (need one of %v)", v.requiredRoles)
	}
	return nil
}

// hasRequiredRole returns true when no roles are required, or when at least one
// required role is present in the token's roles.
func hasRequiredRole(tokenRoles, requiredRoles []string) bool {
	if len(requiredRoles) == 0 {
		return true
	}
	set := make(map[string]struct{}, len(tokenRoles))
	for _, r := range tokenRoles {
		set[r] = struct{}{}
	}
	for _, required := range requiredRoles {
		if _, ok := set[required]; ok {
			return true
		}
	}
	return false
}

// Middleware returns a Gin middleware that enforces a valid Bearer token. When
// a token is rejected, onReject is invoked (e.g. to bump a metric) before the
// 401 is written.
func (v *Verifier) Middleware(onReject func()) gin.HandlerFunc {
	return func(c *gin.Context) {
		raw, err := bearerToken(c.GetHeader("Authorization"))
		if err != nil {
			reject(c, onReject, "missing or malformed Authorization header")
			return
		}
		if err := v.verify(c.Request.Context(), raw); err != nil {
			reject(c, onReject, err.Error())
			return
		}
		c.Next()
	}
}

// DisabledMiddleware is a pass-through used when OIDC is turned off (local dev).
func DisabledMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) { c.Next() }
}

func reject(c *gin.Context, onReject func(), reason string) {
	if onReject != nil {
		onReject()
	}
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized", "reason": reason})
}

func bearerToken(header string) (string, error) {
	if header == "" {
		return "", errors.New("no Authorization header")
	}
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return "", errors.New("malformed Authorization header")
	}
	return strings.TrimSpace(parts[1]), nil
}
