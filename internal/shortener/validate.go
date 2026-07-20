package shortener

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	// maxURLLen bounds stored URLs (matches common browser/CDN limits).
	maxURLLen = 2048
	// maxTTL caps client-supplied TTLs at ten years.
	maxTTL = 10 * 365 * 24 * time.Hour
)

// aliasPattern constrains custom aliases to URL-safe characters so every
// alias is also a valid path segment.
var aliasPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{4,32}$`)

// reservedAliases are path prefixes owned by the service itself.
var reservedAliases = map[string]bool{
	"api":     true,
	"healthz": true,
	"readyz":  true,
	"metrics": true,
	"static":  true,
}

// validateLongURL enforces parseability, an http(s) scheme allowlist (no
// javascript:, data:, file: smuggling through the redirect), a non-empty
// host, a length cap, and — when selfHost is set — rejects URLs pointing
// back at this service (redirect loops).
func validateLongURL(raw, selfHost string) error {
	if raw == "" {
		return fmt.Errorf("%w: long_url is required", ErrInvalidURL)
	}
	if len(raw) > maxURLLen {
		return fmt.Errorf("%w: exceeds %d characters", ErrInvalidURL, maxURLLen)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: scheme must be http or https", ErrInvalidURL)
	}
	if u.Host == "" {
		return fmt.Errorf("%w: missing host", ErrInvalidURL)
	}
	if selfHost != "" && sameHost(u.Host, selfHost) {
		return fmt.Errorf("%w: refusing to shorten own domain (redirect loop)", ErrInvalidURL)
	}
	return nil
}

// sameHost compares hosts case-insensitively, ignoring ports.
func sameHost(a, b string) bool {
	return strings.EqualFold(stripPort(a), stripPort(b))
}

func stripPort(h string) string {
	if i := strings.LastIndexByte(h, ':'); i >= 0 && !strings.Contains(h[i:], "]") {
		return h[:i]
	}
	return h
}

// validateAlias enforces the custom-alias grammar and reserved names.
func validateAlias(alias string) error {
	if !aliasPattern.MatchString(alias) {
		return fmt.Errorf("%w: must match %s", ErrInvalidAlias, aliasPattern)
	}
	if reservedAliases[alias] {
		return fmt.Errorf("%w: %q is reserved", ErrInvalidAlias, alias)
	}
	return nil
}

// validateTTL rejects negative and absurd TTLs.
func validateTTL(ttl time.Duration) error {
	if ttl < 0 {
		return fmt.Errorf("%w: ttl_seconds must be >= 0", ErrInvalidTTL)
	}
	if ttl > maxTTL {
		return fmt.Errorf("%w: ttl_seconds exceeds maximum (%d)", ErrInvalidTTL, int64(maxTTL.Seconds()))
	}
	return nil
}
