package shortener

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestValidateLongURL(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		selfHost string
		wantErr  bool
	}{
		{"plain http", "http://example.com/page", "", false},
		{"plain https", "https://example.com", "", false},
		{"query and fragment", "https://example.com/a?b=c#d", "", false},
		{"empty", "", "", true},
		{"no scheme", "example.com/page", "", true},
		{"ftp scheme", "ftp://example.com/file", "", true},
		{"javascript scheme", "javascript:alert(1)", "", true},
		{"data scheme", "data:text/html,hi", "", true},
		{"missing host", "https:///path", "", true},
		{"too long", "https://example.com/" + strings.Repeat("a", maxURLLen), "", true},
		{"exactly at cap", "https://ex.com/" + strings.Repeat("a", maxURLLen-15), "", false},
		{"own domain", "https://sho.rt/abc", "sho.rt", true},
		{"own domain case-insensitive", "https://SHO.RT/abc", "sho.rt", true},
		{"own domain with port", "http://sho.rt:8081/abc", "sho.rt", true},
		{"self host with port matches bare", "https://sho.rt/abc", "sho.rt:8081", true},
		{"other domain ok with selfHost set", "https://example.com", "sho.rt", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLongURL(tt.raw, tt.selfHost)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateLongURL(%q, %q) error = %v, wantErr %v", tt.raw, tt.selfHost, err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidURL) {
				t.Errorf("error %v is not ErrInvalidURL", err)
			}
		})
	}
}

func TestValidateAlias(t *testing.T) {
	tests := []struct {
		name    string
		alias   string
		wantErr bool
	}{
		{"simple", "my-link", false},
		{"min length 4", "abcd", false},
		{"max length 32", strings.Repeat("a", 32), false},
		{"underscore and dash", "a_b-c1", false},
		{"too short", "abc", true},
		{"too long", strings.Repeat("a", 33), true},
		{"space", "my link", true},
		{"slash", "a/b/c", true},
		{"unicode", "línk", true},
		{"reserved api", "api", true},
		{"reserved healthz", "healthz", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAlias(tt.alias)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateAlias(%q) error = %v, wantErr %v", tt.alias, err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidAlias) {
				t.Errorf("error %v is not ErrInvalidAlias", err)
			}
		})
	}
}

func TestValidateTTL(t *testing.T) {
	tests := []struct {
		name    string
		ttl     time.Duration
		wantErr bool
	}{
		{"zero means never", 0, false},
		{"one hour", time.Hour, false},
		{"at max", maxTTL, false},
		{"negative", -time.Second, true},
		{"over max", maxTTL + time.Second, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTTL(tt.ttl)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTTL(%v) error = %v, wantErr %v", tt.ttl, err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidTTL) {
				t.Errorf("error %v is not ErrInvalidTTL", err)
			}
		})
	}
}
