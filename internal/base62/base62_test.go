package base62

import (
	"errors"
	"math"
	"testing"
)

func TestEncodeKnownValues(t *testing.T) {
	tests := []struct {
		name string
		n    uint64
		want string
	}{
		{"zero", 0, "0"},
		{"one", 1, "1"},
		{"nine", 9, "9"},
		{"ten is a", 10, "a"},
		{"thirty-five is z", 35, "z"},
		{"thirty-six is A", 36, "A"},
		{"sixty-one is Z", 61, "Z"},
		{"base rolls over", 62, "10"},
		{"sixty-three", 63, "11"},
		{"62^2", 62 * 62, "100"},
		{"max uint64", math.MaxUint64, "lYGhA16ahyf"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Encode(tt.n); got != tt.want {
				t.Errorf("Encode(%d) = %q, want %q", tt.n, got, tt.want)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	values := []uint64{
		0, 1, 61, 62, 63, 100, 12345, 1 << 20, 1 << 41,
		1<<63 - 1, 1 << 63, math.MaxUint64,
	}
	for _, n := range values {
		s := Encode(n)
		got, err := Decode(s)
		if err != nil {
			t.Fatalf("Decode(Encode(%d)=%q) error: %v", n, s, err)
		}
		if got != n {
			t.Errorf("round trip %d -> %q -> %d", n, s, got)
		}
		if len(s) > MaxLen {
			t.Errorf("Encode(%d) = %q longer than MaxLen %d", n, s, MaxLen)
		}
	}
}

func TestDecodeErrors(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr error // nil means "any error"
	}{
		{"empty", "", ErrEmpty},
		{"space", " ", nil},
		{"dash", "abc-def", nil},
		{"plus", "abc+def", nil},
		{"unicode", "ab©", nil},
		{"overflow max+1 pattern", "lYGhA16ahyg", ErrOverflow},
		{"overflow long string", "ZZZZZZZZZZZZ", ErrOverflow},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode(tt.in)
			if err == nil {
				t.Fatalf("Decode(%q) succeeded, want error", tt.in)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("Decode(%q) error = %v, want %v", tt.in, err, tt.wantErr)
			}
		})
	}
}

func TestDecodeValid(t *testing.T) {
	got, err := Decode("Zik0zj") // arbitrary mixed-case value
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}
	if Encode(got) != "Zik0zj" {
		t.Errorf("re-encode mismatch: %d -> %q", got, Encode(got))
	}
}
