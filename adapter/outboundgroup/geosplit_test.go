package outboundgroup

import (
	"testing"
	"time"
)

func TestParseGoogleCountry(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "marker present RU",
			body: `window.W_jd={};"MgUcDb":"RU","GL":"x"`,
			want: "RU",
		},
		{
			name: "marker present CH",
			body: `prefix "MgUcDb":"CH" suffix`,
			want: "CH",
		},
		{
			name: "marker absent",
			body: `<html><body>captcha</body></html>`,
			want: "",
		},
		{
			name: "empty body",
			body: "",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseGoogleCountry([]byte(tt.body)); got != tt.want {
				t.Errorf("parseGoogleCountry() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClassifyCountry(t *testing.T) {
	tests := []struct {
		code string
		want string
	}{
		{"RU", geoSplitClassRU},
		{"ru", geoSplitClassRU},
		{"CH", geoSplitClassForeign},
		{"NL", geoSplitClassForeign},
		{"", geoSplitClassUnknown},
	}

	for _, tt := range tests {
		if got := classifyCountry(tt.code); got != tt.want {
			t.Errorf("classifyCountry(%q) = %q, want %q", tt.code, got, tt.want)
		}
	}
}

func TestBucketFor(t *testing.T) {
	tests := []struct {
		name     string
		class    string
		preferRU bool
		want     int
	}{
		{"ru class, prefer ru", geoSplitClassRU, true, 0},
		{"foreign class, prefer ru", geoSplitClassForeign, true, 1},
		{"unknown class, prefer ru", geoSplitClassUnknown, true, 2},
		{"ru class, prefer foreign", geoSplitClassRU, false, 1},
		{"foreign class, prefer foreign", geoSplitClassForeign, false, 0},
		{"unknown class, prefer foreign", geoSplitClassUnknown, false, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bucketFor(tt.class, tt.preferRU); got != tt.want {
				t.Errorf("bucketFor(%q, %v) = %d, want %d", tt.class, tt.preferRU, got, tt.want)
			}
		})
	}
}

func TestRegionStoreStaleKeep(t *testing.T) {
	s := &regionStore{
		entries:  map[string]regionEntry{},
		inflight: map[string]struct{}{},
	}

	// initial claim succeeds, failed probe leaves class unknown
	if !s.claim("a", time.Hour) {
		t.Fatal("first claim must succeed")
	}
	s.release("a", geoSplitClassUnknown, "")
	if got := s.classOf("a"); got != geoSplitClassUnknown {
		t.Fatalf("classOf after failed probe = %q, want unknown", got)
	}

	// successful probe stores the class
	if !s.claim("a", time.Hour) {
		t.Fatal("claim after failed probe must succeed (no fresh entry)")
	}
	s.release("a", geoSplitClassRU, "RU")
	if got := s.classOf("a"); got != geoSplitClassRU {
		t.Fatalf("classOf = %q, want ru", got)
	}
	if got := s.countryOf("a"); got != "RU" {
		t.Fatalf("countryOf = %q, want RU", got)
	}

	// fresh entry blocks re-claim
	if s.claim("a", time.Hour) {
		t.Fatal("claim must fail while entry is fresh")
	}

	// a later failed probe must NOT erase the known classification
	if !s.claim("a", 0) {
		t.Fatal("claim with zero maxAge must succeed")
	}
	s.release("a", geoSplitClassUnknown, "")
	if got := s.classOf("a"); got != geoSplitClassRU {
		t.Fatalf("failed probe erased classification: classOf = %q, want ru", got)
	}
	if got := s.countryOf("a"); got != "RU" {
		t.Fatalf("failed probe erased country: countryOf = %q, want RU", got)
	}
}

func TestRegionStoreInflightBlocksConcurrentClaim(t *testing.T) {
	s := &regionStore{
		entries:  map[string]regionEntry{},
		inflight: map[string]struct{}{},
	}

	if !s.claim("a", time.Hour) {
		t.Fatal("first claim must succeed")
	}
	if s.claim("a", time.Hour) {
		t.Fatal("second claim while inflight must fail")
	}
	s.release("a", geoSplitClassForeign, "DE")
	if got := s.classOf("a"); got != geoSplitClassForeign {
		t.Fatalf("classOf = %q, want foreign", got)
	}
	if got := s.countryOf("a"); got != "DE" {
		t.Fatalf("countryOf = %q, want DE", got)
	}
}

// RegionCodeOf reads the package-level globalRegionStore and uppercases the
// stored ISO code so the UI suffix is stable regardless of probe casing.
func TestRegionCodeOfUppercases(t *testing.T) {
	const name = "geosplit-uppercase-probe-fixture"
	globalRegionStore.release(name, geoSplitClassForeign, "nl")
	if got := RegionCodeOf(name); got != "NL" {
		t.Fatalf("RegionCodeOf = %q, want NL (uppercased)", got)
	}
	if got := RegionCodeOf("never-classified-fixture"); got != "" {
		t.Fatalf("RegionCodeOf(unknown) = %q, want empty", got)
	}
}
