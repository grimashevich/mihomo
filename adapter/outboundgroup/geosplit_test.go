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

func TestRegionProviderOverridesStore(t *testing.T) {
	defer SetRegionProvider(nil)

	s := &regionStore{
		entries:  map[string]regionEntry{},
		inflight: map[string]struct{}{},
	}

	// locally probed as RU
	if !s.claim("a", time.Hour) {
		t.Fatal("first claim must succeed")
	}
	s.release("a", geoSplitClassRU, "RU")

	SetRegionProvider(func(name string) (string, string, bool) {
		if name == "a" {
			return geoSplitClassForeign, "CH", true
		}
		return "", "", false
	})

	if got := s.classOf("a"); got != geoSplitClassForeign {
		t.Fatalf("classOf with provider = %q, want foreign (provider wins)", got)
	}
	if got := s.countryOf("a"); got != "CH" {
		t.Fatalf("countryOf with provider = %q, want CH", got)
	}
	if s.claim("a", 0) {
		t.Fatal("claim must fail while the provider answers for the name")
	}

	// names the provider does not answer for keep using the local store
	if !s.claim("b", time.Hour) {
		t.Fatal("claim for an unprovided name must succeed")
	}
	s.release("b", geoSplitClassRU, "RU")
	if got := s.classOf("b"); got != geoSplitClassRU {
		t.Fatalf("classOf(b) = %q, want ru (local probe)", got)
	}

	// The external verdict is mirrored into the store, so removing the
	// provider does not undo the decision it made — it stands until
	// something measures the name again. (Before remember() existed this
	// asserted the opposite: that the older local probe came back.)
	SetRegionProvider(nil)
	if got := s.classOf("a"); got != geoSplitClassForeign {
		t.Fatalf("classOf after clearing provider = %q, want foreign (last decision kept)", got)
	}
	if got := s.countryOf("a"); got != "CH" {
		t.Fatalf("countryOf after clearing provider = %q, want CH", got)
	}
}

// A provider answering "no data" for a name it used to classify (the
// fleet feed reporting gemini "unknown") must not reshuffle anything:
// the previous decision stays until the local probe is due again.
func TestRegionProviderSilenceKeepsLastVerdict(t *testing.T) {
	defer SetRegionProvider(nil)

	s := &regionStore{
		entries:  map[string]regionEntry{},
		inflight: map[string]struct{}{},
	}

	answering := true
	SetRegionProvider(func(name string) (string, string, bool) {
		if answering && name == "a" {
			return geoSplitClassForeign, "CH", true
		}
		return "", "", false
	})

	if got := s.classOf("a"); got != geoSplitClassForeign {
		t.Fatalf("classOf while provider answers = %q, want foreign", got)
	}

	answering = false

	if got := s.classOf("a"); got != geoSplitClassForeign {
		t.Fatalf("classOf after the provider fell silent = %q, want foreign", got)
	}
	if got := bucketFor(s.classOf("a"), false); got != 0 {
		t.Fatalf("bucket after the provider fell silent = %d, want 0", got)
	}

	// The verdict was confirmed moments ago, so a probe is not due yet …
	if s.claim("a", time.Hour) {
		t.Fatal("claim must fail while the remembered verdict is fresh")
	}
	// … but it is not pinned forever: once it ages past the interval the
	// local probe takes over again.
	if !s.claim("a", 0) {
		t.Fatal("claim must succeed once the remembered verdict is stale")
	}
}

// Re-confirming the same verdict keeps it fresh, so a node the feed has
// been reporting for hours is not instantly probe-eligible the moment
// the feed goes quiet.
func TestRegionProviderConfirmationRestampsEntry(t *testing.T) {
	defer SetRegionProvider(nil)

	s := &regionStore{
		entries:  map[string]regionEntry{},
		inflight: map[string]struct{}{},
	}

	SetRegionProvider(func(name string) (string, string, bool) {
		return geoSplitClassForeign, "CH", true
	})

	s.classOf("a")

	// backdate the entry as if the verdict had first been seen long ago
	s.mu.Lock()
	e := s.entries["a"]
	e.checkedAt = time.Now().Add(-2 * time.Hour)
	s.entries["a"] = e
	s.mu.Unlock()

	s.classOf("a") // same verdict, but stale enough to be re-stamped

	s.mu.Lock()
	age := time.Since(s.entries["a"].checkedAt)
	s.mu.Unlock()

	if age > time.Minute {
		t.Fatalf("re-confirmed verdict kept a %s old timestamp, want it re-stamped", age)
	}
}

func TestRegionProviderRejectsInvalidClass(t *testing.T) {
	defer SetRegionProvider(nil)

	s := &regionStore{
		entries:  map[string]regionEntry{},
		inflight: map[string]struct{}{},
	}

	// ok=true with a class outside {ru, foreign} must be ignored, not
	// injected as a third bucket value
	SetRegionProvider(func(string) (string, string, bool) {
		return "somewhere", "XX", true
	})

	if got := s.classOf("a"); got != geoSplitClassUnknown {
		t.Fatalf("classOf with invalid provider class = %q, want unknown", got)
	}
	if !s.claim("a", time.Hour) {
		t.Fatal("claim must succeed when the provider verdict is invalid")
	}
}
