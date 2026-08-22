package outboundgroup

import "testing"

func TestPickGeminiPriority(t *testing.T) {
	tests := []struct {
		name            string
		candidates      []geminiCandidate
		selectedIdx     int
		wantIdx         int
		wantClearSelect bool
	}{
		{
			name:        "empty list",
			candidates:  nil,
			selectedIdx: -1,
			wantIdx:     -1,
		},
		{
			// The whole point of the group: rank 1 is skipped because
			// Gemini does not answer through it, even though it is alive.
			name: "skips alive server without gemini",
			candidates: []geminiCandidate{
				{alive: true, gemini: false},
				{alive: true, gemini: true},
			},
			selectedIdx: -1,
			wantIdx:     1,
		},
		{
			name: "skips gemini server that is down",
			candidates: []geminiCandidate{
				{alive: false, gemini: true},
				{alive: true, gemini: true},
			},
			selectedIdx: -1,
			wantIdx:     1,
		},
		{
			name: "prefers the highest ranked qualifying server",
			candidates: []geminiCandidate{
				{alive: true, gemini: true},
				{alive: true, gemini: true},
			},
			selectedIdx: -1,
			wantIdx:     0,
		},
		{
			// No verdicts anywhere (a silent feed): degrade to the plain
			// priority fallback rather than blackholing traffic.
			name: "degrades to first alive when no gemini verdicts",
			candidates: []geminiCandidate{
				{alive: false, gemini: false},
				{alive: true, gemini: false},
				{alive: true, gemini: false},
			},
			selectedIdx: -1,
			wantIdx:     1,
		},
		{
			name: "falls back to head when nothing is alive",
			candidates: []geminiCandidate{
				{alive: false, gemini: true},
				{alive: false, gemini: false},
			},
			selectedIdx: -1,
			wantIdx:     0,
		},
		{
			// A manual pin outranks the rule, so the user can override a
			// choice they disagree with.
			name: "alive pin wins over a better ranked gemini server",
			candidates: []geminiCandidate{
				{alive: true, gemini: true},
				{alive: true, gemini: false},
			},
			selectedIdx: 1,
			wantIdx:     1,
		},
		{
			name: "dead pin is dropped and the rule resumes",
			candidates: []geminiCandidate{
				{alive: true, gemini: false},
				{alive: false, gemini: true},
				{alive: true, gemini: true},
			},
			selectedIdx:     1,
			wantIdx:         2,
			wantClearSelect: true,
		},
		{
			// A pin naming a server that left the subscription arrives as
			// -1 and must not be mistaken for "clear the pin", which would
			// be indistinguishable from the server having died.
			name: "absent pin is not treated as dead",
			candidates: []geminiCandidate{
				{alive: true, gemini: true},
			},
			selectedIdx: -1,
			wantIdx:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, clear := pickGeminiPriority(tt.candidates, tt.selectedIdx)

			if idx != tt.wantIdx {
				t.Errorf("index = %d, want %d", idx, tt.wantIdx)
			}
			if clear != tt.wantClearSelect {
				t.Errorf("clearSelected = %v, want %v", clear, tt.wantClearSelect)
			}
		})
	}
}

func TestGeminiAvailableOnlyOnExplicitForeign(t *testing.T) {
	t.Cleanup(func() { SetRegionProvider(nil) })

	tests := []struct {
		name    string
		class   string
		ok      bool
		want    bool
		comment string
	}{
		{
			name:    "gemini available",
			class:   geoSplitClassForeign,
			ok:      true,
			want:    true,
			comment: "the only case that qualifies",
		},
		{
			name:    "gemini blocked",
			class:   geoSplitClassRU,
			ok:      true,
			want:    false,
			comment: "an explicit refusal",
		},
		{
			name:    "no data",
			class:   "",
			ok:      false,
			want:    false,
			comment: "unknown is an absence of data, never a verdict",
		},
		{
			name:    "provider reports a class we do not know",
			class:   "elsewhere",
			ok:      true,
			want:    false,
			comment: "externalRegion rejects out-of-range classes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetRegionProvider(func(string) (string, string, bool) {
				return tt.class, "", tt.ok
			})

			if got := geminiAvailable("node"); got != tt.want {
				t.Errorf("geminiAvailable() = %v, want %v (%s)", got, tt.want, tt.comment)
			}
		})
	}
}

func TestGeminiAvailableWithoutProvider(t *testing.T) {
	t.Cleanup(func() { SetRegionProvider(nil) })

	SetRegionProvider(nil)

	// No feed installed at all — e.g. the user never set a feed URL. The
	// group must degrade, not select on stale or invented data.
	if geminiAvailable("node") {
		t.Error("geminiAvailable() = true with no provider installed, want false")
	}
}

func TestFirstIndexOf(t *testing.T) {
	names := []string{"NL", "DE", "NL", "FI"}

	tests := []struct {
		name   string
		lookup string
		want   int
	}{
		{"present once", "FI", 3},
		// Review round 1 (MAJOR): resolving a repeated name to its last
		// occurrence would pin the LOWEST-ranked copy, since the list is
		// in priority order. fallback returns on its first match; so does
		// this.
		{"repeated name resolves to the highest ranked", "NL", 0},
		{"absent", "PL", -1},
		{"empty lookup is not a pin", "", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := firstIndexOf(names, tt.lookup); got != tt.want {
				t.Errorf("firstIndexOf(%q) = %d, want %d", tt.lookup, got, tt.want)
			}
		})
	}
}
