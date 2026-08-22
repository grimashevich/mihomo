package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type GeminiPriorityOption struct{}

// GeminiPriority is a fallback whose ordering is the user's manual
// server ranking and whose "good enough" test is the external Gemini
// verdict rather than latency.
//
// Why it exists: a fallback picks the first *alive* server, and a
// url-test picks the *fastest* — neither notices that Gemini refuses to
// answer through an exit, which is the thing that actually matters for
// this fork's users. The proxies list is injected in priority order (see
// orderByPriority in the CMFA config layer), so walking it in order and
// stopping at the first server that both responds and has Gemini working
// is exactly "my preferred server that is currently usable".
//
// Selection rule, in order:
//
//  1. first proxy that is alive AND whose external verdict says Gemini is
//     available;
//  2. failing that, the first alive proxy — i.e. plain fallback
//     behaviour;
//  3. failing that, the head of the list, as fallback does.
//
// Step 2 is what keeps the group usable when the hourly sweep has no
// data: with no verdicts at all this group degrades exactly into the
// priority fallback it refines, rather than blackholing traffic.
//
// Only an explicit "Gemini available" counts in step 1. A node the sweep
// could not reach, or one whose verdict aged out, reports no class at
// all — and this fork treats that as an absence of data, never as a
// verdict (see docs/fleet_status.md). Selecting on "not known to be
// blocked" would make the group's name a lie whenever the feed is
// incomplete.
type GeminiPriority struct {
	*GroupBase
	disableUDP     bool
	testUrl        string
	selected       string
	expectedStatus string
}

func (g *GeminiPriority) Now() string {
	return g.findProxy(false).Name()
}

// DialContext implements C.ProxyAdapter
func (g *GeminiPriority) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	proxy := g.findProxy(true)
	c, err := proxy.DialContext(ctx, metadata)
	if err == nil {
		c.AppendToChains(g)
	} else {
		g.onDialFailed(proxy.Type(), err, g.healthCheck)
	}

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				g.onDialSuccess()
			} else {
				g.onDialFailed(proxy.Type(), err, g.healthCheck)
			}
		})
	}

	return c, err
}

// ListenPacketContext implements C.ProxyAdapter
func (g *GeminiPriority) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := g.findProxy(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(g)
	}

	return pc, err
}

// SupportUDP implements C.ProxyAdapter
func (g *GeminiPriority) SupportUDP() bool {
	if g.disableUDP {
		return false
	}

	return g.findProxy(false).SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (g *GeminiPriority) IsL3Protocol(metadata *C.Metadata) bool {
	return g.findProxy(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (g *GeminiPriority) MarshalJSON() ([]byte, error) {
	all := []string{}
	// Which servers currently qualify. Exposed because "why did it pick
	// that one?" is otherwise unanswerable from the app, which has no
	// clash API listener: this lands in the proxy screen's JSON and in
	// logcat.
	ready := []string{}

	for _, proxy := range g.GetProxies(false) {
		all = append(all, proxy.Name())

		if geminiAvailable(proxy.Name()) {
			ready = append(ready, proxy.Name())
		}
	}

	return json.Marshal(map[string]any{
		"type":           g.Type().String(),
		"now":            g.Now(),
		"all":            all,
		"geminiReady":    ready,
		"testUrl":        g.testUrl,
		"expectedStatus": g.expectedStatus,
		"fixed":          g.selected,
		"hidden":         g.Hidden(),
		"icon":           g.Icon(),
		"emptyFallback":  g.EmptyFallback().Name(),
	})
}

// Unwrap implements C.ProxyAdapter
func (g *GeminiPriority) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return g.findProxy(touch)
}

// geminiAvailable reports whether the external sweep says Gemini answers
// through this server.
//
// It asks the provider directly rather than going through the geo-split
// regionStore on purpose. That store merges external verdicts with the
// on-device google.com probe, and the probe classifies by Google's
// country attribution — which is not the same question. A server the
// probe calls "foreign" may still have Gemini blocked, and this group
// must not select it on that basis.
func geminiAvailable(name string) bool {
	class, _, ok := externalRegion(name)

	return ok && class == geoSplitClassForeign
}

// geminiCandidate is what the selection rule needs to know about one
// server, resolved once per pass. Keeping the rule over these plain
// facts rather than over C.Proxy is what makes it unit-testable: C.Proxy
// embeds the whole ProxyAdapter surface, and stubbing it would cost more
// than the rule it verifies.
type geminiCandidate struct {
	alive  bool
	gemini bool
}

// pickGeminiPriority implements the documented three-step rule.
//
// selectedIdx is the index of the manually pinned server, or -1 when
// nothing is pinned or the pin names a server no longer in the list. It
// returns the index to use (-1 only for an empty list) and whether the
// pin should be dropped because the pinned server is no longer alive.
func pickGeminiPriority(candidates []geminiCandidate, selectedIdx int) (idx int, clearSelected bool) {
	if len(candidates) == 0 {
		return -1, false
	}

	if selectedIdx >= 0 && selectedIdx < len(candidates) {
		if candidates[selectedIdx].alive {
			return selectedIdx, false
		}

		clearSelected = true
	}

	for i, candidate := range candidates {
		if candidate.alive && candidate.gemini {
			return i, clearSelected
		}
	}

	// No confirmed Gemini anywhere: degrade to the priority fallback.
	for i, candidate := range candidates {
		if candidate.alive {
			return i, clearSelected
		}
	}

	return 0, clearSelected
}

func (g *GeminiPriority) findProxy(touch bool) C.Proxy {
	proxies := g.GetProxies(touch)
	if len(proxies) == 0 {
		// GroupBase guarantees a non-empty list (it falls back to the
		// empty-fallback proxy), but findProxy indexes into it, so do not
		// rely on that from here.
		return g.EmptyFallback()
	}

	candidates := make([]geminiCandidate, len(proxies))
	selectedIdx := -1

	for i, proxy := range proxies {
		if len(g.selected) > 0 && proxy.Name() == g.selected {
			selectedIdx = i
		}

		candidates[i] = geminiCandidate{
			alive:  proxy.AliveForTestUrl(g.testUrl),
			gemini: geminiAvailable(proxy.Name()),
		}
	}

	idx, clearSelected := pickGeminiPriority(candidates, selectedIdx)

	if clearSelected {
		g.selected = ""
	}

	return proxies[idx]
}

// Set implements outboundgroup.SelectAble
func (g *GeminiPriority) Set(name string) error {
	var p C.Proxy
	for _, proxy := range g.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}

	if p == nil {
		return errors.New("proxy not exist")
	}

	g.selected = name
	if !p.AliveForTestUrl(g.testUrl) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(5000))
		defer cancel()
		expectedStatus, _ := utils.NewUnsignedRanges[uint16](g.expectedStatus)
		_, _ = p.URLTest(ctx, g.testUrl, expectedStatus)
	}

	return nil
}

// ForceSet implements outboundgroup.SelectAble
func (g *GeminiPriority) ForceSet(name string) {
	g.selected = name
}

func (g *GeminiPriority) Providers() []P.ProxyProvider {
	return g.providers
}

func (g *GeminiPriority) Proxies() []C.Proxy {
	return g.GetProxies(false)
}

func NewGeminiPriority(
	option GroupCommonOption,
	geminiPriorityOption GeminiPriorityOption,
	emptyFallback C.Proxy,
	providers []P.ProxyProvider,
) (*GeminiPriority, error) {
	if emptyFallback == nil {
		return nil, errors.New("empty fallback proxy not exist")
	}

	return &GeminiPriority{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.GeminiPriority,
			Hidden:         option.Hidden,
			Icon:           option.Icon,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			EmptyFallback:  emptyFallback,
			Providers:      providers,
		}),
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
	}, nil
}

var _ ProxyGroup = (*GeminiPriority)(nil)
var _ SelectAble = (*GeminiPriority)(nil)
