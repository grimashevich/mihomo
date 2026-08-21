package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"regexp"
	"strings"
	"sync"
	stdatomic "sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/singledo"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/ca"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"

	// mihomo's forked net/http, compatible with the forked TLS package
	// that ca.GetTLSConfig returns (same pattern as adapter.Proxy.URLTest).
	"github.com/metacubex/http"
)

const (
	geoSplitClassRU      = "ru"
	geoSplitClassForeign = "foreign"
	geoSplitClassUnknown = ""

	defaultRegionCheckInterval = 30 * time.Minute
	regionProbeTimeout         = 15 * time.Second
	regionProbeConcurrency     = 4
	regionProbeMaxBody         = 1 << 20

	// www.google.com embeds `"MgUcDb":"XX"` (ISO-3166 country Google
	// attributes to the requesting IP) in its homepage HTML. This is the
	// same marker the ipregion.vrnt.xyz diagnostic script greps for, so
	// classifications here match what that script reports per server.
	regionProbeURL = "https://www.google.com/"
	// Desktop-browser UA: Google serves the marker-bearing page to real
	// browsers; default Go UA gets a stripped page without it.
	regionProbeUserAgent = "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0"
)

var googleCountryRegex = regexp.MustCompile(`"MgUcDb":"([A-Za-z]{2})"`)

// parseGoogleCountry extracts the ISO country code from a Google
// homepage body, or "" when the marker is absent.
func parseGoogleCountry(body []byte) string {
	m := googleCountryRegex.FindSubmatch(body)
	if m == nil {
		return ""
	}
	return string(m[1])
}

func classifyCountry(code string) string {
	if code == "" {
		return geoSplitClassUnknown
	}
	if code == "RU" || code == "ru" {
		return geoSplitClassRU
	}
	return geoSplitClassForeign
}

// bucketFor ranks a classification for selection: 0 = preferred class,
// 1 = opposite class, 2 = not classified yet. Lower sorts first.
func bucketFor(class string, preferRU bool) int {
	switch class {
	case geoSplitClassRU:
		if preferRU {
			return 0
		}
		return 1
	case geoSplitClassForeign:
		if preferRU {
			return 1
		}
		return 0
	default:
		return 2
	}
}

type regionEntry struct {
	class     string
	country   string // raw ISO code Google attributed (e.g. "RU", "DE"); "" if unknown
	checkedAt time.Time
}

// regionStore caches Google-country classifications keyed by proxy
// name. It is package-level so the RU-first and foreign-first group
// instances (and reloaded configs) share probe results instead of
// hitting Google once per group per server.
type regionStore struct {
	mu       sync.Mutex
	entries  map[string]regionEntry
	inflight map[string]struct{}
}

var globalRegionStore = &regionStore{
	entries:  map[string]regionEntry{},
	inflight: map[string]struct{}{},
}

func (s *regionStore) classOf(name string) string {
	if class, _, ok := externalRegion(name); ok {
		return class
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[name].class
}

func (s *regionStore) countryOf(name string) string {
	if _, country, ok := externalRegion(name); ok {
		return country
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[name].country
}

// RegionCodeOf returns the uppercased ISO country code Google attributed
// to the proxy's exit IP, or "" when it has not been classified yet (no
// geo-split group has driven traffic through it, or every probe failed).
//
// Exported so the CMFA bridge (native/tunnel) can annotate proxy display
// titles with the attributed country — e.g. "🇫🇮 Финляндия [RU]" makes it
// visible that Google places a given server in the Russian bucket. Reads
// the shared store, so it reflects whatever the last geo-split sweep found.
func RegionCodeOf(name string) string {
	return strings.ToUpper(globalRegionStore.countryOf(name))
}

// RegionProvider answers "how is this proxy classified" from outside the
// core. It returns (class, country, ok); ok=false means "no verdict for
// this name", which leaves the local Google probe in charge.
//
// CMFA installs one backed by the fleet-status service (an hourly
// server-side Gemini/YouTube sweep): a verdict measured from a real
// datacenter is both fresher and cheaper than probing google.com through
// every server from the phone. class must be "ru" or "foreign"; anything
// else is treated as no verdict.
type RegionProvider func(name string) (class string, country string, ok bool)

// stored as a pointer so the provider can be cleared with nil, which
// atomic.Value cannot express for a func type.
var regionProvider stdatomic.Pointer[RegionProvider]

// SetRegionProvider installs (nil removes) the external classification
// source. Safe to call at any time; the next classOf/claim picks it up.
func SetRegionProvider(provider RegionProvider) {
	if provider == nil {
		regionProvider.Store(nil)
		return
	}
	regionProvider.Store(&provider)
}

// externalRegion asks the installed provider, validating its class so a
// buggy provider cannot inject a third bucket value into the ordering.
func externalRegion(name string) (string, string, bool) {
	provider := regionProvider.Load()
	if provider == nil {
		return "", "", false
	}

	class, country, ok := (*provider)(name)
	if !ok || (class != geoSplitClassRU && class != geoSplitClassForeign) {
		return "", "", false
	}

	return class, country, true
}

// claim marks a proxy as being probed if it needs probing. It returns
// false when the cached entry is still fresh or another worker is
// already probing the same name.
func (s *regionStore) claim(name string, maxAge time.Duration) bool {
	if _, _, ok := externalRegion(name); ok {
		// an external verdict covers this proxy: probing google.com
		// through it would only spend traffic to be overruled
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, busy := s.inflight[name]; busy {
		return false
	}
	if e, ok := s.entries[name]; ok && time.Since(e.checkedAt) < maxAge {
		return false
	}
	s.inflight[name] = struct{}{}
	return true
}

// release stores the probe outcome. A failed probe (unknown class)
// keeps a previously known classification: a transient Google error
// must not reshuffle an established ordering. country is the raw ISO
// code surfaced to the UI alongside the bucket class.
func (s *regionStore) release(name string, class string, country string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, name)
	if class == geoSplitClassUnknown {
		return // retried on the next worker tick; old entry (if any) stays
	}
	s.entries[name] = regionEntry{class: class, country: country, checkedAt: time.Now()}
}

type geoSplitOption func(*GeoSplit)

func geoSplitWithPreferRU(preferRU bool) geoSplitOption {
	return func(g *GeoSplit) {
		g.preferRU = preferRU
	}
}

func geoSplitWithRegionInterval(interval time.Duration) geoSplitOption {
	return func(g *GeoSplit) {
		g.regionInterval = interval
	}
}

type GeoSplit struct {
	*GroupBase
	selected       string
	testUrl        string
	expectedStatus string
	disableUDP     bool
	preferRU       bool
	regionInterval time.Duration
	fastSingle     *singledo.Single[C.Proxy]

	// lazy classification state: mihomo never calls Close() on adapters
	// dropped by a config reload, so a ticker goroutine would leak. A
	// one-shot sweep is instead kicked from pick(): instances that no
	// longer receive traffic simply stop classifying and get GC'd.
	classifying stdatomic.Bool
	lastKick    stdatomic.Int64
}

// Now implements outboundgroup.ProxyGroup
func (g *GeoSplit) Now() string {
	return g.pick(false).Name()
}

// Set implements outboundgroup.SelectAble
func (g *GeoSplit) Set(name string) error {
	for _, proxy := range g.GetProxies(false) {
		if proxy.Name() == name {
			g.ForceSet(name)
			return nil
		}
	}
	return errors.New("proxy not exist")
}

// ForceSet implements outboundgroup.SelectAble
func (g *GeoSplit) ForceSet(name string) {
	g.selected = name
	g.fastSingle.Reset()
}

// DialContext implements C.ProxyAdapter
func (g *GeoSplit) DialContext(ctx context.Context, metadata *C.Metadata) (c C.Conn, err error) {
	proxy := g.pick(true)
	c, err = proxy.DialContext(ctx, metadata)
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
func (g *GeoSplit) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := g.pick(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(g)
	} else {
		g.onDialFailed(proxy.Type(), err, g.healthCheck)
	}

	return pc, err
}

// Unwrap implements C.ProxyAdapter
func (g *GeoSplit) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return g.pick(touch)
}

// SupportUDP implements C.ProxyAdapter
func (g *GeoSplit) SupportUDP() bool {
	if g.disableUDP {
		return false
	}
	return g.pick(false).SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (g *GeoSplit) IsL3Protocol(metadata *C.Metadata) bool {
	return g.pick(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (g *GeoSplit) MarshalJSON() ([]byte, error) {
	all := []string{}
	classifications := map[string]string{}
	countries := map[string]string{}
	for _, proxy := range g.GetProxies(false) {
		name := proxy.Name()
		all = append(all, name)
		classifications[name] = globalRegionStore.classOf(name)
		countries[name] = RegionCodeOf(name)
	}
	prefer := geoSplitClassForeign
	if g.preferRU {
		prefer = geoSplitClassRU
	}
	return json.Marshal(map[string]any{
		"type":            g.Type().String(),
		"now":             g.Now(),
		"all":             all,
		"testUrl":         g.testUrl,
		"expectedStatus":  g.expectedStatus,
		"fixed":           g.selected,
		"hidden":          g.Hidden(),
		"icon":            g.Icon(),
		"emptyFallback":   g.EmptyFallback().Name(),
		"prefer":          prefer,
		"classifications": classifications,
		"countries":       countries,
	})
}

// Providers implements outboundgroup.ProxyGroup
func (g *GeoSplit) Providers() []P.ProxyProvider {
	return g.providers
}

// Proxies implements outboundgroup.ProxyGroup
func (g *GeoSplit) Proxies() []C.Proxy {
	return g.GetProxies(false)
}

// URLTest implements outboundgroup.ProxyGroup
func (g *GeoSplit) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	return g.GroupBase.URLTest(ctx, g.testUrl, expectedStatus)
}

func (g *GeoSplit) healthCheck() {
	g.fastSingle.Reset()
	g.GroupBase.healthCheck()
	g.fastSingle.Reset()
}

func (g *GeoSplit) bucketOf(name string) int {
	return bucketFor(globalRegionStore.classOf(name), g.preferRU)
}

// pick chooses the best proxy: alive only, ordered by
// (classification bucket, latency). Dead proxies are never picked
// unless nothing is alive, in which case the first proxy is returned —
// the same degenerate behaviour as url-test and fallback.
func (g *GeoSplit) pick(touch bool) C.Proxy {
	elm, _, shared := g.fastSingle.Do(func() (C.Proxy, error) {
		proxies := g.GetProxies(touch)

		if g.selected != "" {
			for _, proxy := range proxies {
				if proxy.Name() != g.selected {
					continue
				}
				if proxy.AliveForTestUrl(g.testUrl) {
					return proxy, nil
				}
				break // pinned proxy is dead: fall through to automatic choice
			}
		}

		var best C.Proxy
		bestBucket := int(^uint(0) >> 1)
		var bestDelay uint16
		for _, proxy := range proxies {
			if !proxy.AliveForTestUrl(g.testUrl) {
				continue
			}
			bucket := g.bucketOf(proxy.Name())
			delay := proxy.LastDelayForTestUrl(g.testUrl)
			if best == nil || bucket < bestBucket || (bucket == bestBucket && delay < bestDelay) {
				best = proxy
				bestBucket = bucket
				bestDelay = delay
			}
		}
		if best == nil {
			best = proxies[0]
		}
		return best, nil
	})
	if shared && touch { // a shared fastSingle.Do() may cause providers untouched, so we touch them again
		g.Touch()
	}

	g.maybeClassify()

	return elm
}

// maybeClassify kicks one background classification sweep, rate-limited
// to one attempt per 30s per instance. Sweeps are cheap no-ops while
// every classification in the shared store is fresh.
func (g *GeoSplit) maybeClassify() {
	const kickInterval = 30 // seconds

	now := time.Now().Unix()
	last := g.lastKick.Load()
	if now-last < kickInterval || !g.lastKick.CompareAndSwap(last, now) {
		return
	}
	if !g.classifying.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer g.classifying.Store(false)
		g.classifyAll()
	}()
}

// probeRegion asks Google which country it attributes to the proxy's
// exit IP. The transport dials *every* requested address through the
// proxy (not a single pinned connection), so consent/captcha redirects
// to other Google hosts still flow through the same exit.
//
// Returns (class, rawCountry): class is ru/foreign/unknown, rawCountry is
// the literal ISO code Google returned ("" when the marker was absent),
// surfaced in the classification log line for debugging.
func probeRegion(ctx context.Context, proxy C.Proxy) (string, string) {
	tlsConfig, err := ca.GetTLSConfig(ca.Option{})
	if err != nil {
		return geoSplitClassUnknown, ""
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			var meta C.Metadata
			// NetWork=TCP and Type=HTTP are already the zero values, and
			// mihomo's own probe builder (adapter.urlToMetadata, used by
			// every url-test/fallback health check) relies on exactly that.
			// Set them explicitly anyway so the dial's intent is self-
			// documenting and independent of enum iota ordering.
			meta.NetWork = C.TCP
			meta.Type = C.HTTP
			if err := meta.SetRemoteAddress(address); err != nil {
				return nil, err
			}
			return proxy.DialContext(ctx, &meta)
		},
		TLSClientConfig:   tlsConfig,
		DisableKeepAlives: true,
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{Transport: transport, Timeout: regionProbeTimeout}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, regionProbeURL, nil)
	if err != nil {
		return geoSplitClassUnknown, ""
	}
	req.Header.Set("User-Agent", regionProbeUserAgent)
	// EEA exits get a consent interstitial; this cookie keeps Google on
	// the marker-bearing homepage (same trick widely used by IP-region
	// checkers). Harmless for non-EEA exits.
	req.Header.Set("Cookie", "CONSENT=YES+")

	resp, err := client.Do(req)
	if err != nil {
		return geoSplitClassUnknown, ""
	}
	defer resp.Body.Close()

	// A partial body with a trailing read error is still usable: the
	// MgUcDb marker sits near the top of the page, so we only bail when
	// nothing at all was read. Do not "fix" this into discarding partial
	// bodies on error.
	body, err := io.ReadAll(io.LimitReader(resp.Body, regionProbeMaxBody))
	if err != nil && len(body) == 0 {
		return geoSplitClassUnknown, ""
	}

	raw := parseGoogleCountry(body)
	return classifyCountry(raw), raw
}

func (g *GeoSplit) classifyAll() {
	proxies := g.GetProxies(false)

	sem := make(chan struct{}, regionProbeConcurrency)
	var wg sync.WaitGroup
	changed := false
	var changedMu sync.Mutex

	for _, proxy := range proxies {
		if _, ok := proxy.Adapter().(ProxyGroup); ok {
			continue // classify servers only, never nested groups
		}
		if !proxy.AliveForTestUrl(g.testUrl) {
			continue // dead servers cannot be probed; retried next tick
		}
		name := proxy.Name()
		if !globalRegionStore.claim(name, g.regionInterval) {
			continue
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(proxy C.Proxy, name string) {
			defer wg.Done()
			defer func() { <-sem }()

			ctx, cancel := context.WithTimeout(context.Background(), regionProbeTimeout)
			defer cancel()

			before := globalRegionStore.classOf(name)
			class, raw := probeRegion(ctx, proxy)
			globalRegionStore.release(name, class, raw)
			after := globalRegionStore.classOf(name)

			if after != before {
				changedMu.Lock()
				changed = true
				changedMu.Unlock()
				rawLabel := raw
				if rawLabel == "" {
					rawLabel = "?"
				}
				log.Infoln("[GeoSplit] %s classified as %q (google country: %s)", name, after, rawLabel)
			} else if class == geoSplitClassUnknown {
				log.Debugln("[GeoSplit] %s region probe failed, keeping %q", name, before)
			}
		}(proxy, name)
	}

	wg.Wait()

	if changed {
		g.fastSingle.Reset()
	}
}

func parseGeoSplitOption(config map[string]any) []geoSplitOption {
	opts := []geoSplitOption{}

	preferRU := false
	if elm, ok := config["prefer"]; ok {
		if prefer, ok := elm.(string); ok && prefer == geoSplitClassRU {
			preferRU = true
		}
	}
	opts = append(opts, geoSplitWithPreferRU(preferRU))

	if elm, ok := config["region-check-interval"]; ok {
		if interval, ok := elm.(int); ok && interval > 0 {
			opts = append(opts, geoSplitWithRegionInterval(time.Duration(interval)*time.Second))
		}
	}

	return opts
}

func NewGeoSplit(option *GroupCommonOption, emptyFallback C.Proxy, providers []P.ProxyProvider, options ...geoSplitOption) *GeoSplit {
	geoSplit := &GeoSplit{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.GeoSplit,
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
		fastSingle:     singledo.NewSingle[C.Proxy](time.Second * 10),
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
		regionInterval: defaultRegionCheckInterval,
	}

	for _, opt := range options {
		opt(geoSplit)
	}

	return geoSplit
}

var _ ProxyGroup = (*GeoSplit)(nil)
var _ SelectAble = (*GeoSplit)(nil)
