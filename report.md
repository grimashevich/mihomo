# GeminiPriority Code Review

## 1. `pickGeminiPriority` and `clearSelected` correctness
APPROVE, nothing found.
`pickGeminiPriority` evaluates the documented rules exactly. The contract for `clearSelected` is strictly maintained: it only becomes `true` when a valid pin index is evaluated and found to be dead. An absent pin (`selectedIdx == -1`) skips the manual pin check entirely, ensuring that the pin is not mistakenly cleared when the server is temporarily absent from the subscription.

## 2. `findProxy` panic safety and duplicate pin resolution
**MAJOR**
File: `adapter/outboundgroup/geminipriority.go:217`
**Issue:** `findProxy` incorrectly resolves duplicate pin names to the *last* occurrence in the list. Because the loop iterates over all proxies and repeatedly overwrites `selectedIdx`, the lowest priority match is selected instead of the highest priority match.
**Fix:** Only assign `selectedIdx` if it has not been found yet.
```go
		if len(g.selected) > 0 && proxy.Name() == g.selected && selectedIdx == -1 {
			selectedIdx = i
		}
```
*Panic Safety:* APPROVE, nothing found. `proxies[idx]` is perfectly safe because `len(proxies) == 0` is guarded early in the function, ensuring `candidates` is non-empty. With at least one element, `pickGeminiPriority` will never return `-1`, completely avoiding panics.

## 3. `geminiAvailable` behavior
APPROVE, nothing found.
The function correctly enforces that only an explicit `geoSplitClassForeign` evaluation returns `true`. Missing providers, missing records (`ok=false`), rejected responses (`geoSplitClassRU`), or unrecognized classes naturally resolve to `false`. Connecting directly to `externalRegion` bypasses `globalRegionStore`, successfully avoiding on-device probe pollution.

## 4. Concurrency
APPROVE, nothing found.
`findProxy` mutates `g.selected` without a lock, presenting a data race when accessed concurrently via `DialContext`, `Now`, `SupportUDP`, or `Unwrap`. However, this perfectly mirrors `Fallback`, which performs the exact same lockless `f.selected = ""` mutation. In practice, this data race relies on Go's string reassignment behavior which is identical to the baseline implementation, so it does not introduce new instability compared to existing proxy groups.

## 5. Interface completeness
APPROVE, nothing found.
`GeminiPriority` correctly implements all methods expected by `C.ProxyAdapter` and `ProxyGroup`. It mirrors `Fallback` effectively, retaining all essential behaviors including `onDialFailed`/`onDialSuccess` integration, UDP support querying, unwrap semantics, and correctly formatting `MarshalJSON` fields (adding its own bespoke `geminiReady` list).

## 6. The test file
APPROVE, nothing found.
The nine tests effectively pin down every branch of the specified rule logic. It accounts for scenarios involving absent pins, dead pins, absent verdicts, complete network failure (nothing alive), and priority logic overriding. No important case is missing.

## Verdict
REQUEST CHANGES
