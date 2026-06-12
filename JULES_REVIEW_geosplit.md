# Code Review for `geosplit.go`

## CRITICAL

### Missing `NetWork` and `Type` fields in `C.Metadata` when dialing probe proxy
**File**: `adapter/outboundgroup/geosplit.go:387`
**Issue**: Inside `probeRegion`, the `DialContext` function for the HTTP client's transport creates a `C.Metadata` to dial through the proxy. It calls `meta.SetRemoteAddress(address)` but does not initialize `meta.NetWork` and `meta.Type`. Many proxy protocols require `NetWork` (e.g., `C.TCP`) and `Type` (e.g., `C.HTTP`) to be set to function properly. Without them, dials could fail or behave unexpectedly.
**Suggested Fix**: Set `meta.NetWork = C.TCP` and `meta.Type = C.HTTP` before dialing.

```go
<<<<<<< SEARCH
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			var meta C.Metadata
			if err := meta.SetRemoteAddress(address); err != nil {
				return nil, err
			}
			return proxy.DialContext(ctx, &meta)
		},
=======
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			var meta C.Metadata
			meta.NetWork = C.TCP
			meta.Type = C.HTTP
			if err := meta.SetRemoteAddress(address); err != nil {
				return nil, err
			}
			return proxy.DialContext(ctx, &meta)
		},
>>>>>>> REPLACE
```

## MAJOR

*No major issues found.*

## MINOR

*No minor issues found.*
