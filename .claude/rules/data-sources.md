---
paths:
  - "src/data/**/*.py"
---

# Data Source Rules

## H-share Data
- Futu OpenAPI is the sole source for H-share data (real-time and historical)
- Use `autype=AuType.NONE` for K-line — unadjusted prices only
- Real-time: `get_market_snapshot()` — fast, no subscription slot needed
- Fallback to AKShare `stock_hk_daily()` only when OpenD is unavailable
- Always handle OpenD disconnection gracefully — log and surface status in UI

## A-share Data
- AKShare `stock_zh_a_hist_tx()` (Tencent source) for historical K-line
- Sina HTTP (`hq.sinajs.cn`) for real-time quotes, Tencent (`qt.gtimg.cn`) as fallback
- **Volume from Tencent is in lots (手) — always ×100 to get shares**
- Use `adjust=""` (unadjusted) for premium calculation
- EastMoney endpoints (`*_em`) are BLOCKED — never use them

## FX Rate (CNH/HKD)
- Live: Yahoo Finance `HKDCNH=X` (offshore CNH, tradeable)
- Historical: Yahoo Finance `HKDCNY=X` as proxy (CNH has no Yahoo history; spread < 0.1%)
- Backup: AKShare `fx_spot_quote()`
- Cache daily rates in SQLite `fx_rates` table
- Convention: rate ≈ 0.92 = CNH per 1 HKD
- Convert HKD→CNH: multiply by rate (NOT divide)
- `DEFAULT_FX_RATE` (0.9170) is centralized in `src/config/settings.py` — used as last-resort fallback when all sources + cache fail; import from there, never hardcode

## General
- Minimum 3-second interval between AKShare calls (rate limits)
- Real-time updates only during market hours (9:15–16:15 UTC+8, weekdays)
- K-line fetching is parallelized with `ThreadPoolExecutor` — H-share, A-share, FX range, and FX spot all run concurrently in `app.py`; bulk A-share sync uses `SYNC_A_WORKERS` (10) threads in `src/data/sync.py`
