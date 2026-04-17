# A/H Premium Arbitrage Monitor

## Project Overview

Real-time monitor for A-share / H-share premium arbitrage opportunities across dual-listed Chinese stocks. Tracks price differentials between Shanghai/Shenzhen (A-shares) and Hong Kong (H-shares) exchanges.

## Tech Stack

- **Language**: Python 3.10+
- **H-share data**: Futu OpenAPI (py-futu-api)
  - Historical K-line: `request_history_kline()` with `autype=AuType.NONE` (unadjusted)
  - Real-time: `get_market_snapshot()` for live price snapshots
  - OpenD gateway: TCP connection (default `127.0.0.1:11111`)
- **A-share data**: AKShare + Sina/Tencent HTTP
  - K-line: `stock_zh_a_hist_tx()` (Tencent source), `adjust=""` (unadjusted)
  - Real-time: Sina HTTP API (`hq.sinajs.cn`), Tencent fallback (`qt.gtimg.cn`)
  - **Volume unit**: Tencent returns lots (手), must ×100 to convert to shares
  - NOTE: EastMoney endpoints are BLOCKED from this network
- **FX rate (CNH/HKD)**:
  - Live: Yahoo Finance `HKDCNH=X` (offshore CNH, tradeable)
  - Historical: Yahoo Finance `HKDCNY=X` as proxy (CNH has no Yahoo history; spread < 0.1%)
  - Backup: AKShare `fx_spot_quote()`
  - Cached daily in SQLite `fx_rates` table
  - Convention: rate ≈ 0.92 = **CNH per 1 HKD** (1 HKD = 0.92 CNH)
  - To convert HKD→CNH: `H_CNH = H_HKD × rate`
- **A/H mapping**: Single root CSV (`ah_pairs.csv`) — auto-grown by daily HKEX widget discovery; manual edits for red-chip A+H pairs
- **Dashboard**: Streamlit + Plotly (candlestick + stacked volume subplots)
- **Real-time updates**: `@st.fragment(run_every=5s)` — only during market hours (9:15–16:15 UTC+8, weekdays)
- **Task scheduling**: APScheduler (background sync jobs)
- **Storage**: SQLite (`~/.ah-arb/data.db`) — watchlist, FX rate cache, K-line cache, sync metadata

## Key Formulas

- **H/A Ratio** = `(H_HKD × CNH_per_HKD) / A_CNY` — ratio > 1 means H premium
- **H Premium %** = `(ratio - 1) × 100`
- **Ratio K-line high** ≈ `(H_high × fx) / A_low`
- **Ratio K-line low** ≈ `(H_low × fx) / A_high`

## Key Constraints

- Futu OpenAPI does NOT serve A-share data to HK-based accounts — hence the hybrid architecture
- User has Futu account with HK Level 2 market data (10-level depth, broker queue, tick-by-tick)
- Must use **unadjusted prices** (不复权) for premium calculation — qfq adjustments differ between A/H shares
- Market hours: A-share 9:30–15:00, HK 9:30–16:10 (UTC+8), real-time fragment covers 9:15–16:15

## Cross-Platform

- Must run on both Windows and macOS (OpenD has builds for both)
- Use `pathlib.Path` for all file paths, never hardcode OS-specific separators
- Use `Path.home()` for data directories — no hardcoded `C:\` or `/Users/` paths
- OpenD host/port configurable via `src/config/settings.py`

## Build & Run

```bash
pip install -r requirements.txt
# OpenD gateway must be running
streamlit run app.py
# Tests
pytest
```

## Project Structure

```
ah-arb/
├── CLAUDE.md
├── requirements.txt
├── ah_pairs.csv                # Canonical A/H pair registry (status, is_red_chip, source, ...)
├── app.py                      # Streamlit dashboard (historical load + live fragment)
├── src/
│   ├── config/settings.py      # OPEND_HOST/PORT, DB_PATH, lookback days, DEFAULT_FX_RATE
│   ├── data/
│   │   ├── ah_mapping.py       # CSV-backed HK↔A lookup + add/delisted helpers
│   │   ├── pair_discovery.py   # Daily HKEX widget scan + Telegram alerts (unknown / dead-A)
│   │   ├── futu_client.py      # H-share K-line (Futu, AKShare fallback)
│   │   ├── akshare_client.py   # A-share K-line (Tencent source)
│   │   ├── fx_client.py        # FX rates (Yahoo Finance, AKShare, SQLite cache, 5-min 429 cooldown)
│   │   ├── realtime.py         # Live snapshots (Futu snapshot, Sina/Tencent HTTP)
│   │   └── sync.py             # K-line sync orchestration (historical + daily snapshots)
│   ├── alerts/
│   │   ├── checker.py          # Alert condition evaluation + rate limiting
│   │   └── telegram.py         # Telegram bot notification delivery
│   ├── calc/
│   │   ├── premium.py          # Ratio OHLCV computation, premium %
│   │   └── screener.py         # Real-time A/H premium screener (all active pairs)
│   └── storage/
│       ├── db.py               # SQLite: watchlist CRUD + FX rate cache + sync/scan metadata
│       └── kline_cache.py      # K-line cache storage — bulk read/write for A/H daily bars
├── tests/
│   ├── test_mapping.py
│   ├── test_premium.py
│   └── test_db.py
└── .claude/
    ├── skills/
    └── rules/
```

## Coding Standards

- Type hints for all function signatures
- `logging` module only, never `print()`
- Logger calls use `%s` formatting, never f-strings: `logger.info("Got %s rows", n)` not `logger.info(f"Got {n} rows")`
- Keep modules small — one responsibility per file
- Unadjusted prices for all premium/ratio calculations
- Default FX rate defined once in `src/config/settings.py` (`DEFAULT_FX_RATE`) — import from there, never hardcode

## Agent Behavior

- **Maximize parallelism**: Always prefer launching multiple subagents concurrently over doing work sequentially in the main agent
- **Main agent should not block**: Delegate all heavy work (file writes, data fetching, research, code generation) to subagents; the main agent's role is to orchestrate, review results, and communicate with the user
- Use `run_in_background` for long-running bash commands; use the Agent tool for complex multi-step work
- When multiple independent tasks exist, launch them as parallel agents in a single message — never serialize work that can be parallelized

## Testing & Verification

- After completing changes, **always self-test**: run `pytest`, launch the app, and verify the feature works end-to-end
- Use screenshot analysis (Read tool on screenshots) to verify UI changes visually when needed
- Do not ask the user to help test unless there is a hard dependency (e.g. requires a live trading gateway the user must start)
- If a test fails, fix it autonomously — iterate until green

## Claude Discretion

Claude has discretion to add, modify, and remove:
- Skills files under `.claude/skills/`
- Memory files under the project memory directory
- Rules files under `.claude/rules/`

Claude should proactively create or update these when it learns something useful about the project, the user's preferences, or recurring workflows — without needing explicit permission each time.
