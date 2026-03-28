# A/H Premium Arbitrage Monitor

Real-time monitor for A-share / H-share premium arbitrage opportunities across 169 dual-listed Chinese stocks. Tracks price differentials between Shanghai/Shenzhen (A-shares) and Hong Kong (H-shares) exchanges, with interactive candlestick charts, a premium screener, and Telegram alerts.

![Chart view](docs/screenshot-chart.png)
![Screener view](docs/screenshot-screener.png)

## Features

- **Real-time premium monitoring** -- live H/A ratio and premium % updates every 5 seconds during market hours
- **Historical charts** -- interactive line charts (Plotly) for A-share price, H-share price, and H/A premium ratio
- **Premium screener** -- scan all 169 A/H pairs at once to find the widest dislocations
- **Telegram alerts** -- configurable threshold-based notifications when premium crosses user-defined levels
- **FX rate tracking** -- live CNH/HKD rate from Yahoo Finance with SQLite caching and fallback sources
- **Configurable watchlist** -- add/remove pairs from the sidebar; persisted in local SQLite

## Architecture

The project uses a hybrid data architecture because Futu OpenAPI does not serve A-share data to HK-based accounts:

| Component | Source | Notes |
|-----------|--------|-------|
| H-share K-line & real-time | Futu OpenAPI (OpenD gateway) | Unadjusted prices; AKShare fallback |
| A-share K-line & real-time | AKShare + Sina/Tencent HTTP | Tencent K-line source; Sina real-time |
| FX rate (CNH/HKD) | Yahoo Finance | AKShare backup; cached daily in SQLite |
| Dashboard | Streamlit + Plotly | Fragment-based live updates (`run_every=5s`) |
| Storage | SQLite (`~/.ah-arb/data.db`) | Watchlist, FX cache, K-line cache, sync metadata |
| Scheduling | APScheduler | Background sync jobs for historical data |

## Prerequisites

- Python 3.10+
- [Futu OpenD](https://openapi.futunn.com/futu-api-doc/en/intro/intro.html) gateway running locally (for H-share data)
- Futu account with HK market data subscription
- (Optional) A proxy for Sina/Tencent APIs if running outside mainland China

## Installation

```bash
git clone https://github.com/xcnecon/ah-arb.git
cd ah-arb
cp .env.example .env
# Edit .env with your credentials (see Configuration below)
pip install -r requirements.txt
```

## Configuration

Copy `.env.example` to `.env` and edit as needed:

```bash
# Required for Telegram alerts (leave blank to disable)
TELEGRAM_BOT_TOKEN=your_token_here
TELEGRAM_CHAT_ID=your_chat_id_here

# Futu OpenD gateway (defaults shown)
OPEND_HOST=127.0.0.1
OPEND_PORT=11111

# Override default data directory (~/.ah-arb)
# AH_ARB_DB_DIR=/path/to/your/data/dir

# Proxy for A-share APIs (Sina/Tencent) -- needed outside mainland China
# A_SHARE_PROXY_URL=http://user:pass@host:port

# Thread pool sizes for historical sync
# SYNC_A_WORKERS=10
# SYNC_H_WORKERS=4
```

All settings are loaded via `python-dotenv` in `src/config/settings.py`.

## Usage

1. Start the Futu OpenD gateway.
2. Launch the dashboard:

```bash
streamlit run app.py
```

The dashboard auto-refreshes every 5 seconds during market hours (9:15--16:15 UTC+8, weekdays). Outside market hours, only historical data is displayed.

- **A-share market hours**: 9:30--15:00 (UTC+8)
- **H-share market hours**: 9:30--16:10 (UTC+8)

## Key Formulas

All calculations use **unadjusted prices** to ensure accurate cross-market comparison.

| Formula | Definition |
|---------|------------|
| H/A Ratio | `(H_HKD * CNH_per_HKD) / A_CNY` |
| H Premium % | `(ratio - 1) * 100` |
| Ratio Close | `(H_close * fx) / A_close` |

A ratio > 1 (positive premium %) means the H-share trades at a premium to its A-share counterpart.

The FX rate convention is CNH per 1 HKD (approximately 0.92).

## Project Structure

```
ah-arb/
├── app.py                      # Streamlit dashboard (historical + live fragment)
├── requirements.txt
├── .env.example                # Environment variable template
├── src/
│   ├── config/settings.py      # OPEND_HOST/PORT, DB_PATH, DEFAULT_FX_RATE, etc.
│   ├── data/
│   │   ├── ah_pairs.json       # Static A/H pair mapping (169 pairs)
│   │   ├── ah_mapping.py       # HK <-> A code lookup
│   │   ├── futu_client.py      # H-share K-line (Futu, AKShare fallback)
│   │   ├── akshare_client.py   # A-share K-line (Tencent source)
│   │   ├── fx_client.py        # FX rates (Yahoo Finance, AKShare, SQLite cache)
│   │   ├── realtime.py         # Live snapshots (Futu snapshot, Sina/Tencent HTTP)
│   │   └── sync.py             # K-line sync orchestration
│   ├── calc/
│   │   ├── premium.py          # Ratio OHLCV computation, premium %
│   │   └── screener.py         # Real-time A/H premium screener (all 169 pairs)
│   └── storage/
│       ├── db.py               # SQLite: watchlist CRUD, FX cache, sync metadata
│       └── kline_cache.py      # K-line cache read/write for daily bars
├── scripts/
│   └── bootstrap_ah_pairs.py   # Regenerate ah_pairs.json from web sources
└── tests/
    ├── test_mapping.py
    ├── test_premium.py
    └── test_db.py
```

## Development

```bash
pip install -r requirements.txt
pytest
```

The project runs on both Windows and macOS. All file paths use `pathlib.Path` for cross-platform compatibility.

## License

[Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0)
