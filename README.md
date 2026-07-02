# A/H Premium Arbitrage Monitor

Real-time monitor for A-share / H-share premium arbitrage opportunities across all dual-listed Chinese stocks. Tracks price differentials between Shanghai/Shenzhen (A-shares) and Hong Kong (H-shares) exchanges, with interactive charts, a premium screener, and Telegram alerts. The pair list grows automatically — a daily background job scans HKEX for newly-listed A+H pairs.

![Chart view](docs/screenshot-chart.png)
![Screener view](docs/screenshot-screener.png)

## Features

- **Real-time premium monitoring** -- live H/A ratio and premium % updates every 5 seconds during market hours
- **Historical charts** -- interactive line charts (Plotly) for A-share price, H-share price, and H/A premium ratio
- **Premium screener** -- scan every active A/H pair at once to find the widest dislocations
- **Telegram alerts** -- configurable threshold notifications during the A/H overlap session (9:30--15:00 HKT)
- **Dividend ex-date flag** -- syncs the HKEX "Dividends & Other Entitlements" table once per day; the chart highlights H-share ex-dates falling within the next 30 days
- **Special-event alerts** -- detects single-side trading halts (one leg not trading for 2+ sessions while the other trades) and sends Telegram notices on both onset and resolution
- **FX rate tracking** -- live HKD/CNH and USD/HKD rates from Eastmoney with SQLite caching and fallbacks
- **Configurable watchlist** -- add/remove pairs from the sidebar; persisted in local SQLite

## Architecture

The project uses a hybrid data architecture because Futu OpenAPI does not serve A-share data to HK-based accounts:

| Component | Source | Notes |
|-----------|--------|-------|
| H-share K-line & real-time | Futu OpenAPI (OpenD gateway) | Unadjusted prices; AKShare fallback |
| A-share K-line & real-time | AKShare + Sina/Tencent HTTP | Tencent K-line source; Sina real-time; A-share trading calendar skips mainland holidays |
| FX rates (HKD/CNH, USD/HKD) | Eastmoney + AKShare + Yahoo Finance fallback | HKD/CNH uses 60-second SQLite spot cache for intraday updates; USD/HKD uses 1-hour SQLite spot cache |
| Dashboard | Streamlit + Plotly | Fragment-based live updates (`run_every=5s`) |
| Storage | SQLite (`~/.ah-arb/data.db`) | Watchlist, FX cache, K-line cache, HKEX entitlements, special-event state, sync metadata |
| Scheduling | Background daemon threads at app startup | Historical K-line sync, daily HKEX pair-discovery scan, daily entitlements refresh |

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
# Optional dedicated proxy/timeout for Telegram Bot API delivery.
# If unset, Telegram falls back only to HTTP/HTTPS YAHOO_PROXY_URL.
# TELEGRAM_PROXY_URL=http://user:pass@host:port
# TELEGRAM_TIMEOUT=15

# Futu OpenD gateway (defaults shown)
OPEND_HOST=127.0.0.1
OPEND_PORT=11111

# Override default data directory (~/.ah-arb)
# AH_ARB_DB_DIR=/path/to/your/data/dir

# Proxy for A-share APIs (Sina/Tencent) -- needed outside mainland China
# A_SHARE_PROXY_URL=http://user:pass@host:port

# Proxy for the Yahoo Finance FX fallback (also inherited by Telegram when
# TELEGRAM_PROXY_URL is unset) -- needed inside mainland China
# YAHOO_PROXY_URL=http://127.0.0.1:7890

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

The dashboard uses Streamlit fragments for live updates: watchlist every 5 seconds, chart every 10 seconds, and screener every 20 seconds. Telegram premium alerts are sent only during the true A/H arbitrage overlap window (9:30--15:00 UTC+8, weekdays).

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

The FX label is HKD/CNH, quoted as CNH per 1 HKD (approximately 0.92).

## Project Structure

```
ah-arb/
├── ah_pairs.csv                # Canonical A/H pair registry (status, is_red_chip, source, ...)
├── app.py                      # Streamlit dashboard (historical + live fragment)
├── requirements.txt
├── .env.example                # Environment variable template
├── src/
│   ├── config/settings.py      # OPEND_HOST/PORT, DB_PATH, DEFAULT_FX_RATE, etc.
│   ├── data/
│   │   ├── ah_mapping.py       # CSV-backed HK <-> A code lookup + add/delisted helpers
│   │   ├── pair_discovery.py   # Daily HKEX widget scan + Telegram alerts
│   │   ├── futu_client.py      # H-share K-line (Futu, AKShare fallback)
│   │   ├── futu_ctx.py         # Shared singleton Futu OpenQuoteContext
│   │   ├── akshare_client.py   # A-share K-line (Tencent source)
│   │   ├── fx_client.py        # FX rates (Eastmoney, AKShare, Yahoo fallback, SQLite cache)
│   │   ├── hkex_entitlements.py # Daily HKEX Dividends & Other Entitlements table sync
│   │   ├── realtime.py         # Live snapshots (Futu snapshot, Sina/Tencent HTTP)
│   │   └── sync.py             # K-line sync orchestration
│   ├── alerts/
│   │   ├── checker.py          # Alert condition evaluation + rate limiting
│   │   ├── special_events.py   # Single-side trading-halt detection + notifications
│   │   └── telegram.py         # Telegram bot notification delivery
│   ├── calc/
│   │   ├── premium.py          # Ratio OHLCV computation, premium %
│   │   └── screener.py         # Real-time A/H premium screener (all active pairs)
│   └── storage/
│       ├── db.py               # SQLite: watchlist CRUD, FX cache, sync/scan metadata
│       └── kline_cache.py      # K-line cache read/write for daily bars
└── tests/                      # pytest suite covering the modules above
```

## Development

```bash
pip install -r requirements.txt
pytest
```

The project runs on both Windows and macOS. All file paths use `pathlib.Path` for cross-platform compatibility.

## License

[Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0)

---

# A/H 溢价套利监控

实时监控全部 A+H 双重上市股票的 A/H 溢价套利机会。追踪沪深 A 股与香港 H 股之间的价差，提供交互式图表、全市场筛选器和 Telegram 预警。配对列表自动扩充——每日后台任务扫描港交所新上市的 A+H 股。

## 功能

- **实时溢价监控** -- 盘中每 5 秒刷新 H/A 比值和溢价率
- **历史走势图** -- 交互式折线图（Plotly），展示 A 股价格、H 股价格及 H/A 溢价比值
- **溢价筛选器** -- 一键扫描全部 A/H 股，找出偏离最大的标的
- **Telegram 预警** -- 仅在 A/H 共同套利时段（HKT 9:30--15:00）推送阈值突破通知
- **除息日提示** -- 每日同步港交所"股息及其他权益"表，图表中标注 30 天内的 H 股除息日
- **特殊事件预警** -- 检测单边停牌（一边连续 2 个及以上交易日无成交而另一边正常交易），事件发生与恢复时均推送 Telegram 通知
- **汇率追踪** -- 东方财富实时 HKD/CNH 与 USD/HKD 汇率，SQLite 缓存 + 多源备用
- **自选股管理** -- 侧边栏添加/删除，本地 SQLite 持久化

## 数据架构

由于富途 OpenAPI 不向香港账户提供 A 股数据，项目采用混合数据源架构：

| 组件 | 数据源 | 说明 |
|------|--------|------|
| H 股行情（历史 + 实时） | 富途 OpenAPI（OpenD 网关） | 不复权价格；AKShare 备用 |
| A 股行情（历史 + 实时） | AKShare + 新浪/腾讯 HTTP | 腾讯历史 K 线；新浪实时快照；A 股交易日历跳过内地假期 |
| 汇率（HKD/CNH、USD/HKD） | 东方财富 + AKShare + Yahoo Finance 兜底 | HKD/CNH 使用 60 秒 SQLite spot 缓存保留盘中更新；USD/HKD 使用 1 小时 SQLite spot 缓存 |
| 前端 | Streamlit + Plotly | Fragment 局部刷新（`run_every=5s`） |
| 存储 | SQLite（`~/.ah-arb/data.db`） | 自选股、汇率缓存、K 线缓存、港交所权益表、特殊事件状态、同步元数据 |
| 调度 | 启动时后台守护线程 | 历史 K 线同步、每日港交所新配对扫描、每日权益表刷新 |

## 前置条件

- Python 3.10+
- [富途 OpenD](https://openapi.futunn.com/futu-api-doc/intro/intro.html) 网关已启动
- 富途账户已订阅港股行情
- （可选）中国大陆以外地区需要代理访问新浪/腾讯 API

## 安装

```bash
git clone https://github.com/xcnecon/ah-arb.git
cd ah-arb
cp .env.example .env
# 编辑 .env 填入你的凭证（见下方"配置"）
pip install -r requirements.txt
```

## 配置

将 `.env.example` 复制为 `.env` 并按需编辑：

```bash
# Telegram 预警（留空则禁用）
TELEGRAM_BOT_TOKEN=your_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
# Telegram Bot API 专用代理/超时（可选；未设置时仅回落到 HTTP/HTTPS YAHOO_PROXY_URL）
# TELEGRAM_PROXY_URL=http://user:pass@host:port
# TELEGRAM_TIMEOUT=15

# 富途 OpenD 网关（以下为默认值）
OPEND_HOST=127.0.0.1
OPEND_PORT=11111

# 数据目录（默认 ~/.ah-arb）
# AH_ARB_DB_DIR=/path/to/your/data/dir

# A 股 API 代理（大陆以外地区需要）
# A_SHARE_PROXY_URL=http://user:pass@host:port

# Yahoo Finance 汇率兜底代理（未设置 TELEGRAM_PROXY_URL 时 Telegram 也会继承）——大陆地区需要
# YAHOO_PROXY_URL=http://127.0.0.1:7890

# 历史同步线程池大小
# SYNC_A_WORKERS=10
# SYNC_H_WORKERS=4
```

## 使用

1. 启动富途 OpenD 网关
2. 运行面板：

```bash
streamlit run app.py
```

前端使用 Streamlit fragments 局部刷新：自选股每 5 秒、图表每 10 秒、筛选器每 20 秒。Telegram 溢价预警仅在真正 A/H 套利重叠时段（UTC+8 9:30--15:00，工作日）发送。

- **A 股交易时段**：9:30--15:00（UTC+8）
- **港股交易时段**：9:30--16:10（UTC+8）

## 核心公式

所有计算均使用**不复权价格**，确保跨市场比较准确。

| 公式 | 定义 |
|------|------|
| H/A 比值 | `(H股港币价 * 每港币离岸人民币) / A股人民币价` |
| H 股溢价率 | `(比值 - 1) * 100%` |

比值 > 1（溢价率为正）表示 H 股相对 A 股存在溢价。汇率显示为 HKD/CNH，约定为每 1 港币兑离岸人民币（约 0.92）。

## 开发

```bash
pip install -r requirements.txt
pytest
```

项目支持 Windows 和 macOS，所有文件路径使用 `pathlib.Path` 确保跨平台兼容。

## 许可证

[Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0)
