# A/H Premium Arbitrage Monitor

Real-time monitor for A-share / H-share premium arbitrage opportunities across 169 dual-listed Chinese stocks. Tracks price differentials between Shanghai/Shenzhen (A-shares) and Hong Kong (H-shares) exchanges, with interactive charts, a premium screener, and Telegram alerts.

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

---

# A/H 溢价套利监控

实时监控 169 只 A+H 双重上市股票的 A/H 溢价套利机会。追踪沪深 A 股与香港 H 股之间的价差，提供交互式图表、全市场筛选器和 Telegram 预警。

## 功能

- **实时溢价监控** -- 盘中每 5 秒刷新 H/A 比值和溢价率
- **历史走势图** -- 交互式折线图（Plotly），展示 A 股价格、H 股价格及 H/A 溢价比值
- **溢价筛选器** -- 一键扫描全部 169 对 A/H 股，找出偏离最大的标的
- **Telegram 预警** -- 溢价突破自定义阈值时推送通知
- **汇率追踪** -- Yahoo Finance 实时离岸人民币/港币汇率，SQLite 缓存 + 多源备用
- **自选股管理** -- 侧边栏添加/删除，本地 SQLite 持久化

## 数据架构

由于富途 OpenAPI 不向香港账户提供 A 股数据，项目采用混合数据源架构：

| 组件 | 数据源 | 说明 |
|------|--------|------|
| H 股行情（历史 + 实时） | 富途 OpenAPI（OpenD 网关） | 不复权价格；AKShare 备用 |
| A 股行情（历史 + 实时） | AKShare + 新浪/腾讯 HTTP | 腾讯历史 K 线；新浪实时快照 |
| 汇率（CNH/HKD） | Yahoo Finance | AKShare 备用；SQLite 日缓存 |
| 前端 | Streamlit + Plotly | Fragment 局部刷新（`run_every=5s`） |
| 存储 | SQLite（`~/.ah-arb/data.db`） | 自选股、汇率缓存、K 线缓存、同步元数据 |
| 调度 | APScheduler | 后台历史数据同步任务 |

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

# 富途 OpenD 网关（以下为默认值）
OPEND_HOST=127.0.0.1
OPEND_PORT=11111

# 数据目录（默认 ~/.ah-arb）
# AH_ARB_DB_DIR=/path/to/your/data/dir

# A 股 API 代理（大陆以外地区需要）
# A_SHARE_PROXY_URL=http://user:pass@host:port

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

盘中（UTC+8 9:15--16:15 工作日）每 5 秒自动刷新；非盘中时间仅展示历史数据。

- **A 股交易时段**：9:30--15:00（UTC+8）
- **港股交易时段**：9:30--16:10（UTC+8）

## 核心公式

所有计算均使用**不复权价格**，确保跨市场比较准确。

| 公式 | 定义 |
|------|------|
| H/A 比值 | `(H股港币价 * 每港币离岸人民币) / A股人民币价` |
| H 股溢价率 | `(比值 - 1) * 100%` |

比值 > 1（溢价率为正）表示 H 股相对 A 股存在溢价。汇率约定为每 1 港币兑离岸人民币（约 0.92）。

## 开发

```bash
pip install -r requirements.txt
pytest
```

项目支持 Windows 和 macOS，所有文件路径使用 `pathlib.Path` 确保跨平台兼容。

## 许可证

[Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0)
