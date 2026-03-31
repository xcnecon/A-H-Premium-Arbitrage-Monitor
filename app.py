"""A/H Premium Arbitrage Monitor — Streamlit Dashboard."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from src.alerts.checker import evaluate_alerts
from src.calc.premium import compute_premium_pct, compute_ratio_ohlcv
from src.calc.screener import compute_screener_table
from src.data.ah_mapping import get_a_code, get_all_pairs, get_pair_name
from src.data.akshare_client import get_a_kline
from src.data.futu_client import get_h_kline
from src.data.fx_client import get_fx_latest, get_fx_range
from src.data.realtime import (
    get_a_snapshot,
    get_a_snapshots_batch,
    get_h_snapshot,
    get_h_snapshots_batch,
)
from src.data.sync import sync_all
from src.storage.db import (
    add_pair,
    delete_alert_rule,
    get_alert_history,
    get_alert_rules,
    get_all_alert_rules_with_state,
    get_watchlist,
    init_db,
    remove_pair,
    upsert_alert_rule,
)
from src.storage.kline_cache import get_premium_history

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
# Suppress noisy Futu SDK connect/disconnect logs
logging.getLogger("futu").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# ─── Theme system ───
def _build_theme_css(dark: bool) -> str:
    """Generate complete theme CSS for Claude-inspired warm UI."""
    if dark:
        bg, surface, sidebar = "#1C1917", "#292524", "#1C1917"
        text, text2 = "#F5F0EB", "#A8A29E"
        border, accent = "#44403C", "#E8956A"
        positive, negative, neutral = "#4ADE80", "#F87171", "#78716C"
        hover, input_bg = "#44403C", "#292524"
    else:
        bg, surface, sidebar = "#FAF9F6", "#FFFFFF", "#F5F0EB"
        text, text2 = "#1A1816", "#6B6560"
        border, accent = "#E8E2DB", "#D97757"
        positive, negative, neutral = "#16A34A", "#DC2626", "#9CA3AF"
        hover, input_bg = "#F0EBE4", "#FFFFFF"

    return f"""<style>
:root {{
  --bg:{bg}; --surface:{surface}; --sidebar:{sidebar};
  --text:{text}; --text-2:{text2};
  --border:{border}; --accent:{accent};
  --positive:{positive}; --negative:{negative}; --neutral:{neutral};
  --hover:{hover}; --input-bg:{input_bg};
  --radius:10px; --radius-sm:6px;
  --font:'Inter',-apple-system,'PingFang SC','Microsoft YaHei',sans-serif;
  --mono:'JetBrains Mono','SF Mono',Consolas,monospace;
}}

/* ── Hide Streamlit chrome ── */
div[data-testid="stToolbar"] {{display:none !important;}}
div[data-testid="stDecoration"] {{display:none !important;}}
footer {{display:none !important;}}
header[data-testid="stHeader"] {{
  background:var(--bg) !important; height:2.5rem !important;
}}

/* ── Global ── */
html, body, .stApp,
[data-testid="stAppViewContainer"],
[data-testid="stMain"] {{
  background-color:var(--bg) !important;
  color:var(--text) !important;
  font-family:var(--font) !important;
}}

/* ── Main content ── */
.stMainBlockContainer,
[data-testid="stMainBlockContainer"],
section.stMain .block-container {{
  padding:2.5rem 2rem 0 2rem !important;
}}

/* ── Sidebar ── */
[data-testid="stSidebar"] {{
  background-color:var(--sidebar) !important;
  border-right:1px solid var(--border) !important;
  min-width:380px; width:380px !important;
  transform:none !important;
}}
/* Hide native collapse/expand — we use our own toggle */
[data-testid="stSidebarCollapseButton"],
[data-testid="stExpandSidebarButton"] {{
  display:none !important;
}}
[data-testid="stSidebar"] > div {{background-color:var(--sidebar) !important;}}
[data-testid="stSidebarUserContent"] {{padding:0.75rem 0.5rem 0.5rem 0.5rem;}}

/* ── Typography ── */
.stMarkdown, .stMarkdown p, .stMarkdown li,
.stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown h4,
label, [data-testid="stWidgetLabel"] {{
  color:var(--text) !important; font-family:var(--font) !important;
}}
.stCaption, [data-testid="stCaption"] {{color:var(--text-2) !important;}}

/* ── Tabs ── */
[data-testid="stTabs"] button[role="tab"] {{
  color:var(--text-2) !important; font-family:var(--font) !important;
  font-weight:500; border-bottom:2px solid transparent;
  background:transparent !important;
}}
[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {{
  color:var(--accent) !important; border-bottom-color:var(--accent) !important;
}}

/* ── Buttons ── */
[data-testid="stButton"] button {{
  background-color:var(--accent) !important; color:#fff !important;
  border:none !important; border-radius:var(--radius) !important;
  font-family:var(--font) !important; font-weight:500;
  transition:all .15s ease;
}}
[data-testid="stButton"] button:hover {{
  filter:brightness(0.9); box-shadow:0 2px 8px rgba(217,119,87,0.3);
}}

/* ── Toggle switch ── */
[data-testid="stToggle"] label span {{
  font-family:var(--font) !important; color:var(--text-2) !important;
  font-size:0.85rem;
}}

/* ── Selectbox ── */
[data-testid="stSelectbox"] div[data-baseweb="select"] > div {{
  background-color:var(--input-bg) !important;
  border-color:var(--border) !important;
  border-radius:var(--radius) !important;
  color:var(--text) !important;
}}

/* ── Pills ── */
[data-testid="stPills"] button {{
  border-radius:20px !important; font-family:var(--font) !important;
  font-weight:500; border:1px solid var(--border) !important;
  background:var(--surface) !important; color:var(--text-2) !important;
  transition:all .15s ease;
}}
[data-testid="stPills"] button[aria-checked="true"] {{
  background:var(--accent) !important; color:#fff !important;
  border-color:var(--accent) !important;
}}

/* ── Metric cards ── */
div[data-testid="metric-container"] {{
  background:var(--surface); border:1px solid var(--border);
  padding:12px 16px; border-radius:var(--radius);
  box-shadow:0 1px 3px rgba(0,0,0,0.04);
}}
div[data-testid="metric-container"] label[data-testid="stMetricLabel"] div {{
  color:var(--text-2); font-size:0.75rem; font-family:var(--font);
  text-transform:uppercase; letter-spacing:0.05em;
}}
div[data-testid="metric-container"] div[data-testid="stMetricValue"] div {{
  font-size:1.3rem; font-weight:600; font-family:var(--mono);
  color:var(--text);
}}

/* ── Expander ── */
[data-testid="stExpander"] {{
  background:var(--surface) !important; border:1px solid var(--border) !important;
  border-radius:var(--radius) !important;
}}

/* ── DataFrame ── */
[data-testid="stDataFrame"] {{border-radius:var(--radius) !important; overflow:hidden;}}

/* ── Alert boxes ── */
[data-testid="stAlert"] {{border-radius:var(--radius) !important;}}

/* ── Divider ── */
hr {{border-color:var(--border) !important;}}

/* ── Scrollbar ── */
::-webkit-scrollbar {{width:6px; height:6px;}}
::-webkit-scrollbar-track {{background:transparent;}}
::-webkit-scrollbar-thumb {{background:var(--border); border-radius:3px;}}
::-webkit-scrollbar-thumb:hover {{background:var(--neutral);}}

/* ── Dropdown/Popover (baseui) ── */
[data-baseweb="popover"] {{background-color:var(--surface) !important;}}
[data-baseweb="menu"] {{background-color:var(--surface) !important;}}
[data-baseweb="menu"] li {{color:var(--text) !important;}}
[data-baseweb="menu"] li:hover {{background-color:var(--hover) !important;}}
[data-baseweb="select"] input {{color:var(--text) !important;}}
[data-baseweb="input"] {{background-color:var(--input-bg) !important;}}

/* ── Toast ── */
[data-testid="stToast"] {{
  background:var(--surface) !important; color:var(--text) !important;
  border:1px solid var(--border) !important; border-radius:var(--radius) !important;
}}

/* ── Spinner ── */
[data-testid="stSpinner"] {{color:var(--accent) !important;}}

/* ── Progress bar ── */
[data-testid="stProgress"] > div > div {{
  background-color:var(--accent) !important;
}}

/* ── Watchlist table ── */
.tv-wl {{
  width:100%; border-collapse:collapse;
  font-family:var(--mono); font-variant-numeric:tabular-nums lining-nums;
  font-size:12px; line-height:1; color:var(--text);
  table-layout:fixed; user-select:none;
}}
.tv-wl th {{
  color:var(--text-2); font-weight:400; font-size:11px;
  padding:8px 8px; white-space:nowrap;
}}
.tv-wl td {{
  padding:2px 8px; border:none; white-space:nowrap;
  overflow:hidden; text-overflow:ellipsis; vertical-align:middle;
}}
.tv-wl .c-sym {{width:42%; text-align:left;}}
.tv-wl .c-prm {{width:28%; text-align:right;}}
.tv-wl .c-chg {{width:30%; text-align:right; position:relative;}}

.tv-wl tbody tr {{
  height:42px; cursor:pointer; transition:background .12s ease;
  position:relative;
}}
.tv-wl tbody tr + tr {{border-top:1px solid var(--border);}}
.tv-wl tbody tr:hover {{background:var(--hover);}}
.tv-wl tbody tr.sel {{
  background:var(--hover); box-shadow:inset 3px 0 0 0 var(--accent);
}}

.tv-wl .sym-code {{color:var(--text); font-weight:500; font-size:12px;}}
.tv-wl .sym-name {{
  color:var(--text-2); font-size:10px; margin-top:1px;
  font-family:var(--font); overflow:hidden; text-overflow:ellipsis;
}}
.tv-up {{color:var(--positive) !important;}}
.tv-dn {{color:var(--negative) !important;}}
.tv-fl {{color:var(--neutral) !important;}}

.tv-wl td a:not(.row-del) {{text-decoration:none; color:inherit; display:block;}}
.tv-wl .row-del a {{color:inherit; text-decoration:none;}}

.tv-wl .row-del {{
  position:absolute; right:4px; top:50%; transform:translateY(-50%);
  opacity:0; transition:opacity .12s; pointer-events:none;
  background:rgba(239,83,80,0.12); color:var(--negative);
  border:none; border-radius:var(--radius-sm); width:18px; height:18px;
  display:inline-flex; align-items:center; justify-content:center;
  font-size:12px; cursor:pointer;
}}
.tv-wl tbody tr:hover .row-del {{opacity:1; pointer-events:auto;}}
.tv-wl .row-del:hover {{background:rgba(239,83,80,0.3);}}
.tv-wl tbody tr:hover .chg-val {{opacity:0.3;}}

/* ── Screener table ── */
.scr-tbl {{
  width:100%; border-collapse:collapse;
  font-family:var(--mono); font-variant-numeric:tabular-nums lining-nums;
  font-size:12px; line-height:1; color:var(--text);
  table-layout:fixed; user-select:none;
}}
.scr-tbl th {{
  color:var(--text-2); font-weight:400; font-size:11px;
  padding:8px 6px; white-space:nowrap; text-align:right;
  position:sticky; top:0; background:var(--surface); z-index:1;
  cursor:pointer; user-select:none;
}}
.scr-tbl th:hover {{color:var(--text);}}
.scr-tbl th .arr {{font-size:9px; margin-left:2px;}}
.scr-tbl th:first-child, .scr-tbl th:nth-child(2) {{text-align:left;}}
.scr-tbl td {{
  padding:6px 6px; border:none; white-space:nowrap;
  overflow:hidden; text-overflow:ellipsis; vertical-align:middle;
  text-align:right;
}}
.scr-tbl td:first-child, .scr-tbl td:nth-child(2) {{text-align:left;}}
.scr-tbl tbody tr {{
  height:34px; cursor:pointer; transition:background .12s ease;
}}
.scr-tbl tbody tr + tr {{border-top:1px solid var(--border);}}
.scr-tbl tbody tr:hover {{background:var(--hover);}}
.scr-tbl td a {{text-decoration:none; color:inherit; display:block;}}
.scr-wrap {{max-height:600px; overflow-y:auto; border-radius:var(--radius);}}
</style>"""


def _chart_colors(dark: bool) -> dict:
    """Get Plotly chart color scheme for current theme."""
    if dark:
        return dict(
            paper="#1C1917",
            plot="#1C1917",
            grid="rgba(255,255,255,0.06)",
            text="#F5F0EB",
            hover="#292524",
            up="#4ADE80",
            up_fill="rgba(74,222,128,0.8)",
            dn="#F87171",
            dn_fill="rgba(248,113,113,0.8)",
            a_bar="rgba(248,113,113,0.45)",
            h_bar="rgba(232,149,106,0.45)",
            parity="rgba(255,255,255,0.25)",
        )
    return dict(
        paper="#FFFFFF",
        plot="#FFFFFF",
        grid="rgba(0,0,0,0.06)",
        text="#1A1816",
        hover="#F5F0EB",
        up="#16A34A",
        up_fill="rgba(22,163,74,0.8)",
        dn="#DC2626",
        dn_fill="rgba(220,38,38,0.8)",
        a_bar="rgba(220,38,38,0.35)",
        h_bar="rgba(217,119,87,0.4)",
        parity="rgba(0,0,0,0.2)",
    )


# ─── Page config ───
st.set_page_config(
    page_title="A/H Premium Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Theme CSS ───
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = False
st.markdown(_build_theme_css(st.session_state.dark_mode), unsafe_allow_html=True)


# ─── Init ───
if "db_initialized" not in st.session_state:
    init_db()
    st.session_state.db_initialized = True
if "selected_hk" not in st.session_state:
    st.session_state.selected_hk = ""

# Handle watchlist click via query_params (<a href="?sel=CODE">)
_qp_sel = st.query_params.get("sel")
if _qp_sel:
    st.session_state.selected_hk = _qp_sel
    st.session_state.ticker_select = None
    st.session_state.active_tab = "Chart"
    st.session_state["_cache_key"] = None
    del st.query_params["sel"]
_qp_del = st.query_params.get("del")
if _qp_del:
    remove_pair(_qp_del)
    if st.session_state.get("selected_hk") == _qp_del:
        st.session_state.selected_hk = ""
    del st.query_params["del"]

# ─── Startup sync: download/update historical K-lines (once per session) ───
if "sync_done" not in st.session_state:
    try:
        with st.spinner("Syncing market data..."):
            summary = sync_all()
        deferred = summary.get("today_deferred", 0) + summary.get("gap_deferred", 0)
        if deferred:
            # Gap-fill and/or today missing — sync_all returned instantly.
            # Do the real work in a background thread so the dashboard loads now.
            import threading
            from src.data.sync import sync_background

            threading.Thread(
                target=sync_background, daemon=True, name="bg-sync"
            ).start()
            logger.info("Deferred %d pairs to background sync", deferred)
        elif summary.get("history_backfill", 0) > 0:
            # First-time full downloads — sync_all already did them (blocking).
            logger.info("Full sync complete: %s", summary)
        st.session_state.sync_done = True
        st.session_state.sync_ok = True
        logger.info("Sync complete: %s", summary)
    except Exception as e:
        logger.error("Sync failed: %s", e)
        st.session_state.sync_done = True
        st.session_state.sync_ok = False
        st.session_state["sync_error"] = str(e)

if st.session_state.get("sync_error"):
    st.warning(f"Data sync failed: {st.session_state['sync_error']}. Showing cached data.")

TIMEFRAMES = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365}


@st.cache_data(ttl=25, show_spinner="Fetching data for all A/H pairs...")
def _cached_screener() -> pd.DataFrame:
    """Cached screener with historical premium changes."""
    df = compute_screener_table()
    if df.empty:
        return df

    # Enrich with historical premium changes from cached premium_daily
    hk_codes = df["hk_code"].tolist()
    hist = get_premium_history(hk_codes, offsets=[1, 5, 20])
    if not hist.empty:
        df = df.merge(hist, on="hk_code", how="left")
        # Compute weekly/monthly premium change (pp)
        for offset, col in [(5, "wk_chg"), (20, "mo_chg")]:
            ratio_col = f"ratio_{offset}d"
            if ratio_col in df.columns:
                current = df["premium"]
                historical = (df[ratio_col] - 1) * 100
                df[col] = current - historical
                df[col] = df[col].round(2)
                df.drop(columns=[ratio_col], inplace=True)
        # Drop ratio_1d if present (daily change already computed from snapshots)
        if "ratio_1d" in df.columns:
            df.drop(columns=["ratio_1d"], inplace=True)
    return df


@st.cache_data(ttl=3600)
def _pair_options() -> list[str]:
    """Return 'HK_CODE — Name' strings for selectbox."""
    pairs = get_all_pairs()
    return [f"{hk} — {info['name']}" for hk, info in sorted(pairs.items())]


def _parse_hk_code(option: str) -> str:
    """Extract HK code from 'HK_CODE — Name'."""
    return option.split("—")[0].strip()


def _is_market_hours() -> bool:
    """Check if current time is within trading hours (UTC+8 09:15–16:15, Mon–Fri)."""
    from datetime import timezone

    now = datetime.now(timezone(timedelta(hours=8)))
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (
        datetime.strptime("09:15", "%H:%M").time()
        <= t
        <= datetime.strptime("16:15", "%H:%M").time()
    )


def _on_ticker_change() -> None:
    opt = st.session_state.get("ticker_select")
    if opt:
        st.session_state.selected_hk = _parse_hk_code(opt)


# ─── Sidebar: Watchlist ───
@st.fragment(run_every=timedelta(seconds=5) if _is_market_hours() else None)
def _watchlist_panel() -> None:
    watchlist = get_watchlist()
    if not watchlist:
        st.caption("Use the top search bar to add stocks.")
        return

    # Batch-fetch live snapshots
    hk_codes = [item["hk_code"] for item in watchlist]
    a_codes = [item["a_code"] for item in watchlist]

    cache_age = time.time() - st.session_state.get("_snap_ts", 0)
    if cache_age < 8 and "_h_snaps" in st.session_state and "_a_snaps" in st.session_state:
        h_snaps = st.session_state["_h_snaps"]
        a_snaps = st.session_state["_a_snaps"]
    else:
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                fut_h = pool.submit(get_h_snapshots_batch, hk_codes)
                fut_a = pool.submit(get_a_snapshots_batch, a_codes)
                h_snaps = fut_h.result(timeout=10)
                a_snaps = fut_a.result(timeout=10)
        except Exception:
            logger.warning("Parallel snapshot fetch failed, falling back to sequential")
            h_snaps = get_h_snapshots_batch(hk_codes)
            a_snaps = get_a_snapshots_batch(a_codes)
        st.session_state["_h_snaps"] = h_snaps
        st.session_state["_a_snaps"] = a_snaps
        st.session_state["_snap_ts"] = time.time()

    # FX rate barely changes intraday — cache for 5 min in session_state
    fx_age = time.time() - st.session_state.get("_fx_ts", 0)
    if fx_age < 300 and "_fx_rate" in st.session_state:
        fx = st.session_state["_fx_rate"]
    else:
        fx = get_fx_latest()
        st.session_state["_fx_rate"] = fx
        st.session_state["_fx_ts"] = time.time()

    # Build row data
    rows: list[dict] = []
    for item in watchlist:
        hk = item["hk_code"]
        h = h_snaps.get(hk)
        a = a_snaps.get(item["a_code"])
        ratio = premium = daily_chg = None
        if h and a and a["price"] > 0:
            ratio = round((h["price"] * fx) / a["price"], 4)
            premium = round((ratio - 1) * 100, 1)
            h_prev = h.get("prev_close", 0)
            a_prev = a.get("prev_close", 0)
            if h_prev > 0 and a_prev > 0:
                prev_ratio = (h_prev * fx) / a_prev
                prev_prem = (prev_ratio - 1) * 100
                daily_chg = round(premium - prev_prem, 2)
        rows.append(
            {
                "code": hk,
                "name": item["name"],
                "ratio": ratio,
                "premium": premium,
                "daily_chg": daily_chg,
            }
        )

    # ── Evaluate alert rules against current premiums ──
    if rows:
        premium_data: dict[str, dict] = {}
        for item, r in zip(watchlist, rows, strict=False):
            if r["premium"] is not None:
                h = h_snaps.get(item["hk_code"])
                a = a_snaps.get(item["a_code"])
                premium_data[item["hk_code"]] = {
                    "premium": r["premium"],
                    "a_price": a["price"] if a else 0,
                    "h_price": h["price"] if h else 0,
                    "daily_chg": r["daily_chg"],
                }
        if premium_data:
            try:
                alert_events = evaluate_alerts(premium_data, fx)
                for ev in alert_events:
                    if ev["event"] == "fired":
                        st.toast(
                            f"Alert: {ev['hk_code']} premium {ev['premium']:+.1f}%",
                            icon="\U0001f514",
                        )
            except Exception as e:
                logger.error("Alert evaluation failed: %s", e)

    # ── Render watchlist as styled HTML table ──
    selected = st.session_state.get("selected_hk", "")
    html = (
        '<table class="tv-wl"><thead><tr>'
        '<th class="c-sym">Symbol</th>'
        '<th class="c-prm">Prem%</th>'
        '<th class="c-chg">Chg</th>'
        "</tr></thead><tbody>"
    )
    for r in rows:
        hk = r["code"]
        sel = "sel" if hk == selected else ""
        if r["premium"] is not None:
            cls = "tv-up" if r["premium"] > 0 else ("tv-dn" if r["premium"] < 0 else "tv-fl")
            prem_s = f"{r['premium']:+.1f}%"
        else:
            cls, prem_s = "tv-fl", "—"
        if r["daily_chg"] is not None:
            chg_cls = (
                "tv-up" if r["daily_chg"] > 0 else ("tv-dn" if r["daily_chg"] < 0 else "tv-fl")
            )
            chg_s = f"{r['daily_chg']:+.2f}%"
        else:
            chg_cls, chg_s = "tv-fl", "—"
        html += (
            f'<tr class="{sel}">'
            f'<td class="c-sym">'
            f'<a href="?sel={hk}" target="_self">'
            f'<div class="sym-code">{hk}</div>'
            f'<div class="sym-name">{r["name"]}</div>'
            f"</a></td>"
            f'<td class="c-prm">'
            f'<a href="?sel={hk}" target="_self">'
            f'<span class="{cls}">{prem_s}</span></a></td>'
            f'<td class="c-chg">'
            f'<a href="?sel={hk}" target="_self"><span class="chg-val {chg_cls}">{chg_s}</span></a>'
            f'<a href="?del={hk}" target="_self" class="row-del" title="Remove">×</a>'
            f"</td></tr>"
        )
    html += "</tbody></table>"
    st.markdown(html, unsafe_allow_html=True)


def _alert_config_panel() -> None:
    """Alert configuration UI — crossover thresholds (max 3 per stock)."""
    watchlist = get_watchlist()
    if not watchlist:
        st.caption("Add stocks to watchlist first.")
        return

    stock_options = [f"{w['hk_code']} \u2014 {w['name']}" for w in watchlist]
    selected = st.selectbox(
        "Stock",
        stock_options,
        key="alert_stock_select",
        label_visibility="collapsed",
        placeholder="Select stock...",
    )
    if not selected:
        return

    alert_hk = selected.split("\u2014")[0].strip()

    # Current crossover levels for this stock
    rules = get_alert_rules(alert_hk)

    # Show existing thresholds with delete buttons
    for r in rules:
        col_val, col_del = st.columns([4, 1])
        with col_val:
            side = r.get("last_side") or ""
            icon = "\u25b2" if side == "above" else ("\u25bc" if side == "below" else "\u2014")
            st.caption(f"{icon} {r['threshold']:+.1f}%")
        with col_del:
            if st.button("\u00d7", key=f"adel_{r['id']}"):
                delete_alert_rule(alert_hk, r["threshold"])
                st.toast(f"Deleted {r['threshold']:+.1f}%", icon="\U0001f5d1")
                st.rerun()

    # Add new threshold (if under limit)
    if len(rules) < 3:
        col_inp, col_add = st.columns([4, 1])
        with col_inp:
            new_val = st.number_input(
                "Threshold %",
                value=0.0,
                step=1.0,
                format="%.1f",
                key="alert_new_val",
                label_visibility="collapsed",
            )
        with col_add:
            if st.button("+", key="alert_add_btn"):
                result = upsert_alert_rule(alert_hk, new_val)
                if result == -1:
                    st.toast("Max 3 alerts per stock", icon="\u26a0\ufe0f")
                else:
                    st.toast(f"Added alert @ {new_val:+.1f}%", icon="\u2705")
                st.rerun()
    elif rules:
        st.caption("Max 3 alerts per stock")

    # Show all configured crossover levels across all stocks
    all_rules = get_all_alert_rules_with_state()
    if all_rules:
        st.markdown("**All Crossover Levels:**")
        for r in all_rules:
            side = r.get("last_side") or ""
            icon = "\u25b2" if side == "above" else ("\u25bc" if side == "below" else "\u2014")
            st.caption(f"{icon} {r['hk_code']} @ {r['threshold']:+.1f}%")


def _recent_alerts_panel() -> None:
    """Show recent crossover alert history in sidebar."""
    history = get_alert_history(limit=5)
    if not history:
        return
    st.markdown("**Recent Alerts:**")
    for h in history:
        if h["event"] in ("fired", "send_failed"):
            icon = "\U0001f514" if h["event"] == "fired" else "\u274c"
            prem = h.get("premium_value")
            prem_s = f"{prem:+.1f}%" if prem is not None else "—"
            dir_icon = "\u2191" if h["direction"] == "cross_up" else "\u2193"
            ts_raw = h.get("created_at", "")
            try:
                ts_utc = datetime.strptime(str(ts_raw)[:19], "%Y-%m-%d %H:%M:%S")
                ts_hkt = ts_utc + timedelta(hours=8)
                ts = ts_hkt.strftime("%m-%d %H:%M")
            except (ValueError, TypeError):
                ts = str(ts_raw)[:16]
            st.caption(f"{icon} {h['hk_code']} {dir_icon} {prem_s} ({ts})")


with st.sidebar:
    st.toggle("Dark Mode", key="dark_mode")
    st.markdown("#### Watchlist")
    _watchlist_panel()

    # ── Alert Configuration ──
    st.markdown("#### Alerts")
    with st.expander("Alert Rules", expanded=False):
        _alert_config_panel()

    # Show recent alerts
    _recent_alerts_panel()

    if st.session_state.get("sync_ok"):
        st.caption("Data sync: OK")
    elif st.session_state.get("sync_error"):
        st.caption(f"Data sync: Failed \u2014 {st.session_state['sync_error'][:50]}")


# ─── Chart builder ───
def _build_chart(df: pd.DataFrame, colors: dict) -> go.Figure:
    dates = pd.to_datetime(df["date"])
    c = colors
    fig = make_subplots(
        rows=2,
        cols=1,
        row_heights=[0.7, 0.3],
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=("H/A Ratio", "Volume (M shares)"),
    )
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=df["close"],
            mode="lines",
            line=dict(color=c["up"], width=1.5),
            name="H/A Ratio",
            hovertemplate="%{y:.4f}<extra></extra>",
        ),
        row=1,
        col=1,
    )
    fig.add_hline(
        y=1.0,
        line_dash="dash",
        line_color=c["parity"],
        opacity=0.6,
        annotation_text="Parity",
        annotation_position="bottom right",
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=dates,
            y=df["a_volume"] / 1e6,
            name="A Volume",
            marker_color=c["a_bar"],
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=dates,
            y=df["h_volume"] / 1e6,
            name="H Volume",
            marker_color=c["h_bar"],
        ),
        row=2,
        col=1,
    )
    fig.update_layout(
        template=None,
        paper_bgcolor=c["paper"],
        plot_bgcolor=c["plot"],
        height=580,
        barmode="stack",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(color=c["text"]),
        ),
        margin=dict(l=50, r=15, t=40, b=30),
        font=dict(
            family="Inter, -apple-system, PingFang SC, sans-serif",
            size=11,
            color=c["text"],
        ),
        hoverlabel=dict(bgcolor=c["hover"], font_size=12),
    )
    fig.update_yaxes(
        title_text="H/A Ratio",
        gridcolor=c["grid"],
        hoverformat=".2f",
        title_font_color=c["text"],
        tickfont_color=c["text"],
        showspikes=True,
        spikecolor="grey",
        spikethickness=1,
        spikedash="dot",
        row=1,
        col=1,
    )
    fig.update_yaxes(
        title_text="Volume (M)",
        gridcolor=c["grid"],
        hoverformat=".2f",
        tickformat=".1f",
        title_font_color=c["text"],
        tickfont_color=c["text"],
        showspikes=True,
        spikecolor="grey",
        spikethickness=1,
        spikedash="dot",
        row=2,
        col=1,
    )
    all_bdays = pd.bdate_range(dates.min(), dates.max())
    data_dates = set(dates.dt.normalize())
    gap_dates = [d for d in all_bdays if d not in data_dates]
    breaks = [dict(bounds=["sat", "mon"])]
    if gap_dates:
        breaks.append(dict(values=[d.strftime("%Y-%m-%d") for d in gap_dates]))
    fig.update_xaxes(
        gridcolor=c["grid"],
        tickfont_color=c["text"],
        rangebreaks=breaks,
        showspikes=True,
        spikecolor="grey",
        spikethickness=1,
        spikedash="dot",
        spikemode="across",
    )
    # Style subplot titles
    for ann in fig.layout.annotations:
        ann.font.color = c["text"]
    return fig


# ─── Chart fragment: data loading + live updates + rendering ───
@st.fragment(run_every=timedelta(seconds=10) if _is_market_hours() else None)
def _chart_panel(timeframe: str) -> None:
    display_hk = st.session_state.get("selected_hk", "")
    if not display_hk:
        st.info("Select a stock from the search bar, or add stocks to the watchlist.")
        return

    a_code = get_a_code(display_hk)
    if not a_code:
        st.error(f"**{display_hk}** not found in A/H pair mapping.")
        return

    stock_name = get_pair_name(display_hk) or display_hk
    st.markdown(f"### {stock_name}  `HK.{display_hk}` / `A.{a_code}`")

    lookback = TIMEFRAMES.get(timeframe, 90)
    end_date = date.today()
    start_date = end_date - timedelta(days=lookback)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    # ── Load K-line data if stock/timeframe changed ──
    cache_key = f"{display_hk}_{timeframe}"
    if st.session_state.get("_cache_key") != cache_key:
        with st.spinner("Loading..."), ThreadPoolExecutor(max_workers=4) as pool:
            fut_h = pool.submit(get_h_kline, display_hk, start_str, end_str)
            fut_a = pool.submit(get_a_kline, a_code, start_str, end_str)
            fut_fx_spot = pool.submit(get_fx_latest)
            fut_fx = pool.submit(get_fx_range, start_str, end_str)
            df_h = fut_h.result(timeout=30)
            df_a = fut_a.result(timeout=30)
            fx_spot = fut_fx_spot.result(timeout=30)
            df_fx = fut_fx.result(timeout=30)

        data_ok = True
        if df_h.empty:
            st.error(
                "**Failed to fetch H-share data.** Possible causes:\n"
                "- Futu OpenD gateway is not running (check `localhost:11111`)\n"
                "- Network connection to OpenD is blocked or timed out\n"
                "- The stock code may be invalid or delisted\n\n"
                "AKShare fallback was also attempted. "
                "If cached data is available, try again later."
            )
            data_ok = False
        elif df_a.empty:
            st.error("Failed to fetch A-share data.")
            data_ok = False
        else:
            df_ratio = compute_ratio_ohlcv(df_a, df_h, df_fx)
            if df_ratio.empty:
                st.error("No overlapping trading dates.")
                data_ok = False

        if not data_ok:
            return

        st.session_state["_cache_key"] = cache_key
        st.session_state["df_ratio"] = df_ratio
        st.session_state["fx_spot"] = fx_spot
        st.session_state["hk_code"] = display_hk
        st.session_state["a_code_display"] = a_code
        # Invalidate chart cache so new stock gets a fresh figure
        st.session_state.pop("_cached_fig", None)
        st.session_state.pop("_chart_h_price", None)
        st.session_state.pop("_chart_a_price", None)

    # ── Live price update ──
    if st.session_state.get("_cache_key") != cache_key:
        return

    hk = st.session_state["hk_code"]
    a_cd = st.session_state["a_code_display"]
    fx = st.session_state.get("_fx_rate") or st.session_state["fx_spot"]

    h_snaps_cache = st.session_state.get("_h_snaps", {})
    a_snaps_cache = st.session_state.get("_a_snaps", {})
    snap_cache_age = time.time() - st.session_state.get("_snap_ts", 0)

    if hk in h_snaps_cache and a_cd in a_snaps_cache and snap_cache_age < 30:
        h_snap = h_snaps_cache[hk]
        a_snap = a_snaps_cache[a_cd]
    else:
        h_snap = get_h_snapshot(hk)
        a_snap = get_a_snapshot(a_cd)

    # Track whether data actually changed to skip expensive chart rebuild
    _prev_h = st.session_state.get("_chart_h_price")
    _prev_a = st.session_state.get("_chart_a_price")

    df_ratio = st.session_state["df_ratio"]
    data_changed = False

    if h_snap and a_snap and h_snap["price"] > 0 and a_snap["price"] > 0:
        # Skip update if prices are identical to last rerun
        if h_snap["price"] != _prev_h or a_snap["price"] != _prev_a:
            data_changed = True
            df_ratio = df_ratio.copy()
            st.session_state["_chart_h_price"] = h_snap["price"]
            st.session_state["_chart_a_price"] = a_snap["price"]

            today = date.today()
            last_date = df_ratio.iloc[-1]["date"]
            if hasattr(last_date, "date"):
                last_date = last_date.date()
            elif isinstance(last_date, str):
                last_date = datetime.strptime(str(last_date)[:10], "%Y-%m-%d").date()

            h_cny = h_snap["price"] * fx
            live_ratio = h_cny / a_snap["price"]
            live_open = (h_snap["open"] * fx) / a_snap["open"] if a_snap["open"] > 0 else live_ratio
            live_high = (
                max(live_ratio, (h_snap["high"] * fx) / a_snap["low"])
                if a_snap["low"] > 0
                else live_ratio
            )
            live_low = (
                min(live_ratio, (h_snap["low"] * fx) / a_snap["high"])
                if a_snap["high"] > 0
                else live_ratio
            )

            if last_date == today:
                idx = df_ratio.index[-1]
                df_ratio.at[idx, "close"] = live_ratio
                df_ratio.at[idx, "high"] = max(df_ratio.at[idx, "high"], live_high)
                df_ratio.at[idx, "low"] = min(df_ratio.at[idx, "low"], live_low)
                df_ratio.at[idx, "a_volume"] = a_snap["volume"]
                df_ratio.at[idx, "h_volume"] = h_snap["volume"]
            else:
                new_row = pd.DataFrame(
                    [
                        {
                            "date": pd.Timestamp(today),
                            "open": live_open,
                            "high": live_high,
                            "low": live_low,
                            "close": live_ratio,
                            "a_volume": a_snap["volume"],
                            "h_volume": h_snap["volume"],
                        }
                    ]
                )
                df_ratio = pd.concat([df_ratio, new_row], ignore_index=True)

            st.session_state["df_ratio"] = df_ratio

    # ── Render metrics + chart ──
    latest = df_ratio.iloc[-1]
    current_ratio = latest["close"]
    premium_pct = compute_premium_pct(current_ratio)

    tail_7 = df_ratio.tail(7)
    h_vol_7 = tail_7["h_volume"].sum()
    a_vol_7 = tail_7["a_volume"].sum()
    vol_ratio_7d = h_vol_7 / a_vol_7 if a_vol_7 > 0 else 0.0

    h_vol_1d = latest["h_volume"]
    a_vol_1d = latest["a_volume"]
    vol_ratio_1d = h_vol_1d / a_vol_1d if a_vol_1d > 0 else 0.0

    h_vol_avg_7d = tail_7["h_volume"].mean()

    h_vol_today = h_snap["volume"] if h_snap else latest["h_volume"]

    col_m1, col_m2, col_m3, col_m4, col_m5, col_m6 = st.columns(6)
    col_m1.metric("H Premium", f"{premium_pct:+.2f}%")
    col_m2.metric("FX (CNH/HKD)", f"{fx:.4f}")
    col_m3.metric("1D H/A Vol", f"{vol_ratio_1d:.2f}x")
    col_m4.metric("7D H/A Vol", f"{vol_ratio_7d:.2f}x")
    col_m5.metric("7D H Avg Vol", f"{h_vol_avg_7d / 1e6:.1f}M")
    col_m6.metric("H Vol Today", f"{h_vol_today / 1e6:.1f}M")

    # Reuse cached figure if prices haven't changed (avoids expensive rebuild + serialization)
    if data_changed or "_cached_fig" not in st.session_state:
        fig = _build_chart(df_ratio, _chart_colors(st.session_state.dark_mode))
        st.session_state["_cached_fig"] = fig
    else:
        fig = st.session_state["_cached_fig"]
    st.plotly_chart(fig, width="stretch", key="live_chart")

    with st.expander("Raw Ratio Data"):
        disp = df_ratio.copy()
        disp["date"] = pd.to_datetime(disp["date"]).dt.strftime("%Y-%m-%d")
        disp["a_volume"] = disp["a_volume"].apply(lambda x: f"{x / 1e6:.1f}M" if x > 0 else "—")
        disp["h_volume"] = disp["h_volume"].apply(lambda x: f"{x / 1e6:.1f}M" if x > 0 else "—")
        st.dataframe(disp, width="stretch", hide_index=True)


# ─── Tabs ───
tab_chart, tab_screener = st.tabs(
    ["Chart", "Screener"],
    key="active_tab",
)

with tab_chart:
    col_ticker, col_tf, col_add = st.columns([4, 3, 1], vertical_alignment="bottom")
    with col_ticker:
        st.selectbox(
            "Stock",
            options=_pair_options(),
            index=None,
            placeholder="Search ticker...",
            label_visibility="collapsed",
            key="ticker_select",
            on_change=_on_ticker_change,
        )
    with col_tf:
        timeframe = st.pills(
            "Timeframe",
            options=list(TIMEFRAMES.keys()),
            default="3M",
            label_visibility="collapsed",
            key="tf_pills",
        )
        if not timeframe:
            timeframe = "3M"
    with col_add:
        if st.button("+ Watch", width="stretch", help="Add to watchlist"):
            hk = st.session_state.selected_hk
            if hk:
                a = get_a_code(hk)
                name = get_pair_name(hk) or hk
                if a:
                    add_pair(hk, a, name)
                    st.rerun()

    _chart_panel(timeframe)


@st.fragment(run_every=timedelta(seconds=20) if _is_market_hours() else None)
def _screener_panel() -> None:
    df_scr = _cached_screener()

    if df_scr.empty:
        st.warning(
            "**No screener data available.** Possible causes:\n"
            "- Futu OpenD gateway is not running (check `localhost:11111`)\n"
            "- Sina/Tencent A-share APIs are unreachable\n"
            "- Network connection issues\n\n"
            "Start OpenD and refresh the page to load live data."
        )
        return

    # Pre-sort by premium desc (JS re-applies saved sort on load)
    if "premium" in df_scr.columns:
        df_scr = df_scr.sort_values("premium", ascending=False, na_position="last")

    st.caption(f"{len(df_scr)} pairs | Click a row to view chart")

    def _fmt(val, fmt: str) -> str:
        if pd.isna(val) or val is None:
            return "\u2014"
        return fmt.format(val)

    def _cls(val) -> str:
        if pd.isna(val) or val is None:
            return "tv-fl"
        return "tv-up" if val > 0 else ("tv-dn" if val < 0 else "tv-fl")

    html = (
        '<div class="scr-wrap"><table class="scr-tbl" id="scr-tbl"><thead><tr>'
        '<th data-t="s">HK<span class="arr"></span></th>'
        '<th data-t="s">Name<span class="arr"></span></th>'
        '<th data-t="n">Premium<span class="arr"></span></th>'
        '<th data-t="n">H/A Tvr<span class="arr"></span></th>'
        '<th data-t="n">1D<span class="arr"></span></th>'
        '<th data-t="n">5D<span class="arr"></span></th>'
        '<th data-t="n">20D<span class="arr"></span></th>'
        "</tr></thead><tbody>"
    )
    for _, r in df_scr.iterrows():
        hk = r["hk_code"]
        name = r.get("name", "")
        prem = r.get("premium")
        vol = r.get("vol_ratio")
        d1 = r.get("daily_chg")
        d5 = r.get("wk_chg")
        d20 = r.get("mo_chg")
        html += (
            f"<tr>"
            f'<td><a href="?sel={hk}" target="_self">{hk}</a></td>'
            f'<td><a href="?sel={hk}" target="_self">{name}</a></td>'
            f'<td><a href="?sel={hk}" target="_self">'
            f'<span class="{_cls(prem)}">{_fmt(prem, "{:.2f}%")}</span></a></td>'
            f"<td>{_fmt(vol * 100 if pd.notna(vol) else None, '{:.1f}%')}</td>"
            f'<td><span class="{_cls(d1)}">{_fmt(d1, "{:+.2f}%")}</span></td>'
            f'<td><span class="{_cls(d5)}">{_fmt(d5, "{:+.2f}%")}</span></td>'
            f'<td><span class="{_cls(d20)}">{_fmt(d20, "{:+.2f}%")}</span></td>'
            f"</tr>"
        )
    html += "</tbody></table></div>"
    st.markdown(html, unsafe_allow_html=True)

    # Sort JS — st.html injects into parent page (not iframe), so JS can
    # access the table rendered by st.markdown above.
    st.html(
        """<script>
(function(){
  var SK='ah_scr_sort',tbl=document.getElementById('scr-tbl');
  if(!tbl||tbl._sortBound)return;
  tbl._sortBound=true;
  var ths=tbl.querySelectorAll('thead th');
  var s=JSON.parse(localStorage.getItem(SK)||'{"c":2,"a":false}');
  function doSort(ci,asc){
    var body=tbl.tBodies[0],rows=Array.from(body.rows),tp=ths[ci].dataset.t;
    rows.sort(function(a,b){
      var at=a.cells[ci].textContent.trim(),bt=b.cells[ci].textContent.trim();
      if(tp==='n'){
        var an=parseFloat(at.replace(/[^\\d.\\-]/g,'')),bn=parseFloat(bt.replace(/[^\\d.\\-]/g,''));
        if(isNaN(an))an=asc?1e9:-1e9;if(isNaN(bn))bn=asc?1e9:-1e9;
        return asc?an-bn:bn-an;
      }
      return asc?at.localeCompare(bt):bt.localeCompare(at);
    });
    rows.forEach(function(r){body.appendChild(r);});
    ths.forEach(function(h,i){
      var ar=h.querySelector('.arr');
      if(ar)ar.textContent=i===ci?(asc?' \\u25B2':' \\u25BC'):'';
    });
  }
  doSort(s.c,s.a);
  ths.forEach(function(th,idx){
    th.addEventListener('click',function(){
      var asc=(s.c===idx)?!s.a:false;
      s={c:idx,a:asc};
      localStorage.setItem(SK,JSON.stringify(s));
      doSort(idx,asc);
    });
  });
})();
</script>""",
        unsafe_allow_javascript=True,
    )

    if not df_scr.empty:
        csv = df_scr.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="ah_screener.csv",
            mime="text/csv",
        )


with tab_screener:
    _screener_panel()
