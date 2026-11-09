"""UI-only translations. English is returned verbatim; data keys stay stable."""

TRANSLATIONS = {
    "Stock options: Not verified": "个股期权：未核验",
    "Listed (weekly expiries available)": "有（含周期权）",
    "Listed (no weekly expiries)": "有（无周期权）",
    "Not on the listed stock options list": "未列入上市个股期权名单",
    "A-share stock options: No exchange-listed product | H-share stock options: {status}": "A 股个股期权：暂无场内产品｜H 股个股期权：{status}",
    " | Standard contract: {size:,} shares | HKATS: {hkats}": "｜标准合约：{size:,} 股/张｜HKATS 代码：{hkats}",
    "HKEX options list: {date}; A-share market checked: {checked}. Exchange-listed stock options only; excludes ETF options, warrants and OTC options.": "港交所期权名单：{date}；A 股市场核验：{checked}。仅统计场内个股期权，不含 ETF 期权、窝轮及场外期权。",
    "3Y": "3年",
    "5Y": "5年",
    "Eligible": "具备资格",
    "Not on the list": "未列入名单",
    "Short selling eligibility: A-share — Not verified | H-share — Not verified": "做空资格：A 股—未核验｜H 股—未核验",
    "A-share margin short: {a_status} ({a_date}) | H-share short selling: {h_status} ({h_date})": "A 股融券：{a_status}（{a_date}）｜H 股卖空：{h_status}（{h_date}）",
    "Exchange-list eligibility only; broker borrow availability and fees must be checked separately.": "仅指交易所名单资格，实际券源和借券费用须向券商另行确认。",
    "A/H Premium Monitor": "A/H 溢价监控",
    "Use the top search bar to add stocks.": "使用顶部搜索栏添加股票。",
    "Add stocks to watchlist first.": "请先将股票加入自选股。",
    "Stock": "股票",
    "Select stock...": "选择股票…",
    "Threshold %": "溢价率阈值（%）",
    "Max 3 alerts per stock": "每只股票最多设置 3 条预警",
    "**All Crossover Levels:**": "**全部穿越预警阈值：**",
    "**Recent Alerts:**": "**最近预警：**",
    "Select a stock to size a trade.": "请选择股票以计算交易股数。",
    "Waiting for price data…": "正在等待行情数据…",
    "USD per leg": "每条交易腿的金额（美元）",
    "Dark Mode": "深色模式",
    "#### Watchlist": "#### 自选股",
    "#### Trade": "#### 交易",
    "#### Alerts": "#### 预警",
    "Alert Rules": "预警规则",
    "Data sync: OK": "行情同步：正常",
    "H/A Ratio": "H/A 价格比值",
    "Volume (M shares)": "成交量（百万股）",
    "Parity": "平价线",
    "A Volume": "A 股成交量",
    "H Volume": "H 股成交量",
    "A Volume (shares)": "A 股成交量（股）",
    "H Volume (shares)": "H 股成交量（股）",
    "Volume (M)": "成交量（百万股）",
    "Select a stock from the search bar, or add stocks to the watchlist.": "请从搜索栏选择股票，或添加自选股。",
    "Shanghai Stock Exchange": "上海证券交易所",
    "Shenzhen Stock Exchange": "深圳证券交易所",
    "Beijing Stock Exchange": "北京证券交易所",
    "Loading...": "正在加载…",
    "Fetching live data...": "正在获取最新行情…",
    "Failed to fetch A-share data.": "获取 A 股行情失败。",
    "No overlapping trading dates.": "A/H 股行情没有共同交易日。",
    "A Price (¥)": "A 股价格（元）",
    "H Price (HK$)": "H 股价格（港元）",
    "H Premium": "H 股溢价率",
    "FX (HKD/CNH)": "汇率（港元/离岸人民币）",
    "1D H/A Vol": "当日 H/A 成交量比",
    "7D H/A Vol": "近7日 H/A 成交量比",
    "7D H Avg Vol": "近7日港股平均成交量",
    "H Vol Today": "港股当日成交量",
    "Raw Ratio Data": "价格比值原始数据",
    "Screener": "筛选器",
    "Search ticker...": "搜索股票代码…",
    "Timeframe": "时间范围",
    "+ Watch": "+ 自选",
    "Add to watchlist": "加入自选股",
    "Download CSV": "下载 CSV",
    "Fetching data for all A/H pairs...": "正在获取全部 A/H 股行情…",
    "**No screener data available.** Possible causes:\n- Futu OpenD gateway is not running (check `localhost:11111`)\n- Sina/Tencent A-share APIs are unreachable\n- Network connection issues\n\nStart OpenD and refresh the page to load live data.": "**暂无筛选数据。** 可能原因：\n- 富途 OpenD 网关未启动（请检查 `localhost:11111`）\n- 无法访问新浪或腾讯 A 股行情接口\n- 网络连接异常\n\n请启动 OpenD 后刷新页面，以加载最新行情。",
    "**Failed to fetch H-share data.** Possible causes:\n- Futu OpenD gateway is not running (check `localhost:11111`)\n- Network connection to OpenD is blocked or timed out\n- The stock code may be invalid or delisted\n\nAKShare fallback was also attempted. If cached data is available, try again later.": "**获取港股行情失败。** 可能原因：\n- 富途 OpenD 网关未启动（请检查 `localhost:11111`）\n- 无法连接 OpenD，或连接超时\n- 股票代码无效，或股票已退市\n\n已尝试 AKShare 备用数据源。如有缓存行情，可稍后重试。",
    "Chart": "走势图",
    "1M": "1个月",
    "3M": "3个月",
    "6M": "6个月",
    "1Y": "1年",
    "HK div ex-date:": "港股除息日：",
    "Alert: {code} premium {premium:+.1f}%": "预警：{code} 溢价率 {premium:+.1f}%",
    "Deleted {threshold:+.1f}%": "已删除 {threshold:+.1f}% 预警",
    "Added alert @ {threshold:+.1f}%": "已添加 {threshold:+.1f}% 预警",
    "**{code}** not found in A/H pair mapping.": "A/H 股配对表中未找到 **{code}**。",
    "{count} pairs | Click a row to view chart": "{count} 组 A/H 股 | 点击行查看走势图",
    "Follow trading rules before placing orders": "下单前请遵守相关交易规则",
    "Pair discovery failed: {error}. New A/H listings may not be detected today.": "新增配对发现失败：{error}。今日可能无法识别新上市的 A/H 股。",
    "date": "日期",
    "open": "开盘比值",
    "high": "最高比值",
    "low": "最低比值",
    "close": "收盘比值",
    "a_volume": "A 股成交量",
    "h_volume": "H 股成交量",
    "hk_code": "港股代码",
    "a_code": "A 股代码",
    "name": "名称",
    "h_price": "港股价格",
    "a_price": "A 股价格",
    "premium": "H 股溢价率",
    "vol_ratio": "H/A 成交额比",
    "daily_chg": "溢价率日变动",
    "wk_chg": "溢价率5日变动",
    "mo_chg": "溢价率20日变动",
    "ratio_1d": "1日价格比值",
    "fx_rate": "汇率",
    "Data sync: Failed — {error}": "行情同步失败：{error}"
}

HTML_TRANSLATIONS = {
    ">Symbol</th>": ">代码</th>",
    ">Prem%</th>": ">溢价率</th>",
    ">Chg</th>": ">日变动</th>",
    "title=\"Remove\"": "title=\"移出自选股\"",
    "Follow trading rules before placing orders": "下单前请遵守相关交易规则",
    "title=\"A-share is on US sanctions list — brokers may block trading\"": "title=\"A 股被列入美国制裁名单，券商可能限制交易\"",
    "title=\"Red chip — HK-listed entity incorporated outside mainland China\"": "title=\"红筹股：在中国内地以外注册、于香港上市的公司\"",
    ">HK<span": ">港股代码<span",
    ">Name<span": ">名称<span",
    ">Premium<span": ">溢价率<span",
    ">H/A Tvr<span": ">H/A 成交额比<span",
    ">1D<span": ">1日变动<span",
    ">5D<span": ">5日变动<span",
    ">20D<span": ">20日变动<span"
}

def translate(text: str, language: str = "en") -> str:
    if language != "zh":
        return text
    if text in TRANSLATIONS:
        return TRANSLATIONS[text]
    for english, chinese in HTML_TRANSLATIONS.items():
        text = text.replace(english, chinese)
    if text.startswith("Downloading H-share history ("):
        return text.replace("Downloading H-share history", "正在下载港股历史行情").replace("...", "…")
    if text.startswith("Downloading A-share history ("):
        return text.replace("Downloading A-share history", "正在下载 A 股历史行情").replace("...", "…")
    if text.startswith("H-share "):
        return text.replace("H-share ", "港股 ", 1)
    if text.startswith("A-share "):
        return text.replace("A-share ", "A 股 ", 1)
    if text == "Updating today from snapshots...":
        return "正在通过行情快照更新当日数据…"
    return text
