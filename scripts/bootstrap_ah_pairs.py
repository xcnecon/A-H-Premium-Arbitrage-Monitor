"""
Bootstrap script to generate the complete A/H dual-listed stock mapping.

Data source: AAStocks A+H comparison page (http://www.aastocks.com/en/stocks/market/ah.aspx)
Cross-referenced with Huaxi Securities AH concept list for Simplified Chinese names.

This script embeds the mapping data collected from multiple sources and outputs
a JSON file in the format:
    {"HK_CODE": {"a_code": "A_CODE", "name": "CHINESE_NAME"}, ...}

HK codes are 5-digit zero-padded (e.g., "00939").
A-share codes are 6-digit (e.g., "601939").

Usage:
    python scripts/bootstrap_ah_pairs.py
"""

import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Complete A/H mapping compiled from:
# 1. AAStocks A+H page (filter=1 Shanghai, filter=2 Shenzhen, filter=3 all)
# 2. Huaxi Securities AH concept list (Simplified Chinese names)
# 3. Manual verification for key stocks
#
# Format: (hk_code, a_code, simplified_chinese_name)
# Last updated: 2026-03-25

RAW_PAIRS: list[tuple[str, str, str]] = [
    # --- Shanghai Main Board (601xxx, 600xxx, 603xxx, 605xxx) ---
    ("00038", "601038", "一拖股份"),
    ("00107", "601107", "四川成渝"),
    ("00168", "600600", "青岛啤酒"),
    ("00177", "600377", "宁沪高速"),
    ("00187", "600860", "京城股份"),
    ("00317", "600685", "中船防务"),
    ("00323", "600808", "马钢股份"),
    ("00338", "600688", "上海石化"),
    ("00386", "600028", "中国石化"),
    ("00390", "601390", "中国中铁"),
    ("00525", "601333", "广深铁路"),
    ("00548", "600548", "深高速"),
    ("00553", "600775", "南京熊猫"),
    ("00564", "601717", "郑煤机"),
    ("00588", "601588", "北辰实业"),
    ("00598", "601598", "中国外运"),
    ("00670", "600115", "中国东航"),
    ("00699", "600699", "均胜电子"),
    ("00728", "601728", "中国电信"),
    ("00753", "601111", "中国国航"),
    ("00811", "601811", "新华文轩"),
    ("00857", "601857", "中国石油"),
    ("00874", "600332", "白云山"),
    ("00883", "600938", "中国海油"),
    ("00902", "600011", "华能国际"),
    ("00914", "600585", "海螺水泥"),
    ("00939", "601939", "建设银行"),
    ("00941", "600941", "中国移动"),
    ("00956", "600956", "新天绿能"),
    ("00991", "601991", "大唐发电"),
    ("00995", "600012", "皖通高速"),
    ("00998", "601998", "中信银行"),
    ("01033", "600871", "石化油服"),
    ("01055", "600029", "南方航空"),
    ("01065", "600874", "创业环保"),
    ("01071", "600027", "华电国际"),
    ("01072", "600875", "东方电气"),
    ("01088", "601088", "中国神华"),
    ("01108", "600876", "凯盛新能"),
    ("01138", "600026", "中远海能"),
    ("01171", "600188", "兖矿能源"),
    ("01186", "601186", "中国铁建"),
    ("01288", "601288", "农业银行"),
    ("01330", "601330", "绿色动力"),
    ("01336", "601336", "新华保险"),
    ("01339", "601319", "中国人保"),
    ("01375", "601375", "中原证券"),
    ("01398", "601398", "工商银行"),
    ("01456", "601456", "国联证券"),
    ("01528", "601828", "美凯龙"),
    ("01618", "601618", "中国中冶"),
    ("01635", "600635", "大众公用"),
    ("01658", "601658", "邮储银行"),
    ("01766", "601766", "中国中车"),
    ("01787", "600547", "山东黄金"),
    ("01800", "601800", "中国交建"),
    ("01898", "601898", "中煤能源"),
    ("01919", "601919", "中远海控"),
    ("01963", "601963", "重庆银行"),
    ("01988", "600016", "民生银行"),
    ("02009", "601992", "金隅集团"),
    ("02016", "601916", "浙商银行"),
    ("02068", "601068", "中铝国际"),
    ("02196", "600196", "复星医药"),
    ("02218", "605198", "安德利"),
    ("02238", "601238", "广汽集团"),
    ("02318", "601318", "中国平安"),
    ("02333", "601633", "长城汽车"),
    ("02465", "603906", "龙蟠科技"),
    ("02600", "601600", "中国铝业"),
    ("02601", "601601", "中国太保"),
    ("02607", "601607", "上海医药"),
    ("02611", "601211", "国泰海通"),
    ("02628", "601628", "中国人寿"),
    ("02648", "603345", "安井食品"),
    ("02727", "601727", "上海电气"),
    ("02866", "601866", "中远海发"),
    ("02880", "601880", "辽港股份"),
    ("02883", "601808", "中海油服"),
    ("02899", "601899", "紫金矿业"),
    ("03288", "603288", "海天味业"),
    ("03328", "601328", "交通银行"),
    ("03369", "601326", "秦港股份"),
    ("03606", "600660", "福耀玻璃"),
    ("03618", "601077", "渝农商行"),
    ("03750", "300750", "宁德时代"),  # ChiNext but listed here for visibility
    ("03866", "002948", "青岛银行"),  # SME but listed here for visibility
    ("03898", "688187", "时代电气"),  # STAR but listed here for visibility
    ("03908", "601995", "中金公司"),
    ("03958", "600958", "东方证券"),
    ("03968", "600036", "招商银行"),
    ("03969", "688009", "中国通号"),
    ("03988", "601988", "中国银行"),
    ("03993", "603993", "洛阳钼业"),
    ("03996", "601868", "中国能建"),
    ("06030", "600030", "中信证券"),
    ("06031", "600031", "三一重工"),
    ("06066", "601066", "中信建投"),
    ("06099", "600999", "招商证券"),
    ("06127", "603127", "昭衍新药"),
    ("06166", "603083", "剑桥科技"),
    ("06178", "601788", "光大证券"),
    ("06185", "688185", "康希诺"),
    ("06198", "601298", "青岛港"),
    ("06655", "600801", "华新水泥"),
    ("06690", "600690", "海尔智家"),
    ("06818", "601818", "光大银行"),
    ("06837", "600837", "海通证券"),
    ("06865", "601865", "福莱特"),
    ("06869", "601869", "长飞光纤"),
    ("06881", "601881", "中国银河"),
    ("06886", "601688", "华泰证券"),
    ("09927", "601127", "赛力斯"),
    ("01053", "601005", "重庆钢铁"),
    ("01276", "600276", "恒瑞医药"),
    ("01880", "601888", "中国中免"),
    ("02359", "603259", "药明康德"),
    ("06693", "600988", "赤峰黄金"),
    # --- Shenzhen Main Board (000xxx) ---
    ("00347", "000898", "鞍钢股份"),
    ("00719", "000756", "新华制药"),
    ("00763", "000063", "中兴通讯"),
    ("00921", "000921", "海信家电"),
    ("01513", "000513", "丽珠集团"),
    ("01812", "000488", "晨鸣纸业"),
    ("02039", "000039", "中集集团"),
    ("02202", "000002", "万科A"),
    # --- Shenzhen SME Board (002xxx) ---
    ("00568", "002490", "山东墨龙"),
    ("00895", "002672", "东江环保"),
    ("01057", "002703", "浙江世宝"),
    ("01157", "000157", "中联重科"),
    ("01211", "002594", "比亚迪"),
    ("01772", "002460", "赣锋锂业"),
    ("01776", "000776", "广发证券"),
    ("02050", "002050", "三花智控"),
    ("02208", "002202", "金风科技"),
    ("02338", "000338", "潍柴动力"),
    ("02579", "300919", "中伟股份"),
    ("02603", "002803", "吉宏股份"),
    ("02865", "002865", "钧达股份"),
    ("06196", "002936", "郑州银行"),
    ("06806", "000166", "申万宏源"),
    ("06821", "002821", "凯莱英"),
    ("06936", "002352", "顺丰控股"),
    ("09696", "002466", "天齐锂业"),
    ("09989", "002399", "海普瑞"),
    # --- Shenzhen ChiNext (300xxx) ---
    ("00300", "000333", "美的集团"),
    ("00638", "300638", "广和通"),
    ("03347", "300347", "泰格医药"),
    ("03759", "300759", "康龙化成"),
    ("06613", "300433", "蓝思科技"),
    ("06680", "300748", "金力永磁"),
    # --- Shenzhen Main Board new codes (001xxx, 003xxx) ---
    ("00916", "001289", "龙源电力"),
    ("01816", "003816", "中广核电力"),
    ("03678", "001236", "弘业期货"),
    # --- STAR Market / 科创板 (688xxx) ---
    ("00981", "688981", "中芯国际"),
    ("01304", "688279", "峰岹科技"),
    ("01347", "688347", "华虹公司"),
    ("01349", "688505", "复旦张江"),
    ("01385", "688385", "复旦微电"),
    ("01858", "688236", "春立医疗"),
    ("01877", "688180", "君实生物"),
    ("02315", "688796", "百奥赛图"),
    ("02402", "688339", "亿华通"),
    ("02631", "688234", "天岳先进"),
    ("02676", "688052", "纳芯微"),
    ("06160", "688235", "百济神州"),
    ("06826", "688366", "昊海生科"),
    ("09969", "688428", "诺诚健华"),
    ("09995", "688331", "荣昌生物"),
]


def build_ah_pairs() -> dict[str, dict[str, str]]:
    """Build the A/H pairs mapping dictionary from the raw data."""
    pairs: dict[str, dict[str, str]] = {}
    seen_a_codes: dict[str, str] = {}

    for hk_code, a_code, name in RAW_PAIRS:
        # Validate format
        if len(hk_code) != 5 or not hk_code.isdigit():
            logger.warning("Invalid HK code format: %s (%s)", hk_code, name)
            continue
        if len(a_code) != 6 or not a_code.isdigit():
            logger.warning("Invalid A-share code format: %s (%s)", a_code, name)
            continue

        # Check for duplicate HK codes
        if hk_code in pairs:
            logger.warning(
                "Duplicate HK code: %s -> existing=%s, new=(%s, %s)",
                hk_code,
                pairs[hk_code],
                a_code,
                name,
            )
            continue

        # Check for duplicate A-share codes (some legit, e.g. 000338 潍柴动力
        # has both 01071 and 02338, but 01071 is actually 华电国际/600027)
        if a_code in seen_a_codes:
            logger.info(
                "A-share code %s appears for both HK:%s and HK:%s (%s)",
                a_code,
                seen_a_codes[a_code],
                hk_code,
                name,
            )

        seen_a_codes[a_code] = hk_code
        pairs[hk_code] = {"a_code": a_code, "name": name}

    return pairs


def main() -> None:
    pairs = build_ah_pairs()

    # Sort by HK code for readability
    sorted_pairs = dict(sorted(pairs.items()))

    # Output path
    output_path = Path(__file__).resolve().parent.parent / "src" / "data" / "ah_pairs.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(sorted_pairs, f, ensure_ascii=False, indent=2)

    logger.info("Written %d A/H pairs to %s", len(sorted_pairs), output_path)

    # Verify key stocks
    key_stocks = {
        "00939": ("601939", "建设银行"),
        "03750": ("300750", "宁德时代"),
        "00916": ("001289", "龙源电力"),
        "02318": ("601318", "中国平安"),
        "01398": ("601398", "工商银行"),
    }
    for hk, (expected_a, expected_name) in key_stocks.items():
        entry = sorted_pairs.get(hk)
        if entry is None:
            logger.error("MISSING key stock: HK:%s (%s)", hk, expected_name)
        elif entry["a_code"] != expected_a:
            logger.error(
                "WRONG A-code for HK:%s: got %s, expected %s",
                hk,
                entry["a_code"],
                expected_a,
            )
        else:
            logger.info("OK: HK:%s -> A:%s (%s)", hk, entry["a_code"], entry["name"])


if __name__ == "__main__":
    main()
