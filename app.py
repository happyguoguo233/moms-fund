import streamlit as st
import pandas as pd
import akshare as ak
import json
import os
import time
import re
import requests
import random
from datetime import datetime
import concurrent.futures
import pytz

import traceback

# ==========================================
# 配置与常量
# ==========================================
DATA_FILE = "funds.json"
UPDATE_INTERVAL = 30  # 自动刷新间隔（秒）
COLOR_UP = "#D22222"  # 红色（涨）
COLOR_DOWN = "#008000"  # 绿色（跌）
COLOR_NEUTRAL = "#333333"  # 灰色（平）
COLOR_RED = "#D22222"
COLOR_GREEN = "#008000"
COLOR_GRAY = "#333333"
LAST_A_STOCK_CACHE = {"price_map": {}, "change_map": {}, "update_time": None}
# 这里的逻辑是：只从配置文件读取。
# 本地运行时，它会自动读你电脑里的 .streamlit/secrets.toml
# 云端运行时，它会自动读 Streamlit Cloud 的后台配置
# 这样代码里就不包含任何密码，非常安全！
JSONBIN_API_KEY = st.secrets["JSONBIN_API_KEY"]
JSONBIN_BIN_ID = st.secrets["JSONBIN_BIN_ID"]

# 页面配置
st.set_page_config(
    page_title="长辈基金助手",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 初始化 Session State
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = False
if "last_update" not in st.session_state:
    st.session_state.last_update = datetime.now()
if "all_funds_list" not in st.session_state:
    st.session_state.all_funds_list = None


# ==========================================
# CSS 样式注入 (针对长辈优化)
# ==========================================
def inject_custom_css():
    st.markdown(f"""
        <style>
        /* 全局字体放大 */
        html, body, [class*="css"] {{
            font-family: "Microsoft YaHei", sans-serif;
            font-size: 20px !important;
        }}
        
        /* 标题增强 */
        h1 {{ font-size: 40px !important; font-weight: bold; }}
        h2 {{ font-size: 30px !important; color: #111; }}
        h3 {{ font-size: 26px !important; }}
        
        /* 表格数字特大号加粗 */
        div[data-testid="stMetricValue"] {{
            font-size: 34px !important;
            font-weight: 900 !important;
        }}
        
        /* 侧边栏字体 */
        section[data-testid="stSidebar"] label {{
            font-size: 18px !important;
            font-weight: bold;
        }}
        
        /* 按钮放大 */
        button {{
            height: auto !important;
            padding-top: 10px !important;
            padding-bottom: 10px !important;
        }}
        div[data-testid="stMarkdownContainer"] p {{
            font-size: 20px !important;
        }}
        
        /* 自定义涨跌幅样式类 */
        .trend-up {{ color: {COLOR_UP}; font-weight: bold; }}
        .trend-down {{ color: {COLOR_DOWN}; font-weight: bold; }}
        .trend-flat {{ color: {COLOR_NEUTRAL}; font-weight: bold; }}
        
        /* 调整 Metric 组件的 Label 颜色，增加对比度 */
        div[data-testid="stMetricLabel"] {{
            color: #222 !important;
            font-weight: bold;
        }}
        
        /* 强制 Metric 值颜色，防止在白色背景下变白 */
        div[data-testid="stMetricValue"] {{
            color: #111 !important;
        }}

        div[data-testid="stHorizontalBlock"] {{
            gap: 0.75rem !important;
        }}
        div[data-testid="column"] {{
            padding-left: 0.25rem !important;
            padding-right: 0.25rem !important;
        }}
        
        /* 优化 Selectbox 下拉框宽度和换行 */
        div[data-baseweb="select"] > div {{
            white-space: normal !important;
            word-wrap: break-word !important;
            min-width: 300px !important; /* 强制加宽 */
        }}
        
        /* 下拉菜单选项换行 */
        ul[data-baseweb="menu"] li span {{
             white-space: normal !important;
             max-width: 100% !important;
        }}
        details, details > summary {{
            width: 100% !important;
        }}
        div[data-testid="stExpander"] {{
            width: 100% !important;
        }}
        [data-testid="stSidebarCollapsedControl"] {{
            transform: scale(1.3);
            background: #FFE5E5;
            border-radius: 10px;
            padding: 6px;
        }}

        @media (max-width: 600px) {{
            html, body, [class*="css"] {{
                font-size: 16px !important;
            }}
            div[data-testid="stMetricValue"] {{
                font-size: 24px !important;
            }}
            div[data-testid="stHorizontalBlock"] {{
                gap: 0.35rem !important;
            }}
            div[data-testid="column"] {{
                padding-left: 0.1rem !important;
                padding-right: 0.1rem !important;
            }}
        }}
        </style>
    """, unsafe_allow_html=True)

def normalize_stock_code(value):
    value_str = str(value).strip()
    if not value_str:
        return ""
    if value_str.lower().startswith("hk"):
        digits = re.sub(r"\D", "", value_str)
        return digits.zfill(5) if digits else ""
    digits = re.sub(r"\D", "", value_str)
    if len(digits) >= 6:
        return digits[-6:]
    if len(digits) == 5 and digits.startswith("0"):
        return digits
    return digits

def is_hk_stock(code, name):
    digits = re.sub(r"\D", "", str(code))
    if digits and len(digits) == 5 and digits.startswith("0"):
        return True
    if "HK" in str(name).upper():
        return True
    if str(code).lower().startswith("hk"):
        return True
    return False

# ==========================================
# 数据存储管理
# ==========================================
def load_funds():
    try:
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {"X-Master-Key": JSONBIN_API_KEY}
        resp = requests.get(url, headers=headers, timeout=8)
        resp.raise_for_status()
        payload = resp.json()
        record = payload.get("record")
        if isinstance(record, list):
            return record
        return []
    except Exception as e:
        st.error(f"云端连接错误: {e}")
        if not os.path.exists(DATA_FILE):
            return []
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

def save_funds(data):
    try:
        url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
        headers = {
            "X-Master-Key": JSONBIN_API_KEY,
            "Content-Type": "application/json"
        }
        resp = requests.put(url, headers=headers, json=data, timeout=8)
        resp.raise_for_status()
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.error(f"云端连接错误: {e}")

# ==========================================
# 核心数据获取逻辑 (并发加速)
# ==========================================
def get_market_index(symbol_name, symbol_code):
    """获取单个大盘指数"""
    try:
        url = f"http://qt.gtimg.cn/q={symbol_code}"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, timeout=5, headers=headers)
        resp.encoding = "gbk"
        text = resp.text.strip()
        if not text:
            return None
        for line in text.split(";"):
            line = line.strip()
            if not line or "=" not in line or "v_" not in line:
                continue
            _, right = line.split("=", 1)
            data = right.strip().strip('"')
            fields = data.split("~") if data else []
            if len(fields) < 5:
                continue
            name = fields[1]
            price = pd.to_numeric(fields[3], errors="coerce")
            change_pct = pd.to_numeric(fields[32], errors="coerce") if len(fields) > 32 else None
            if pd.isna(price):
                price = 0.0
            if pd.isna(change_pct):
                change_pct = 0.0
            return {"name": name, "symbol": symbol_code, "price": float(price), "change_pct": float(change_pct)}
        return None
    except Exception:
        return None

@st.cache_data(ttl=3600, persist="disk")
def get_all_funds_list():
    """获取所有基金列表（用于搜索）"""
    try:
        return ak.fund_name_em()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_stock_realtime_price_batch(stock_codes):
    """
    批量获取股票实时行情 (利用 A 股实时接口)
    """
    if not stock_codes:
        if LAST_A_STOCK_CACHE["price_map"] and LAST_A_STOCK_CACHE["change_map"]:
            return LAST_A_STOCK_CACHE["price_map"], LAST_A_STOCK_CACHE["change_map"]
        return {}, {}

    if isinstance(stock_codes, (list, tuple, set)):
        wanted = {normalize_stock_code(c) for c in stock_codes}
    else:
        wanted = {normalize_stock_code(stock_codes)}
    wanted = {c for c in wanted if c}

    def to_tencent_code(code):
        if len(code) == 5 and code.startswith("0"):
            return f"hk{code}"
        if code.startswith("6"):
            return f"sh{code}"
        if code.startswith("0") or code.startswith("3"):
            return f"sz{code}"
        if code.startswith("8") or code.startswith("4") or code.startswith("9"):
            return f"bj{code}"
        return None

    tencent_items = [(to_tencent_code(c), c) for c in wanted]
    tencent_items = [(t, c) for t, c in tencent_items if t]
    tencent_codes = [t for t, _ in tencent_items]
    code_map = {t: c for t, c in tencent_items}
    if not tencent_codes:
        if LAST_A_STOCK_CACHE["price_map"] and LAST_A_STOCK_CACHE["change_map"]:
            price_map = LAST_A_STOCK_CACHE["price_map"]
            change_map = LAST_A_STOCK_CACHE["change_map"]
            price_map = {k: v for k, v in price_map.items() if k in wanted}
            change_map = {k: v for k, v in change_map.items() if k in wanted}
            return price_map, change_map
        return {}, {}

    price_map = {}
    change_map = {}
    batch_size = 80
    headers = {"User-Agent": "Mozilla/5.0"}
    for i in range(0, len(tencent_codes), batch_size):
        batch = tencent_codes[i:i + batch_size]
        url = "http://qt.gtimg.cn/q=" + ",".join(batch)
        try:
            resp = requests.get(url, timeout=5, headers=headers)
            resp.encoding = "gbk"
            text = resp.text.strip()
        except Exception as e:
            traceback.print_exc()
            print(f"获取股票行情失败: {e}")
            continue
        if not text:
            continue
        lines = text.split(";")
        for line in lines:
            line = line.strip()
            if not line or "=" not in line or "v_" not in line:
                continue
            try:
                left, right = line.split("=", 1)
                code_with_prefix = left.split("v_")[-1]
                data = right.strip().strip('"')
                if not data:
                    continue
                fields = data.split("~")
                if len(fields) < 5:
                    continue
                latest = pd.to_numeric(fields[3], errors="coerce")
                prev_close = pd.to_numeric(fields[4], errors="coerce")
                change_pct = pd.to_numeric(fields[32], errors="coerce") if len(fields) > 32 else None
                if pd.isna(latest):
                    latest = 0.0
                if pd.isna(prev_close):
                    prev_close = 0.0
                if pd.isna(change_pct):
                    change_pct = (latest - prev_close) / prev_close * 100 if prev_close > 0 else 0.0
                code_key = code_map.get(code_with_prefix)
                if not code_key:
                    if code_with_prefix.startswith("hk"):
                        code_key = code_with_prefix[2:]
                    else:
                        code_key = code_with_prefix[-6:]
                price_map[code_key] = float(latest)
                change_map[code_key] = float(change_pct)
            except Exception:
                continue

    if price_map and change_map:
        LAST_A_STOCK_CACHE["price_map"] = price_map
        LAST_A_STOCK_CACHE["change_map"] = change_map
        LAST_A_STOCK_CACHE["update_time"] = datetime.now()
        return price_map, change_map

    if LAST_A_STOCK_CACHE["price_map"] and LAST_A_STOCK_CACHE["change_map"]:
        price_map = LAST_A_STOCK_CACHE["price_map"]
        change_map = LAST_A_STOCK_CACHE["change_map"]
        price_map = {k: v for k, v in price_map.items() if k in wanted}
        change_map = {k: v for k, v in change_map.items() if k in wanted}
        return price_map, change_map
    return {}, {}

def get_fund_portfolio(fund_code):
    """获取基金前十大重仓股"""
    try:
        current_year = datetime.now().year
        df = ak.fund_portfolio_hold_em(symbol=fund_code, date=current_year)
        if df.empty:
            df = ak.fund_portfolio_hold_em(symbol=fund_code, date=current_year - 1)
        
        if df.empty:
            return []

        def pick_col(dataframe, candidates, contains=None):
            for c in candidates:
                if c in dataframe.columns:
                    return c
            if contains:
                for c in dataframe.columns:
                    if any(k in str(c) for k in contains):
                        return c
            return None

        quarter_col = pick_col(df, ["季度"], contains=["季度"])
        ratio_col = pick_col(df, ["占净值比例"], contains=["占净值"])
        code_col = pick_col(df, ["股票代码"], contains=["股票代码", "证券代码", "代码"])
        name_col = pick_col(df, ["股票名称"], contains=["股票名称", "证券简称", "名称"])
        if not quarter_col or not ratio_col or not code_col or not name_col:
            return []

        def quarter_key(text):
            s = str(text)
            nums = [int(x) for x in re.findall(r"\d+", s)]
            if not nums:
                return (-1, -1)
            year = nums[0]
            q = -1
            if len(nums) >= 2:
                q = nums[1]
            else:
                m = re.search(r"q([1-4])", s.lower())
                if m:
                    q = int(m.group(1))
            return (year, q)

        quarters = df[quarter_col].dropna().astype(str)
        if quarters.empty:
            return []
        latest_quarter = max(quarters.unique().tolist(), key=quarter_key)
        df_latest = df[df[quarter_col].astype(str) == str(latest_quarter)].copy()
        if df_latest.empty:
            return []

        ratio_series = df_latest[ratio_col].astype(str).str.replace("%", "", regex=False)
        df_latest[ratio_col] = pd.to_numeric(ratio_series, errors="coerce").fillna(0.0)
        df_latest = df_latest.sort_values(by=ratio_col, ascending=False).head(10)

        portfolio = []
        for _, row in df_latest.iterrows():
            code_value = str(row[code_col])
            name_value = row[name_col]
            digits = re.sub(r"\D", "", code_value)
            code_norm = digits[-6:] if len(digits) >= 6 else digits
            if is_hk_stock(code_value, name_value):
                if digits:
                    code_norm = digits[-5:].zfill(5)
            portfolio.append({
                "code": code_norm,
                "name": name_value,
                "ratio": float(row[ratio_col])
            })
        return portfolio
    except Exception as e:
        traceback.print_exc()
        print(f"获取基金持仓失败: {e}")
        return []

@st.cache_data(ttl=60)
def get_all_market_indices():
    target = [
        ("上证指数", "sh000001"),
        ("深证成指", "sz399001"),
        ("创业板指", "sz399006"),
        ("科创50", "sh000688"),
    ]
    results = [{"name": n, "symbol": s, "price": 0.0, "change_pct": 0.0} for n, s in target]
    headers = {"User-Agent": "Mozilla/5.0"}
    url = "http://qt.gtimg.cn/q=" + ",".join([s for _, s in target])
    try:
        resp = requests.get(url, timeout=5, headers=headers)
        resp.encoding = "gbk"
        text = resp.text.strip()
        if not text:
            return results

        parsed = {}
        for line in text.split(";"):
            line = line.strip()
            if not line or "=" not in line or "v_" not in line:
                continue
            try:
                left, right = line.split("=", 1)
                code_with_prefix = left.split("v_")[-1]
                fields = right.strip().strip('"').split("~")
                if len(fields) < 5:
                    continue
                name = fields[1]
                price = pd.to_numeric(fields[3], errors="coerce")
                change_pct = pd.to_numeric(fields[32], errors="coerce") if len(fields) > 32 else None
                if pd.isna(price):
                    price = 0.0
                if pd.isna(change_pct):
                    change_pct = 0.0
                parsed[code_with_prefix] = {"name": name, "price": float(price), "change_pct": float(change_pct)}
            except Exception:
                continue

        for i, (_, symbol) in enumerate(target):
            item = parsed.get(symbol)
            if item:
                results[i]["name"] = item["name"] or results[i]["name"]
                results[i]["price"] = item["price"]
                results[i]["change_pct"] = item["change_pct"]
        return results
    except Exception as e:
        traceback.print_exc()
        print(f"获取大盘指数失败: {e}")
        return results

def pick_col(dataframe, candidates, contains=None):
    for c in candidates:
        if c in dataframe.columns:
            return c
    if contains:
        for c in dataframe.columns:
            if any(k in str(c) for k in contains):
                return c
    return None

def normalize_board_keyword(text):
    if text is None:
        return ""
    s = str(text).strip().lower()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", "", s)
    for w in ["基金", "混合", "指数", "行业", "概念", "板块", "主题", "赛道"]:
        s = s.replace(w, "")
    return s

def suggest_board_candidates(key, pool_pairs, top=3):
    if not key:
        return []
    key_set = set(key)
    scored = []
    for name, norm in pool_pairs:
        if not norm:
            continue
        common = len(key_set.intersection(set(norm)))
        if common <= 0:
            continue
        score = common / max(len(key_set), 1)
        scored.append((score, len(norm), name))
    scored.sort(key=lambda x: (-x[0], x[1], x[2]))
    return [x[2] for x in scored[:top]]

@st.cache_data(ttl=300)
def get_board_spot_map():
    result = {}
    try:
        industry = ak.stock_board_industry_spot_em()
    except Exception:
        industry = pd.DataFrame()
    try:
        concept = ak.stock_board_concept_spot_em()
    except Exception:
        concept = pd.DataFrame()

    def update(df):
        if df.empty:
            return
        name_col = pick_col(df, ["板块名称", "名称", "概念名称", "行业名称"], contains=["板块", "概念", "行业", "名称"])
        price_col = pick_col(df, ["最新价", "最新", "最新点数", "指数", "收盘"], contains=["最新", "点", "指数", "收盘"])
        change_col = pick_col(df, ["涨跌幅", "涨跌幅%", "涨跌幅(%)"], contains=["涨跌幅"])
        if not name_col:
            return
        for _, row in df.iterrows():
            name = str(row[name_col]).strip()
            if not name:
                continue
            price_val = pd.to_numeric(row[price_col], errors="coerce") if price_col else None
            change_val = pd.to_numeric(row[change_col], errors="coerce") if change_col else None
            result[name] = {
                "price": None if price_val is None or pd.isna(price_val) else float(price_val),
                "change": None if change_val is None or pd.isna(change_val) else float(change_val)
            }

    update(industry)
    update(concept)
    return result

@st.cache_data(ttl=3600, persist="disk")
def get_board_name_pool_fallback():
    names = []
    try:
        industry = ak.stock_board_industry_name_em()
    except Exception:
        industry = pd.DataFrame()
    try:
        concept = ak.stock_board_concept_name_em()
    except Exception:
        concept = pd.DataFrame()
    if not industry.empty:
        name_col = pick_col(industry, ["板块名称", "名称", "行业名称"], contains=["板块", "名称", "行业"])
        if name_col:
            names.extend(industry[name_col].dropna().astype(str).tolist())
    if not concept.empty:
        name_col = pick_col(concept, ["板块名称", "名称", "概念名称"], contains=["板块", "名称", "概念"])
        if name_col:
            names.extend(concept[name_col].dropna().astype(str).tolist())
    names = [n.strip() for n in names if str(n).strip()]
    return list(dict.fromkeys(names))

# ==========================================
# 核心数据获取逻辑 (并发加速 + 重仓股估值)
# ==========================================
@st.cache_data(ttl=60)
def calculate_fund_valuation(fund_code, fund_name, a_prices, a_changes, portfolio=None):
    """
    计算基金实时估值
    逻辑：实时估值涨跌幅 = Σ(重仓股涨跌幅 * 持仓占比) / Σ(已知持仓占比)
    """
    try:
        # 1. 获取基础净值 (昨天的)
        df_nav = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
        if df_nav.empty:
            return None
            
        last_nav = float(df_nav.iloc[-1]['单位净值'])
        last_date = str(df_nav.iloc[-1]['净值日期'])
        last_nav_date = pd.to_datetime(df_nav.iloc[-1]['净值日期']).date()
        now = datetime.now(pytz.timezone('Asia/Shanghai'))
        if last_nav_date == now.date():
            return {
                "code": fund_code,
                "name": fund_name,
                "current_price": last_nav,
                "change_pct": 0.0,
                "nav_date": last_date,
                "update_time": "✅ 官方更新",
                "is_estimated": False,
                "portfolio": [],
                "last_nav": last_nav
            }
        
        if not a_changes:
            return {
                "code": fund_code,
                "name": fund_name,
                "current_price": last_nav,
                "change_pct": 0.0,
                "nav_date": last_date,
                "update_time": "暂无实时 (昨日净值)",
                "is_estimated": False,
                "portfolio": [],
                "last_nav": last_nav
            }

        # 2. 获取持仓
        if portfolio is None:
            portfolio = get_fund_portfolio(fund_code)
        
        if not portfolio:
            # 如果没有持仓数据，只能返回昨日数据
            return {
                "code": fund_code,
                "name": fund_name,
                "current_price": last_nav,
                "change_pct": 0.0, # 无法估算
                "nav_date": last_date,
                "update_time": last_date + " (无持仓数据)",
                "is_estimated": False,
                "portfolio": []
            }
            
        # 3. 计算实时涨跌幅
        weighted_change_sum = 0.0
        total_ratio = 0.0
        
        portfolio_details = []
        
        for stock in portfolio:
            s_code = normalize_stock_code(stock['code'])
            ratio = stock['ratio']
            
            change = 0.0
            
            if s_code in a_changes:
                change = a_changes[s_code]
            
            # 累加
            weighted_change_sum += change * ratio
            total_ratio += ratio
            
            portfolio_details.append({
                "name": stock['name'],
                "change": change,
                "ratio": ratio
            })
            
        # 归一化估算 (假设未持仓部分涨跌幅为 0 或跟随大盘，这里简单处理为只看重仓股)
        # 如果 total_ratio 太小（比如 < 30%），估算可能极不准
        estimated_change_pct = weighted_change_sum / 100.0
        
        estimated_price = last_nav * (1 + estimated_change_pct / 100)
        
        return {
            "code": fund_code,
            "name": fund_name,
            "current_price": estimated_price,
            "change_pct": estimated_change_pct,
            "nav_date": last_date,
            "update_time": now.strftime("%H:%M:%S") + " (估)",
            "is_estimated": True,
            "portfolio": portfolio_details,
            "last_nav": last_nav
        }
        
    except Exception as e:
        print(f"Valuation error {fund_code}: {e}")
        return None

def fetch_all_funds_data(funds_list):
    """
    稳定优先获取：
    1. 预加载所有股票实时行情 (1次请求)
    2. 顺序计算每只基金估值 (避免多线程导致的崩溃)
    """
    results = {}
    portfolio_map = {}
    wanted_codes = set()
    total_steps = max(len(funds_list) * 2, 1)
    completed = 0
    bar = st.progress(0, text="正在帮妈妈去交易所抄价格...")

    def fetch_portfolio_item(fund):
        code = fund.get("code")
        try:
            time.sleep(random.uniform(0.1, 0.3))
            portfolio = get_fund_portfolio(code)
            return code, portfolio
        except Exception as e:
            traceback.print_exc()
            print(f"获取持仓失败 {code}: {e}")
            return code, []

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(fetch_portfolio_item, fund) for fund in funds_list]
            for future in concurrent.futures.as_completed(futures):
                code, portfolio = future.result()
                portfolio_map[code] = portfolio or []
                for stock in portfolio_map[code]:
                    s_code = normalize_stock_code(stock['code'])
                    if s_code:
                        wanted_codes.add(s_code)
                completed += 1
                bar.progress(min(completed / total_steps, 1.0), text="正在帮妈妈去交易所抄价格...")

        a_prices, a_changes = {}, {}
        if wanted_codes:
            try:
                a_prices, a_changes = get_stock_realtime_price_batch(list(wanted_codes))
            except Exception as e:
                traceback.print_exc()
                print(f"获取全市场行情失败: {e}")
        
        for f in funds_list:
            code = f.get("code")
            try:
                data = calculate_fund_valuation(
                    f.get("code"),
                    f.get("name"),
                    a_prices,
                    a_changes,
                    portfolio_map.get(code, [])
                )
                if data:
                    results[code] = data
                else:
                    results[code] = None
            except Exception:
                traceback.print_exc()
                results[code] = None
            completed += 1
            bar.progress(min(completed / total_steps, 1.0), text="正在帮妈妈去交易所抄价格...")
        return results
    finally:
        bar.empty()

def validate_fund_code(code):
    """验证基金代码并返回名称"""
    try:
        # 简单验证：尝试获取一次数据，如果有数据则认为有效
        # 或者使用 ak.fund_name_em() 获取所有基金代码列表进行匹配（较慢）
        # 这里用一种快速探测法
        df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
        if not df.empty:
             # 遗憾的是 akshare 这个接口不直接返回名字，我们需要另一个接口查名字
             # 使用 fund_individual_basic_info_em
             # 注意：这个接口比较慢
             return "未知基金" # 暂时返回默认，依靠用户输入或后续优化
        return None
    except:
        return None

# 为了更准确的名字验证，我们在添加时可以使用 ak.fund_em_fund_name() 
# 但数据量太大。优化：假设用户输入的代码是正确的，或者仅在前端做简单校验。
# 修正：根据需求“调用 akshare 验证基金名称”。
def get_fund_name(code):
    try:
        # 尝试获取基金基本信息
        # 这是一个比较重的操作，仅在添加时调用
        # 替代：使用 fund_name_em 搜索
        # 这里为了演示，我们假设如果能取到净值就是存在的，名字暂时让用户输入或默认
        # 实际开发中，可以维护一个本地的 code-name 映射表
        return "新基金" 
    except:
        return None

# ==========================================
# 侧边栏逻辑
# ==========================================
def render_sidebar(current_funds):
    with st.sidebar:
        st.header("🛠 管理与操作")
        
        # 自动刷新开关
        st.toggle("自动刷新 (每30秒)", key="auto_refresh")
        if st.session_state.auto_refresh:
            time_diff = (datetime.now() - st.session_state.last_update).seconds
            st.caption(f"上次更新: {st.session_state.last_update.strftime('%H:%M:%S')}")
            if time_diff >= UPDATE_INTERVAL:
                st.session_state.last_update = datetime.now()
                st.rerun()
            else:
                # 倒计时进度条
                st.progress(time_diff / UPDATE_INTERVAL)
                time.sleep(1) # 简单的轮询等待
                st.rerun()

        def _do_add_fund(code, cost, shares, group_name):
            try:
                df_nav = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
            except Exception:
                df_nav = pd.DataFrame()
            if df_nav.empty:
                st.error("基金代码不存在")
                return

            fund_name = None
            all_funds_df = get_all_funds_list()
            if not all_funds_df.empty and "基金代码" in all_funds_df.columns and "基金简称" in all_funds_df.columns:
                match = all_funds_df[all_funds_df["基金代码"].astype(str) == code]
                if not match.empty:
                    fund_name = str(match.iloc[0].get("基金简称", "")).strip() or None
            if not fund_name:
                fund_name = f"基金{code}"

            new_entry = {
                "code": code,
                "name": fund_name,
                "cost": float(cost),
                "shares": float(shares),
                "group": group_name
            }

            updated = False
            for i, f in enumerate(current_funds):
                if f.get("code") == code:
                    current_funds[i] = new_entry
                    updated = True
                    break
            if not updated:
                current_funds.append(new_entry)

            save_funds(current_funds)
            st.session_state.pop("add_fund_candidates", None)
            st.session_state.pop("add_fund_pending_payload", None)
            st.success(f"已添加: {fund_name}")
            time.sleep(1)
            st.rerun()

        st.subheader("➕ 添加基金")
        with st.form("add_fund_all_in_one_sidebar"):
            query = st.text_input("基金代码/名称", key="add_fund_query")
            col1, col2 = st.columns(2)
            with col1:
                f_cost = st.number_input("成本 (元)", min_value=0.0, value=0.0, step=0.01, format="%.4f", key="add_fund_cost")
            with col2:
                f_shares = st.number_input("份额 (份)", min_value=0.0, value=0.0, step=100.0, key="add_fund_shares")

            all_tags = sorted(list(set(f.get("group", "默认") for f in current_funds)))
            if "默认" not in all_tags:
                all_tags.append("默认")
            tag_options = all_tags + ["➕新建..."]
            f_group = st.selectbox("分组标签", options=tag_options, index=tag_options.index("默认") if "默认" in tag_options else 0, key="add_fund_group")
            new_group_name = ""
            if f_group == "➕新建...":
                new_group_name = st.text_input("新建标签名", key="add_fund_new_group")

            submitted = st.form_submit_button("➕ 添加基金", use_container_width=True)
            if submitted:
                q = str(query or "").strip()
                group_name = new_group_name.strip() if f_group == "➕新建..." else f_group
                if f_group == "➕新建..." and not group_name:
                    st.error("请输入新建标签名")
                elif not q:
                    st.error("请输入基金代码或名称")
                elif re.fullmatch(r"\d{6}", q):
                    _do_add_fund(q, f_cost, f_shares, group_name)
                else:
                    all_funds_df = get_all_funds_list()
                    if all_funds_df.empty or "基金代码" not in all_funds_df.columns or "基金简称" not in all_funds_df.columns:
                        st.error("基金列表加载失败，请稍后重试")
                    else:
                        df = all_funds_df.copy()
                        df["基金代码"] = df["基金代码"].astype(str)
                        df["基金简称"] = df["基金简称"].astype(str)
                        mask = df["基金代码"].str.contains(q, case=False, na=False) | df["基金简称"].str.contains(q, case=False, na=False)
                        cand = df.loc[mask, ["基金代码", "基金简称"]].dropna()
                        if cand.empty:
                            st.error("未找到匹配的基金，请输入更完整的名称")
                        else:
                            exact = cand[cand["基金简称"].str.strip() == q]
                            if not exact.empty:
                                row = exact.iloc[0]
                                _do_add_fund(str(row["基金代码"]), f_cost, f_shares, group_name)
                            else:
                                top = cand.head(30).to_dict("records")
                                st.session_state["add_fund_candidates"] = top
                                st.session_state["add_fund_pending_payload"] = {
                                    "cost": float(f_cost),
                                    "shares": float(f_shares),
                                    "group": group_name
                                }
                                st.warning("匹配到多只基金，请在下方选择后确认添加")

        candidates = st.session_state.get("add_fund_candidates") or []
        payload = st.session_state.get("add_fund_pending_payload") or {}
        if candidates and payload:
            def _fmt(opt):
                return f"{opt.get('基金代码','')} | {opt.get('基金简称','')}"

            selected = st.selectbox("请选择匹配基金", candidates, format_func=_fmt, key="add_fund_candidate_selected")
            if st.button("确认添加", use_container_width=True, key="confirm_add_fund"):
                code = str(selected.get("基金代码", "")).strip()
                if not re.fullmatch(r"\d{6}", code):
                    st.error("基金代码无效")
                else:
                    _do_add_fund(code, payload.get("cost", 0.0), payload.get("shares", 0.0), payload.get("group", "默认"))

        with st.expander("🏷️ 标签管理"):
            tags = sorted(list(set(f.get("group", "默认") for f in current_funds)))
            if not tags:
                st.info("暂无标签")
            else:
                selected_tag = st.selectbox("选择标签", tags)
                new_tag_name = st.text_input("新标签名称", value=selected_tag)
                col_rename, col_delete = st.columns(2)
                if col_rename.button("重命名", use_container_width=True):
                    if not new_tag_name.strip():
                        st.error("请输入新标签名称")
                    else:
                        for f in current_funds:
                            if f.get("group", "默认") == selected_tag:
                                f["group"] = new_tag_name.strip()
                        save_funds(current_funds)
                        st.success("标签已更新")
                        time.sleep(1)
                        st.rerun()
                if col_delete.button("删除", use_container_width=True):
                    for f in current_funds:
                        if f.get("group", "默认") == selected_tag:
                            f["group"] = "默认"
                    save_funds(current_funds)
                    st.success("标签已删除")
                    time.sleep(1)
                    st.rerun()

        with st.expander("📋 无法命中？点此查询官方板块名"):
            pool = get_board_name_pool_fallback()
            kw = st.text_input("搜索板块名称", key="board_name_search")
            df = pd.DataFrame({"板块名称": pool})
            if kw.strip():
                df = df[df["板块名称"].astype(str).str.contains(kw.strip(), case=False, na=False)]
            st.dataframe(df, use_container_width=True, height=320)

        with st.expander("📂 批量导入"):
            st.caption("输入多个代码，用逗号分隔 (例如: 000001,000002)")
            batch_codes = st.text_area("基金代码列表")
            if st.button("一键导入"):
                codes = [c.strip() for c in batch_codes.replace("，", ",").split(",") if c.strip()]
                count = 0
                for c in codes:
                    if len(c) == 6:
                        # 查重
                        if not any(f['code'] == c for f in current_funds):
                            current_funds.append({
                                "code": c,
                                "name": f"导入{c}",
                                "cost": 0.0,
                                "shares": 0.0,
                                "group": "默认"
                            })
                            count += 1
                if count > 0:
                    save_funds(current_funds)
                    st.success(f"成功导入 {count} 只基金")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.warning("未识别到新的有效代码")

        st.divider()
        st.markdown("### 关于")
        st.info("数据来源: Akshare\n\n红色: 上涨 | 绿色: 下跌")

# ==========================================
# 主界面逻辑
# ==========================================
def main():
    inject_custom_css()

    C_RED = "#D22222"
    C_GREEN = "#008000"
    C_GRAY = "#666666"

    def render_change_html(label, value_text, color, arrow, value_size="24px", label_size="16px", arrow_size="20px", padding="6px 8px"):
        return (
            f"<div style=\"padding:{padding}; border-radius:8px; background:#fff;\">"
            f"<div style=\"font-size:{label_size}; font-weight:700; color:#666;\">{label}</div>"
            f"<div style=\"display:flex; align-items:baseline; gap:6px;\">"
            f"<div style=\"font-size:{arrow_size}; line-height:1; font-weight:900; color:{color} !important;\">{arrow}</div>"
            f"<div style=\"font-size:{value_size}; font-weight:900; color:{color} !important;\">{value_text}</div>"
            f"</div>"
            f"</div>"
        )

    current_funds = load_funds()
    if "selected_group" not in st.session_state:
        st.session_state.selected_group = None
    groups = ["全部"] + sorted(list(set(f.get("group", "默认") for f in current_funds)))

    indices = get_all_market_indices()
    st.markdown("## 📊 市场大盘")
    if indices:
        for start in range(0, len(indices), 2):
            cols = st.columns(2)
            for j in range(2):
                pos = start + j
                with cols[j]:
                    if pos >= len(indices):
                        st.empty()
                        continue
                    idx = indices[pos]
                    val = float(idx.get("price", 0.0) or 0.0)
                    chg = float(idx.get("change_pct", 0.0) or 0.0)
                    if chg > 0:
                        change_color = "#d62728"
                        change_emoji = "🔴"
                    elif chg < 0:
                        change_color = "#2ca02c"
                        change_emoji = "🟢"
                    else:
                        change_color = "#7f7f7f"
                        change_emoji = "⚪"
                    st.markdown(
                        f"""
                        <div style="background-color: #ffffff; color: #000000; padding: 15px; border-radius: 10px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 12px;">
                          <div style="font-weight:700; font-size:16px;">{idx.get('name','')}</div>
                          <div style="display:flex; justify-content:space-between; align-items:baseline; margin-top:8px;">
                            <div style="font-size:24px; font-weight:800;">{val:.2f}</div>
                            <div style="font-size:24px; font-weight:800; color:{change_color};">{change_emoji} {chg:+.2f}%</div>
                          </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

    st.markdown("## 🧭 我的赛道")
    tags = sorted(list(set(f.get("group", "默认") for f in current_funds)))
    if not tags:
        st.info("暂无标签")
    else:
        board_bar = st.progress(0, text="正在帮妈妈去交易所抄价格...")
        try:
            spot_map = get_board_spot_map()
            spot_names = list(spot_map.keys())
            name_pool = get_board_name_pool_fallback()
            all_names = list(dict.fromkeys(spot_names + name_pool))
            normalized_all_pool = [(n, normalize_board_keyword(n)) for n in all_names]
            total = max(len(tags), 1)
            for i, tag in enumerate(tags, start=1):
                raw_key = str(tag).strip()
                key = normalize_board_keyword(raw_key)
                matches = [n for n, norm in normalized_all_pool if key and norm and (key in norm or norm in key)]
                if matches:
                    match = sorted(
                        matches,
                        key=lambda x: (len(normalize_board_keyword(x)) or 10**9, len(x), x)
                    )[0]
                    info = spot_map.get(match, {}) if match in spot_map else {}
                    price = info.get("price") if match in spot_map else None
                    change = info.get("change") if match in spot_map else None
                    price_text = "-" if price is None else f"{price:.2f}"
                    change_text = "-" if change is None else f"{change:+.2f}%"
                    if change is None:
                        color = "#7f7f7f"
                    elif change > 0:
                        color = "#d62728"
                    elif change < 0:
                        color = "#2ca02c"
                    else:
                        color = "#7f7f7f"
                    extra = "" if match in spot_map else " <span style='color:#7f7f7f; font-size:12px;'>(暂无实时行情)</span>"
                    st.markdown(
                        f"**{tag}**：{match}{extra} | {price_text} | <span style='color:{color}; font-weight:700;'>{change_text}</span>",
                        unsafe_allow_html=True
                    )
                else:
                    candidates = suggest_board_candidates(key, normalized_all_pool, top=3)
                    if candidates:
                        suggest_text = " / ".join(candidates)
                        st.markdown(
                            f"<span style='color:#7f7f7f; font-size:12px;'>⚠️ 未找到与【{tag}】相关的板块，请尝试修改标签名（候选：{suggest_text}）</span>",
                            unsafe_allow_html=True
                        )
                    else:
                        st.markdown(
                            f"<span style='color:#7f7f7f; font-size:12px;'>⚠️ 未找到与【{tag}】相关的板块，请尝试修改标签名</span>",
                            unsafe_allow_html=True
                        )
                board_bar.progress(min(i / total, 1.0), text="正在帮妈妈去交易所抄价格...")
        finally:
            board_bar.empty()

    st.markdown("### 持仓详情")
    col_filter, col_refresh = st.columns([3, 1])
    with col_filter:
        default_group = st.session_state.selected_group if st.session_state.selected_group in groups else None
        try:
            st.pills("选择分组", groups, key="selected_group", default=default_group)
        except AttributeError:
            st.radio("选择分组", groups, horizontal=True, key="selected_group", index=0)
    with col_refresh:
        if st.button("🔄 手动刷新", use_container_width=True, type="primary"):
            st.rerun()

    selected_group = st.session_state.selected_group
    if selected_group is None:
        st.info("👈 请点击上方分组标签查看详情")
        render_sidebar(current_funds)
        return

    if not current_funds:
        st.info("👋 暂无基金，请在左侧添加。")
        render_sidebar(current_funds)
        return

    display_funds = current_funds if selected_group == "全部" else [f for f in current_funds if f.get('group') == selected_group]
    if not display_funds:
        st.info("当前分组暂无基金。")
        render_sidebar(current_funds)
        return

    with st.spinner('正在并发加载数据，请稍候...'):
        market_data = fetch_all_funds_data(display_funds)

    total_market_value = 0.0
    total_day_profit = 0.0

    cards = []

    for fund in display_funds:
        code = fund['code']
        name = fund['name']
        m_data = market_data.get(code)

        shares = float(fund.get('shares', 0))
        cost = float(fund.get('cost', 0))
        group = fund.get('group', "默认")

        current_price = None
        change_pct = None
        update_time = "-"
        nav_date = "-"
        if m_data:
            current_price = m_data.get('current_price')
            try:
                change_pct = float(m_data.get('change_pct', 0.0))
                if pd.isna(change_pct):
                    change_pct = 0.0
            except Exception:
                change_pct = 0.0
            update_time = m_data.get('update_time', "-")
            nav_date = m_data.get("nav_date", "-")

        market_value = current_price * shares if current_price is not None else 0.0
        if current_price is not None and change_pct is not None:
            prev_price = current_price / (1 + change_pct / 100) if (1 + change_pct / 100) != 0 else current_price
            day_profit = (current_price - prev_price) * shares
        else:
            day_profit = 0.0
        total_market_value += market_value
        total_day_profit += day_profit

        holding_profit = None
        if current_price is not None:
            holding_profit = (current_price - cost) * shares

        change_color = C_GRAY
        change_arrow = ""
        if change_pct is not None:
            if change_pct > 0:
                change_color = C_RED
                change_arrow = "▲"
            elif change_pct < 0:
                change_color = C_GREEN
                change_arrow = "▼"
            else:
                change_color = C_GRAY

        profit_color = C_GRAY
        profit_arrow = ""
        if holding_profit is not None:
            if holding_profit > 0:
                profit_color = C_RED
                profit_arrow = "▲"
            elif holding_profit < 0:
                profit_color = C_GREEN
                profit_arrow = "▼"
            else:
                profit_color = C_GRAY

        cards.append({
            "code": code,
            "name": name,
            "group": group,
            "cost": cost,
            "shares": shares,
            "current_price": current_price,
            "change_pct": change_pct,
            "nav_date": nav_date,
            "update_time": update_time,
            "holding_profit": holding_profit,
            "change_color": change_color,
            "profit_color": profit_color,
            "m_data": m_data
        })

    st.markdown("### 💰 资产概览")
    c1, c2, c3 = st.columns(3)
    c1.metric("总持仓市值", f"¥ {total_market_value:,.0f}")
    c2.metric("今日预估收益", f"¥ {total_day_profit:,.0f}", delta=f"{total_day_profit:,.0f}", delta_color="inverse")
    c3.metric("持仓基金数", f"{len(display_funds)} 支")

    for card in cards:
        code = card["code"]
        name = card["name"]
        group = card["group"]
        current_price = card["current_price"]
        change_pct = card["change_pct"]
        update_time = card["update_time"]
        holding_profit = card["holding_profit"]
        change_color = card["change_color"]
        profit_color = card["profit_color"]
        m_data = card["m_data"]
        cost = card["cost"]
        shares = card["shares"]
        nav_date = card["nav_date"]
        price_text = "-" if current_price is None else f"{current_price:.4f}"
        if change_pct is None:
            change_text = "-"
            change_color = "#7f7f7f"
            change_emoji = "⚪"
        else:
            change_text = f"{change_pct:+.2f}%"
            if change_pct > 0:
                change_color = "#d62728"
                change_emoji = "🔴"
            elif change_pct < 0:
                change_color = "#2ca02c"
                change_emoji = "🟢"
            else:
                change_color = "#7f7f7f"
                change_emoji = "⚪"

        st.markdown(
            f"""
            <div style="background-color: #ffffff; color: #000000; padding: 15px; border-radius: 10px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 12px;">
              <div style="display:flex; justify-content:space-between; align-items:center;">
                <div style="font-weight:700; font-size:16px;">{name} ({code})</div>
                <div style="color:#7f7f7f; font-size:12px;">{nav_date}</div>
              </div>
              <div style="display:flex; justify-content:space-between; align-items:baseline; margin-top:8px;">
                <div style="font-size:24px; font-weight:800;">{price_text}</div>
                <div style="font-size:24px; font-weight:800; color:{change_color};">{change_emoji} {change_text}</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True
        )

        st.markdown(f"更新时间：{update_time}")
        with st.expander("查看详情/操作"):
                edit_cost = st.number_input("持仓成本 (元)", min_value=0.0, value=float(cost), step=0.01, format="%.4f", key=f"edit_cost_{code}")
                edit_shares = st.number_input("持有份额 (份)", min_value=0.0, value=float(shares), step=100.0, key=f"edit_shares_{code}")
                if st.button("💾 更新持仓", key=f"save_holding_{code}"):
                    for i, f in enumerate(current_funds):
                        if f['code'] == code:
                            current_funds[i]['cost'] = edit_cost
                            current_funds[i]['shares'] = edit_shares
                            break
                    save_funds(current_funds)
                    st.rerun()

                existing_groups = sorted(list(set(f.get("group", "默认") for f in current_funds)))
                if group not in existing_groups:
                    existing_groups.append(group)
                if "默认" not in existing_groups:
                    existing_groups.append("默认")
                group_options = existing_groups + ["➕ 新建标签..."]
                group_key = f"group_{code}"
                new_group = st.selectbox("分组标签", group_options, index=group_options.index(group) if group in group_options else 0, key=group_key)
                new_group_name = ""
                if new_group == "➕ 新建标签...":
                    new_group_name = st.text_input("新标签名称", key=f"group_new_{code}")
                if st.button("保存标签", key=f"save_group_{code}"):
                    if new_group == "➕ 新建标签..." and not new_group_name.strip():
                        st.error("请输入新标签名称")
                    else:
                        for i, f in enumerate(current_funds):
                            if f['code'] == code:
                                current_funds[i]['group'] = new_group_name.strip() if new_group == "➕ 新建标签..." else new_group
                                break
                        save_funds(current_funds)
                        st.rerun()

                if st.button("🗑 删除", key=f"del_{code}", type="secondary"):
                    new_list = [f for f in current_funds if f['code'] != code]
                    save_funds(new_list)
                    st.rerun()

                st.markdown("###### 重仓股持仓")
                portfolio = []
                has_realtime_change = False
                if m_data and m_data.get('portfolio'):
                    portfolio = m_data['portfolio']
                    has_realtime_change = True

                if portfolio:
                    title = "###### 重仓股持仓 (最新季报，涨跌幅为实时)" if has_realtime_change else "###### 重仓股持仓 (最新季报)"
                    st.markdown(title)
                    p_cols = st.columns(5)
                    for i, stock in enumerate(portfolio):
                        with p_cols[i % 5]:
                            val_change = float(stock.get('change', 0))
                            if val_change > 0:
                                text_color = "#d62728"
                                change_emoji = "🔴"
                            elif val_change < 0:
                                text_color = "#2ca02c"
                                change_emoji = "🟢"
                            else:
                                text_color = "#7f7f7f"
                                change_emoji = "⚪"

                            st.markdown(
                                f"""
                                <div style="background-color: #ffffff; color: #000000; padding: 15px; border-radius: 10px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 12px; text-align:center;">
                                    <div style="font-size:14px; font-weight:bold;">{stock['name']}</div>
                                    <div style="font-size:12px; color:#666;">占比 {stock['ratio']}%</div>
                                    <div style="font-size:16px; font-weight:800; color:{text_color}; margin-top:6px;">{change_emoji} {val_change:+.2f}%</div>
                                </div>
                                """,
                                unsafe_allow_html=True
                            )
                else:
                    st.warning("暂无重仓股数据。")

    render_sidebar(current_funds)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # 打印详细堆栈到控制台，方便调试
        traceback.print_exc()
        st.error(f"程序发生严重错误: {str(e)}")
        # 生产环境可以记录日志
