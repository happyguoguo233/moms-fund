import streamlit as st
import pandas as pd
import akshare as ak
import json
import os
import time
import re
import requests
from datetime import datetime
import concurrent.futures
import plotly.graph_objects as go

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

@st.cache_data(ttl=3600)
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
    def normalize_code(value):
        return str(value).split(".")[-1][-6:]

    if not stock_codes:
        if LAST_A_STOCK_CACHE["price_map"] and LAST_A_STOCK_CACHE["change_map"]:
            return LAST_A_STOCK_CACHE["price_map"], LAST_A_STOCK_CACHE["change_map"]
        return {}, {}

    if isinstance(stock_codes, (list, tuple, set)):
        wanted = {normalize_code(c) for c in stock_codes}
    else:
        wanted = {normalize_code(stock_codes)}

    def to_tencent_code(code):
        if code.startswith("6"):
            return f"sh{code}"
        if code.startswith("0") or code.startswith("3"):
            return f"sz{code}"
        if code.startswith("8") or code.startswith("4") or code.startswith("9"):
            return f"bj{code}"
        return None

    tencent_codes = [to_tencent_code(c) for c in wanted]
    tencent_codes = [c for c in tencent_codes if c]
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
                code = code_with_prefix[-6:]
                price_map[code] = float(latest)
                change_map[code] = float(change_pct)
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

@st.cache_data(ttl=86400) # 每天更新一次持仓即可
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
            portfolio.append({
                "code": str(row[code_col]),
                "name": row[name_col],
                "ratio": float(row[ratio_col])
            })
        return portfolio
    except Exception as e:
        traceback.print_exc()
        print(f"获取基金持仓失败: {e}")
        return []

@st.cache_data(ttl=3600)
def get_fund_history(fund_code):
    """获取基金历史净值走势"""
    try:
        # 获取单位净值走势
        # 修正参数名为 symbol
        df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
        if not df.empty:
            df['净值日期'] = pd.to_datetime(df['净值日期'])
            df['单位净值'] = pd.to_numeric(df['单位净值'], errors='coerce')
            return df
        return pd.DataFrame()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_market_index_history(symbol):
    """获取大盘指数历史走势"""
    try:
        # 特殊处理恒生科技
        if symbol == "HK_HSTECH":
            # 暂时无法获取港股指数历史，返回空
            return pd.DataFrame()
            
        # A股指数
        df = ak.stock_zh_index_daily_em(symbol=symbol)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            return df
        return pd.DataFrame()
    except:
        return pd.DataFrame()

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

# ==========================================
# 核心数据获取逻辑 (并发加速 + 重仓股估值)
# ==========================================
@st.cache_data(ttl=60)
def calculate_fund_valuation(fund_code, fund_name, a_prices, a_changes):
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
        
        if not a_changes:
            return {
                "code": fund_code,
                "name": fund_name,
                "current_price": last_nav,
                "change_pct": 0.0,
                "update_time": "暂无实时 (昨日净值)",
                "is_estimated": False,
                "portfolio": [],
                "last_nav": last_nav
            }

        # 2. 获取持仓
        portfolio = get_fund_portfolio(fund_code)
        
        if not portfolio:
            # 如果没有持仓数据，只能返回昨日数据
            return {
                "code": fund_code,
                "name": fund_name,
                "current_price": last_nav,
                "change_pct": 0.0, # 无法估算
                "update_time": last_date + " (无持仓数据)",
                "is_estimated": False,
                "portfolio": []
            }
            
        # 3. 计算实时涨跌幅
        weighted_change_sum = 0.0
        total_ratio = 0.0
        
        portfolio_details = []
        
        for stock in portfolio:
            s_code = str(stock['code']).split(".")[-1][-6:]
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
        estimated_change_pct = weighted_change_sum / total_ratio if total_ratio > 0 else 0.0
        
        # 修正：如果 total_ratio 只有 50%，剩下的 50% 假设不动？
        # 通常做法：estimated_change_pct = weighted_change_sum / 100 (假设其他部分不动)
        # 或者 estimated_change_pct = weighted_change_sum / total_ratio (假设其他部分和重仓股同频)
        # 这里采用折中：weighted_change_sum / 100 比较保守，但更真实（因为债券部分通常波动小）
        # 也就是： 基金涨跌 = Σ(股票涨跌 * 占比%) 
        estimated_change_pct = weighted_change_sum / 100.0
        
        estimated_price = last_nav * (1 + estimated_change_pct / 100)
        
        return {
            "code": fund_code,
            "name": fund_name,
            "current_price": estimated_price,
            "change_pct": estimated_change_pct,
            "update_time": datetime.now().strftime("%H:%M:%S") + " (估)",
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

    wanted_codes = set()
    for fund in funds_list:
        portfolio = get_fund_portfolio(fund['code'])
        time.sleep(0.2)
        for stock in portfolio:
            s_code = str(stock['code']).split(".")[-1][-6:]
            if s_code:
                wanted_codes.add(s_code)

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
                a_changes
            )
            if data:
                results[code] = data
            else:
                results[code] = None
        except Exception:
            traceback.print_exc()
            results[code] = None
    return results

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

        with st.expander("➕ 添加单个基金", expanded=True):
            st.markdown("##### 🔍 基金搜索")
            all_funds_df = get_all_funds_list()
            selected_fund = None
            if not all_funds_df.empty and "基金代码" in all_funds_df.columns and "基金简称" in all_funds_df.columns:
                options = all_funds_df[["基金代码", "基金简称"]].dropna().to_dict("records")
                def format_option(option):
                    name = str(option.get("基金简称", ""))
                    if len(name) > 12:
                        name = name[:12] + "…"
                    return f"{option.get('基金代码', '')} | {name}"
                try:
                    selected_option = st.selectbox(
                        "输入名称或代码搜索",
                        options,
                        index=None,
                        placeholder="如: 华夏成长 / 000001",
                        format_func=format_option
                    )
                except TypeError:
                    selected_option = st.selectbox(
                        "输入名称或代码搜索",
                        options,
                        index=None,
                        format_func=format_option
                    )
                if selected_option:
                    selected_fund = {"name": selected_option.get("基金简称", ""), "code": selected_option.get("基金代码", "")}
            else:
                st.warning("基金列表加载失败，请稍后重试")

            st.markdown("---")
            with st.form("add_fund_form"):
                # 如果从搜索选择了，自动填充
                default_code = selected_fund['code'] if selected_fund else ""
                default_name = selected_fund['name'] if selected_fund else ""
                
                f_code = st.text_input("基金代码 (6位)", value=default_code, max_chars=6)
                f_name = st.text_input("基金名称 (方便记忆)", value=default_name)
                f_cost = st.number_input("持仓成本 (元)", min_value=0.0, value=0.0, step=0.01, format="%.4f")
                f_shares = st.number_input("持有份额 (份)", min_value=0.0, value=0.0, step=100.0)
                f_group = st.text_input("分组标签", value="默认")
                
                submitted = st.form_submit_button("添加 / 更新")
                if submitted:
                    if len(f_code) != 6:
                        st.error("请输入正确的6位基金代码")
                    else:
                        # 检查是否已存在，存在则更新，不存在则追加
                        new_entry = {
                            "code": f_code,
                            "name": f_name if f_name else f"基金{f_code}",
                            "cost": f_cost,
                            "shares": f_shares,
                            "group": f_group
                        }
                        
                        # 更新逻辑
                        updated = False
                        for i, f in enumerate(current_funds):
                            if f['code'] == f_code:
                                current_funds[i] = new_entry
                                updated = True
                                break
                        if not updated:
                            current_funds.append(new_entry)
                        
                        save_funds(current_funds)
                        st.success(f"已保存: {new_entry['name']}")
                        time.sleep(1)
                        st.rerun()

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
        st.session_state.selected_group = "全部"
    groups = ["全部"] + sorted(list(set(f.get("group", "默认") for f in current_funds)))
    selected_group = st.session_state.selected_group
    display_funds = current_funds if selected_group == "全部" else [f for f in current_funds if f.get('group') == selected_group]

    indices = get_all_market_indices()
    st.markdown("## 📊 市场大盘")
    if indices:
        cols = st.columns(len(indices))
        for i, idx in enumerate(indices):
            with cols[i]:
                val = float(idx.get("price", 0.0) or 0.0)
                chg = float(idx.get("change_pct", 0.0) or 0.0)
                if chg > 0:
                    change_color = C_RED
                    change_arrow = "▲"
                elif chg < 0:
                    change_color = C_GREEN
                    change_arrow = "▼"
                else:
                    change_color = C_GRAY
                    change_arrow = ""
                st.markdown(
                    f"""
                    <div style="padding:8px 10px; border:1px solid #eee; border-radius:10px; background:#fff;">
                      <div style="font-size:18px; font-weight:800; color:#111 !important;">{idx.get('name','')}</div>
                      <div style="font-size:30px; font-weight:900; color:#111 !important; line-height:1.1;">{val:.2f}</div>
                      {render_change_html("涨跌幅", f"{chg:+.2f}%", change_color, change_arrow, value_size="18px", label_size="14px", arrow_size="18px", padding="4px 6px")}
                    </div>
                    """,
                    unsafe_allow_html=True
                )

    if not display_funds:
        st.info("👋 暂无基金，请在左侧添加。")
        render_sidebar(current_funds)
        return

    with st.spinner('正在获取最新行情...'):
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
        if m_data:
            current_price = m_data.get('current_price')
            try:
                change_pct = float(m_data.get('change_pct', 0.0))
                if pd.isna(change_pct):
                    change_pct = 0.0
            except Exception:
                change_pct = 0.0
            update_time = m_data.get('update_time', "-")

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
            "current_price": current_price,
            "change_pct": change_pct,
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

    st.markdown("### 持仓详情")
    col_filter, col_refresh = st.columns([3, 1])
    with col_filter:
        try:
            st.pills("选择分组", groups, key="selected_group", default=st.session_state.selected_group)
        except AttributeError:
            st.radio("选择分组", groups, horizontal=True, key="selected_group", index=0)
    with col_refresh:
        if st.button("🔄 手动刷新", use_container_width=True, type="primary"):
            st.rerun()

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

        with st.container(border=True):
            st.markdown(
                f"<div style='font-size:22px; font-weight:800;'>{name} <span style='color:#888; font-size:14px;'>{code}</span></div>",
                unsafe_allow_html=True
            )
            c1, c2, c3 = st.columns(3)
            with c1:
                value = "-" if current_price is None else f"{current_price:.4f}"
                st.markdown(f"<div>预估净值</div><div style='font-size:20px; font-weight:800;'>{value}</div>", unsafe_allow_html=True)
            with c2:
                if change_pct is None:
                    st.markdown(render_change_html("估算涨跌", "-", C_GRAY, "", value_size="24px", label_size="16px", arrow_size="20px"), unsafe_allow_html=True)
                else:
                    st.markdown(render_change_html("估算涨跌", f"{change_pct:+.2f}%", change_color, change_arrow, value_size="24px", label_size="16px", arrow_size="20px"), unsafe_allow_html=True)
            with c3:
                if holding_profit is None:
                    st.markdown(render_change_html("持有收益", "-", C_GRAY, "", value_size="24px", label_size="16px", arrow_size="20px"), unsafe_allow_html=True)
                else:
                    st.markdown(render_change_html("持有收益", f"{holding_profit:+.2f}", profit_color, profit_arrow, value_size="24px", label_size="16px", arrow_size="20px"), unsafe_allow_html=True)

            st.markdown(f"更新时间：{update_time}")
            with st.expander("查看详情/操作"):
                group_key = f"group_{code}"
                new_group = st.text_input("分组标签", value=group, key=group_key)
                if st.button("保存标签", key=f"save_group_{code}"):
                    for i, f in enumerate(current_funds):
                        if f['code'] == code:
                            current_funds[i]['group'] = new_group
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
                                text_color = C_RED
                                change_arrow = "▲"
                            elif val_change < 0:
                                text_color = C_GREEN
                                change_arrow = "▼"
                            else:
                                text_color = C_GRAY
                                change_arrow = ""

                            st.markdown(
                                f"""
                                <div style="border:1px solid #ddd; padding:5px; border-radius:5px; text-align:center; margin-bottom:5px; background-color: #fff;">
                                    <div style="font-size:14px; font-weight:bold;">{stock['name']}</div>
                                    <div style="font-size:12px; color:#666;">占比 {stock['ratio']}%</div>
                                    {render_change_html("涨跌幅", f"{val_change:+.2f}%", text_color, change_arrow, value_size="16px", label_size="12px", arrow_size="14px", padding="2px 4px")}
                                </div>
                                """,
                                unsafe_allow_html=True
                            )
                else:
                    st.warning("暂无重仓股数据。")

                st.markdown("###### 📈 业绩走势")
                hist_df = get_fund_history(code)
                if not hist_df.empty:
                    hist_df = hist_df.sort_values('净值日期')
                    range_options = ["当日", "近1周", "近1月", "近3月", "近半年", "近1年"]
                    range_key = f"range_{code}"
                    try:
                        selected_range = st.segmented_control("选择区间", range_options, default="当日", key=range_key)
                    except AttributeError:
                        selected_range = st.radio("选择区间", range_options, horizontal=True, index=0, key=range_key)

                    if selected_range == "当日":
                        view_df = hist_df.tail(2)
                    elif selected_range == "近1周":
                        view_df = hist_df.tail(5)
                    elif selected_range == "近1月":
                        view_df = hist_df.tail(20)
                    elif selected_range == "近3月":
                        view_df = hist_df.tail(60)
                    elif selected_range == "近半年":
                        view_df = hist_df.tail(120)
                    else:
                        view_df = hist_df.tail(240)

                    start_date = view_df['净值日期'].min()
                    end_date = view_df['净值日期'].max()

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=view_df['净值日期'],
                        y=view_df['单位净值'],
                        mode='lines',
                        name='单位净值',
                        line=dict(color='#D22222', width=3),
                        fill='tozeroy',
                        fillcolor='rgba(210, 34, 34, 0.1)'
                    ))
                    fig.update_layout(
                        margin=dict(l=0, r=0, t=10, b=0),
                        height=350,
                        xaxis=dict(
                            type="date",
                            tickformat="%m-%d"
                        ),
                        yaxis=dict(
                            autorange=True,
                            fixedrange=False
                        ),
                        hovermode="x unified"
                    )
                    st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True, "displayModeBar": False})
                else:
                    st.warning("暂无历史数据")

    render_sidebar(current_funds)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # 打印详细堆栈到控制台，方便调试
        traceback.print_exc()
        st.error(f"程序发生严重错误: {str(e)}")
        # 生产环境可以记录日志
