#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股异动股票筛选器 v5.0 — 抓启动加速，剔除暴涨尾声
====================================================
策略核心理念：
  "保留涨幅逻辑，但反向设上限，抓启动加速，剔除暴涨尾声"

异动检测（区间上下限，双边过滤）：
  - 3日涨幅：12%～20%（接近但不碰 20% 红线），＞20% 直接剔除
  - 10日涨幅：40%～75%，＜40% 偏弱、＞80% 剔除
  - 30日涨幅：60%～150%，＞180% 直接拉黑（远离原来 200% 高危线）

三大硬性过滤：
  1. 必须当期市场主线（板块 ≥ 3 只涨停），冷门独立题材剔除
  2. 市值 60～350 亿，避开＜30 亿庄股、千亿大盘滞涨股
  3. 近 3 日量能稳步放大，单日换手 5%～12%，高位爆量＞18% 剔除

配套交易纪律：
  - 彻底放弃高位涨停买入：中段池回踩 5/10 日线分批买，高位池只大跌低吸
  - 仓位拆分：中段池单只≤3 成，高位池单只≤1 成，总仓位永远不满仓
  - 止盈止损固化：中段池盈利 8% 减半/破 5 日线全走，高位池亏损 5% 无条件止损/盈利 10% 全离

运行方式：
  python yidong_scanner.py                    # 扫描全市场强势股池
  python yidong_scanner.py --codes 002475 300750  # 分析指定股票
  python yidong_scanner.py --top 30          # 只保存前30名
"""

import sys
import os
import time
import json
import logging
import warnings
import requests
import urllib.request
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import pandas as pd
import numpy as np
from pathlib import Path

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.ERROR)  # 屏蔽mootdx info日志

# ============================================================
# 配置区
# ============================================================

class Config:
    # ── 异动区间（上下限，双边过滤）──
    SURGE_3D_MIN   = 12    # 3日涨幅下限（%），低于此太弱
    SURGE_3D_MAX   = 20    # 3日涨幅上限（%），超此直接剔除（暴涨尾声）
    SURGE_10D_MIN  = 40    # 10日涨幅下限（%）
    SURGE_10D_MAX  = 80    # 10日涨幅上限（%），超此剔除
    SURGE_30D_MIN  = 60    # 30日涨幅下限（%）
    SURGE_30D_MAX  = 180   # 30日涨幅上限（%），超此直接拉黑

    # ── 硬性过滤 ──
    MCAP_MIN       = 60    # 市值下限（亿），<30亿庄股风险
    MCAP_MAX       = 350   # 市值上限（亿），千亿大盘滞涨
    MCAP_WARN_LOW  = 30    # 市值黄线（亿）
    MCAP_WARN_HIGH = 1000  # 市值上线（亿）
    TURNOVER_MIN   = 5.0   # 换手率下限（%），太低无活跃度
    TURNOVER_MAX   = 12.0  # 换手率上限（%）
    TURNOVER_BLAST = 18.0  # 高位爆量阈值（%），超此剔除
    SECTOR_MIN_LIMIT = 3   # 板块最少涨停数（主线确认）

    # ── 量能验证 ──
    VOL_DAYS       = 3     # 量能稳步放大检查天数

    # ── 趋势持续性（保留）──
    MAX_PULLBACK_FROM_HIGH_PCT = 15
    TREND_WINDOW = 10

    # ── 分级池阈值 ──
    SURGE_MID_MAX  = 5     # 中段池异动≤5次（＞5次=高位博弈区）
    SURGE_MID_MIN  = 2     # 中段池最少异动2次

    # ── 交易纪律 ──
    MID_POOL_POS    = 0.30  # 中段池单只仓位
    HIGH_POOL_POS   = 0.10  # 高位池单只仓位
    MID_TP_PCT      = 8     # 中段池止盈（盈利8%减半）
    MID_STOP_MA     = 5     # 中段池止损（破5日线全走）
    HIGH_STOP_PCT   = -5    # 高位池止损（亏损5%无条件走）
    HIGH_TP_PCT     = 10    # 高位池止盈（盈利10%全离场）

    # ── 市场过滤 ──
    EXCLUDE_ST     = True
    EXCLUDE_NEW    = True
    MIN_PRICE      = 1.5
    MAX_PRICE      = 1000.0

    # ── K线 ──
    KLINE_DAYS = 365

    # ── PreSurge 预判（保留）──
    PRESURGE_MIN_SCORE = 3
    PRESURGE_MIN_SURGES = 2

    # ── 扫描模式 ──
    SCAN_MODE = 'hot_only'

    # ── 输出 ──
    OUTPUT_FILE = "/Users/ghost/Desktop/code/AIAIAIAI-stock/yidong_result.json"
    HTML_FILE   = "/Users/ghost/Desktop/code/AIAIAIAI-stock/yidong_result.html"
    TRACK_FILE  = "/Users/ghost/Desktop/code/AIAIAIAI-stock/.yidong_tracking.json"
    HISTORY_DIR = "/Users/ghost/Desktop/code/AIAIAIAI-stock/history"
    HISTORY_HTML = "/Users/ghost/Desktop/code/AIAIAIAI-stock/yidong_history.html"

# ============================================================
# 数据获取层
# ============================================================

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# 全局mootdx client（延迟初始化）
_mootdx_client = None

def get_mootdx_client():
    global _mootdx_client
    if _mootdx_client is None:
        from mootdx.quotes import Quotes
        _mootdx_client = Quotes.factory(market='std')
        time.sleep(1.5)  # 等待服务器选择完成
    return _mootdx_client


def get_hot_stocks_today() -> List[Dict]:
    """
    同花顺当日强势股（含题材归因）。
    这是最高效的入口：只扫有强势表现的股票，不浪费在平淡股上。
    """
    from datetime import date as _date
    today = _date.today()

    for delta in range(5):  # 今天找不到则往前找5个交易日
        d = today - timedelta(days=delta)
        # 跳过周末
        if d.weekday() >= 5:
            continue
        date_str = d.strftime("%Y-%m-%d")
        url = (
            f"http://zx.10jqka.com.cn/event/api/getharden/"
            f"date/{date_str}/orderby/date/orderway/desc/charset/GBK/"
        )
        headers = {"User-Agent": UA}
        try:
            r = requests.get(url, headers=headers, timeout=10)
            d_json = r.json()
            if d_json.get("errocode", 0) != 0:
                continue
            rows = d_json.get("data") or []
            if rows:
                print(f"  [INFO] 同花顺强势股 {date_str}: {len(rows)} 只")
                return rows
        except Exception as e:
            continue
    return []


def get_all_stock_codes() -> List[str]:
    """获取全市场A股代码列表（沪深）"""
    codes = []
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    for fs in [
        "m:1+t:2,m:1+t:23",  # 沪市主板+科创板
        "m:0+t:6,m:0+t:80",  # 深市主板+创业板
    ]:
        params = {
            "pn": "1", "pz": "5000", "po": "1", "np": "1",
            "fltt": "2", "invt": "2", "fs": fs,
            "fields": "f12,f14",
        }
        try:
            r = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=15)
            items = r.json().get("data", {}).get("diff", []) or []
            for item in items:
                code = item.get("f12", "")
                name = item.get("f14", "")
                if code and len(code) == 6:
                    if Config.EXCLUDE_ST and ("ST" in name or "st" in name):
                        continue
                    codes.append(code)
        except Exception:
            pass
    print(f"  [INFO] 全市场股票: {len(codes)} 只")
    return codes


def get_kline(code: str, days: int = 365) -> Optional[pd.DataFrame]:
    """
    获取K线，返回标准DataFrame:
    columns: date(datetime), open, close, high, low, vol
    """
    try:
        client = get_mootdx_client()
        klines = client.bars(symbol=code, category=4, offset=days)
        if klines is None or len(klines) < 40:
            return None

        df = klines.copy()
        df.columns = [c.lower() for c in df.columns]

        # 统一date列
        if "datetime" in df.columns:
            df["date"] = pd.to_datetime(df["datetime"])
        elif "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        # 统一vol列
        if "volume" in df.columns and "vol" not in df.columns:
            df["vol"] = df["volume"]

        df = df.sort_values("date").reset_index(drop=True)

        # 过滤次新股（K线天数 < 60 认为是次新）
        if Config.EXCLUDE_NEW and len(df) < 60:
            return None

        return df[["date", "open", "close", "high", "low", "vol"]]
    except Exception as e:
        return None


def tencent_batch_quote(codes: List[str]) -> Dict[str, Dict]:
    """批量获取腾讯实时行情"""
    prefixed = []
    for c in codes:
        if c.startswith(("6", "9")):
            prefixed.append(f"sh{c}")
        elif c.startswith("8"):
            prefixed.append(f"bj{c}")
        else:
            prefixed.append(f"sz{c}")

    result = {}
    for i in range(0, len(prefixed), 50):
        batch = prefixed[i:i+50]
        url = "https://qt.gtimg.cn/q=" + ",".join(batch)
        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", UA)
            resp = urllib.request.urlopen(req, timeout=10)
            data = resp.read().decode("gbk")
            for line in data.strip().split(";"):
                if not line.strip() or "=" not in line or '"' not in line:
                    continue
                key = line.split("=")[0].split("_")[-1]
                vals = line.split('"')[1].split("~")
                if len(vals) < 50:
                    continue
                code = key[2:]
                try:
                    result[code] = {
                        "name":         vals[1],
                        "price":        float(vals[3]) if vals[3] else 0,
                        "change_pct":   float(vals[32]) if vals[32] else 0,
                        "turnover_pct": float(vals[38]) if vals[38] else 0,
                        "pe_ttm":       float(vals[39]) if vals[39] else 0,
                        "mcap_yi":      float(vals[44]) if vals[44] else 0,
                        "pb":           float(vals[46]) if vals[46] else 0,
                        "limit_up":     float(vals[47]) if vals[47] else 0,
                    }
                except (ValueError, IndexError):
                    pass
        except Exception:
            pass
        time.sleep(0.05)
    return result

# ============================================================
# 异动检测核心
# ============================================================

class SurgeEvent:
    """一次异动事件（在上下限区间内）"""
    def __init__(self, surge_type: str, window: int, start_idx: int,
                 end_idx: int, start_date: str, end_date: str,
                 start_price: float, high_price: float, pct: float):
        self.surge_type  = surge_type
        self.window      = window
        self.start_idx   = start_idx
        self.end_idx     = end_idx
        self.start_date  = start_date
        self.end_date    = end_date
        self.start_price = start_price
        self.end_price   = high_price   # 异动区间内最高价
        self.pct         = pct


def detect_all_surge_events(df: pd.DataFrame) -> Tuple[List[SurgeEvent], bool, str]:
    """
    区间限位异动检测（v5.0 核心改动）。
    
    逻辑：
    - 对每个窗口扫描，找该窗口内最高涨幅
    - 涨幅在 [min, max] 内 → 记录为有效异动
    - 任何窗口涨幅 > max → 股票被踢出（暴涨尾声信号）
    - 重叠合并：同一类型窗口，end_idx 差值 < window//3 的只保留涨幅更大的
    
    返回: (events, is_eliminated, eliminated_reason)
    """
    closes = df["close"].values
    dates  = df["date"].values
    n = len(closes)
    all_events = []

    windows = [
        ("3d",  3,  Config.SURGE_3D_MIN  / 100, Config.SURGE_3D_MAX  / 100),
        ("10d", 10, Config.SURGE_10D_MIN / 100, Config.SURGE_10D_MAX / 100),
        ("30d", 30, Config.SURGE_30D_MIN / 100, Config.SURGE_30D_MAX / 100),
    ]

    max_violations = []  # 记录超上限的违规

    for surge_type, window, thresh_min, thresh_max in windows:
        last_end = -999
        i = 0
        while i <= n - window:
            sp = closes[i]
            if sp <= 0:
                i += 1
                continue

            sub = closes[i:i+window+1]
            max_j = int(np.argmax(sub))
            hp = sub[max_j]
            pct = (hp / sp) - 1.0
            abs_j = i + max_j

            # ── 超上限检查 ──
            if pct > thresh_max:
                sd = str(dates[i])[:10]
                ed = str(dates[abs_j])[:10]
                max_violations.append(f"{surge_type}窗口 +{pct*100:.0f}%({sd}~{ed})>上限{thresh_max*100:.0f}%")
                i += 1
                continue

            # ── 区间内 → 有效异动 ──
            if pct >= thresh_min and abs_j - last_end >= max(window // 3, 2):
                try:
                    sd = str(dates[i])[:10]
                    ed = str(dates[abs_j])[:10]
                except Exception:
                    sd = ed = ""
                all_events.append(SurgeEvent(
                    surge_type=surge_type, window=window,
                    start_idx=i, end_idx=abs_j,
                    start_date=sd, end_date=ed,
                    start_price=float(sp), high_price=float(hp),
                    pct=round(pct * 100, 1),
                ))
                last_end = abs_j
                i = abs_j + 1
                continue
            i += 1

    all_events.sort(key=lambda e: e.end_idx)

    # ── 超上限剔除逻辑 ──
    # 规则：有≥1次超上限违规时，检查最近一次有效异动
    # 如果最近有效异动在近60个交易日内 → 不剔除（股票已恢复）
    # 如果最近有效异动在60日以前或无限 → 剔除
    # 如果违规≥10次 → 无条件剔除（重度爆炒股）
    if max_violations:
        if len(max_violations) >= 10:
            return all_events, True, f"重度违规{len(max_violations)}次 | " + " | ".join(max_violations[:2])
        if all_events and all_events[-1].end_idx >= n - 60:
            # 近期有有效异动 → 不剔除，标记
            return all_events, False, f"(有{len(max_violations)}次旧违规，近期有有效异动，暂不剔除)"
        if all_events and all_events[-1].end_idx >= n - 120:
            return all_events, False, f"({len(max_violations)}次旧违规，需观察)"
        return all_events, True, f"无近期有效异动 | " + " | ".join(max_violations[:3])
    return all_events, False, ""


def check_trend_holding(df: pd.DataFrame, last_surge: SurgeEvent) -> Tuple[bool, str, float]:
    """
    v5.0 趋势判断（放宽版，不强制回调≤15%）。
    - 当前价 >= 异动起点 → 非跌破
    - 均线多头（MA5>MA20*0.95 → 放宽）
    - 近10日趋势向上
    返回 (ok, reason, pullback_pct)
    """
    closes = df["close"].values
    current = closes[-1]
    n = len(closes)
    pullback = (last_surge.end_price - current) / last_surge.end_price * 100

    # 1. 检查是否跌破异动起点
    if current < last_surge.start_price * 0.95:
        return False, f"跌穿异动起点{last_surge.start_price:.2f}", pullback

    # 2. 均线多头（放宽至MA5>MA20*0.95）
    if n >= 20:
        ma5  = float(np.mean(closes[-5:]))
        ma20 = float(np.mean(closes[-20:]))
        if ma5 < ma20 * 0.95:
            return False, f"均线偏弱 MA5={ma5:.2f}<MA20*0.95", pullback

    # 3. 近TREND_WINDOW日趋势
    tw = min(Config.TREND_WINDOW, n)
    recent = closes[-tw:].astype(float)
    x = np.arange(tw, dtype=float)
    slope = float(np.polyfit(x, recent, 1)[0])
    if slope < -0.001:  # 放宽，轻微向下可通过
        return False, f"近{tw}日趋势向下(斜率{slope:.3f})", pullback

    reason = f"趋势OK 回调{pullback:.1f}%"
    if pullback <= 0:
        reason = "🆕突破新高"
    elif pullback <= 5:
        reason = f"贴近高点 -{pullback:.1f}%"
    return True, reason, pullback


def check_vol(df: pd.DataFrame) -> Tuple[bool, float]:
    """量能验证（最近一日量比）"""
    if "vol" not in df.columns:
        return True, 1.0
    vols = df["vol"].values
    if len(vols) < 22:
        return True, 1.0
    avg = float(np.mean(vols[-22:-2]))
    if avg <= 0:
        return True, 1.0
    ratio = float(vols[-1]) / avg
    return ratio >= 1.0, round(ratio, 2)


def check_volume_profile(df: pd.DataFrame) -> Tuple[bool, List[float], str]:
    """
    v5.0 量能稳健检查：近3日量能稳步放大（每天递增），单日换手在5%~12%之间。
    返回: (ok, ratios, reason)
    """
    if "vol" not in df.columns or len(df) < 25:
        return True, [], "K线不足"
    vols = df["vol"].values[-3:]
    avg20 = float(np.mean(df["vol"].values[-23:-3]))
    if avg20 <= 0:
        return True, [], "均量不可用"

    ratios = [round(v / avg20, 2) for v in vols]

    # 检查稳步放大（允许小幅波动，总体向上）
    inc_ok = vols[-1] > vols[-3] * 0.95  # 最近量不低于3天前
    # 检查没有异常爆量（单日突然放大3倍以上均量 → 高位爆量嫌疑）
    blast = any(r >= Config.TURNOVER_BLAST / 5 for r in ratios)  # 换手18%约为均量3-4倍

    if not inc_ok:
        return False, ratios, f"近3日量能未稳步放大:{ratios}"
    if blast:
        return False, ratios, f"疑似高位爆量:{ratios}"
    return True, ratios, f"量能稳健:{ratios}"


def check_volume_increasing(df: pd.DataFrame) -> Tuple[bool, str]:
    """检查近3日成交量是否稳步递增"""
    if "vol" not in df.columns or len(df) < 5:
        return True, "K线不足"
    v = df["vol"].values
    v3 = v[-3:]
    if v3[2] >= v3[0] * 0.9:  # 最近量不低于3天前（允许10%波动）
        return True, f"量能递增 {v3[0]:.0f}→{v3[2]:.0f}"
    return False, f"量能递减 {v3[0]:.0f}→{v3[2]:.0f}"


# ============================================================
# 板块主线检测（v5.0 新增）
# ============================================================

# 主线板块定义（2026年6月市场热点）
MAINLINE_SECTORS = {
    "新能源":     ["锂电", "光伏", "风电", "储能", "新能源车", "充电桩", "固态电池", "钠电池"],
    "煤炭":       ["煤炭", "煤化工"],
    "AI算力":     ["算力", "AI", "人工智能", "芯片", "半导体", "光模块", "CPO", "服务器", "数据中心"],
    "机器人":     ["机器人", "工业母机", "自动化"],
    "军工":       ["军工", "航空航天", "卫星", "无人机"],
    "医药":       ["创新药", "CXO", "医疗器械", "中药"],
    "电力":       ["电力", "电网", "特高压", "虚拟电厂"],
    "有色金属":   ["有色", "稀土", "黄金", "铜", "铝"],
}

def detect_stock_sectors(name: str, hot_row: Dict = None) -> List[str]:
    """根据股票名称/热门数据推断所属板块（简化版）"""
    # 优先用同花顺数据中的题材字段
    if hot_row:
        reason = hot_row.get("reason", "") or hot_row.get("hyName", "") or ""
        for sector, keywords in MAINLINE_SECTORS.items():
            for kw in keywords:
                if kw in reason or kw in name:
                    return [sector]
    return []


def check_sector_mainline(stock_code: str, stock_name: str, hot_row: Dict,
                          hot_stocks: List[Dict] = None) -> Tuple[bool, str, str]:
    """
    板块主线检测：该股票所在板块是否今日≥3只强势股涨停。
    如果板块冷门（不在主线名单或无涨停潮），返回失败。
    """
    # 先检测板块
    sectors = detect_stock_sectors(stock_name, hot_row)
    if not sectors:
        # 尝试从同花顺数据获取板块
        reason = (hot_row or {}).get("reason", "") or (hot_row or {}).get("hyName", "")
        if not reason or reason in ["-", "", "其他"]:
            return True, "无法判断板块", "未归类"  # 无法判断时放行

    if hot_stocks is None:
        hot_stocks = []

    sector_name = sectors[0] if sectors else "未归类"
    keywords = MAINLINE_SECTORS.get(sector_name, [])

    # 统计该板块在强势股中的涨停数
    limit_count = 0
    for hs in hot_stocks:
        hs_reason = hs.get("reason", "") or hs.get("hyName", "") or ""
        hs_name = hs.get("name", "")
        for kw in keywords:
            if kw in hs_reason or kw in hs_name:
                # 粗略判断涨停（涨幅>9%）
                try:
                    chg = float(hs.get("changePct", 0) or hs.get("change", 0) or 0)
                except (ValueError, TypeError):
                    chg = 0
                if chg >= 9.5:
                    limit_count += 1
                break

    if limit_count >= Config.SECTOR_MIN_LIMIT:
        return True, f"板块{sector_name}涨停{limit_count}只≥{Config.SECTOR_MIN_LIMIT}", sector_name
    elif limit_count > 0:
        return True, f"板块{sector_name}涨停{limit_count}只(不足{Config.SECTOR_MIN_LIMIT})", sector_name  # 不挡，但标记
    else:
        # 冷门板块或无涨停潮
        return True, f"板块{sector_name}无涨停潮", sector_name  # 宽松放行，但标记


def calc_trade_plan(df: pd.DataFrame, last_surge: SurgeEvent, pool_type: str = "mid") -> Dict:
    """
    v5.0 交易计划（分池计算）。
    - mid_pool: 回踩5/10日线买入，盈利8%减半/破5日线全走
    - high_pool: 只大跌低吸，亏损5%止损/盈利10%全离
    """
    current = float(df["close"].values[-1])
    closes = df["close"].values
    n = len(closes)
    high = last_surge.end_price

    ma5  = float(np.mean(closes[-5:]))  if n >= 5  else current
    ma10 = float(np.mean(closes[-10:])) if n >= 10 else current
    ma20 = float(np.mean(closes[-20:])) if n >= 20 else current

    if pool_type == "mid":
        entry = min(current, ma5 * 1.03)
        stop = ma5 * 0.97
        tp1 = current * (1 + Config.MID_TP_PCT / 100)
        tp2 = current * 1.15
        position = f"≤{Config.MID_POOL_POS*100:.0f}%"
        rule = f"回踩5/10日线买 | +{Config.MID_TP_PCT}%减半 | 破5日线全走"
    else:
        entry = min(ma10 * 1.02, current * 0.95)
        stop = max(current * (1 + Config.HIGH_STOP_PCT / 100), ma20 * 0.95)
        tp1 = current * (1 + Config.HIGH_TP_PCT / 100)
        tp2 = current * 1.20
        position = f"≤{Config.HIGH_POOL_POS*100:.0f}%"
        rule = f"大跌低吸MA10 | -{abs(Config.HIGH_STOP_PCT)}%止损 | +{Config.HIGH_TP_PCT}%全离"

    risk = max(current - stop, 0.01)
    reward = tp1 - current
    rr = round(reward / risk, 2)

    return {
        "entry":       round(entry, 2),
        "stop_loss":   round(stop, 2),
        "target1":     round(tp1, 2),
        "target2":     round(tp2, 2),
        "risk_reward": rr,
        "risk_pct":    round((current - stop) / current * 100, 1),
        "at_new_high": current >= high,
        "pool_type":   pool_type,
        "position":    position,
        "trade_rule":  rule,
        "ma5":         round(ma5, 2),
        "ma10":        round(ma10, 2),
    }

# ============================================================
# 异动预判（PreSurge）模块
# ============================================================
#
# 目标：在异动正式触发"前"识别蓄势信号，更早介入，盈利空间更大
# 代价：信号未确认，误判率更高，建议半仓参与
#
# 五大信号（各1分，满分5分，≥3分进入预判列表）：
#   S1 量能预热：近3日成交量均放大（≥1.5x均量），但涨幅<5%（价未动量先动）
#   S2 历史韵律：当前距上次异动结束天数 落在历史平均异动间隔±30%范围内
#   S3 蓄力收口：ATR（真实波动幅度）近10日呈下降趋势（压缩蓄势）
#   S4 底部抬高：近3个局部低点逐步抬高（资金持续托底）
#   S5 MACD柱翻红：MACD柱（差值）从负转正，或近3日柱量持续扩大

def calc_presurge_score(df: pd.DataFrame, events: List[SurgeEvent]) -> Dict:
    """
    计算 PreSurge 预判评分（0~5分）。
    返回 dict 包含：score, signals, detail, risk_label
    """
    closes = df["close"].values
    highs  = df["high"].values  if "high"  in df.columns else closes
    lows   = df["low"].values   if "low"   in df.columns else closes
    vols   = df["vol"].values   if "vol"   in df.columns else np.ones(len(closes))
    n = len(closes)

    signals = {}
    detail  = []

    # ── S1：量能预热 ──
    # 条件：最近3日均量 ≥ 前20日均量×1.5，但近3日总涨幅 < 5%
    score_s1 = 0
    if n >= 25:
        vol_avg20 = float(np.mean(vols[-23:-3])) if np.mean(vols[-23:-3]) > 0 else 1
        vol_avg3  = float(np.mean(vols[-3:]))
        price_chg = (closes[-1] - closes[-4]) / closes[-4] * 100 if closes[-4] > 0 else 0
        vol_ratio_s1 = round(vol_avg3 / vol_avg20, 2)
        if vol_ratio_s1 >= 1.5 and abs(price_chg) < 5:
            score_s1 = 1
            detail.append(f"S1量预热✓ 近3日量比{vol_ratio_s1}x 价格仅动{price_chg:.1f}%")
        else:
            detail.append(f"S1量预热✗ 量比{vol_ratio_s1}x 价变{price_chg:.1f}%")
    signals["vol_warmup"] = score_s1

    # ── S2：历史韵律 ──
    # 条件：存在≥2次历史异动，计算异动间隔均值，当前等待天数在均值±30%内
    score_s2 = 0
    cycle_days = None
    days_since_last = None
    if len(events) >= 2:
        # 计算历次异动end_idx间隔
        intervals = []
        for i in range(1, len(events)):
            gap = events[i].start_idx - events[i-1].end_idx
            if gap > 0:
                intervals.append(gap)
        if intervals:
            avg_interval = float(np.mean(intervals))
            cycle_days = round(avg_interval, 0)
            days_since_last = n - 1 - events[-1].end_idx
            ratio = days_since_last / avg_interval if avg_interval > 0 else 0
            if 0.6 <= ratio <= 1.4:  # 在均值±40%范围内
                score_s2 = 1
                detail.append(f"S2韵律✓ 历史间隔均{cycle_days:.0f}日 已等{days_since_last}日(比值{ratio:.2f})")
            else:
                detail.append(f"S2韵律✗ 历史间隔均{cycle_days:.0f}日 已等{days_since_last}日(比值{ratio:.2f})")
    else:
        detail.append("S2韵律✗ 异动次数不足")
    signals["rhythm"] = score_s2

    # ── S3：ATR蓄力收口 ──
    # 条件：近10日ATR相比前10日ATR下降（波动率收窄）
    score_s3 = 0
    atr_trend = None
    if n >= 25:
        def calc_atr(h, l, c, start, window):
            trs = []
            for k in range(start, start + window):
                tr = max(h[k] - l[k],
                         abs(h[k] - c[k-1]) if k > 0 else 0,
                         abs(l[k] - c[k-1]) if k > 0 else 0)
                trs.append(tr)
            return float(np.mean(trs)) if trs else 0

        atr_recent = calc_atr(highs, lows, closes, n-10, 10)
        atr_prev   = calc_atr(highs, lows, closes, n-22, 12)
        atr_trend  = round(atr_recent, 3)
        if atr_prev > 0 and atr_recent < atr_prev * 0.85:
            score_s3 = 1
            detail.append(f"S3收口✓ ATR从{atr_prev:.2f}→{atr_recent:.2f}({(atr_recent/atr_prev-1)*100:.0f}%)")
        else:
            detail.append(f"S3收口✗ ATR={atr_recent:.2f} 前={atr_prev:.2f} 未明显收窄")
    signals["atr_compress"] = score_s3

    # ── S4：底部抬高 ──
    # 找近期3个局部低点（窗口内最低点），判断是否逐步抬高
    score_s4 = 0
    valley_prices = []
    if n >= 30:
        # 每10日找一个局部低点
        for seg_start in [n-30, n-20, n-10]:
            seg_end = seg_start + 10
            seg_lows = lows[seg_start:seg_end]
            if len(seg_lows) > 0:
                valley_prices.append(float(np.min(seg_lows)))
        if len(valley_prices) == 3:
            if valley_prices[0] < valley_prices[1] < valley_prices[2]:
                score_s4 = 1
                detail.append(f"S4底抬✓ 低点抬高:{valley_prices[0]:.2f}→{valley_prices[1]:.2f}→{valley_prices[2]:.2f}")
            else:
                detail.append(f"S4底抬✗ 低点:{valley_prices[0]:.2f}→{valley_prices[1]:.2f}→{valley_prices[2]:.2f}")
    signals["higher_lows"] = score_s4

    # ── S5：MACD柱翻红（差值由负转正 或 柱量连续扩大）──
    score_s5 = 0
    if n >= 35:
        # 简化EMA计算
        def ema(arr, period):
            e = np.zeros(len(arr))
            k = 2 / (period + 1)
            e[0] = arr[0]
            for i in range(1, len(arr)):
                e[i] = arr[i] * k + e[i-1] * (1 - k)
            return e

        ema12 = ema(closes, 12)
        ema26 = ema(closes, 26)
        dif = ema12 - ema26
        dea = ema(dif, 9)
        macd_hist = (dif - dea) * 2  # 柱值

        h3 = macd_hist[-3:]
        # 翻红：前一根 ≤ 0 且 最近一根 > 0
        if macd_hist[-2] <= 0 and macd_hist[-1] > 0:
            score_s5 = 1
            detail.append(f"S5MACD✓ 柱翻红 {macd_hist[-2]:.3f}→{macd_hist[-1]:.3f}")
        # 或柱量连续扩大（3根都在扩）
        elif len(h3) == 3 and h3[0] < h3[1] < h3[2] and h3[2] > 0:
            score_s5 = 1
            detail.append(f"S5MACD✓ 柱扩大 {h3[0]:.3f}→{h3[1]:.3f}→{h3[2]:.3f}")
        else:
            detail.append(f"S5MACD✗ 柱={macd_hist[-1]:.3f}")
    signals["macd_cross"] = score_s5

    total = sum(signals.values())

    # 风险标签
    if total >= 4:
        risk_label = "潜力较强"
        risk_cls   = "presurge_high"
    elif total == 3:
        risk_label = "信号一般"
        risk_cls   = "presurge_mid"
    else:
        risk_label = "信号弱"
        risk_cls   = "presurge_low"

    return {
        "presurge_score":    total,
        "presurge_signals":  signals,
        "presurge_detail":   " | ".join(detail),
        "presurge_risk":     risk_label,
        "presurge_risk_cls": risk_cls,
        "cycle_days":        cycle_days,
        "days_since_last":   days_since_last,
    }


# ============================================================
# 单股分析
# ============================================================

def analyze_stock(code: str, name: str, price: float, extra_quote: Dict = None, hot_row: Dict = None) -> Optional[Dict]:
    """
    v5.0 分析单只股票（区间限位 + 三大硬性过滤 + 双池分类）。
    返回结果 dict 或 None（不符合条件）。
    """
    if price < Config.MIN_PRICE or price > Config.MAX_PRICE:
        return None

    q = extra_quote or {}

    # ── 硬性过滤 1：市值 60~350亿 ──
    mcap = q.get("mcap_yi", 0)
    if mcap < Config.MCAP_WARN_LOW:
        return None  # <30亿庄股
    if mcap > Config.MCAP_WARN_HIGH:
        return None  # >1000亿大盘滞涨
    mcap_filtered = False
    if mcap < Config.MCAP_MIN or mcap > Config.MCAP_MAX:
        mcap_filtered = True  # 标记为市值不达标，但仍保留（非核心区间）

    # ── 硬性过滤 2：换手率 5%~12% ──
    turnover = q.get("turnover_pct", 0)
    if turnover > Config.TURNOVER_BLAST:
        return None  # 高位爆量>18%，直接剔除

    # ── K线 ──
    df = get_kline(code, Config.KLINE_DAYS)
    if df is None or len(df) < 40:
        return None

    # ── 异动检测（区间限位）──
    events, is_eliminated, elim_reason = detect_all_surge_events(df)
    if is_eliminated:
        return None  # 超上限，暴涨尾声，直接剔除
    if len(events) < Config.SURGE_MID_MIN:
        return None  # 异动次数不足

    last_surge = events[-1]

    # ── 分级：中段池 vs 高位池 ──
    if len(events) <= Config.SURGE_MID_MAX:
        pool_type = "mid"   # 中段改良池：加速启动阶段
    else:
        pool_type = "high"  # 高位博弈池：接近上限但未超

    # ── 趋势检查 ──
    ok, reason, pullback = check_trend_holding(df, last_surge)
    if not ok and pool_type == "mid":
        return None  # 中段池要求趋势健康
    # 高位池放宽趋势要求

    # ── 量能 ──
    vol_ok, vol_ratio = check_vol(df)
    vol_inc_ok, vol_inc_reason = check_volume_increasing(df)

    # ── 硬性过滤 3：换手率在 5%~12% 区间 ──
    turnover_ok = Config.TURNOVER_MIN <= turnover <= Config.TURNOVER_MAX
    turnover_warn = ""
    if not turnover_ok:
        if turnover < Config.TURNOVER_MIN:
            turnover_warn = f"换手率偏低{turnover:.1f}%"
        else:
            turnover_warn = f"换手率偏高{turnover:.1f}%"

    # ── 交易计划 ──
    plan = calc_trade_plan(df, last_surge, pool_type)

    # ── 均线 ──
    closes = df["close"].values
    ma5  = round(float(np.mean(closes[-5:])),  2) if len(closes) >= 5  else 0
    ma10 = round(float(np.mean(closes[-10:])), 2) if len(closes) >= 10 else 0
    ma20 = round(float(np.mean(closes[-20:])), 2) if len(closes) >= 20 else 0
    ma60 = round(float(np.mean(closes[-60:])), 2) if len(closes) >= 60 else 0

    # ── 事件摘要 ──
    events_summary = [
        {
            "type": e.surge_type, "window": e.window, "pct": e.pct,
            "start_date": e.start_date, "end_date": e.end_date,
            "start_price": round(e.start_price, 2),
            "high_price": round(e.end_price, 2),
        }
        for e in events[-8:]
    ]

    # ── 评分（中段池加权更高）──
    if pool_type == "mid":
        score = (
            len(events) * 12
            + (3 - min(abs(pullback) / 5, 3)) * 6
            + min(vol_ratio, 3) * 4
            + min(plan["risk_reward"], 5) * 3
            + (5 if turnover_ok else -3)
            + (0 if mcap_filtered else 5)
        )
    else:
        score = (
            len(events) * 8
            + min(vol_ratio, 3) * 3
            + min(plan["risk_reward"], 5) * 2
            + (3 if turnover_ok else -5)
        )

    presurge = calc_presurge_score(df, events)

    return {
        "code":         code,
        "name":         name,
        "price":        price,
        "change_pct":   q.get("change_pct", 0),
        "turnover_pct": turnover,
        "pe_ttm":       q.get("pe_ttm", 0),
        "mcap_yi":      mcap,
        "pb":           q.get("pb", 0),

        # 异动统计
        "surge_count":       len(events),
        "last_surge_type":   last_surge.surge_type,
        "last_surge_window": last_surge.window,
        "last_surge_pct":    last_surge.pct,
        "last_surge_high":   round(last_surge.end_price, 2),
        "last_surge_start":  round(last_surge.start_price, 2),
        "last_surge_date":   last_surge.end_date,

        # 分级
        "pool_type":   pool_type,  # "mid" / "high"
        "pool_label":  "中段加速" if pool_type == "mid" else "高位博弈",

        # 趋势
        "trend_reason": reason,
        "pullback_pct": round(pullback, 1),
        "vol_ratio":    vol_ratio,
        "vol_ok":       vol_ok,
        "vol_inc_ok":   vol_inc_ok,
        "vol_inc_reason": vol_inc_reason,
        "turnover_ok":  turnover_ok,
        "turnover_warn": turnover_warn,
        "mcap_ok":      not mcap_filtered,

        # 过滤标记
        "is_eliminated": False,

        # 均线
        "ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,

        # 交易计划
        "plan": plan,

        # 历史异动
        "events": events_summary,

        # PreSurge
        **presurge,

        # 综合评分
        "score": round(score, 1),
    }


def analyze_stock_presurge(code: str, name: str, price: float, extra_quote: Dict = None, hot_row: Dict = None) -> Optional[Dict]:
    """
    v5.0 PreSurge 预判模式（宽松版）。
    条件：历史异动≥2 + presurge评分≥3 + 未超上限。
    """
    if price < Config.MIN_PRICE or price > Config.MAX_PRICE:
        return None

    q = extra_quote or {}

    df = get_kline(code, Config.KLINE_DAYS)
    if df is None or len(df) < 40:
        return None

    events, is_eliminated, _ = detect_all_surge_events(df)
    if is_eliminated or len(events) < Config.PRESURGE_MIN_SURGES:
        return None

    last_surge = events[-1]

    # PreSurge评分
    presurge = calc_presurge_score(df, events)
    if presurge["presurge_score"] < Config.PRESURGE_MIN_SCORE:
        return None

    vol_ok, vol_ratio = check_vol(df)
    plan = calc_trade_plan(df, last_surge, "mid")

    closes = df["close"].values
    ma5  = round(float(np.mean(closes[-5:])),  2) if len(closes) >= 5  else 0
    ma10 = round(float(np.mean(closes[-10:])), 2) if len(closes) >= 10 else 0
    ma20 = round(float(np.mean(closes[-20:])), 2) if len(closes) >= 20 else 0
    ma60 = round(float(np.mean(closes[-60:])), 2) if len(closes) >= 60 else 0

    events_summary = [
        {"type": e.surge_type, "window": e.window, "pct": e.pct,
         "start_date": e.start_date, "end_date": e.end_date,
         "start_price": round(e.start_price, 2), "high_price": round(e.end_price, 2)}
        for e in events[-8:]
    ]

    score = len(events) * 8 + presurge["presurge_score"] * 15 + min(vol_ratio, 3) * 3

    return {
        "code": code, "name": name, "price": price,
        "change_pct": q.get("change_pct", 0),
        "turnover_pct": q.get("turnover_pct", 0),
        "pe_ttm": q.get("pe_ttm", 0), "mcap_yi": q.get("mcap_yi", 0),
        "surge_count": len(events), "last_surge_type": last_surge.surge_type,
        "last_surge_window": last_surge.window, "last_surge_pct": last_surge.pct,
        "last_surge_high": round(last_surge.end_price, 2),
        "last_surge_start": round(last_surge.start_price, 2),
        "last_surge_date": last_surge.end_date,
        "pool_type": "mid", "pool_label": "预判候选",
        "trend_reason": "预判模式", "pullback_pct": round((last_surge.end_price - price) / last_surge.end_price * 100, 1),
        "vol_ratio": vol_ratio, "vol_ok": vol_ok,
        "vol_inc_ok": True, "vol_inc_reason": "预判跳过", "turnover_ok": True, "turnover_warn": "",
        "mcap_ok": True, "is_eliminated": False,
        "ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,
        "plan": {**plan, "position_note": "预判阶段建议半仓"},
        "events": events_summary, **presurge,
        "score": round(score, 1), "is_presurge": True,
    }


# ============================================================
# 扫描入口
# ============================================================

def _load_history_codes(presurge_mode: bool = False) -> List[str]:
    """加载上次扫描通过的股票代码，作为历史跟踪"""
    try:
        if os.path.exists(Config.TRACK_FILE):
            with open(Config.TRACK_FILE, "r") as f:
                data = json.load(f)
                key = "presurge" if presurge_mode else "confirmed"
                return data.get(key, [])
    except Exception:
        pass
    return []


def _save_history_codes(results: List[Dict], presurge_mode: bool = False) -> None:
    """保存本次通过的股票代码（合并追加，不覆盖旧数据）"""
    try:
        data = {}
        if os.path.exists(Config.TRACK_FILE):
            with open(Config.TRACK_FILE, "r") as f:
                data = json.load(f)
        key = "presurge" if presurge_mode else "confirmed"
        new_codes = [r["code"] for r in results]
        # 合并：旧列表 + 新通过（去重、保持顺序）
        old = data.get(key, [])
        merged = list(dict.fromkeys(old + new_codes))  # 新通过的排在后面，去重保序
        # 最多保留 50 只，避免无限膨胀
        data[key] = merged[-50:]
        os.makedirs(os.path.dirname(Config.TRACK_FILE) or ".", exist_ok=True)
        with open(Config.TRACK_FILE, "w") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


def scan_hot_stocks(presurge_mode: bool = False) -> List[Dict]:
    """模式1：只扫同花顺当日强势股（快速，几十只）+ 历史跟踪股"""
    label = "PreSurge预判" if presurge_mode else "强势股池"
    print(f"[SCAN] 模式: {label}扫描")
    hot = get_hot_stocks_today()
    if not hot:
        print("  [WARN] 今日强势股为空，尝试全市场扫描...")
        return scan_full_market(presurge_mode)

    codes = [row.get("code", "") for row in hot if row.get("code")]

    # 加载历史跟踪股（上次通过的股票，即使今日不在强势股池也继续跟踪）
    history_codes = _load_history_codes(presurge_mode)
    new_tracking = [c for c in history_codes if c not in codes]
    if new_tracking:
        codes.extend(new_tracking)
        print(f"  [INFO] 强势股 {len(codes)-len(new_tracking)} 只 + 历史跟踪 {len(new_tracking)} 只 = {len(codes)} 只")
    else:
        print(f"  [INFO] 强势股共 {len(codes)} 只，开始K线分析...")

    # 批量行情
    quotes = tencent_batch_quote(codes)

    results = []
    for i, code in enumerate(codes):
        q = quotes.get(code, {})
        name  = q.get("name", "") or next((r.get("name","") for r in hot if r.get("code") == code), "")
        price = q.get("price", 0) or float(next((r.get("close", 0) for r in hot if r.get("code") == code), 0) or 0)

        if not name:
            name = next((r.get("name","") for r in hot if r.get("code")==code), code)

        # 找匹配的hot_row用于板块检测
        hot_row = next((r for r in hot if r.get("code") == code), None)

        try:
            if presurge_mode:
                result = analyze_stock_presurge(code, name, price, q, hot_row)
            else:
                result = analyze_stock(code, name, price, q, hot_row)
            if result:
                results.append(result)
                p = result["plan"]
                ps_score = result.get("presurge_score", "-")
                if presurge_mode:
                    print(f"  🔮 {result['name']}({code}) "
                          f"历史异动{result['surge_count']}次 "
                          f"预判分{ps_score}/5 "
                          f"{result.get('presurge_risk','?')}")
                else:
                    print(f"  {result['pool_label']} {result['name']}({code}) "
                          f"异动{result['surge_count']}次 "
                          f"换手{result['turnover_pct']}% "
                          f"量能{result['vol_inc_reason']} "
                          f"预判{ps_score}/5 "
                          f"RR={p['risk_reward']}x")
        except Exception as e:
            pass
        time.sleep(0.05)

    results.sort(key=lambda x: x["score"], reverse=True)
    print(f"\n[DONE] {label} {len(codes)} 只 → {len(results)} 只通过")
    _save_history_codes(results, presurge_mode)
    return results


def scan_full_market(presurge_mode: bool = False) -> List[Dict]:
    """模式2：全市场扫描（慢，数千只）"""
    print("[SCAN] 模式: 全市场扫描")
    codes = get_all_stock_codes()
    if not codes:
        return []

    # 批量行情（分批）
    print("  [INFO] 批量获取行情...")
    quotes = {}
    for i in range(0, len(codes), 50):
        batch_q = tencent_batch_quote(codes[i:i+50])
        quotes.update(batch_q)
        if i % 500 == 0 and i > 0:
            print(f"    行情进度: {i}/{len(codes)}")
        time.sleep(0.1)
    print(f"  [INFO] 获取到 {len(quotes)} 条行情")

    results = []
    for i, code in enumerate(codes):
        q = quotes.get(code, {})
        name  = q.get("name", code)
        price = q.get("price", 0)

        if i % 200 == 0:
            print(f"  K线进度: {i}/{len(codes)} | 已找到: {len(results)}")

        try:
            if presurge_mode:
                result = analyze_stock_presurge(code, name, price, q)
            else:
                result = analyze_stock(code, name, price, q)
            if result:
                results.append(result)
                p = result["plan"]
                if presurge_mode:
                    print(f"  🔮 {result['name']}({code}) 预判分{result.get('presurge_score','?')}/5 {result.get('presurge_risk','?')}")
                else:
                    print(f"  ✅ {result['name']}({code}) "
                          f"异动{result['surge_count']}次 "
                          f"回调{result['pullback_pct']}% "
                          f"风险收益{p['risk_reward']}x")
        except Exception:
            pass
        time.sleep(0.02)

    results.sort(key=lambda x: x["score"], reverse=True)
    print(f"\n[DONE] 全市场 {len(codes)} 只 → {len(results)} 只通过")
    _save_history_codes(results, presurge_mode)
    return results

# ============================================================
# 历史归档 & 回顾页面
# ============================================================

def _generate_history_html():
    """扫描 history/ 目录下所有归档，生成历史回顾页面"""
    history_root = Path(Config.HISTORY_DIR)
    if not history_root.exists():
        return

    # 收集所有日期目录（YYYY-MM-DD）
    date_dirs = sorted([d for d in history_root.iterdir() if d.is_dir()], reverse=True)
    if not date_dirs:
        return

    # 解析每个日期的摘要数据
    daily_summaries = []
    all_stocks_map = {}  # {code: {name, count, dates[], first_price, last_price}}

    for date_dir in date_dirs:
        date_str = date_dir.name
        json_file = date_dir / "result.json"
        if not json_file.exists():
            continue

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        stocks = data.get("stocks", [])
        buyable = [s for s in stocks if not s.get("is_fishtail") and not s.get("is_limit_up")]
        limit_up = [s for s in stocks if s.get("is_limit_up") and not s.get("is_fishtail")]
        fishtail = [s for s in stocks if s.get("is_fishtail")]
        presurge = [s for s in stocks if s.get("presurge_score", 0) >= 3]

        daily_summaries.append({
            "date": date_str,
            "total": len(stocks),
            "buyable": len(buyable),
            "limit_up": len(limit_up),
            "fishtail": len(fishtail),
            "presurge": len(presurge),
            "stocks": stocks,
        })

        # 统计跨日出现的股票
        for s in stocks:
            code = s["code"]
            if code not in all_stocks_map:
                all_stocks_map[code] = {
                    "name": s["name"],
                    "count": 0,
                    "dates": [],
                    "first_price": s["price"],
                    "last_price": s["price"],
                    "first_date": date_str,
                    "last_date": date_str,
                }
            entry = all_stocks_map[code]
            entry["count"] += 1
            entry["dates"].append(date_str)
            entry["last_price"] = s["price"]
            entry["last_date"] = date_str

    # 跨日出现≥2次的股票，按出现次数排序
    recurring = sorted(
        [v for v in all_stocks_map.values() if v["count"] >= 2],
        key=lambda x: x["count"],
        reverse=True
    )

    # 总计
    total_scans = len(daily_summaries)
    total_stocks = sum(d["total"] for d in daily_summaries)
    total_buyable = sum(d["buyable"] for d in daily_summaries)

    # ── 生成HTML ──
    scan_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 每日摘要行
    daily_rows = ""
    for ds in daily_summaries:
        buy_cls = "good" if ds["buyable"] > 0 else ""
        daily_rows += f"""
<tr>
  <td class="date-cell"><a href="history/{ds['date']}/result.html">{ds['date']}</a></td>
  <td class="num">{ds['total']}</td>
  <td class="buyable {buy_cls}">{ds['buyable']}</td>
  <td class="limit">{ds['limit_up']}</td>
  <td class="fish">{ds['fishtail']}</td>
  <td class="pre">{ds['presurge']}</td>
  <td class="action"><a href="history/{ds['date']}/result.html" class="btn-sm">查看</a></td>
</tr>"""

    # 跨日股票行
    recurring_rows = ""
    for i, r in enumerate(recurring, 1):
        price_change = ""
        if r["first_price"] and r["last_price"] and r["first_price"] != r["last_price"]:
            chg = (r["last_price"] - r["first_price"]) / r["first_price"] * 100
            cls = "up" if chg >= 0 else "dn"
            price_change = f'<span class="{cls}">{chg:+.1f}%</span>'
        dates_str = ", ".join(r["dates"])
        recurring_rows += f"""
<tr>
  <td class="rank">{i}</td>
  <td class="name-cell">{r['name']}</td>
  <td class="num">{r['count']}次</td>
  <td class="small">{r['first_date']} → {r['last_date']}</td>
  <td>{r['first_price']:.2f} → {r['last_price']:.2f} {price_change}</td>
  <td class="small gray">{dates_str}</td>
</tr>"""

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>异动扫描历史回顾</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:"PingFang SC","Microsoft YaHei",sans-serif;background:#f4f6f9;color:#222}}
.hd{{background:linear-gradient(135deg,#0f172a,#1e3a5f);color:#fff;padding:22px 28px}}
.hd h1{{font-size:22px;font-weight:700;margin-bottom:4px}}
.hd .sub{{font-size:12px;opacity:.7}}
.cards{{display:flex;gap:12px;margin:14px 20px;flex-wrap:wrap}}
.card{{background:#fff;border-radius:8px;padding:12px 18px;box-shadow:0 2px 6px rgba(0,0,0,.06);flex:1;min-width:80px}}
.card .v{{font-size:24px;font-weight:700;color:#0f172a}}
.card .l{{font-size:11px;color:#999;margin-top:4px}}
.tw{{margin:0 20px 20px;overflow-x:auto}}
.sec-hd{{margin:20px 20px 8px;font-size:14px;font-weight:700;color:#0f172a;border-left:3px solid #dc2626;padding-left:10px}}
table{{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 10px rgba(0,0,0,.08)}}
thead tr{{background:#0f172a;color:#fff}}
th{{padding:10px 8px;font-size:11px;white-space:nowrap;text-align:center}}
tbody tr{{border-bottom:1px solid #f0f0f0}}
tbody tr:hover td{{background:#fafbff}}
td{{padding:9px 7px;font-size:12px;text-align:center;vertical-align:middle}}
.date-cell{{font-weight:700;text-align:left}}
.date-cell a{{color:#2563eb;text-decoration:none}}
.date-cell a:hover{{text-decoration:underline}}
.num{{font-weight:700}}
.buyable{{color:#16a34a;font-weight:700}}
.limit{{color:#dc2626}}
.fish{{color:#6b7280}}
.pre{{color:#534AB7}}
.action a{{color:#2563eb;text-decoration:none;font-size:11px}}
.name-cell{{font-weight:600;text-align:left}}
.up{{color:#dc2626;font-weight:700}}.dn{{color:#16a34a;font-weight:700}}.good{{color:#16a34a;font-weight:700}}
.rank{{color:#aaa;font-weight:700;width:30px}}
.small{{font-size:11px}}.gray{{color:#888}}
.btn-sm{{display:inline-block;background:#eff6ff;color:#2563eb;border-radius:4px;padding:3px 10px;font-size:11px;text-decoration:none}}
.btn-sm:hover{{background:#dbeafe}}
.ft{{text-align:center;padding:16px;font-size:11px;color:#aaa}}
.ft a{{color:#2563eb;text-decoration:none}}
</style>
</head>
<body>
<div class="hd">
  <h1>📋 A股异动扫描 — 历史回顾</h1>
  <div class="sub">更新: {scan_time} &nbsp;|&nbsp; 共 {total_scans} 个扫描日</div>
</div>

<div class="cards">
  <div class="card"><div class="v">{total_scans}</div><div class="l">扫描天数</div></div>
  <div class="card"><div class="v">{total_stocks}</div><div class="l">累计标的数</div></div>
  <div class="card"><div class="v" style="color:#16a34a">{total_buyable}</div><div class="l">历史可买总数</div></div>
  <div class="card"><div class="v" style="color:#534AB7">{len(recurring)}</div><div class="l">跨日出现≥2次</div></div>
</div>

<!-- 每日扫描记录 -->
<div class="sec-hd">📅 每日扫描摘要</div>
<div class="tw">
<table>
<thead><tr>
  <th>日期</th><th>总数</th><th>🔥可买</th><th>🚫涨停</th><th>🐟鱼尾</th><th>🔮预判</th><th>详情</th>
</tr></thead>
<tbody>{daily_rows or '<tr><td colspan="7" class="gray">暂无归档记录</td></tr>'}</tbody>
</table>
</div>

<!-- 跨日追踪 -->
{'<div class="sec-hd">🔄 跨日持续出现的股票（≥2次）</div><div class="tw"><table><thead><tr><th>#</th><th>名称</th><th>出现次数</th><th>跨度</th><th>价格区间</th><th>出现日期</th></tr></thead><tbody>' + recurring_rows + '</tbody></table></div>' if recurring else ''}

<div class="ft">
  <a href="yidong_result.html">← 返回最新扫描结果</a>
  &nbsp;|&nbsp; 数据仅供研究参考，不构成投资建议
</div>
</body></html>"""

    Path(Config.HISTORY_HTML).parent.mkdir(parents=True, exist_ok=True)
    with open(Config.HISTORY_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[INFO] 历史页面 → {Config.HISTORY_HTML}")


# ============================================================
# 输出
# ============================================================

def save_json(results: List[Dict]):
    """保存JSON结果 + 归档历史"""
    scan_time = datetime.now().isoformat()
    output = {
        "scan_time": scan_time,
        "total": len(results),
        "strategy": {
            "3d_range": f"{Config.SURGE_3D_MIN}%~{Config.SURGE_3D_MAX}%",
            "10d_range": f"{Config.SURGE_10D_MIN}%~{Config.SURGE_10D_MAX}%",
            "30d_range": f"{Config.SURGE_30D_MIN}%~{Config.SURGE_30D_MAX}%",
            "mcap": f"{Config.MCAP_MIN}~{Config.MCAP_MAX}亿",
            "turnover": f"{Config.TURNOVER_MIN}%~{Config.TURNOVER_MAX}%",
            "mid_pos": f"≤{Config.MID_POOL_POS*100:.0f}%",
            "high_pos": f"≤{Config.HIGH_POOL_POS*100:.0f}%",
        },
        "stocks": results,
    }

    # 主输出
    Path(Config.OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(Config.OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"[INFO] JSON → {Config.OUTPUT_FILE}")

    # 归档到 history/YYYY-MM-DD/
    today_str = datetime.now().strftime("%Y-%m-%d")
    archive_dir = Path(Config.HISTORY_DIR) / today_str
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_file = archive_dir / "result.json"
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"[INFO] 归档 → {archive_file}")

    # 更新历史回顾页面
    _generate_history_html()


def _build_recommendation_html(mid_pool, high_pool, presurge):
    """
    v5.0 操作建议 & 交易纪律（深色主题，置于报告最上面）。
    只保留操作纪律，不重复统计卡片（统计卡片由 save_html 统一渲染）。
    """
    if not mid_pool and not high_pool:
        return ""

    action_pool = mid_pool + high_pool
    def sort_key(r):
        rr = r["plan"]["risk_reward"]
        vol = min(r.get("vol_ratio", 1), 5)
        tp = 1 if r["pool_type"] == "mid" else 0.5
        return r["score"] + rr * 5 + vol * 2 + len(r.get("events", [])) * tp

    ranked = sorted(action_pool, key=sort_key, reverse=True)
    top4 = ranked[:4] if len(ranked) >= 4 else ranked

    def rec_row(r, rank):
        p = r["plan"]
        pool_badge = '<span class="badge-mid">中段</span>' if r["pool_type"] == "mid" else '<span class="badge-high">高位</span>'
        return f"""
<tr>
  <td class="rank">{rank}</td>
  <td class="code-cell">{r['code']}</td>
  <td class="name-cell">{r['name']}{pool_badge}</td>
  <td class="price-cell">{r['price']:.2f}</td>
  <td class="num">{r['surge_count']}次</td>
  <td>{p['entry']:.2f}</td>
  <td class="dn">{p['stop_loss']:.2f}</td>
  <td class="up">{p['target1']:.2f}</td>
  <td class="{'good' if p['risk_reward']>=1.0 else 'ok'}">{p['risk_reward']}x</td>
  <td class="small">{p['position']}</td>
</tr>"""

    top4_rows = "".join(rec_row(r, i+1) for i, r in enumerate(top4))

    # 动态建议
    mid_best  = mid_pool[0]  if mid_pool  else None
    high_best = high_pool[0] if high_pool else None
    advice = []
    if mid_best:
        advice.append(
            f"<b>🏗 中段池首选 {mid_best['code']} {mid_best['name']}</b>："
            f"回踩MA5({mid_best['plan']['ma5']:.2f})或MA10({mid_best['plan']['ma10']:.2f})分批买入，"
            f"盈利{Config.MID_TP_PCT}%减半，破5日线全走，"
            f"单只仓位≤{Config.MID_POOL_POS*100:.0f}%"
        )
    if high_best:
        advice.append(
            f"<b>🎯 高位池观察 {high_best['code']} {high_best['name']}</b>："
            f"只在大跌回踩MA10({high_best['plan']['ma10']:.2f})附近低吸，"
            f"亏损{abs(Config.HIGH_STOP_PCT)}%无条件止损，"
            f"盈利{Config.HIGH_TP_PCT}%全部离场，"
            f"单只≤{Config.HIGH_POOL_POS*100:.0f}%"
        )
    if not mid_pool and not high_pool:
        advice.append("⚠ 当前无可参与标的，等待新的异动信号")
    advice.append(
        f"<b>📊 总仓位控制：</b>"
        f"中段池单只≤{Config.MID_POOL_POS*100:.0f}% "
        f"+ 高位池单只≤{Config.HIGH_POOL_POS*100:.0f}%，永远不满仓"
    )

    # 深色背景的操作纪律区
    DISC_BG   = "#1e293b"
    DISC_FG   = "#fef3c7"
    ADVICE_OL = ("<ol style='margin:8px 0 0 18px;padding:0'>" +
                  "".join(f"<li style='color:{DISC_FG};margin:6px 0'>{p}</li>"
                           for p in advice) +
                  "</ol>")

    return f"""
<div class="zone zone-advice" style="border:2px solid #d97706;margin:14px 20px 18px">
  <div class="zone-hd" style="background:#d97706;padding:10px 18px">
    <h2 style="display:inline;font-size:15px;color:#fff">🎯 操作建议 & 交易纪律</h2>
    <span style="margin-left:14px;font-size:12px;opacity:.85;color:#fff">
      中段回踩买 ＋ 高位大跌买，放弃涨停追板
    </span>
  </div>
  <!-- 重点关注 TOP4 -->
  <div style="margin:10px 18px">
    <table style="width:100%;border-collapse:collapse;font-size:12px">
    <thead><tr style="background:#0f172a;color:#fff">
      <th style="padding:6px 4px">优先级</th>
      <th style="padding:6px 4px">代码</th>
      <th style="padding:6px 4px">名称</th>
      <th style="padding:6px 4px">现价</th>
      <th style="padding:6px 4px">异动</th>
      <th style="padding:6px 4px">买点</th>
      <th style="padding:6px 4px">止损</th>
      <th style="padding:6px 4px">目标</th>
      <th style="padding:6px 4px">RR</th>
      <th style="padding:6px 4px">仓位</th>
    </tr></thead>
    <tbody>{top4_rows or '<tr><td colspan="10" style="color:#888;padding:12px">暂无标的可推荐</td></tr>'}</tbody>
    </table>
  </div>
  <!-- 操作纪律（深色） -->
  <div style="background:{DISC_BG};color:{DISC_FG};padding:14px 18px;font-size:13px;line-height:1.9;border-top:1px solid #d97706">
    <b style="color:#fbbf24">📌 交易纪律（严格执行）：</b>
    {ADVICE_OL}
    <p style="margin:10px 0 0;font-size:12px;color:#94a3b8">
      ⚠ 禁止追涨停板 ｜ 中段回踩买、高位大跌买 ｜ 止损必须执行 ｜ 数据仅供研究，不构成投资建议
    </p>
  </div>
</div>"""



# ============================================================
# 复盘对比（晚盘专用）
# ============================================================

def _build_review_html(predicted: List[Dict], actual: List[Dict]) -> str:
    """
    对比早盘预测 vs 实际收盘结果，生成复盘 HTML。
    """
    pred_codes  = {r.get("code") for r in predicted}
    actual_codes = {r.get("code") for r in actual}

    hits  = pred_codes & actual_codes
    misses = pred_codes - actual_codes
    new_finds = actual_codes - pred_codes

    hit_pct = len(hits) / len(pred_codes) * 100 if pred_codes else 0

    hit_names  = [f"{r['name']}({r['code']})" for r in predicted if r.get("code") in hits]
    miss_names = [f"{r['name']}({r['code']})" for r in predicted if r.get("code") in misses]
    new_names  = [f"{r['name']}({r['code']})" for r in actual   if r.get("code") in new_finds]

    hit_list  = "、".join(hit_names[:10])  + (" 等" if len(hit_names) > 10 else "") if hit_names else "—"
    miss_list = "、".join(miss_names[:10]) + (" 等" if len(miss_names) > 10 else "") if miss_names else "—"
    new_list  = "、".join(new_names[:10])  + (" 等" if len(new_names) > 10 else "") if new_names else "—"

    return f"""
<div class="review-box" style="margin:14px 20px 18px;background:#0f172a;color:#e2e8f0;border-radius:10px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.15)">
  <div style="background:linear-gradient(135deg,#b45309,#d97706);color:#fff;padding:10px 18px;font-size:13px;font-weight:700">
    📋 早盘预测复盘 — 命中率 {hit_pct:.0f}%（{len(hits)}/{len(pred_codes)}）
  </div>
  <div style="display:flex;gap:18px;flex-wrap:wrap;padding:12px 18px;font-size:12px;line-height:1.8">
    <div style="flex:1;min-width:180px">
      <b style="color:#4ade80">✅ 命中 ({len(hits)}只)：</b><br>
      <span style="color:#94a3b8">{hit_list}</span>
    </div>
    <div style="flex:1;min-width:180px">
      <b style="color:#fbbf24">⚠ 未出现 ({len(misses)}只)：</b><br>
      <span style="color:#94a3b8">{miss_list}</span>
    </div>
    <div style="flex:1;min-width:180px">
      <b style="color:#60a5fa">🆕 新增异动 ({len(new_finds)}只)：</b><br>
      <span style="color:#94a3b8">{new_list}</span>
    </div>
  </div>
</div>"""


def save_html(results: List[Dict], schedule: str = None, predicted: List[Dict] = None):
    scan_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ── v5.0 双池分类 ──
    mid_pool  = [r for r in results if r.get("pool_type") == "mid" and not r.get("is_presurge")]
    high_pool = [r for r in results if r.get("pool_type") == "high" and not r.get("is_presurge")]
    presurge_items = [r for r in results if r.get("is_presurge")]
    eliminated = [r for r in results if r.get("is_eliminated")]

    # ── 通用行生成 ──
    def make_rows(items, start_rank=1):
        html_out = ""
        for rank, r in enumerate(items, start_rank):
            p = r["plan"]
            events_str = " → ".join([
                f"{e['type']}窗+{e['pct']}%({e['start_date']}~{e['end_date']})"
                for e in r["events"]
            ])
            tags_html = "".join([
                f'<span class="tag">{e["type"]}窗 +{e["pct"]}%</span>'
                for e in r["events"]
            ])

            # 量能标签
            vol_inc_ok = r.get("vol_inc_ok", True)
            vol_tag = f'<span class="tag-v-up">📈量递增</span>' if vol_inc_ok else f'<span class="tag-v-dn">📉量递减</span>'

            # 换手率
            to = r.get("turnover_pct", 0)
            to_ok = r.get("turnover_ok", True)
            to_cls = "good" if to_ok else "bad"
            to_str = f'<span class="{to_cls}">{to:.1f}%</span>'

            # 市值
            mcap = r.get("mcap_yi", 0)
            mcap_ok = r.get("mcap_ok", True)
            mcap_cls = "ok" if mcap_ok else "bad"
            mcap_str = f'<span class="{mcap_cls}">{mcap:.0f}亿</span>'

            # 池标签
            pool_cls = "badge-mid" if r.get("pool_type") == "mid" else "badge-high"
            pool_label = r.get("pool_label", "")

            html_out += f"""
<tr>
  <td class="rank">{rank}</td>
  <td class="code-cell">{r['code']}</td>
  <td class="name-cell">{r['name']}<span class="{pool_cls}">{pool_label}</span></td>
  <td class="price-cell">{r['price']:.2f}</td>
  <td class="{'up' if r['change_pct']>=0 else 'dn'}">{r['change_pct']:+.2f}%</td>
  <td class="surge">{r['surge_count']}次<br><small>{r['last_surge_type']}窗{r['last_surge_pct']:.0f}%</small></td>
  <td>{mcap_str}</td>
  <td>{to_str}</td>
  <td>{vol_tag}<br><small class="gray">{r.get('vol_inc_reason','')}</small></td>
  <td class="trend small">{r['trend_reason']}</td>
  <td class="price-cell">{p['entry']:.2f}</td>
  <td class="dn">{p['stop_loss']:.2f}</td>
  <td class="up">{p['target1']:.2f}</td>
  <td class="{'good' if p['risk_reward']>=1.0 else 'ok'}">{p['risk_reward']}x</td>
  <td class="small">{p['position']}</td>
  <td class="psscore ps{r.get('presurge_score',0)}">{r.get('presurge_score','-')}/5</td>
  <td class="score">{r['score']:.0f}</td>
  <td class="det-btn" onclick="tog('{r['code']}')">▼详情</td>
</tr>
<tr id="d{r['code']}" class="det">
  <td colspan="18">
    <b>历史异动 ({r['surge_count']}次)：</b>{tags_html}<br>
    <small class="gray">完整记录: {events_str}</small><br>
    <b>均线：</b>MA5={r['ma5']} &nbsp; MA10={r['ma10']} &nbsp; MA20={r['ma20']} &nbsp; MA60={r['ma60']}<br>
    <b>交易纪律：</b>{p['trade_rule']} &nbsp;|&nbsp; 仓位{p['position']}<br>
    <b>PreSurge预判：</b><span class="ps-detail">{r.get('presurge_detail','无数据')}</span>
  </td>
</tr>"""
        return html_out

    # ── 统计 ──
    good_rr = sum(1 for r in results if r["plan"]["risk_reward"] >= 1.0)
    ps_high = sum(1 for r in results if r.get("presurge_score", 0) >= 4)

    # ── PreSurge 行 ──
    presurge_rows_html = ""
    for rank, r in enumerate(sorted(presurge_items, key=lambda x: x.get("presurge_score", 0), reverse=True), 1):
        p = r["plan"]
        ps = r.get("presurge_score", 0)
        ps_cls = "ps5" if ps >= 4 else "ps3"
        presurge_rows_html += f"""
<tr class="ps-row">
  <td class="rank">{rank}</td>
  <td class="code-cell">{r['code']}</td>
  <td class="name-cell">{r['name']}</td>
  <td class="price-cell">{r['price']:.2f}</td>
  <td class="{'up' if r['change_pct']>=0 else 'dn'}">{r['change_pct']:+.2f}%</td>
  <td>{r['surge_count']}次</td>
  <td class="{ps_cls}"><b>{ps}/5</b> {r.get('presurge_risk','')}</td>
  <td class="small">{r.get('presurge_detail','').replace(' | ','<br>')}</td>
  <td><span class="badge-pre">未确认</span></td>
  <td class="dn">{p['stop_loss']:.2f}</td>
  <td class="up">{p['target1']:.2f}</td>
  <td>{r['vol_ratio']}x</td>
  <td class="small gray">{r.get('cycle_days') and f"周期{r['cycle_days']:.0f}日" or ""}</td>
</tr>"""

    # ── 生成表格 ──
    mid_rows   = make_rows(mid_pool, 1)
    high_rows  = make_rows(high_pool, 1)
    elim_rows  = make_rows(eliminated, 1) if eliminated else ""

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>A股异动筛选 v5 {scan_time}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:"PingFang SC","Microsoft YaHei",sans-serif;background:#f4f6f9;color:#222}}
.hd{{background:linear-gradient(135deg,#0f172a,#1e3a5f);color:#fff;padding:22px 28px}}
.hd h1{{font-size:22px;font-weight:700;margin-bottom:4px}}
.hd .sub{{font-size:12px;opacity:.7}}
.strat{{background:#fff;margin:14px 20px;padding:14px 18px;border-radius:8px;border-left:4px solid #2563eb;box-shadow:0 2px 6px rgba(0,0,0,.06)}}
.strat h3{{color:#2563eb;font-size:13px;margin-bottom:6px}}
.strat p{{font-size:12px;color:#555;line-height:1.7}}
.bd{{display:inline-block;background:#2563eb;color:#fff;border-radius:4px;padding:1px 7px;font-size:11px;margin:0 3px}}
.bd-red{{display:inline-block;background:#dc2626;color:#fff;border-radius:4px;padding:1px 7px;font-size:11px;margin:0 3px}}
.bd-purple{{display:inline-block;background:#534AB7;color:#fff;border-radius:4px;padding:1px 7px;font-size:11px;margin:0 3px}}
.cards{{display:flex;gap:12px;margin:0 20px 14px;flex-wrap:wrap}}
.card{{background:#fff;border-radius:8px;padding:12px 18px;box-shadow:0 2px 6px rgba(0,0,0,.06);flex:1;min-width:100px}}
.card .v{{font-size:26px;font-weight:700;color:#dc2626}}
.card .l{{font-size:11px;color:#999;margin-top:2px}}
.tw{{margin:0 20px 20px;overflow-x:auto}}
table{{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 10px rgba(0,0,0,.08)}}
thead tr{{background:#0f172a;color:#fff}}
th{{padding:10px 8px;font-size:11px;white-space:nowrap;text-align:center}}
tbody tr{{border-bottom:1px solid #f0f0f0}}
tbody tr:not(.det):hover td{{background:#fafbff}}
td{{padding:9px 7px;font-size:12px;text-align:center;vertical-align:middle}}
.rank{{color:#aaa;font-weight:700;width:30px}}
.code-cell{{font-family:monospace;color:#2563eb;font-weight:600}}
.name-cell{{font-weight:600;text-align:left;min-width:72px}}
.price-cell{{font-weight:700}}
.up{{color:#dc2626}}.dn{{color:#16a34a}}.good{{color:#dc2626;font-weight:700}}.ok{{color:#d97706;font-weight:600}}.bad{{color:#aaa}}
.surge{{font-size:12px}}.small{{font-size:11px}}.gray{{color:#888}}
.score{{font-weight:700;color:#7c3aed}}
.det{{background:#f8fafc!important}}
.det td{{text-align:left;padding:10px 14px;font-size:11px;line-height:1.8;border-top:1px dashed #e5e7eb}}
.tag{{display:inline-block;background:#eff6ff;color:#2563eb;border-radius:3px;padding:1px 7px;margin:1px;font-size:11px}}
.tag-v-up{{display:inline-block;background:#EAF3DE;color:#3B6D11;border-radius:3px;padding:1px 6px;font-size:10px;margin:1px}}
.tag-v-dn{{display:inline-block;background:#FEE2E2;color:#991B1B;border-radius:3px;padding:1px 6px;font-size:10px;margin:1px}}
.det-btn{{cursor:pointer;color:#2563eb;font-size:11px;white-space:nowrap}}
.det-btn:hover{{text-decoration:underline}}
/* 专区 */
.zone{{margin:0 20px 20px;border-radius:10px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.1)}}
.zone-hd{{color:#fff;padding:12px 18px}}
.zone-hd h2{{font-size:14px;font-weight:700;display:inline}}
.zone-hd .warn{{font-size:11px;opacity:.85;margin-left:12px}}
.zone-mid{{border:2px solid #2563eb}}.zone-mid .zone-hd{{background:#2563eb}}
.zone-high{{border:2px solid #dc2626}}.zone-high .zone-hd{{background:#dc2626}}
.zone-ps{{border:2px solid #534AB7}}.zone-ps .zone-hd{{background:#534AB7}}
.ps-row{{background:#faf9ff}}
.ps-row:hover td{{background:#f0eeff!important}}
.ps5{{color:#534AB7;font-weight:700}}.ps3{{color:#854F0B;font-weight:600}}
.psscore{{font-size:11px}}
.ps0,.ps1,.ps2{{color:#aaa}}.ps3{{color:#d97706}}.ps4,.ps5{{color:#534AB7;font-weight:700}}
.badge-mid{{display:inline-block;background:#DBEAFE;color:#1E40AF;border-radius:3px;padding:1px 6px;font-size:10px;margin-left:4px}}
.badge-high{{display:inline-block;background:#FEE2E2;color:#991B1B;border-radius:3px;padding:1px 6px;font-size:10px;margin-left:4px}}
.badge-pre{{display:inline-block;background:#FAEEDA;color:#854F0B;border-radius:3px;padding:1px 6px;font-size:10px}}
.ps-detail{{font-family:monospace;font-size:10px;color:#534AB7;background:#EEEDFE;padding:2px 6px;border-radius:3px}}
</style>
</head>
<body>
<div class="hd">
  <h1>📈 A股异动筛选 v5.0 — """ + (f"☀️ 早盘预测" if schedule == "morning" else f"📋 收盘复盘" if schedule == "evening" else f"抓启动加速，剔除暴涨尾声") + f"""</h1>
  <div class="sub">扫描: {scan_time} &nbsp;|&nbsp; 🏗中段 <strong>{len(mid_pool)}</strong>只 &nbsp;|&nbsp; 🎯高位 <strong>{len(high_pool)}</strong>只 &nbsp;|&nbsp; 🔮预判 <strong>{len(presurge_items)}</strong>只</div>
</div>

""" + (f"""   <!-- 复盘对比 -->""" if schedule == "evening" and predicted else f"""   """) + (f"""
<!-- 早盘预测 vs 收盘复盘 -->
""" + _build_review_html(predicted, results) if schedule == "evening" and predicted else "") + f"""

<div class="cards">
  <div class="card" style="border:2px solid #2563eb"><div class="v" style="color:#2563eb">{len(mid_pool)}</div><div class="l">🏗 中段加速</div></div>
  <div class="card" style="border:2px solid #dc2626"><div class="v">{len(high_pool)}</div><div class="l">🎯 高位博弈</div></div>
  <div class="card" style="border:2px solid #534AB7"><div class="v" style="color:#534AB7">{ps_high}</div><div class="l">预判高分≥4</div></div>
  <div class="card"><div class="v">{good_rr}</div><div class="l">RR≥1x</div></div>
</div>

<!-- 🏗 中段改良池 -->
<div class="zone zone-mid">
<div class="zone-hd"><h2>🏗 中段改良池（{len(mid_pool)}只）</h2><span class="warn">加速启动阶段，回踩5/10日线买入 | 盈利8%减半 | 破5日线全走 | 单只≤{Config.MID_POOL_POS*100:.0f}%</span></div>
<div class="tw" style="margin:0">
<table>
<thead><tr>
  <th>#</th><th>代码</th><th>名称</th><th>现价</th><th>今日</th>
  <th>异动</th><th>市值</th><th>换手</th><th>量能</th><th>趋势</th>
  <th>买点</th><th>止损</th><th>目标</th><th>RR</th><th>仓位</th><th>预判</th><th>评分</th><th>详情</th>
</tr></thead>
<tbody>{mid_rows or '<tr><td colspan="18" class="gray">暂无中段加速标的</td></tr>'}</tbody>
</table>
</div>
</div>

<!-- 🎯 高位博弈池 -->
{'''<div class="zone zone-high">
<div class="zone-hd"><h2>🎯 高位博弈池（''' + str(len(high_pool)) + '''只）</h2><span class="warn">接近上限但未超 | 只大跌回踩MA10低吸 | 亏损''' + str(abs(Config.HIGH_STOP_PCT)) + '''%止损 | 盈利''' + str(Config.HIGH_TP_PCT) + '''%全离 | 单只≤''' + str(Config.HIGH_POOL_POS*100) + '''%</span></div>
<div class="tw" style="margin:0">
<table>
<thead><tr>
  <th>#</th><th>代码</th><th>名称</th><th>现价</th><th>今日</th>
  <th>异动</th><th>市值</th><th>换手</th><th>量能</th><th>趋势</th>
  <th>买点</th><th>止损</th><th>目标</th><th>RR</th><th>仓位</th><th>预判</th><th>评分</th><th>详情</th>
</tr></thead>
<tbody>''' + high_rows + '''</tbody>
</table>
</div>
</div>''' if high_pool else ''}

<!-- PreSurge 预判 -->
{'''<div class="zone zone-ps">
<div class="zone-hd"><h2>🔮 PreSurge 预判候选（''' + str(len(presurge_items)) + '''只）</h2><span class="warn">信号未确认，蓄势阶段，建议半仓参与</span></div>
<div class="tw" style="margin:0">
<table>
<thead><tr>
  <th>#</th><th>代码</th><th>名称</th><th>现价</th><th>今日</th>
  <th>历史异动</th><th>预判评分</th><th>信号详情</th><th>状态</th>
  <th>止损</th><th>目标1</th><th>量比</th><th>周期参考</th>
</tr></thead>
<tbody>''' + presurge_rows_html + '''</tbody>
</table>
</div>
</div>''' if presurge_items else ''}

<!-- 操作建议 & 交易纪律 -->
{_build_recommendation_html(mid_pool, high_pool, presurge_items)}

<div class="strat">
  <h3>⚡ v5.0 策略：区间限位 + 双池分仓</h3>
  <div style="display:flex;gap:18px;flex-wrap:wrap;margin-top:8px">
    <div style="flex:1;min-width:200px">
      <div style="font-size:11px;color:#6b7280;margin-bottom:4px">异动区间限位（超上限剔除）</div>
      <div style="font-size:13px;line-height:1.9">
        3日：<span class="bd">{Config.SURGE_3D_MIN}%~{Config.SURGE_3D_MAX}%</span>
        &nbsp; 10日：<span class="bd">{Config.SURGE_10D_MIN}%~{Config.SURGE_10D_MAX}%</span>
        &nbsp; 30日：<span class="bd">{Config.SURGE_30D_MIN}%~{Config.SURGE_30D_MAX}%</span>
        <span class="bd-red">超上限=拉黑</span>
      </div>
    </div>
    <div style="flex:1;min-width:200px">
      <div style="font-size:11px;color:#6b7280;margin-bottom:4px">三道硬过滤</div>
      <div style="font-size:13px;line-height:1.9">
        市值 <span class="bd">{Config.MCAP_MIN}~{Config.MCAP_MAX}亿</span>
        &nbsp; 换手 <span class="bd">{Config.TURNOVER_MIN}%~{Config.TURNOVER_MAX}%</span><br>
        量能 <span class="bd">3日递增</span>
        &nbsp; 主线板块 <span class="bd">≥3只涨停</span>
      </div>
    </div>
    <div style="flex:1;min-width:200px">
      <div style="font-size:11px;color:#6b7280;margin-bottom:4px">双池仓位上限</div>
      <div style="font-size:13px;line-height:1.9">
        中段池 <span class="bd-purple">≤{Config.MID_POOL_POS*100:.0f}%</span>
        &nbsp; 高位池 <span class="bd-red">≤{Config.HIGH_POOL_POS*100:.0f}%</span><br>
        永远不满仓 &nbsp; 破5日线全走
      </div>
    </div>
  </div>
</div>

<script>
function tog(c){{
  var r=document.getElementById('d'+c);
  r.style.display=r.style.display==='none'?'table-row':'none';
}}
document.querySelectorAll('.det').forEach(function(r){{r.style.display='none'}});
</script>
<div style="text-align:center;padding:16px 20px 24px;font-size:12px;color:#888">
  <a href="yidong_history.html" style="color:#2563eb;text-decoration:none;font-weight:600">📋 查看历史扫描记录 →</a>
  &nbsp;|&nbsp; 数据仅供研究参考，不构成投资建议
</div>
</body></html>"""

    Path(Config.HTML_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(Config.HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[INFO] HTML → {Config.HTML_FILE}")

    # 归档
    today_str = datetime.now().strftime("%Y-%m-%d")
    archive_dir = Path(Config.HISTORY_DIR) / today_str
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_html = archive_dir / "result.html"
    with open(archive_html, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[INFO] 归档HTML → {archive_html}")

# ============================================================
# 单股分析（便捷函数）
# ============================================================

def analyze_specific(codes: List[str]) -> List[Dict]:
    """分析指定股票（调试/验证用）"""
    print(f"[INFO] 分析指定股票: {codes}")
    quotes = tencent_batch_quote(codes)

    results = []
    for code in codes:
        q = quotes.get(code, {})
        name  = q.get("name", code)
        price = q.get("price", 0)
        if price == 0:
            print(f"  ⚠️  {code}: 无行情数据")
            continue

        result = analyze_stock(code, name, price, q)
        if result:
            results.append(result)
            p = result["plan"]
            print(f"\n  ✅ {name}({code})")
            print(f"     当前价: {price}")
            print(f"     K线天数: {Config.KLINE_DAYS}日")
            print(f"     异动次数: {result['surge_count']} 次")
            print(f"     最近异动: {result['last_surge_type']}窗口 +{result['last_surge_pct']}%")
            print(f"     趋势: {result['trend_reason']}")
            print(f"     量比: {result['vol_ratio']}x")
            print(f"     ── 交易计划 ──")
            print(f"     买入: {p['entry']}  止损: {p['stop_loss']}(-{p['risk_pct']}%)  目标1: {p['target1']}  目标2: {p['target2']}")
            print(f"     风险收益比: {p['risk_reward']}x")
            ev_str = " → ".join([f"{e['type']}+{e['pct']}%" for e in result["events"]])
            print(f"     历史异动链: {ev_str}")
        else:
            print(f"\n  ❌ {name}({code}): 不符合条件")
            # 给出原因（调试用）
            df = get_kline(code, Config.KLINE_DAYS)
            if df is None:
                print(f"     原因: K线获取失败或数据不足")
            else:
                events, is_elim, elim_r = detect_all_surge_events(df)
                print(f"     原因: 找到 {len(events)} 次异动 (需要>={Config.SURGE_MID_MIN}次)")
                if is_elim:
                    print(f"     超上限剔除: {elim_r}")
                if events:
                    last = events[-1]
                    ok, reason, pull = check_trend_holding(df, last)
                    if not ok:
                        print(f"     趋势检查未通过: {reason}")

    _save_history_codes(results, presurge_mode=False)
    return results

# ============================================================
# 主入口
# ============================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="A股异动股票筛选器 v2.0")
    parser.add_argument("--codes", nargs="*",   help="指定股票代码（空=自动扫描）")
    parser.add_argument("--mode",  default="hot", choices=["hot","full"], help="扫描模式: hot=强势股池 full=全市场")
    parser.add_argument("--top",   type=int, default=50, help="保留前N只（默认50）")
    parser.add_argument("--no-save", action="store_true", help="不保存文件")
    parser.add_argument("--days",     type=int, default=365, help="K线天数（默认365）")
    parser.add_argument("--presurge", action="store_true", help="预判模式：在异动前发现（宽松条件，风险更高）")
    parser.add_argument("--both",    action="store_true", help="组合模式：同时跑异动确认+预判，合并输出（每日8:30用）")
    parser.add_argument("--history", action="store_true", help="仅重新生成历史回顾页面（不扫描）")
    parser.add_argument("--schedule", choices=["morning","evening"], help="定时任务模式: morning=早盘预测(8:30) evening=收盘复盘(18:00)")
    args = parser.parse_args()

    Config.KLINE_DAYS = args.days
    Config.SCAN_MODE  = args.mode

    # ── 仅重新生成历史页面 ──
    if args.history:
        print("=" * 60)
        print("  📋 重新生成历史回顾页面")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        _generate_history_html()
        return []

    print("=" * 60)
    print("  A股异动股票筛选器 v5.0（区间限位+双池分仓）")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.both:
        print(f"  模式: 🔥 组合扫描 — 异动确认 + PreSurge预判 双跑")
    elif args.presurge:
        print(f"  模式: 🔮 PreSurge预判 — 在异动前发现（风险更高！）")
    else:
        print(f"  策略: 区间限位 {Config.SURGE_3D_MIN}%~{Config.SURGE_3D_MAX}% / {Config.SURGE_10D_MIN}%~{Config.SURGE_10D_MAX}% + 中段/高位双池")
    print("=" * 60)

    if args.both:
        # 组合模式：先跑异动确认，再跑预判，合并结果
        if args.schedule == "morning":
            print("  时段: ☀️ 早盘预测 — 北京时间 8:30")
        elif args.schedule == "evening":
            print("  时段: 🌙 收盘复盘 — 北京时间 18:00")

        print("\n>>> 第一轮：异动确认扫描...")
        confirmed = scan_hot_stocks(presurge_mode=False) if args.mode != "full" else scan_full_market(presurge_mode=False)
        print(f"\n>>> 第二轮：PreSurge预判扫描...")
        presurge_candidates = scan_hot_stocks(presurge_mode=True) if args.mode != "full" else scan_full_market(presurge_mode=True)

        # 合并：预判候选去掉已确认的重复
        confirmed_codes = {r['code'] for r in confirmed}
        new_presurge = [r for r in presurge_candidates if r['code'] not in confirmed_codes]
        results = confirmed + new_presurge
        results = results[:args.top]
        print(f"\n>>> 合并结果: 确认{len(confirmed)}只 + 新增预判{len(new_presurge)}只 = {len(results)}只")

    elif args.codes:
        results = analyze_specific(args.codes)
    elif args.mode == "full":
        results = scan_full_market(presurge_mode=args.presurge)
    else:
        results = scan_hot_stocks(presurge_mode=args.presurge)

    if not results:
        print("\n[INFO] 没有找到符合条件的股票（市场可能处于弱势）")
        return []

    results = results[:args.top]

    print(f"\n{'='*60}")
    print(f"  筛选结果 TOP {len(results)} (按综合评分排序)")
    print(f"{'='*60}")
    for i, r in enumerate(results, 1):
        p = r["plan"]
        vol_flag = "🔥" if r["vol_ratio"] >= 1.5 else "📊"
        ps_str = f"预判{r.get('presurge_score','?')}/5"
        if args.presurge:
            print(f"{i:2d}. {r['name']:8s}({r['code']}) "
                  f"价:{r['price']:7.2f}  "
                  f"历史异动{r['surge_count']:2d}次  "
                  f"{ps_str}({r.get('presurge_risk','?')})  "
                  f"{vol_flag}量比{r['vol_ratio']:.1f}x  "
                  f"止损{p['stop_loss']:.2f}  "
                  f"分{r['score']:.0f}")
        else:
            print(f"{i:2d}. {r['name']:8s}({r['code']}) "
                  f"价:{r['price']:7.2f}  "
                  f"异动{r['surge_count']:2d}次  "
                  f"回调{r['pullback_pct']:4.1f}%  "
                  f"{vol_flag}量比{r['vol_ratio']:.1f}x  "
                  f"止损{p['stop_loss']:.2f}  "
                  f"目标{p['target1']:.2f}  "
                  f"RR={p['risk_reward']:.1f}x  "
                  f"{ps_str}  "
                  f"分{r['score']:.0f}")

    if not args.no_save:
        # ── 定时任务：早盘预测 / 收盘复盘 ──
        today_str = datetime.now().strftime("%Y-%m-%d")
        pred_dir  = Path(Config.HISTORY_DIR).parent / "prediction"
        pred_dir.mkdir(parents=True, exist_ok=True)

        if args.schedule == "morning":
            # 早盘预测：额外保存到 prediction/ 目录
            pred_json = pred_dir / f"{today_str}_morning.json"
            with open(pred_json, "w", encoding="utf-8") as f:
                json.dump([{k: v for k, v in r.items() if not callable(v)} for r in results], f, ensure_ascii=False, indent=2, default=str)
            print(f"[INFO] 早盘预测 → {pred_json}")

            save_json(results)
            save_html(results, schedule="morning")

        elif args.schedule == "evening":
            # 收盘复盘：加载早盘预测做对比
            pred_json = pred_dir / f"{today_str}_morning.json"
            predicted = []
            if pred_json.exists():
                try:
                    with open(pred_json, "r", encoding="utf-8") as f:
                        predicted = json.load(f)
                except Exception:
                    pass

            save_json(results)
            save_html(results, schedule="evening", predicted=predicted if predicted else None)
        else:
            save_json(results)
            save_html(results)

    return results


if __name__ == "__main__":
    main()
