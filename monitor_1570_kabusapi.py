#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1570 monitor v1.5
Purpose: patch-oriented monitor focused on 1m/3m logic.
Changes vs v1.4 concept:
- relax OVEREXTENDED / VWAP gap gate
- adaptive VWAP gate by regime
- slightly longer minimum holding time
- softer hard edge-break immediately after entry
- richer report fields

This file is designed to be practical and robust rather than minimal.
It uses REST polling against kabu station API.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib import error, request

import pandas as pd

JST = timezone(timedelta(hours=9))
API_BASE_DEFAULT = "http://localhost:18080/kabusapi"
SYMBOL_DEFAULT = "1570"
EXCHANGE_DEFAULT = 1
POLL_INTERVAL_SEC = 3.0
LIVE_ENTRY_TIMEOUT_SEC = 8
LIVE_EXIT_TIMEOUT_SEC = 10
LIVE_RETRY_MAX = 1

# ===== user-editable direct settings =====
API_PASSWORD_HARDCODED = "enmasa1023"  # ここにAPIパスワードを入れる
# =======================================


TRADE_WINDOWS = [
    ("09:03:00", "11:25:00"),
    ("12:35:00", "15:20:00"),
]
STOP_AFTER = "15:30:00"

# v1.6 exit-tuned parameters
SPREAD_TICKS_MAX = 2.0
VWAP_GAP_BPS_MAX_BASE = 120.0
VWAP_GAP_BPS_MAX_TREND = 160.0
VWAP_GAP_BPS_MAX_RANGE = 72.0
ENTRY_COOLDOWN_SEC = 45
REENTRY_AFTER_STOP_SEC = 90
MIN_HOLD_SEC_1M = 45
MIN_HOLD_SEC_3M = 75
MAX_HOLD_SEC_1M = 150
MAX_HOLD_SEC_3M = 300
STOP_TICKS_1M = 4
STOP_TICKS_3M = 5
TAKE_TICKS_1M = 8
TAKE_TICKS_3M = 10
PROB_UPPER_1M = 0.58
PROB_UPPER_3M = 0.54
PROB_EXIT_EDGE = 0.49


def now_jst() -> datetime:
    return datetime.now(JST)


def load_config(path: Optional[str]) -> dict[str, Any]:
    cfg: dict[str, Any] = {}
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    return cfg


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=None)
    p.add_argument("--api-password", dest="api_password", default=None)
    p.add_argument("--live-mode", action="store_true")
    p.add_argument("--order-password", default=None)
    p.add_argument("--order-qty", type=int, default=None)
    p.add_argument("--account-type", type=int, default=None)
    p.add_argument("--margin-trade-type", type=int, default=None)
    p.add_argument("--entry-cash-margin", type=int, default=None)
    p.add_argument("--exit-cash-margin", type=int, default=None)
    p.add_argument("--entry-deliv-type", type=int, default=None)
    p.add_argument("--exit-deliv-type", type=int, default=None)
    p.add_argument("--live-entry-timeout-sec", type=int, default=None)
    p.add_argument("--live-exit-timeout-sec", type=int, default=None)
    p.add_argument("--live-retry-max", type=int, default=None)
    p.add_argument("--outdir", default="monitor_output")
    p.add_argument("--runtime-minutes", type=float, default=None)
    p.add_argument("--base-url", default=None)
    return p.parse_args()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def jst_date_str(dt: Optional[datetime] = None) -> str:
    return (dt or now_jst()).strftime("%Y-%m-%d")


def jst_date_compact(dt: Optional[datetime] = None) -> str:
    return (dt or now_jst()).strftime("%Y%m%d")


def time_in_windows(t: str, windows: list[tuple[str, str]]) -> bool:
    return any(s <= t <= e for s, e in windows)


def tick_size_for_1570(price: float) -> float:
    _ = price
    return 10.0


def price_to_ticks(delta_price: float, ref_price: float) -> float:
    ts = tick_size_for_1570(ref_price)
    return delta_price / ts if ts else 0.0


def sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _http_json(
    method: str,
    url: str,
    token: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> Any:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-API-KEY"] = token
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {e.code} {e.reason}: {body}") from e


class KabuApiClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token: Optional[str] = None

    def get_token(self, api_password: str) -> str:
        res = _http_json("POST", f"{self.base_url}/token", payload={"APIPassword": api_password})
        tok = res.get("Token")
        if not tok:
            raise RuntimeError(f"token missing: {res}")
        self.token = tok
        return tok

    def register_symbol(self, symbol: str, exchange: int) -> Any:
        payload = {"Symbols": [{"Symbol": symbol, "Exchange": exchange}]}
        return _http_json("PUT", f"{self.base_url}/register", token=self.token, payload=payload)

    def get_board(self, symbol: str, exchange: int) -> dict[str, Any]:
        sym = f"{symbol}@{exchange}"
        return _http_json("GET", f"{self.base_url}/board/{sym}", token=self.token)

    def send_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _http_json("POST", f"{self.base_url}/sendorder", token=self.token, payload=payload)

    def cancel_order(self, order_id: str, order_password: str) -> dict[str, Any]:
        payload = {"OrderID": order_id, "Password": order_password}
        return _http_json("PUT", f"{self.base_url}/cancelorder", token=self.token, payload=payload)

    def get_positions(self, symbol: str, exchange: int) -> list[dict[str, Any]]:
        url = f"{self.base_url}/positions?product=2&symbol={symbol}&exchange={exchange}"
        res = _http_json("GET", url, token=self.token)
        return res if isinstance(res, list) else []


@dataclass
class TickSnapshot:
    ts: datetime
    price: Optional[float]
    volume: Optional[float]
    vwap: Optional[float]
    sell1_price: Optional[float]
    sell1_qty: Optional[float]
    buy1_price: Optional[float]
    buy1_qty: Optional[float]
    sell2_price: Optional[float] = None
    sell2_qty: Optional[float] = None
    buy2_price: Optional[float] = None
    buy2_qty: Optional[float] = None
    sell3_price: Optional[float] = None
    sell3_qty: Optional[float] = None
    buy3_price: Optional[float] = None
    buy3_qty: Optional[float] = None
    raw_json: Optional[str] = None


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    ma5: Optional[float] = None
    ma25: Optional[float] = None
    ma75: Optional[float] = None
    atr14: Optional[float] = None


@dataclass
class FeatureSnapshot:
    ts: datetime
    price: float
    vwap: float
    spread_ticks: float
    obi_l1: float
    obi_l3: float
    vwap_gap_bps: float
    ret_30s: float
    ret_1m: float
    ret_3m: float
    close_pos_in_bar_1m: float
    close_pos_in_bar_3m: float
    ma_trend_score_3m: float
    pullback_quality: float
    reacceleration_score: float
    overextension_penalty: float
    volume_1m: float
    volume_3m: float
    trade_intensity_30s: float
    regime: str


@dataclass
class PredictionSnapshot:
    ts: datetime
    regime: str
    p_up_1m: float
    p_down_1m: float
    p_up_3m: float
    p_down_3m: float
    signal: str
    reason_1: str
    reason_2: str
    reason_3: str


@dataclass
class PositionState:
    side: str
    strategy: str
    entry_ts: datetime
    entry_price: float
    entry_p_up_1m: float
    entry_p_up_3m: float
    stop_ticks: int
    take_ticks: int
    min_hold_sec: int
    max_hold_sec: int
    entry_order_id: Optional[str] = None
    exit_order_id: Optional[str] = None


@dataclass
class MonitorStatus:
    count: int = 0
    open_position: Optional[PositionState] = None
    last_entry_ts_by_side: dict[str, Optional[datetime]] = None
    reentry_block_until_by_side: dict[str, Optional[datetime]] = None
    midday_written: bool = False

    def __post_init__(self) -> None:
        if self.last_entry_ts_by_side is None:
            self.last_entry_ts_by_side = {"LONG": None, "SHORT": None}
        if self.reentry_block_until_by_side is None:
            self.reentry_block_until_by_side = {"LONG": None, "SHORT": None}


class Storage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        with self._connect() as con:
            cur = con.cursor()
            cur.execute("""
            CREATE TABLE IF NOT EXISTS system_events(
              ts TEXT, level TEXT, event_type TEXT, message TEXT
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS market_snapshots(
              ts TEXT PRIMARY KEY,
              price REAL, volume REAL, vwap REAL,
              sell1_price REAL, sell1_qty REAL, buy1_price REAL, buy1_qty REAL,
              sell2_price REAL, sell2_qty REAL, buy2_price REAL, buy2_qty REAL,
              sell3_price REAL, sell3_qty REAL, buy3_price REAL, buy3_qty REAL,
              spread_ticks REAL, raw_json TEXT
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bars_1m(
              ts TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL, vwap REAL,
              ma5 REAL, ma25 REAL, ma75 REAL, atr14 REAL
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bars_3m(
              ts TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL, vwap REAL,
              ma5 REAL, ma25 REAL, ma75 REAL, atr14 REAL
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS feature_snapshot(
              ts TEXT PRIMARY KEY,
              price REAL, vwap REAL, spread_ticks REAL, obi_l1 REAL, obi_l3 REAL, vwap_gap_bps REAL,
              ret_30s REAL, ret_1m REAL, ret_3m REAL,
              close_pos_in_bar_1m REAL, close_pos_in_bar_3m REAL,
              ma_trend_score_3m REAL, pullback_quality REAL, reacceleration_score REAL,
              overextension_penalty REAL, volume_1m REAL, volume_3m REAL, trade_intensity_30s REAL,
              regime TEXT
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS prediction_snapshot(
              ts TEXT PRIMARY KEY,
              regime TEXT,
              p_up_1m REAL, p_down_1m REAL, p_up_3m REAL, p_down_3m REAL,
              signal TEXT, reason_1 TEXT, reason_2 TEXT, reason_3 TEXT
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades(
              trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
              entry_ts TEXT, exit_ts TEXT, entry_side TEXT, strategy TEXT,
              entry_price REAL, exit_price REAL,
              pnl_ticks REAL, holding_sec REAL, exit_reason TEXT,
              mfe_ticks REAL, mae_ticks REAL
            )""")
            con.commit()

    def log(self, level: str, event_type: str, message: str) -> None:
        with self._connect() as con:
            con.execute(
                "INSERT INTO system_events VALUES (?,?,?,?)",
                (now_jst().isoformat(), level, event_type, message),
            )
            con.commit()

    def insert_snapshot(self, s: TickSnapshot, spread_ticks: Optional[float]) -> None:
        with self._connect() as con:
            con.execute(
                """INSERT OR REPLACE INTO market_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    s.ts.isoformat(),
                    s.price,
                    s.volume,
                    s.vwap,
                    s.sell1_price,
                    s.sell1_qty,
                    s.buy1_price,
                    s.buy1_qty,
                    s.sell2_price,
                    s.sell2_qty,
                    s.buy2_price,
                    s.buy2_qty,
                    s.sell3_price,
                    s.sell3_qty,
                    s.buy3_price,
                    s.buy3_qty,
                    spread_ticks,
                    s.raw_json,
                ),
            )
            con.commit()

    def insert_bar(self, table: str, b: Bar) -> None:
        with self._connect() as con:
            con.execute(
                f"INSERT OR REPLACE INTO {table} VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    b.ts.isoformat(),
                    b.open,
                    b.high,
                    b.low,
                    b.close,
                    b.volume,
                    b.vwap,
                    b.ma5,
                    b.ma25,
                    b.ma75,
                    b.atr14,
                ),
            )
            con.commit()

    def insert_feature(self, f: FeatureSnapshot) -> None:
        with self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO feature_snapshot VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f.ts.isoformat(),
                    f.price,
                    f.vwap,
                    f.spread_ticks,
                    f.obi_l1,
                    f.obi_l3,
                    f.vwap_gap_bps,
                    f.ret_30s,
                    f.ret_1m,
                    f.ret_3m,
                    f.close_pos_in_bar_1m,
                    f.close_pos_in_bar_3m,
                    f.ma_trend_score_3m,
                    f.pullback_quality,
                    f.reacceleration_score,
                    f.overextension_penalty,
                    f.volume_1m,
                    f.volume_3m,
                    f.trade_intensity_30s,
                    f.regime,
                ),
            )
            con.commit()

    def insert_prediction(self, p: PredictionSnapshot) -> None:
        with self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO prediction_snapshot VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    p.ts.isoformat(),
                    p.regime,
                    p.p_up_1m,
                    p.p_down_1m,
                    p.p_up_3m,
                    p.p_down_3m,
                    p.signal,
                    p.reason_1,
                    p.reason_2,
                    p.reason_3,
                ),
            )
            con.commit()

    def insert_trade(self, **kwargs: Any) -> None:
        with self._connect() as con:
            con.execute(
                """INSERT INTO paper_trades(entry_ts,exit_ts,entry_side,strategy,entry_price,exit_price,pnl_ticks,holding_sec,exit_reason,mfe_ticks,mae_ticks)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    kwargs.get("entry_ts"),
                    kwargs.get("exit_ts"),
                    kwargs.get("entry_side"),
                    kwargs.get("strategy"),
                    kwargs.get("entry_price"),
                    kwargs.get("exit_price"),
                    kwargs.get("pnl_ticks"),
                    kwargs.get("holding_sec"),
                    kwargs.get("exit_reason"),
                    kwargs.get("mfe_ticks"),
                    kwargs.get("mae_ticks"),
                ),
            )
            con.commit()


class RollingBars:
    def __init__(self, minutes: int) -> None:
        self.minutes = minutes
        self.current_bucket: Optional[datetime] = None
        self.rows: list[TickSnapshot] = []
        self.history: deque[Bar] = deque(maxlen=400)

    def _bucket(self, ts: datetime) -> datetime:
        minute = (ts.minute // self.minutes) * self.minutes
        return ts.replace(second=0, microsecond=0, minute=minute)

    def update(self, snap: TickSnapshot) -> Optional[Bar]:
        if snap.price is None:
            return None
        bucket = self._bucket(snap.ts)
        if self.current_bucket is None:
            self.current_bucket = bucket
        if bucket != self.current_bucket:
            bar = self._finalize_bar(self.current_bucket, self.rows)
            self.history.append(bar)
            self.current_bucket = bucket
            self.rows = [snap]
            return self._decorate_bar(bar)
        self.rows.append(snap)
        return None

    def force_finalize(self) -> Optional[Bar]:
        if self.current_bucket and self.rows:
            bar = self._finalize_bar(self.current_bucket, self.rows)
            self.history.append(bar)
            self.rows = []
            return self._decorate_bar(bar)
        return None

    def latest(self) -> Optional[Bar]:
        return self.history[-1] if self.history else None

    def prev(self, n: int = 1) -> Optional[Bar]:
        if len(self.history) >= n + 1:
            return list(self.history)[-1 - n]
        return None

    def _finalize_bar(self, ts: datetime, rows: list[TickSnapshot]) -> Bar:
        prices = [r.price for r in rows if r.price is not None]
        vols = [r.volume for r in rows if r.volume is not None]
        vwaps = [r.vwap for r in rows if r.vwap is not None]
        open_ = prices[0]
        high = max(prices)
        low = min(prices)
        close = prices[-1]
        volume = max((vols[-1] - vols[0]) if len(vols) >= 2 else 0.0, 0.0)
        vwap = vwaps[-1] if vwaps else close
        return Bar(ts=ts, open=open_, high=high, low=low, close=close, volume=volume, vwap=vwap)

    def _decorate_bar(self, bar: Bar) -> Bar:
        closes = [b.close for b in self.history]
        if len(closes) >= 5:
            bar.ma5 = float(pd.Series(closes).rolling(5).mean().iloc[-1])
        if len(closes) >= 25:
            bar.ma25 = float(pd.Series(closes).rolling(25).mean().iloc[-1])
        if len(closes) >= 75:
            bar.ma75 = float(pd.Series(closes).rolling(75).mean().iloc[-1])
        if len(self.history) >= 15:
            df = pd.DataFrame([asdict(b) for b in self.history])
            prev_close = df["close"].shift(1)
            tr = pd.concat(
                [
                    df["high"] - df["low"],
                    (df["high"] - prev_close).abs(),
                    (df["low"] - prev_close).abs(),
                ],
                axis=1,
            ).max(axis=1)
            atr = tr.rolling(14).mean().iloc[-1]
            if pd.notna(atr):
                bar.atr14 = float(atr)
        return bar


def extract_snapshot(raw: dict[str, Any]) -> TickSnapshot:
    def g(path: str) -> Any:
        cur: Any = raw
        for part in path.split("."):
            if cur is None:
                return None
            cur = cur.get(part) if isinstance(cur, dict) else None
        return cur

    def d(v: Any) -> Optional[float]:
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    ts_raw = raw.get("CurrentPriceTime") or now_jst().isoformat()
    try:
        ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")).astimezone(JST)
    except Exception:
        ts = now_jst()

    return TickSnapshot(
        ts=ts,
        price=d(raw.get("CurrentPrice")),
        volume=d(raw.get("TradingVolume")),
        vwap=d(raw.get("VWAP")),
        sell1_price=d(g("Sell1.Price")),
        sell1_qty=d(g("Sell1.Qty")),
        buy1_price=d(g("Buy1.Price")),
        buy1_qty=d(g("Buy1.Qty")),
        sell2_price=d(g("Sell2.Price")),
        sell2_qty=d(g("Sell2.Qty")),
        buy2_price=d(g("Buy2.Price")),
        buy2_qty=d(g("Buy2.Qty")),
        sell3_price=d(g("Sell3.Price")),
        sell3_qty=d(g("Sell3.Qty")),
        buy3_price=d(g("Buy3.Price")),
        buy3_qty=d(g("Buy3.Qty")),
        raw_json=json.dumps(raw, ensure_ascii=False),
    )


def calc_spread_ticks(s: TickSnapshot) -> Optional[float]:
    if s.sell1_price is None or s.buy1_price is None or s.price is None:
        return None
    spread_price = s.sell1_price - s.buy1_price
    if spread_price < 0:
        spread_price = abs(spread_price)
    return spread_price / tick_size_for_1570(s.price)


def calc_obi(s: TickSnapshot) -> tuple[float, float]:
    b1 = s.buy1_qty or 0.0
    a1 = s.sell1_qty or 0.0
    obi_l1 = ((b1 - a1) / (b1 + a1)) if (b1 + a1) > 0 else 0.0
    bs = (s.buy1_qty or 0.0) + (s.buy2_qty or 0.0) + (s.buy3_qty or 0.0)
    ss = (s.sell1_qty or 0.0) + (s.sell2_qty or 0.0) + (s.sell3_qty or 0.0)
    obi_l3 = ((bs - ss) / (bs + ss)) if (bs + ss) > 0 else 0.0
    return obi_l1, obi_l3


def calc_close_pos_in_bar(bar: Optional[Bar]) -> float:
    if not bar:
        return 0.5
    rng = max(bar.high - bar.low, 1e-9)
    return (bar.close - bar.low) / rng


def detect_regime(
    bar1: Optional[Bar],
    bar3: Optional[Bar],
    spread_ticks: Optional[float],
    obi_l1: float,
) -> str:
    if not bar1 or not bar3:
        return "chaos"
    if bar3.ma5 is not None and bar3.ma25 is not None:
        if bar3.ma5 > bar3.ma25 and bar3.close > bar3.vwap:
            return "trend_up"
        if bar3.ma5 < bar3.ma25 and bar3.close < bar3.vwap:
            return "trend_down"
    if spread_ticks is not None and spread_ticks <= 1.0 and abs(obi_l1) < 0.05:
        return "range"
    return "chaos"


def build_features(
    ts: datetime,
    tick_buf: deque[TickSnapshot],
    bar1: Optional[Bar],
    prev1: Optional[Bar],
    bar3: Optional[Bar],
    prev3: Optional[Bar],
) -> Optional[FeatureSnapshot]:
    if not tick_buf or bar1 is None or bar3 is None:
        return None
    s = tick_buf[-1]
    if s.price is None or s.vwap is None:
        return None

    spread_ticks = calc_spread_ticks(s) or 0.0
    obi_l1, obi_l3 = calc_obi(s)
    vwap_gap_bps = ((s.price / s.vwap) - 1.0) * 10000.0 if s.vwap else 0.0

    close_30s_ago = None
    volume_30s_ago = None
    target_ts = s.ts - timedelta(seconds=30)
    for old in reversed(tick_buf):
        if old.ts <= target_ts and old.price is not None:
            close_30s_ago = old.price
            volume_30s_ago = old.volume
            break

    ret_30s = ((s.price / close_30s_ago) - 1.0) if close_30s_ago else 0.0
    ret_1m = ((bar1.close / prev1.close) - 1.0) if prev1 and prev1.close else 0.0
    ret_3m = ((bar3.close / prev3.close) - 1.0) if prev3 and prev3.close else 0.0
    close_pos_in_bar_1m = calc_close_pos_in_bar(bar1)
    close_pos_in_bar_3m = calc_close_pos_in_bar(bar3)

    ma_trend_score_3m = 0.0
    if bar3.ma5 and bar3.ma25:
        ma_trend_score_3m = (bar3.ma5 - bar3.ma25) / max(abs(bar3.ma25), 1e-9)

    pullback_quality = 0.0
    if ret_3m > 0 and ret_1m < 0 and s.price >= s.vwap:
        pullback_quality = min(abs(ret_1m) / 0.001, 1.0)
    elif ret_3m < 0 and ret_1m > 0 and s.price <= s.vwap:
        pullback_quality = min(abs(ret_1m) / 0.001, 1.0)

    reacceleration_score = 0.0
    if ret_30s > 0 and obi_l1 > 0.04:
        reacceleration_score = min(ret_30s / 0.0005, 1.0)
    elif ret_30s < 0 and obi_l1 < -0.04:
        reacceleration_score = -min(abs(ret_30s) / 0.0005, 1.0)

    regime = detect_regime(bar1, bar3, spread_ticks, obi_l1)
    if regime in {"trend_up", "trend_down"}:
        threshold = VWAP_GAP_BPS_MAX_TREND
    elif regime == "range":
        threshold = VWAP_GAP_BPS_MAX_RANGE
    else:
        threshold = VWAP_GAP_BPS_MAX_BASE
    overextension_penalty = max(0.0, abs(vwap_gap_bps) - threshold) / 10.0

    volume_1m = bar1.volume
    volume_3m = bar3.volume
    if volume_30s_ago is not None and s.volume is not None:
        trade_intensity_30s = max((s.volume - volume_30s_ago) / 30.0, 0.0)
    else:
        trade_intensity_30s = 0.0

    return FeatureSnapshot(
        ts=ts,
        price=s.price,
        vwap=s.vwap,
        spread_ticks=spread_ticks,
        obi_l1=obi_l1,
        obi_l3=obi_l3,
        vwap_gap_bps=vwap_gap_bps,
        ret_30s=ret_30s,
        ret_1m=ret_1m,
        ret_3m=ret_3m,
        close_pos_in_bar_1m=close_pos_in_bar_1m,
        close_pos_in_bar_3m=close_pos_in_bar_3m,
        ma_trend_score_3m=ma_trend_score_3m,
        pullback_quality=pullback_quality,
        reacceleration_score=reacceleration_score,
        overextension_penalty=overextension_penalty,
        volume_1m=volume_1m,
        volume_3m=volume_3m,
        trade_intensity_30s=trade_intensity_30s,
        regime=regime,
    )


def can_trade_now(ts: datetime, f: FeatureSnapshot) -> tuple[bool, str]:
    t = ts.strftime("%H:%M:%S")
    if not time_in_windows(t, TRADE_WINDOWS):
        return False, "OUT_OF_TRADE_WINDOW"
    if f.spread_ticks > SPREAD_TICKS_MAX:
        return False, "SPREAD_WIDE"
    if f.price <= 0:
        return False, "NO_PRICE"
    if f.volume_1m <= 0:
        return False, "NO_VOLUME"
    if abs(f.obi_l1) < 0.02 and abs(f.obi_l3) < 0.01:
        return False, "BOOK_NEUTRAL"

    if f.regime in {"trend_up", "trend_down"}:
        if abs(f.vwap_gap_bps) > VWAP_GAP_BPS_MAX_TREND:
            return False, "OVEREXTENDED"
    elif f.regime == "range":
        if abs(f.vwap_gap_bps) > VWAP_GAP_BPS_MAX_RANGE:
            return False, "OVEREXTENDED"
    else:
        if abs(f.vwap_gap_bps) > VWAP_GAP_BPS_MAX_BASE:
            return False, "OVEREXTENDED"
    return True, "OK"


def long_score_1m(f: FeatureSnapshot) -> float:
    score = 0.0
    if f.ret_3m > 0:
        score += 1.3
    if f.ret_1m > 0:
        score += 1.0
    if f.price > f.vwap:
        score += 0.8
    score += 0.9 * max(f.obi_l1, 0.0)
    score += 0.7 * max(f.obi_l3, 0.0)
    score += 0.6 * max(f.close_pos_in_bar_1m - 0.5, 0.0)
    score -= 0.5 * f.overextension_penalty
    return score


def short_score_1m(f: FeatureSnapshot) -> float:
    score = 0.0
    if f.ret_3m < 0:
        score += 1.3
    if f.ret_1m < 0:
        score += 1.0
    if f.price < f.vwap:
        score += 0.8
    score += 0.9 * max(-f.obi_l1, 0.0)
    score += 0.7 * max(-f.obi_l3, 0.0)
    score += 0.6 * max(0.5 - f.close_pos_in_bar_1m, 0.0)
    score -= 0.5 * f.overextension_penalty
    return score


def long_score_3m(f: FeatureSnapshot) -> float:
    score = 0.0
    if f.ret_3m > 0:
        score += 1.4
    if f.ma_trend_score_3m > 0:
        score += 1.1
    score += 0.8 * max(f.pullback_quality, 0.0)
    score += 0.8 * max(f.reacceleration_score, 0.0)
    score += 0.5 * max(f.obi_l1, 0.0)
    score -= 0.4 * f.overextension_penalty
    return score


def short_score_3m(f: FeatureSnapshot) -> float:
    score = 0.0
    if f.ret_3m < 0:
        score += 1.4
    if f.ma_trend_score_3m < 0:
        score += 1.1
    score += 0.8 * max(f.pullback_quality, 0.0)
    score += 0.8 * max(-f.reacceleration_score, 0.0)
    score += 0.5 * max(-f.obi_l1, 0.0)
    score -= 0.4 * f.overextension_penalty
    return score


def build_prediction(f: FeatureSnapshot) -> PredictionSnapshot:
    p_up_1m = sigmoid(long_score_1m(f) - short_score_1m(f))
    p_down_1m = 1.0 - p_up_1m
    p_up_3m = sigmoid(long_score_3m(f) - short_score_3m(f))
    p_down_3m = 1.0 - p_up_3m

    gate_ok, gate_reason = can_trade_now(f.ts, f)
    if not gate_ok:
        signal = "NO_ACTION"
        reasons = [gate_reason, f.regime, f"vwap_gap={f.vwap_gap_bps:.1f}bps"]
    else:
        long_strong = p_up_1m >= PROB_UPPER_1M and p_up_3m >= PROB_UPPER_3M
        short_strong = p_down_1m >= PROB_UPPER_1M and p_down_3m >= PROB_UPPER_3M
        if long_strong and not short_strong:
            signal = "LONG_CANDIDATE"
            reasons = ["ALIGN_UP", f"p1={p_up_1m:.2f}", f"p3={p_up_3m:.2f}"]
        elif short_strong and not long_strong:
            signal = "SHORT_CANDIDATE"
            reasons = ["ALIGN_DOWN", f"p1={p_down_1m:.2f}", f"p3={p_down_3m:.2f}"]
        else:
            signal = "NO_ACTION"
            reasons = ["LOW_CONFIDENCE", f"p1u={p_up_1m:.2f}", f"p3u={p_up_3m:.2f}"]

    return PredictionSnapshot(
        ts=f.ts,
        regime=f.regime,
        p_up_1m=p_up_1m,
        p_down_1m=p_down_1m,
        p_up_3m=p_up_3m,
        p_down_3m=p_down_3m,
        signal=signal,
        reason_1=reasons[0],
        reason_2=reasons[1],
        reason_3=reasons[2],
    )


def can_enter(side: str, now_: datetime, state: MonitorStatus) -> tuple[bool, str]:
    if state.open_position is not None:
        return False, "ALREADY_OPEN"
    block_until = state.reentry_block_until_by_side.get(side)
    if block_until and now_ < block_until:
        return False, "REENTRY_BLOCK"
    last_entry = state.last_entry_ts_by_side.get(side)
    if last_entry and (now_ - last_entry).total_seconds() < ENTRY_COOLDOWN_SEC:
        return False, "ENTRY_COOLDOWN"
    return True, "OK"


def create_position(
    pred: PredictionSnapshot,
    f: FeatureSnapshot,
    entry_order_id: Optional[str] = None,
) -> PositionState:
    is_long = pred.signal == "LONG_CANDIDATE"
    if (pred.p_up_3m if is_long else pred.p_down_3m) >= (
        pred.p_up_1m if is_long else pred.p_down_1m
    ):
        strategy = "STRAT_3M"
        min_hold = MIN_HOLD_SEC_3M
        max_hold = MAX_HOLD_SEC_3M
        stop_ticks = STOP_TICKS_3M
        take_ticks = TAKE_TICKS_3M
    else:
        strategy = "STRAT_1M"
        min_hold = MIN_HOLD_SEC_1M
        max_hold = MAX_HOLD_SEC_1M
        stop_ticks = STOP_TICKS_1M
        take_ticks = TAKE_TICKS_1M

    return PositionState(
        side="LONG" if is_long else "SHORT",
        strategy=strategy,
        entry_ts=f.ts,
        entry_price=f.price,
        entry_p_up_1m=pred.p_up_1m,
        entry_p_up_3m=pred.p_up_3m,
        stop_ticks=stop_ticks,
        take_ticks=take_ticks,
        min_hold_sec=min_hold,
        max_hold_sec=max_hold,
        entry_order_id=entry_order_id,
    )


def should_exit(pos: PositionState, f: FeatureSnapshot, pred: PredictionSnapshot) -> tuple[bool, str, float]:
    elapsed = (f.ts - pos.entry_ts).total_seconds()
    pnl_ticks = price_to_ticks(f.price - pos.entry_price, pos.entry_price)
    if pos.side == "SHORT":
        pnl_ticks = -pnl_ticks

    if pnl_ticks <= -pos.stop_ticks:
        return True, "STOP_LOSS", pnl_ticks
    if pnl_ticks >= pos.take_ticks:
        return True, "TAKE_PROFIT", pnl_ticks

    if elapsed < pos.min_hold_sec:
        if (
            pos.side == "LONG"
            and pnl_ticks <= -2
            and f.price < f.vwap
            and f.obi_l1 < -0.18
            and pred.p_down_1m > 0.67
        ):
            return True, "EDGE_BREAK_HARD", pnl_ticks
        if (
            pos.side == "SHORT"
            and pnl_ticks <= -2
            and f.price > f.vwap
            and f.obi_l1 > 0.18
            and pred.p_up_1m > 0.67
        ):
            return True, "EDGE_BREAK_HARD", pnl_ticks
        return False, "MIN_HOLD", pnl_ticks

    if pos.side == "LONG":
        if (
            pnl_ticks >= 1
            and pred.p_up_1m < PROB_EXIT_EDGE - 0.02
            and f.price < f.vwap
            and f.obi_l1 < -0.03
        ):
            return True, "EDGE_DECAY", pnl_ticks
    else:
        if (
            pnl_ticks >= 1
            and pred.p_down_1m < PROB_EXIT_EDGE - 0.02
            and f.price > f.vwap
            and f.obi_l1 > 0.03
        ):
            return True, "EDGE_DECAY", pnl_ticks

    if elapsed >= pos.max_hold_sec:
        return True, "TIME_STOP", pnl_ticks

    return False, "HOLD", pnl_ticks


def write_latest_status(
    outdir: str,
    status: MonitorStatus,
    latest_feature: Optional[FeatureSnapshot],
    latest_pred: Optional[PredictionSnapshot],
    storage: Storage,
) -> None:
    _ = storage
    path = os.path.join(outdir, "latest_status.md")
    lines = [
        "# 1570 latest status",
        "",
        f"- 時刻: {now_jst().isoformat()}",
        f"- count: {status.count}",
    ]
    if latest_feature:
        lines += [
            f"- 価格: {latest_feature.price}",
            f"- VWAP: {latest_feature.vwap}",
            f"- spread_ticks: {latest_feature.spread_ticks:.2f}",
        ]
    if latest_pred:
        lines += [
            f"- p_up_1m: {latest_pred.p_up_1m:.3f}",
            f"- p_up_3m: {latest_pred.p_up_3m:.3f}",
            f"- signal: {latest_pred.signal}",
        ]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def generate_report(db_path: str, report_path: str, midday: bool = False) -> None:
    con = sqlite3.connect(db_path)
    try:
        pred = pd.read_sql_query("SELECT * FROM prediction_snapshot ORDER BY ts", con)
        trades = pd.read_sql_query("SELECT * FROM paper_trades ORDER BY trade_id", con)
        events = pd.read_sql_query("SELECT * FROM system_events ORDER BY ts", con)
        feats = pd.read_sql_query("SELECT * FROM feature_snapshot ORDER BY ts", con)
    finally:
        con.close()

    date_label = os.path.basename(db_path).split("monitor_1570_")[-1].split(".db")[0]
    if len(date_label) == 8:
        date_fmt = f"{date_label[:4]}-{date_label[4:6]}-{date_label[6:8]}"
    else:
        date_fmt = jst_date_str()

    lines = [
        f"# 1570 自動監視 {'前場レポート' if midday else '日次レポート'}",
        "",
        f"対象日: {date_fmt}",
        "",
    ]
    lines.append("## 1. 総括")
    lines.append(f"- スナップショット数: {len(feats)}")
    lines.append(f"- 予測回数: {len(pred)}")
    if not pred.empty:
        vc = pred["signal"].value_counts()
        lines.append(f"- LONG候補: {int(vc.get('LONG_CANDIDATE', 0))}")
        lines.append(f"- SHORT候補: {int(vc.get('SHORT_CANDIDATE', 0))}")
        lines.append(f"- NO_ACTION: {int(vc.get('NO_ACTION', 0))}")

    lines.append("")
    lines.append("## 2. 仮想売買")
    if trades.empty:
        lines.append("- 完了トレード数: 0")
        lines.append("- 勝率: 0.0%")
        lines.append("- 平均損益(ティック): 0.00")
        lines.append("- 総損益(ティック): 0.00")
        lines.append("- 平均保有秒数: 0.0")
    else:
        wins = (trades["pnl_ticks"] > 0).sum()
        lines.append(f"- 完了トレード数: {len(trades)}")
        lines.append(f"- 勝率: {100.0 * wins / len(trades):.1f}%")
        lines.append(f"- 平均損益(ティック): {trades['pnl_ticks'].mean():.2f}")
        lines.append(f"- 総損益(ティック): {trades['pnl_ticks'].sum():.2f}")
        lines.append(f"- 平均保有秒数: {trades['holding_sec'].mean():.1f}")
        lines.append("")
        lines.append("## 3. 戦略別件数")
        for strat, n in trades["strategy"].value_counts().items():
            lines.append(f"- {strat}: {int(n)}")
        lines.append("")
        lines.append("## 4. 出口理由")
        for reason, n in trades["exit_reason"].value_counts().items():
            lines.append(f"- {reason}: {int(n)}")

    err_count = 0 if events.empty else int(events["level"].isin(["WARN", "ERROR"]).sum())
    lines.append("")
    lines.append(f"## {'5' if not trades.empty else '3'}. システム")
    lines.append(f"- WARN/ERROR件数: {err_count}")
    lines.append("")
    lines.append("### 直近イベント")
    if events.empty:
        lines.append("- なし")
    else:
        for _, r in events.tail(8).iterrows():
            lines.append(f"- {r['ts']} [{r['level']}] {r['event_type']}: {r['message']}")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def get_open_position_qty(client: KabuApiClient, config: dict[str, Any], side: str) -> int:
    positions = client.get_positions(config["symbol"], int(config["exchange"]))
    side_code = "2" if side == "LONG" else "1"
    total = 0
    for p in positions:
        if str(p.get("Side")) != side_code:
            continue
        total += max(_to_int(p.get("LeavesQty"), 0), 0)
    return total


def wait_for_position_qty(
    client: KabuApiClient,
    config: dict[str, Any],
    side: str,
    target_qty: int,
    timeout_sec: int,
    comparator: str = "ge",
) -> bool:
    start = time.time()
    while time.time() - start <= timeout_sec:
        qty = get_open_position_qty(client, config, side)
        if comparator == "ge" and qty >= target_qty:
            return True
        if comparator == "eq" and qty == target_qty:
            return True
        time.sleep(0.5)
    return False


def build_entry_order_payload(config: dict[str, Any], side: str) -> dict[str, Any]:
    order_password = config["order_password"]
    qty = int(config["order_qty"])
    side_code = "2" if side == "LONG" else "1"
    payload: dict[str, Any] = {
        "Password": order_password,
        "Symbol": config["symbol"],
        "Exchange": int(config["exchange"]),
        "SecurityType": 1,
        "Side": side_code,
        "CashMargin": int(config["entry_cash_margin"]),
        "MarginTradeType": int(config["margin_trade_type"]),
        "DelivType": int(config["entry_deliv_type"]),
        "AccountType": int(config["account_type"]),
        "Qty": qty,
        "FrontOrderType": int(config["entry_front_order_type"]),
        "Price": float(config.get("entry_price", 0)),
        "ExpireDay": int(config.get("expire_day", 0)),
    }
    overrides = config.get("live_entry_overrides_long" if side == "LONG" else "live_entry_overrides_short", {})
    if isinstance(overrides, dict):
        payload.update(overrides)
    return payload


def build_exit_order_payload(
    config: dict[str, Any],
    side: str,
    close_positions: list[dict[str, Any]],
    qty: int,
) -> dict[str, Any]:
    order_password = config["order_password"]
    exit_side_code = "1" if side == "LONG" else "2"
    payload: dict[str, Any] = {
        "Password": order_password,
        "Symbol": config["symbol"],
        "Exchange": int(config["exchange"]),
        "SecurityType": 1,
        "Side": exit_side_code,
        "CashMargin": int(config["exit_cash_margin"]),
        "MarginTradeType": int(config["margin_trade_type"]),
        "DelivType": int(config["exit_deliv_type"]),
        "AccountType": int(config["account_type"]),
        "Qty": qty,
        "ClosePositions": close_positions,
        "FrontOrderType": int(config["exit_front_order_type"]),
        "Price": float(config.get("exit_price", 0)),
        "ExpireDay": int(config.get("expire_day", 0)),
    }
    overrides = config.get("live_exit_overrides", {})
    if isinstance(overrides, dict):
        payload.update(overrides)
    return payload


def execute_live_entry(client: KabuApiClient, config: dict[str, Any], side: str) -> tuple[bool, str]:
    retries = max(int(config.get("live_retry_max", LIVE_RETRY_MAX)), 0)
    target_qty = int(config.get("entry_min_fill_qty", config["order_qty"]))
    timeout_sec = int(config.get("live_entry_timeout_sec", LIVE_ENTRY_TIMEOUT_SEC))

    last_error = "ENTRY_UNKNOWN_ERROR"
    for _ in range(retries + 1):
        payload = build_entry_order_payload(config, side)
        res = client.send_order(payload)
        order_id = str(res.get("OrderId") or res.get("OrderID") or "")
        if not order_id:
            last_error = f"ENTRY_ORDER_ID_MISSING: {res}"
            continue
        if wait_for_position_qty(client, config, side, target_qty=max(target_qty, 1), timeout_sec=timeout_sec, comparator="ge"):
            return True, order_id
        last_error = f"ENTRY_NOT_FILLED_TIMEOUT order_id={order_id}"
        try:
            client.cancel_order(order_id, config["order_password"])
        except Exception:
            pass
    return False, last_error


def execute_live_exit(client: KabuApiClient, config: dict[str, Any], side: str) -> tuple[bool, str]:
    retries = max(int(config.get("live_retry_max", LIVE_RETRY_MAX)), 0)
    timeout_sec = int(config.get("live_exit_timeout_sec", LIVE_EXIT_TIMEOUT_SEC))
    target_side_code = "2" if side == "LONG" else "1"
    last_error = "EXIT_UNKNOWN_ERROR"

    for _ in range(retries + 1):
        positions = client.get_positions(config["symbol"], int(config["exchange"]))
        if not positions:
            return False, "NO_POSITIONS_FOR_EXIT"

        close_positions: list[dict[str, Any]] = []
        total_qty = 0
        for p in positions:
            if str(p.get("Side")) != target_side_code:
                continue
            leaves = _to_int(p.get("LeavesQty"), 0)
            if leaves <= 0:
                continue
            hold_id = p.get("ExecutionID")
            if not hold_id:
                continue
            take = leaves
            close_positions.append({"HoldID": hold_id, "Qty": take})
            total_qty += take

        if total_qty <= 0 or not close_positions:
            return False, "NO_MATCHING_OPEN_POSITION"

        payload = build_exit_order_payload(config, side, close_positions=close_positions, qty=total_qty)
        res = client.send_order(payload)
        order_id = str(res.get("OrderId") or res.get("OrderID") or "")
        if not order_id:
            last_error = f"EXIT_ORDER_ID_MISSING: {res}"
            continue
        if wait_for_position_qty(client, config, side, target_qty=0, timeout_sec=timeout_sec, comparator="eq"):
            return True, order_id
        last_error = f"EXIT_NOT_FILLED_TIMEOUT order_id={order_id}"
        try:
            client.cancel_order(order_id, config["order_password"])
        except Exception:
            pass
    return False, last_error


def run_monitor(config: dict[str, Any]) -> tuple[str, str]:
    outdir = config["outdir"]
    ensure_dir(outdir)
    db_path = os.path.join(outdir, f"monitor_1570_{jst_date_compact()}.db")
    daily_report_path = os.path.join(outdir, f"daily_report_{jst_date_str()}.md")
    midday_report_path = os.path.join(outdir, f"midday_report_{jst_date_str()}.md")
    storage = Storage(db_path)
    storage.log("INFO", "START", "monitor start")

    client = KabuApiClient(config["base_url"])
    token_ok = False
    for _ in range(3):
        try:
            client.get_token(config["api_password"])
            storage.log("INFO", "TOKEN_OK", "token acquired")
            client.register_symbol(config["symbol"], config["exchange"])
            storage.log("INFO", "REGISTER_OK", "register ok")
            token_ok = True
            break
        except Exception as e:
            storage.log("ERROR", "TOKEN_FAIL", str(e))
            time.sleep(1)
    if not token_ok:
        generate_report(db_path, daily_report_path)
        return db_path, daily_report_path

    tick_buf: deque[TickSnapshot] = deque(maxlen=2000)
    rb1 = RollingBars(1)
    rb3 = RollingBars(3)
    status = MonitorStatus()
    start_ts = now_jst()
    next_status_write = start_ts
    runtime_minutes = config.get("runtime_minutes")
    end_ts = start_ts + timedelta(minutes=runtime_minutes) if runtime_minutes else None

    last_feature: Optional[FeatureSnapshot] = None
    last_pred: Optional[PredictionSnapshot] = None
    mfe_ticks = 0.0
    mae_ticks = 0.0

    while True:
        now_ = now_jst()
        tstr = now_.strftime("%H:%M:%S")
        if end_ts and now_ >= end_ts:
            storage.log("INFO", "STOP_RUNTIME", "runtime end reached")
            break
        if tstr >= STOP_AFTER:
            storage.log("INFO", "STOP_AFTER_SESSION", "session end reached")
            break
        try:
            raw = client.get_board(config["symbol"], config["exchange"])
            snap = extract_snapshot(raw)
            tick_buf.append(snap)
            spread_ticks = calc_spread_ticks(snap)
            storage.insert_snapshot(snap, spread_ticks)
            status.count += 1

            bar1_new = rb1.update(snap)
            if bar1_new:
                storage.insert_bar("bars_1m", bar1_new)
            bar3_new = rb3.update(snap)
            if bar3_new:
                storage.insert_bar("bars_3m", bar3_new)

            f = build_features(snap.ts, tick_buf, rb1.latest(), rb1.prev(1), rb3.latest(), rb3.prev(1))
            if f is not None:
                storage.insert_feature(f)
                p = build_prediction(f)
                storage.insert_prediction(p)
                last_feature = f
                last_pred = p

                if status.open_position is None and p.signal in {"LONG_CANDIDATE", "SHORT_CANDIDATE"}:
                    side = "LONG" if p.signal == "LONG_CANDIDATE" else "SHORT"
                    enter_ok, _ = can_enter(side, f.ts, status)
                    if enter_ok:
                        if config["live_mode"]:
                            ok, info = execute_live_entry(client, config, side)
                            if not ok:
                                storage.log("ERROR", "LIVE_ENTRY_FAIL", info)
                            else:
                                storage.log("INFO", "LIVE_ENTRY_OK", f"{side} order_id={info}")
                                status.open_position = create_position(p, f, entry_order_id=info)
                                status.last_entry_ts_by_side[side] = f.ts
                                mfe_ticks = 0.0
                                mae_ticks = 0.0
                        else:
                            status.open_position = create_position(p, f)
                            status.last_entry_ts_by_side[side] = f.ts
                            mfe_ticks = 0.0
                            mae_ticks = 0.0
                elif status.open_position is not None:
                    pos = status.open_position
                    cur_pnl_ticks = price_to_ticks(f.price - pos.entry_price, pos.entry_price)
                    if pos.side == "SHORT":
                        cur_pnl_ticks = -cur_pnl_ticks
                    mfe_ticks = max(mfe_ticks, cur_pnl_ticks)
                    mae_ticks = min(mae_ticks, cur_pnl_ticks)
                    ex, ex_reason, pnl_ticks = should_exit(pos, f, p)
                    if ex:
                        if config["live_mode"]:
                            ok, info = execute_live_exit(client, config, pos.side)
                            if not ok:
                                storage.log("ERROR", "LIVE_EXIT_FAIL", info)
                                continue
                            pos.exit_order_id = info
                            storage.log("INFO", "LIVE_EXIT_OK", f"{pos.side} order_id={info}")

                        holding_sec = (f.ts - pos.entry_ts).total_seconds()
                        storage.insert_trade(
                            entry_ts=pos.entry_ts.isoformat(),
                            exit_ts=f.ts.isoformat(),
                            entry_side=pos.side,
                            strategy=pos.strategy,
                            entry_price=pos.entry_price,
                            exit_price=f.price,
                            pnl_ticks=pnl_ticks,
                            holding_sec=holding_sec,
                            exit_reason=ex_reason,
                            mfe_ticks=mfe_ticks,
                            mae_ticks=mae_ticks,
                        )
                        if ex_reason in {"STOP_LOSS", "EDGE_BREAK_HARD"}:
                            status.reentry_block_until_by_side[pos.side] = f.ts + timedelta(
                                seconds=REENTRY_AFTER_STOP_SEC
                            )
                        else:
                            status.reentry_block_until_by_side[pos.side] = f.ts + timedelta(
                                seconds=ENTRY_COOLDOWN_SEC
                            )
                        status.open_position = None

            if not status.midday_written and tstr >= "11:30:00":
                generate_report(db_path, midday_report_path, midday=True)
                storage.log(
                    "INFO",
                    "MIDDAY_REPORT",
                    f"midday report written: {os.path.basename(midday_report_path)}",
                )
                status.midday_written = True

            if now_ >= next_status_write:
                write_latest_status(outdir, status, last_feature, last_pred, storage)
                next_status_write = now_ + timedelta(minutes=10)

            if last_feature and last_pred and status.count % 4 == 1:
                print(
                    f"[{now_.strftime('%H:%M:%S')}] count={status.count} "
                    f"price={last_feature.price} p_up_1m={last_pred.p_up_1m*100:.1f}% "
                    f"p_up_3m={last_pred.p_up_3m*100:.1f}% signal={last_pred.signal}"
                )

            time.sleep(config["poll_interval_sec"])
        except KeyboardInterrupt:
            storage.log("INFO", "STOP", "keyboard interrupt")
            break
        except Exception as e:
            print(f"[ERROR] {e}")
            storage.log("ERROR", "LOOP_ERROR", str(e))
            if "401" in str(e) or "Unauthorized" in str(e):
                try:
                    client.get_token(config["api_password"])
                    storage.log("INFO", "TOKEN_REFRESH", "token refreshed")
                    client.register_symbol(config["symbol"], config["exchange"])
                except Exception as e2:
                    storage.log("ERROR", "TOKEN_REFRESH_FAIL", str(e2))
            time.sleep(3)

    b1 = rb1.force_finalize()
    if b1:
        storage.insert_bar("bars_1m", b1)
    b3 = rb3.force_finalize()
    if b3:
        storage.insert_bar("bars_3m", b3)
    storage.log("INFO", "STOP", "monitor stop requested")
    generate_report(db_path, daily_report_path, midday=False)
    return db_path, daily_report_path


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    config = {
        "api_password": args.api_password or cfg.get("api_password") or API_PASSWORD_HARDCODED,
        "live_mode": bool(args.live_mode or cfg.get("live_mode", False)),
        "order_password": args.order_password or cfg.get("order_password") or args.api_password or cfg.get("api_password") or API_PASSWORD_HARDCODED,
        "order_qty": int(args.order_qty if args.order_qty is not None else cfg.get("order_qty", 1)),
        "entry_min_fill_qty": int(cfg.get("entry_min_fill_qty", cfg.get("order_qty", 1))),
        "account_type": int(args.account_type if args.account_type is not None else cfg.get("account_type", 4)),
        "margin_trade_type": int(args.margin_trade_type if args.margin_trade_type is not None else cfg.get("margin_trade_type", 3)),
        "entry_cash_margin": int(args.entry_cash_margin if args.entry_cash_margin is not None else cfg.get("entry_cash_margin", 2)),
        "exit_cash_margin": int(args.exit_cash_margin if args.exit_cash_margin is not None else cfg.get("exit_cash_margin", 3)),
        "entry_deliv_type": int(args.entry_deliv_type if args.entry_deliv_type is not None else cfg.get("entry_deliv_type", 0)),
        "exit_deliv_type": int(args.exit_deliv_type if args.exit_deliv_type is not None else cfg.get("exit_deliv_type", 2)),
        "entry_front_order_type": int(cfg.get("entry_front_order_type", 10)),
        "exit_front_order_type": int(cfg.get("exit_front_order_type", 10)),
        "entry_price": float(cfg.get("entry_price", 0)),
        "exit_price": float(cfg.get("exit_price", 0)),
        "expire_day": int(cfg.get("expire_day", 0)),
        "live_entry_timeout_sec": int(args.live_entry_timeout_sec if args.live_entry_timeout_sec is not None else cfg.get("live_entry_timeout_sec", LIVE_ENTRY_TIMEOUT_SEC)),
        "live_exit_timeout_sec": int(args.live_exit_timeout_sec if args.live_exit_timeout_sec is not None else cfg.get("live_exit_timeout_sec", LIVE_EXIT_TIMEOUT_SEC)),
        "live_retry_max": int(args.live_retry_max if args.live_retry_max is not None else cfg.get("live_retry_max", LIVE_RETRY_MAX)),
        "outdir": args.outdir or cfg.get("outdir", "monitor_output"),
        "runtime_minutes": args.runtime_minutes if args.runtime_minutes is not None else cfg.get("runtime_minutes"),
        "base_url": args.base_url or cfg.get("base_url", API_BASE_DEFAULT),
        "symbol": cfg.get("symbol", SYMBOL_DEFAULT),
        "exchange": int(cfg.get("exchange", EXCHANGE_DEFAULT)),
        "poll_interval_sec": float(cfg.get("poll_interval_sec", POLL_INTERVAL_SEC)),
        "live_entry_overrides_long": cfg.get("live_entry_overrides_long", {}),
        "live_entry_overrides_short": cfg.get("live_entry_overrides_short", {}),
        "live_exit_overrides": cfg.get("live_exit_overrides", {}),
    }
    if not config["api_password"]:
        raise SystemExit(
            "API password is required. Set API_PASSWORD_HARDCODED at the top, "
            "use --api-password, or config file."
        )
    if config["live_mode"] and not config["order_password"]:
        raise SystemExit("order_password is required in live_mode.")
    if config["live_mode"] and int(config["order_qty"]) <= 0:
        raise SystemExit("order_qty must be > 0 in live_mode.")
    if config["live_mode"] and int(config["entry_min_fill_qty"]) <= 0:
        raise SystemExit("entry_min_fill_qty must be > 0 in live_mode.")
    if config["live_mode"] and int(config["entry_min_fill_qty"]) > int(config["order_qty"]):
        raise SystemExit("entry_min_fill_qty must be <= order_qty in live_mode.")
    if config["live_mode"] and int(config["live_entry_timeout_sec"]) <= 0:
        raise SystemExit("live_entry_timeout_sec must be > 0 in live_mode.")
    if config["live_mode"] and int(config["live_exit_timeout_sec"]) <= 0:
        raise SystemExit("live_exit_timeout_sec must be > 0 in live_mode.")
    if config["live_mode"] and int(config["entry_front_order_type"]) <= 0:
        raise SystemExit("entry_front_order_type must be > 0 in live_mode.")
    if config["live_mode"] and int(config["exit_front_order_type"]) <= 0:
        raise SystemExit("exit_front_order_type must be > 0 in live_mode.")
    ensure_dir(config["outdir"])
    print(
        f"Starting monitor: symbol={config['symbol']} exchange={config['exchange']} "
        f"outdir={config['outdir']} live_mode={config['live_mode']} order_qty={config['order_qty']}"
    )
    db_path, report_path = run_monitor(config)
    print(f"DB saved: {os.path.relpath(db_path)}")
    print(f"Report saved: {os.path.relpath(report_path)}")


if __name__ == "__main__":
    main()
