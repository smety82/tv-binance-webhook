# app.py
import csv
import hmac
import hashlib
import html
import json
import math
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse


# ============================================================
# ENV / CONFIG
# ============================================================

def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


API_KEY = require_env("BYBIT_KEY")
API_SECRET = require_env("BYBIT_SECRET")
BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")
SHARED_SECRET = os.getenv("SHARED_SECRET", "CHANGE_ME")
RECV_WINDOW = os.getenv("RECV_WINDOW", "5000")

ENABLE_REAL_ORDERS = os.getenv("ENABLE_REAL_ORDERS", "false").lower() == "true"

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "trade_events")

ORDER_MIN_STOP_DISTANCE_PCT = float(os.getenv("ORDER_MIN_STOP_DISTANCE_PCT", "0.15"))
ORDER_MAX_STOP_DISTANCE_PCT = float(os.getenv("ORDER_MAX_STOP_DISTANCE_PCT", "8.0"))
ORDER_MIN_TP1_RR = float(os.getenv("ORDER_MIN_TP1_RR", "0.8"))
ORDER_MIN_TP2_RR = float(os.getenv("ORDER_MIN_TP2_RR", "1.2"))
ORDER_MAX_SIGNAL_PRICE_DEVIATION_PCT = float(os.getenv("ORDER_MAX_SIGNAL_PRICE_DEVIATION_PCT", "1.0"))
ORDER_SIGNAL_COOLDOWN_MINUTES = int(os.getenv("ORDER_SIGNAL_COOLDOWN_MINUTES", "30"))
ORDER_ALERT_IDEMPOTENCY_LOOKBACK_HOURS = int(os.getenv("ORDER_ALERT_IDEMPOTENCY_LOOKBACK_HOURS", "48"))

# Exposure guard.
# 0 = disabled. Set these in Render Environment Variables to activate hard limits.
MAX_TOTAL_POSITION_VALUE_USDT = float(os.getenv("MAX_TOTAL_POSITION_VALUE_USDT", "0"))
MAX_SYMBOL_POSITION_VALUE_USDT = float(os.getenv("MAX_SYMBOL_POSITION_VALUE_USDT", "0"))
MAX_EQUITY_USAGE_PCT = float(os.getenv("MAX_EQUITY_USAGE_PCT", "0"))
MAX_LEVERAGE_EXPOSURE_PCT = float(os.getenv("MAX_LEVERAGE_EXPOSURE_PCT", "0"))

# Post-order protection verification.
# Verifies after a real order that the position has SL and at least one reduce-only TP order.
POST_ORDER_VERIFY_ENABLED = os.getenv("POST_ORDER_VERIFY_ENABLED", "true").lower() == "true"
POST_ORDER_VERIFY_RETRIES = int(os.getenv("POST_ORDER_VERIFY_RETRIES", "5"))
POST_ORDER_VERIFY_SLEEP_SEC = float(os.getenv("POST_ORDER_VERIFY_SLEEP_SEC", "0.5"))
AUTO_CLOSE_ON_PROTECTION_MISSING = os.getenv("AUTO_CLOSE_ON_PROTECTION_MISSING", "false").lower() == "true"


# Strategy admin / trade limit / auto-downgrade / notification / dashboard v2.
STRATEGY_ADMIN_ENABLED = os.getenv("STRATEGY_ADMIN_ENABLED", "true").lower() == "true"
MAX_DAILY_TRADES_GLOBAL = int(os.getenv("MAX_DAILY_TRADES_GLOBAL", "0"))
MAX_DAILY_TRADES_PER_SYMBOL = int(os.getenv("MAX_DAILY_TRADES_PER_SYMBOL", "0"))
MAX_DAILY_LOSSES_PER_SYMBOL = int(os.getenv("MAX_DAILY_LOSSES_PER_SYMBOL", "0"))
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "0"))

AUTO_DOWNGRADE_ENABLED = os.getenv("AUTO_DOWNGRADE_ENABLED", "true").lower() == "true"
AUTO_DOWNGRADE_TARGET_MODE = os.getenv("AUTO_DOWNGRADE_TARGET_MODE", "PAPER").upper()
AUTO_DOWNGRADE_ON_ORDER_FAILED = os.getenv("AUTO_DOWNGRADE_ON_ORDER_FAILED", "true").lower() == "true"
AUTO_DOWNGRADE_ON_PROTECTION_FAILED = os.getenv("AUTO_DOWNGRADE_ON_PROTECTION_FAILED", "true").lower() == "true"
AUTO_DOWNGRADE_ON_DAILY_LIMIT = os.getenv("AUTO_DOWNGRADE_ON_DAILY_LIMIT", "false").lower() == "true"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "false").lower() == "true"
NOTIFY_ORDER_SENT = os.getenv("NOTIFY_ORDER_SENT", "true").lower() == "true"
NOTIFY_ORDER_FAILED = os.getenv("NOTIFY_ORDER_FAILED", "true").lower() == "true"
NOTIFY_REJECTIONS = os.getenv("NOTIFY_REJECTIONS", "false").lower() == "true"
NOTIFY_PROTECTION_FAILED = os.getenv("NOTIFY_PROTECTION_FAILED", "true").lower() == "true"
NOTIFY_AUTO_DOWNGRADE = os.getenv("NOTIFY_AUTO_DOWNGRADE", "true").lower() == "true"
NOTIFY_TRADING_PAUSE = os.getenv("NOTIFY_TRADING_PAUSE", "true").lower() == "true"
NOTIFY_EMERGENCY_ACTIONS = os.getenv("NOTIFY_EMERGENCY_ACTIONS", "true").lower() == "true"
NOTIFY_DAILY_REPORT = os.getenv("NOTIFY_DAILY_REPORT", "true").lower() == "true"
NOTIFY_RUNTIME_BLOCKED = os.getenv("NOTIFY_RUNTIME_BLOCKED", "false").lower() == "true"

HTTP_TIMEOUT = 15.0

APP_DIR = Path(__file__).resolve().parent
STATE_FILE = APP_DIR / "strategy_state.json"
TRADE_LOG_FILE = APP_DIR / "trade_log.csv"
RUNTIME_STATE_FILE = APP_DIR / "runtime_state.json"

app = FastAPI(title="TradingView Bybit Risk Engine", version="3.5.0")
client = httpx.Client(timeout=HTTP_TIMEOUT)


# ============================================================
# SIMPLE IN-MEMORY GUARD
# ============================================================

_guard = {
    "enabled": False,
    "limit_pct": None,
    "limit_usd": None,
    "baseline": None,
    "equity_now": None,
    "drawdown_usd": 0.0,
    "drawdown_pct": 0.0,
    "block": False,
    "start_date": None,
}


# ============================================================
# BASIC HELPERS
# ============================================================

def now_ms() -> str:
    return str(int(time.time() * 1000))


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def iso_utc_seconds_ago(seconds: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(time.time()) - seconds))


def hmac_sha256(key: str, value: str) -> str:
    return hmac.new(key.encode(), value.encode(), hashlib.sha256).hexdigest()


def sign_v5(ts: str, api_key: str, recv_window: str, payload: str) -> str:
    return hmac_sha256(API_SECRET, ts + api_key + recv_window + payload)


def round_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step


def fmt_qty(qty: float) -> str:
    text = f"{qty:.8f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def fmt_price(price: float, tick: float) -> str:
    rounded = round_step(price, tick)
    text = f"{rounded:.8f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def ok(data: Dict[str, Any]) -> JSONResponse:
    return JSONResponse({"ok": True, **data})


def log(msg: str) -> None:
    print(msg, flush=True)


def normalize_symbol(symbol: str) -> str:
    s = str(symbol).upper().strip()
    s = s.replace(".P", "")
    return s


def normalize_side(side: str) -> str:
    s = str(side).upper().strip()
    if s not in {"LONG", "SHORT"}:
        raise HTTPException(400, f"Invalid side: {side}")
    return s


def bybit_order_side(side: str) -> str:
    return "Buy" if side == "LONG" else "Sell"


def opposite_bybit_side(bybit_side: str) -> str:
    return "Sell" if bybit_side == "Buy" else "Buy"


def utc_range_last_days(days: int) -> tuple[int, int]:
    end_s = int(time.time())
    start_s = end_s - days * 24 * 60 * 60
    return start_s * 1000, end_s * 1000


def to_float_or_none(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def h(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return html.escape(json.dumps(value, ensure_ascii=False))
    return html.escape(str(value))


def fmt_num(value: Any, digits: int = 4) -> str:
    try:
        if value is None:
            return ""
        return f"{float(value):.{digits}f}"
    except Exception:
        return h(value)


# ============================================================
# RUNTIME STATE / TRADING PAUSE
# ============================================================

def default_runtime_state() -> Dict[str, Any]:
    return {
        "trading_paused": False,
        "paused_at": None,
        "resumed_at": None,
        "pause_reason": None,
        "updated_at": now_iso(),
    }


def load_runtime_state() -> Dict[str, Any]:
    if not RUNTIME_STATE_FILE.exists():
        state = default_runtime_state()
        save_runtime_state(state)
        return state

    try:
        with RUNTIME_STATE_FILE.open("r", encoding="utf-8") as file:
            state = json.load(file)
    except Exception:
        state = default_runtime_state()
        save_runtime_state(state)

    for key, value in default_runtime_state().items():
        if key not in state:
            state[key] = value

    return state


def save_runtime_state(state: Dict[str, Any]) -> None:
    state["updated_at"] = now_iso()
    with RUNTIME_STATE_FILE.open("w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False, indent=2)


def is_trading_paused() -> bool:
    return bool(load_runtime_state().get("trading_paused", False))


def set_trading_paused(paused: bool, reason: Optional[str] = None) -> Dict[str, Any]:
    state = load_runtime_state()
    state["trading_paused"] = bool(paused)

    if paused:
        state["paused_at"] = now_iso()
        state["pause_reason"] = reason or "Manual runtime pause"
    else:
        state["resumed_at"] = now_iso()
        state["pause_reason"] = None

    save_runtime_state(state)
    return state


# ============================================================
# STRATEGY STATE / TRADE LOG
# ============================================================

def load_state() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        raise HTTPException(500, f"Missing strategy_state.json at {STATE_FILE}")
    with STATE_FILE.open("r", encoding="utf-8") as file:
        return json.load(file)


def save_state(state: Dict[str, Any]) -> None:
    with STATE_FILE.open("w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False, indent=2)


def require_strategy_admin() -> None:
    if not STRATEGY_ADMIN_ENABLED:
        raise HTTPException(403, "Strategy admin API is disabled")


def get_side_config_ref(state: Dict[str, Any], strategy: str, symbol: str, side: str) -> Dict[str, Any]:
    strategy = str(strategy)
    symbol = normalize_symbol(symbol)
    side = normalize_side(side)

    strategies = state.setdefault("strategies", {})
    strategy_cfg = strategies.setdefault(strategy, {"enabled": True, "symbols": {}})
    symbols_cfg = strategy_cfg.setdefault("symbols", {})
    symbol_cfg = symbols_cfg.setdefault(symbol, {})
    side_cfg = symbol_cfg.setdefault(side, {"mode": "OFF", "risk_pct": 0.0})
    return side_cfg


def get_side_config_copy(state: Dict[str, Any], strategy: str, symbol: str, side: str) -> Dict[str, Any]:
    try:
        return dict(get_side_config_ref(state, strategy, symbol, side))
    except Exception:
        return {}


def set_strategy_side_config(
    strategy: str,
    symbol: str,
    side: str,
    mode: Optional[str] = None,
    risk_pct: Optional[float] = None,
    extra_updates: Optional[Dict[str, Any]] = None,
    reason: str = "manual_update",
) -> Dict[str, Any]:
    require_strategy_admin()
    state = load_state()
    side_cfg = get_side_config_ref(state, strategy, symbol, side)

    before = dict(side_cfg)

    if mode is not None:
        mode_up = str(mode).upper().strip()
        if mode_up not in {"OFF", "PAPER", "MICRO", "LIVE"}:
            raise HTTPException(400, f"Invalid mode: {mode}")
        side_cfg["mode"] = mode_up

    if risk_pct is not None:
        risk_value = float(risk_pct)
        if risk_value < 0:
            raise HTTPException(400, "risk_pct cannot be negative")
        side_cfg["risk_pct"] = risk_value

    if extra_updates:
        for key, value in extra_updates.items():
            if key in {"mode", "risk_pct"}:
                continue
            side_cfg[key] = value

    save_state(state)

    after = dict(side_cfg)
    write_system_log(
        action="strategy_state_update",
        symbol=normalize_symbol(symbol),
        side=normalize_side(side),
        decision="STRATEGY_STATE_UPDATED",
        reason=reason,
        status="logged",
        extra={"strategy": strategy, "before": before, "after": after},
    )

    return {
        "strategy": strategy,
        "symbol": normalize_symbol(symbol),
        "side": normalize_side(side),
        "before": before,
        "after": after,
        "state_saved": True,
    }


def ensure_trade_log() -> None:
    if TRADE_LOG_FILE.exists():
        return

    headers = [
        "timestamp",
        "strategy",
        "symbol",
        "side",
        "mode",
        "signal_price",
        "sl",
        "tp1",
        "tp2",
        "risk_pct_requested",
        "risk_pct_used",
        "decision",
        "decision_reason",
        "order_id",
        "status",
        "raw_payload",
    ]

    with TRADE_LOG_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)


def sanitize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = dict(payload)

    sensitive_keys = [
        "secret",
        "api_key",
        "api_secret",
        "bybit_key",
        "bybit_secret",
        "password",
        "token",
    ]

    for key in sensitive_keys:
        if key in sanitized:
            sanitized[key] = "***"

    return sanitized


def supabase_enabled() -> bool:
    return bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY and SUPABASE_TABLE)


def supabase_headers(prefer: str = "return=minimal") -> Dict[str, str]:
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }

    if prefer:
        headers["Prefer"] = prefer

    return headers


def supabase_table_url() -> str:
    return f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"


def write_supabase_trade_event(
    body: Dict[str, Any],
    mode: str,
    risk_pct_used: float,
    decision: str,
    decision_reason: str,
    order_id: str = "",
    status: str = "logged",
) -> None:
    if not supabase_enabled():
        return

    symbol = normalize_symbol(body.get("symbol", ""))
    side = str(body.get("side", "")).upper()
    strategy = body.get("strategy", "UNKNOWN")

    payload = {
        "timestamp_utc": now_iso(),
        "strategy": strategy,
        "symbol": symbol,
        "side": side,
        "mode": mode,
        "signal_price": to_float_or_none(body.get("signalPrice")),
        "sl": to_float_or_none(body.get("sl")),
        "tp1": to_float_or_none(body.get("tp1")),
        "tp2": to_float_or_none(body.get("tp2")),
        "risk_pct_requested": to_float_or_none(body.get("riskPct")),
        "risk_pct_used": risk_pct_used,
        "decision": decision,
        "decision_reason": decision_reason,
        "order_id": order_id,
        "status": status,
        "raw_payload": sanitize_payload(body),
    }

    try:
        resp = client.post(
            supabase_table_url(),
            headers=supabase_headers(),
            json=payload,
        )
        if resp.status_code >= 400:
            log(f"[WARN] Supabase insert failed: {resp.status_code} {resp.text}")
    except Exception as exc:
        log(f"[WARN] Supabase insert exception: {exc}")


def write_trade_log(
    body: Dict[str, Any],
    mode: str,
    risk_pct_used: float,
    decision: str,
    decision_reason: str,
    order_id: str = "",
    status: str = "logged",
) -> None:
    ensure_trade_log()

    symbol = normalize_symbol(body.get("symbol", ""))
    side = str(body.get("side", "")).upper()
    strategy = body.get("strategy", "UNKNOWN")

    row = [
        now_iso(),
        strategy,
        symbol,
        side,
        mode,
        body.get("signalPrice"),
        body.get("sl"),
        body.get("tp1"),
        body.get("tp2"),
        body.get("riskPct"),
        risk_pct_used,
        decision,
        decision_reason,
        order_id,
        status,
        json.dumps(sanitize_payload(body), ensure_ascii=False),
    ]

    with TRADE_LOG_FILE.open("a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(row)

    write_supabase_trade_event(
        body=body,
        mode=mode,
        risk_pct_used=risk_pct_used,
        decision=decision,
        decision_reason=decision_reason,
        order_id=order_id,
        status=status,
    )


def write_system_log(
    action: str,
    symbol: str = "",
    side: str = "",
    decision: str = "SYSTEM_ACTION",
    reason: str = "",
    order_id: str = "",
    status: str = "logged",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    body = {
        "strategy": "SYSTEM_EMERGENCY",
        "symbol": normalize_symbol(symbol) if symbol else "SYSTEM",
        "side": side,
        "signalPrice": None,
        "sl": None,
        "tp1": None,
        "tp2": None,
        "riskPct": 0.0,
        "action": action,
        "extra": extra or {},
    }

    write_trade_log(
        body=body,
        mode="SYSTEM",
        risk_pct_used=0.0,
        decision=decision,
        decision_reason=reason,
        order_id=order_id,
        status=status,
    )


def read_trade_log_rows(limit: int = 100) -> list[Dict[str, Any]]:
    ensure_trade_log()

    with TRADE_LOG_FILE.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    if limit <= 0:
        return rows

    return rows[-limit:]


def summarize_trade_log() -> Dict[str, Any]:
    rows = read_trade_log_rows(limit=0)

    summary: Dict[str, Any] = {
        "total_rows": len(rows),
        "by_decision": {},
        "by_status": {},
        "by_mode": {},
        "by_symbol": {},
        "by_strategy": {},
        "last_timestamp": None,
    }

    for row in rows:
        decision = row.get("decision", "UNKNOWN")
        status = row.get("status", "UNKNOWN")
        mode = row.get("mode", "UNKNOWN")
        symbol = row.get("symbol", "UNKNOWN")
        strategy = row.get("strategy", "UNKNOWN")

        summary["by_decision"][decision] = summary["by_decision"].get(decision, 0) + 1
        summary["by_status"][status] = summary["by_status"].get(status, 0) + 1
        summary["by_mode"][mode] = summary["by_mode"].get(mode, 0) + 1
        summary["by_symbol"][symbol] = summary["by_symbol"].get(symbol, 0) + 1
        summary["by_strategy"][strategy] = summary["by_strategy"].get(strategy, 0) + 1

        ts = row.get("timestamp")
        if ts:
            summary["last_timestamp"] = ts

    return summary


def fetch_supabase_logs(limit: int = 100) -> list[Dict[str, Any]]:
    if not supabase_enabled():
        return []

    safe_limit = max(1, min(limit, 1000))

    params = {
        "select": "*",
        "order": "created_at.desc",
        "limit": str(safe_limit),
    }

    resp = client.get(
        supabase_table_url(),
        headers=supabase_headers(prefer=""),
        params=params,
    )

    if resp.status_code >= 400:
        raise HTTPException(
            status_code=500,
            detail=f"Supabase fetch failed: {resp.status_code} {resp.text}",
        )

    return resp.json()


def fetch_supabase_logs_since(days: int = 1, limit: int = 5000) -> list[Dict[str, Any]]:
    if not supabase_enabled():
        return []

    safe_days = max(1, min(days, 30))
    safe_limit = max(1, min(limit, 10000))

    start_s = int(time.time()) - safe_days * 24 * 60 * 60
    start_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_s))

    params = {
        "select": "*",
        "created_at": f"gte.{start_iso}",
        "order": "created_at.desc",
        "limit": str(safe_limit),
    }

    resp = client.get(
        supabase_table_url(),
        headers=supabase_headers(prefer=""),
        params=params,
    )

    if resp.status_code >= 400:
        raise HTTPException(
            status_code=500,
            detail=f"Supabase report fetch failed: {resp.status_code} {resp.text}",
        )

    return resp.json()


def fetch_recent_duplicate_candidate(
    strategy: str,
    symbol: str,
    side: str,
    cooldown_minutes: int,
) -> Optional[Dict[str, Any]]:
    if not supabase_enabled():
        return None

    start_iso = iso_utc_seconds_ago(cooldown_minutes * 60)

    params = {
        "select": "*",
        "strategy": f"eq.{strategy}",
        "symbol": f"eq.{symbol}",
        "side": f"eq.{side}",
        "created_at": f"gte.{start_iso}",
        "status": "eq.order_sent",
        "order": "created_at.desc",
        "limit": "1",
    }

    resp = client.get(
        supabase_table_url(),
        headers=supabase_headers(prefer=""),
        params=params,
    )

    if resp.status_code >= 400:
        log(f"[WARN] Supabase duplicate fetch failed: {resp.status_code} {resp.text}")
        return None

    rows = resp.json()
    if not rows:
        return None

    return rows[0]


def fetch_recent_alert_idempotency_candidates(
    strategy: str,
    symbol: str,
    side: str,
    lookback_hours: int,
) -> list[Dict[str, Any]]:
    if not supabase_enabled():
        return []

    safe_hours = max(1, min(lookback_hours, 168))
    start_iso = iso_utc_seconds_ago(safe_hours * 60 * 60)

    params = {
        "select": "*",
        "strategy": f"eq.{strategy}",
        "symbol": f"eq.{symbol}",
        "side": f"eq.{side}",
        "created_at": f"gte.{start_iso}",
        "order": "created_at.desc",
        "limit": "200",
    }

    resp = client.get(
        supabase_table_url(),
        headers=supabase_headers(prefer=""),
        params=params,
    )

    if resp.status_code >= 400:
        log(f"[WARN] Supabase idempotency fetch failed: {resp.status_code} {resp.text}")
        return []

    rows = resp.json()
    if not isinstance(rows, list):
        return []

    return rows


def summarize_supabase_rows(rows: list[Dict[str, Any]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "total_rows_sample": len(rows),
        "by_decision": {},
        "by_status": {},
        "by_mode": {},
        "by_symbol": {},
        "by_strategy": {},
        "latest_created_at": None,
    }

    for row in rows:
        decision = row.get("decision") or "UNKNOWN"
        status = row.get("status") or "UNKNOWN"
        mode = row.get("mode") or "UNKNOWN"
        symbol = row.get("symbol") or "UNKNOWN"
        strategy = row.get("strategy") or "UNKNOWN"

        summary["by_decision"][decision] = summary["by_decision"].get(decision, 0) + 1
        summary["by_status"][status] = summary["by_status"].get(status, 0) + 1
        summary["by_mode"][mode] = summary["by_mode"].get(mode, 0) + 1
        summary["by_symbol"][symbol] = summary["by_symbol"].get(symbol, 0) + 1
        summary["by_strategy"][strategy] = summary["by_strategy"].get(strategy, 0) + 1

        created_at = row.get("created_at")
        if created_at and summary["latest_created_at"] is None:
            summary["latest_created_at"] = created_at

    return summary


# ============================================================
# ORDER QUALITY / LIVE PRICE / DUPLICATE / IDEMPOTENCY GUARDS
# ============================================================

def validate_order_quality(body: Dict[str, Any]) -> Dict[str, Any]:
    symbol = normalize_symbol(body.get("symbol", ""))
    side = normalize_side(body.get("side", ""))

    signal_price = to_float_or_none(body.get("signalPrice"))
    sl = to_float_or_none(body.get("sl"))
    tp1 = to_float_or_none(body.get("tp1"))
    tp2 = to_float_or_none(body.get("tp2"))

    reasons = []

    if signal_price is None:
        reasons.append("MISSING_SIGNAL_PRICE")
    if sl is None:
        reasons.append("MISSING_SL")
    if tp1 is None:
        reasons.append("MISSING_TP1")
    if tp2 is None:
        reasons.append("MISSING_TP2")

    if reasons:
        return {
            "ok": False,
            "symbol": symbol,
            "side": side,
            "reason": ";".join(reasons),
            "details": {
                "signalPrice": signal_price,
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
            },
        }

    assert signal_price is not None
    assert sl is not None
    assert tp1 is not None
    assert tp2 is not None

    if signal_price <= 0:
        reasons.append("INVALID_SIGNAL_PRICE")
    if sl <= 0:
        reasons.append("INVALID_SL")
    if tp1 <= 0:
        reasons.append("INVALID_TP1")
    if tp2 <= 0:
        reasons.append("INVALID_TP2")

    if reasons:
        return {
            "ok": False,
            "symbol": symbol,
            "side": side,
            "reason": ";".join(reasons),
            "details": {
                "signalPrice": signal_price,
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
            },
        }

    if side == "LONG":
        if not (sl < signal_price < tp1 < tp2):
            reasons.append("INVALID_LONG_PRICE_STRUCTURE_EXPECTED_SL_LT_SIGNAL_LT_TP1_LT_TP2")

        risk_distance = signal_price - sl
        tp1_distance = tp1 - signal_price
        tp2_distance = tp2 - signal_price

    else:
        if not (tp2 < tp1 < signal_price < sl):
            reasons.append("INVALID_SHORT_PRICE_STRUCTURE_EXPECTED_TP2_LT_TP1_LT_SIGNAL_LT_SL")

        risk_distance = sl - signal_price
        tp1_distance = signal_price - tp1
        tp2_distance = signal_price - tp2

    if risk_distance <= 0:
        reasons.append("INVALID_RISK_DISTANCE")

    stop_distance_pct = (risk_distance / signal_price) * 100.0 if signal_price > 0 else 0.0
    tp1_rr = tp1_distance / risk_distance if risk_distance > 0 else 0.0
    tp2_rr = tp2_distance / risk_distance if risk_distance > 0 else 0.0

    if stop_distance_pct < ORDER_MIN_STOP_DISTANCE_PCT:
        reasons.append(
            f"STOP_TOO_CLOSE_{stop_distance_pct:.4f}%_MIN_{ORDER_MIN_STOP_DISTANCE_PCT:.4f}%"
        )

    if stop_distance_pct > ORDER_MAX_STOP_DISTANCE_PCT:
        reasons.append(
            f"STOP_TOO_FAR_{stop_distance_pct:.4f}%_MAX_{ORDER_MAX_STOP_DISTANCE_PCT:.4f}%"
        )

    if tp1_rr < ORDER_MIN_TP1_RR:
        reasons.append(
            f"TP1_RR_TOO_LOW_{tp1_rr:.4f}_MIN_{ORDER_MIN_TP1_RR:.4f}"
        )

    if tp2_rr < ORDER_MIN_TP2_RR:
        reasons.append(
            f"TP2_RR_TOO_LOW_{tp2_rr:.4f}_MIN_{ORDER_MIN_TP2_RR:.4f}"
        )

    if tp2_rr <= tp1_rr:
        reasons.append("TP2_RR_NOT_ABOVE_TP1_RR")

    return {
        "ok": len(reasons) == 0,
        "symbol": symbol,
        "side": side,
        "reason": "OK" if not reasons else ";".join(reasons),
        "details": {
            "signalPrice": signal_price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "risk_distance": risk_distance,
            "stop_distance_pct": stop_distance_pct,
            "tp1_rr": tp1_rr,
            "tp2_rr": tp2_rr,
            "min_stop_distance_pct": ORDER_MIN_STOP_DISTANCE_PCT,
            "max_stop_distance_pct": ORDER_MAX_STOP_DISTANCE_PCT,
            "min_tp1_rr": ORDER_MIN_TP1_RR,
            "min_tp2_rr": ORDER_MIN_TP2_RR,
        },
    }


def validate_live_price_deviation(body: Dict[str, Any]) -> Dict[str, Any]:
    symbol = normalize_symbol(body.get("symbol", ""))
    signal_price = to_float_or_none(body.get("signalPrice"))

    if signal_price is None or signal_price <= 0:
        return {
            "ok": False,
            "symbol": symbol,
            "reason": "MISSING_OR_INVALID_SIGNAL_PRICE",
            "details": {
                "signalPrice": signal_price,
                "max_deviation_pct": ORDER_MAX_SIGNAL_PRICE_DEVIATION_PCT,
            },
        }

    live_price = get_ticker_last(symbol)
    deviation_pct = abs(live_price - signal_price) / signal_price * 100.0

    ok_flag = deviation_pct <= ORDER_MAX_SIGNAL_PRICE_DEVIATION_PCT

    reason = "OK"
    if not ok_flag:
        reason = (
            f"SIGNAL_PRICE_DEVIATION_TOO_HIGH_"
            f"{deviation_pct:.4f}%_MAX_{ORDER_MAX_SIGNAL_PRICE_DEVIATION_PCT:.4f}%"
        )

    return {
        "ok": ok_flag,
        "symbol": symbol,
        "reason": reason,
        "details": {
            "signalPrice": signal_price,
            "bybit_last_price": live_price,
            "deviation_pct": deviation_pct,
            "max_deviation_pct": ORDER_MAX_SIGNAL_PRICE_DEVIATION_PCT,
        },
    }


def validate_duplicate_signal(body: Dict[str, Any]) -> Dict[str, Any]:
    strategy = str(body.get("strategy", "UNKNOWN"))
    symbol = normalize_symbol(body.get("symbol", ""))
    side = normalize_side(body.get("side", ""))

    cooldown_minutes = max(0, ORDER_SIGNAL_COOLDOWN_MINUTES)

    if cooldown_minutes <= 0:
        return {
            "ok": True,
            "reason": "COOLDOWN_DISABLED",
            "details": {
                "cooldown_minutes": cooldown_minutes,
            },
        }

    recent = fetch_recent_duplicate_candidate(
        strategy=strategy,
        symbol=symbol,
        side=side,
        cooldown_minutes=cooldown_minutes,
    )

    if recent:
        return {
            "ok": False,
            "reason": f"DUPLICATE_SIGNAL_WITHIN_{cooldown_minutes}_MINUTES",
            "details": {
                "strategy": strategy,
                "symbol": symbol,
                "side": side,
                "cooldown_minutes": cooldown_minutes,
                "recent_event": {
                    "id": recent.get("id"),
                    "created_at": recent.get("created_at"),
                    "timestamp_utc": recent.get("timestamp_utc"),
                    "decision": recent.get("decision"),
                    "status": recent.get("status"),
                    "order_id": recent.get("order_id"),
                    "mode": recent.get("mode"),
                },
            },
        }

    return {
        "ok": True,
        "reason": "OK",
        "details": {
            "strategy": strategy,
            "symbol": symbol,
            "side": side,
            "cooldown_minutes": cooldown_minutes,
        },
    }


def validate_alert_idempotency(body: Dict[str, Any]) -> Dict[str, Any]:
    strategy = str(body.get("strategy", "UNKNOWN"))
    symbol = normalize_symbol(body.get("symbol", ""))
    side = normalize_side(body.get("side", ""))

    bar_time = body.get("barTime")

    if bar_time is None or bar_time == "":
        return {
            "ok": True,
            "reason": "NO_BAR_TIME_PROVIDED_IDEMPOTENCY_SKIPPED",
            "details": {
                "strategy": strategy,
                "symbol": symbol,
                "side": side,
                "barTime": bar_time,
                "lookback_hours": ORDER_ALERT_IDEMPOTENCY_LOOKBACK_HOURS,
            },
        }

    bar_time_str = str(bar_time)
    lookback_hours = max(1, ORDER_ALERT_IDEMPOTENCY_LOOKBACK_HOURS)

    candidates = fetch_recent_alert_idempotency_candidates(
        strategy=strategy,
        symbol=symbol,
        side=side,
        lookback_hours=lookback_hours,
    )

    for row in candidates:
        raw_payload = row.get("raw_payload") or {}

        if isinstance(raw_payload, str):
            try:
                raw_payload = json.loads(raw_payload)
            except Exception:
                raw_payload = {}

        previous_bar_time = raw_payload.get("barTime")

        if previous_bar_time is not None and str(previous_bar_time) == bar_time_str:
            return {
                "ok": False,
                "reason": "DUPLICATE_ALERT_SAME_BAR_TIME",
                "details": {
                    "strategy": strategy,
                    "symbol": symbol,
                    "side": side,
                    "barTime": bar_time,
                    "lookback_hours": lookback_hours,
                    "recent_event": {
                        "id": row.get("id"),
                        "created_at": row.get("created_at"),
                        "timestamp_utc": row.get("timestamp_utc"),
                        "decision": row.get("decision"),
                        "status": row.get("status"),
                        "order_id": row.get("order_id"),
                        "mode": row.get("mode"),
                    },
                },
            }

    return {
        "ok": True,
        "reason": "OK",
        "details": {
            "strategy": strategy,
            "symbol": symbol,
            "side": side,
            "barTime": bar_time,
            "lookback_hours": lookback_hours,
        },
    }


# ============================================================
# REPORTING
# ============================================================

def build_performance_report(days: int = 1) -> Dict[str, Any]:
    safe_days = max(1, min(days, 30))
    rows = fetch_supabase_logs_since(days=safe_days)

    report: Dict[str, Any] = {
        "days": safe_days,
        "event_count": len(rows),
        "by_strategy": {},
        "by_symbol": {},
        "by_side": {},
        "by_mode": {},
        "by_decision": {},
        "by_status": {},
        "strategy_symbol_matrix": {},
        "rejections": {},
        "orders": {
            "order_sent": 0,
            "order_failed": 0,
            "paper_logged": 0,
            "accepted_micro": 0,
            "accepted_live": 0,
            "rejected": 0,
            "runtime_paused": 0,
            "order_quality_rejected": 0,
            "price_deviation_rejected": 0,
            "duplicate_signal_rejected": 0,
            "duplicate_alert_rejected": 0,
            "exposure_rejected": 0,
            "trade_limit_rejected": 0,
        },
        "latest_events": rows[:20],
    }

    for row in rows:
        strategy = row.get("strategy") or "UNKNOWN"
        symbol = row.get("symbol") or "UNKNOWN"
        side = row.get("side") or "UNKNOWN"
        mode = row.get("mode") or "UNKNOWN"
        decision = row.get("decision") or "UNKNOWN"
        status = row.get("status") or "UNKNOWN"
        reason = row.get("decision_reason") or "UNKNOWN"

        report["by_strategy"][strategy] = report["by_strategy"].get(strategy, 0) + 1
        report["by_symbol"][symbol] = report["by_symbol"].get(symbol, 0) + 1
        report["by_side"][side] = report["by_side"].get(side, 0) + 1
        report["by_mode"][mode] = report["by_mode"].get(mode, 0) + 1
        report["by_decision"][decision] = report["by_decision"].get(decision, 0) + 1
        report["by_status"][status] = report["by_status"].get(status, 0) + 1

        matrix_key = f"{strategy}|{symbol}|{side}|{mode}"
        if matrix_key not in report["strategy_symbol_matrix"]:
            report["strategy_symbol_matrix"][matrix_key] = {
                "strategy": strategy,
                "symbol": symbol,
                "side": side,
                "mode": mode,
                "count": 0,
                "decisions": {},
                "statuses": {},
            }

        report["strategy_symbol_matrix"][matrix_key]["count"] += 1
        report["strategy_symbol_matrix"][matrix_key]["decisions"][decision] = (
            report["strategy_symbol_matrix"][matrix_key]["decisions"].get(decision, 0) + 1
        )
        report["strategy_symbol_matrix"][matrix_key]["statuses"][status] = (
            report["strategy_symbol_matrix"][matrix_key]["statuses"].get(status, 0) + 1
        )

        if decision == "PAPER_LOGGED":
            report["orders"]["paper_logged"] += 1
        elif decision == "ACCEPTED_MICRO":
            report["orders"]["accepted_micro"] += 1
        elif decision == "ACCEPTED_LIVE":
            report["orders"]["accepted_live"] += 1
        elif decision == "REJECTED":
            report["orders"]["rejected"] += 1
            report["rejections"][reason] = report["rejections"].get(reason, 0) + 1
        elif decision == "ORDER_FAILED":
            report["orders"]["order_failed"] += 1
        elif decision == "RUNTIME_PAUSED":
            report["orders"]["runtime_paused"] += 1
        elif decision == "ORDER_QUALITY_REJECTED":
            report["orders"]["order_quality_rejected"] += 1
            report["rejections"][reason] = report["rejections"].get(reason, 0) + 1
        elif decision == "ORDER_PRICE_DEVIATION_REJECTED":
            report["orders"]["price_deviation_rejected"] += 1
            report["rejections"][reason] = report["rejections"].get(reason, 0) + 1
        elif decision == "DUPLICATE_SIGNAL_REJECTED":
            report["orders"]["duplicate_signal_rejected"] += 1
            report["rejections"][reason] = report["rejections"].get(reason, 0) + 1
        elif decision == "DUPLICATE_ALERT_REJECTED":
            report["orders"]["duplicate_alert_rejected"] += 1
            report["rejections"][reason] = report["rejections"].get(reason, 0) + 1
        elif decision == "EXPOSURE_REJECTED":
            report["orders"]["exposure_rejected"] += 1
            report["rejections"][reason] = report["rejections"].get(reason, 0) + 1
        elif decision == "TRADE_LIMIT_REJECTED":
            report["orders"]["trade_limit_rejected"] += 1
            report["rejections"][reason] = report["rejections"].get(reason, 0) + 1

        if status == "order_sent":
            report["orders"]["order_sent"] += 1

    start_ms, end_ms = utc_range_last_days(safe_days)
    closed_rows = get_closed_pnl(start_ms=start_ms, end_ms=end_ms)
    report["bybit_closed_pnl"] = summarize_closed_pnl(closed_rows)
    report["open_risk"] = summarize_open_risk()

    return report


def classify_health(
    event_count: int,
    paper_logged: int,
    order_sent: int,
    rejected: int,
    order_failed: int,
    order_quality_rejected: int,
    price_deviation_rejected: int,
    duplicate_signal_rejected: int,
    duplicate_alert_rejected: int,
    exposure_rejected: int,
    trade_limit_rejected: int,
    net_pnl: Optional[float],
    profit_factor: Optional[float],
    mode: str,
) -> Dict[str, Any]:
    reasons = []

    if event_count == 0:
        return {
            "status": "NO_DATA",
            "score": 0,
            "reasons": ["No events in selected period"],
        }

    rejection_rate = rejected / event_count if event_count > 0 else 0.0
    error_rate = order_failed / event_count if event_count > 0 else 0.0
    quality_reject_rate = order_quality_rejected / event_count if event_count > 0 else 0.0
    price_deviation_reject_rate = price_deviation_rejected / event_count if event_count > 0 else 0.0
    duplicate_reject_rate = duplicate_signal_rejected / event_count if event_count > 0 else 0.0
    duplicate_alert_reject_rate = duplicate_alert_rejected / event_count if event_count > 0 else 0.0
    exposure_reject_rate = exposure_rejected / event_count if event_count > 0 else 0.0
    trade_limit_reject_rate = trade_limit_rejected / event_count if event_count > 0 else 0.0

    score = 50

    if event_count < 3:
        score -= 15
        reasons.append("Low sample size")

    if rejection_rate > 0.30:
        score -= 25
        reasons.append(f"High rejection rate: {rejection_rate:.1%}")
    elif rejection_rate > 0.10:
        score -= 10
        reasons.append(f"Moderate rejection rate: {rejection_rate:.1%}")

    if quality_reject_rate > 0.30:
        score -= 30
        reasons.append(f"High order quality rejection rate: {quality_reject_rate:.1%}")
    elif quality_reject_rate > 0:
        score -= 15
        reasons.append(f"Order quality rejections detected: {order_quality_rejected}")

    if price_deviation_reject_rate > 0.30:
        score -= 30
        reasons.append(f"High price deviation rejection rate: {price_deviation_reject_rate:.1%}")
    elif price_deviation_reject_rate > 0:
        score -= 15
        reasons.append(f"Price deviation rejections detected: {price_deviation_rejected}")

    if duplicate_reject_rate > 0.30:
        score -= 15
        reasons.append(f"High duplicate signal rate: {duplicate_reject_rate:.1%}")
    elif duplicate_reject_rate > 0:
        score -= 5
        reasons.append(f"Duplicate signals detected: {duplicate_signal_rejected}")

    if duplicate_alert_reject_rate > 0.30:
        score -= 15
        reasons.append(f"High duplicate alert rate: {duplicate_alert_reject_rate:.1%}")
    elif duplicate_alert_reject_rate > 0:
        score -= 5
        reasons.append(f"Duplicate alerts detected: {duplicate_alert_rejected}")

    if exposure_reject_rate > 0.30:
        score -= 25
        reasons.append(f"High exposure rejection rate: {exposure_reject_rate:.1%}")
    elif exposure_reject_rate > 0:
        score -= 10
        reasons.append(f"Exposure guard rejections detected: {exposure_rejected}")

    if trade_limit_reject_rate > 0.30:
        score -= 15
        reasons.append(f"High trade-limit rejection rate: {trade_limit_reject_rate:.1%}")
    elif trade_limit_reject_rate > 0:
        score -= 5
        reasons.append(f"Trade-limit rejections detected: {trade_limit_rejected}")

    if error_rate > 0:
        score -= 35
        reasons.append(f"Order failures detected: {order_failed}")

    mode_upper = mode.upper()

    if mode_upper == "PAPER":
        if paper_logged > 0 and order_failed == 0:
            score += 10
            reasons.append("Paper events logged successfully")

        if event_count >= 5:
            score += 10
            reasons.append("Enough paper events for initial observation")

    if mode_upper in {"MICRO", "LIVE"}:
        if order_sent > 0:
            score += 10
            reasons.append("Real orders sent successfully")

        if net_pnl is not None:
            if net_pnl > 0:
                score += 15
                reasons.append(f"Positive closed PnL: {net_pnl:.4f}")
            elif net_pnl < 0:
                score -= 15
                reasons.append(f"Negative closed PnL: {net_pnl:.4f}")

        if profit_factor is not None:
            if profit_factor >= 1.3:
                score += 15
                reasons.append(f"Profit factor acceptable: {profit_factor:.2f}")
            elif profit_factor < 1.0:
                score -= 20
                reasons.append(f"Profit factor below 1: {profit_factor:.2f}")

    score = max(0, min(score, 100))

    if score >= 75:
        status = "GOOD"
    elif score >= 45:
        status = "WATCH"
    else:
        status = "BAD"

    return {
        "status": status,
        "score": score,
        "reasons": reasons,
    }


def build_strategy_health(days: int = 7) -> Dict[str, Any]:
    safe_days = max(1, min(days, 30))
    rows = fetch_supabase_logs_since(days=safe_days, limit=10000)

    start_ms, end_ms = utc_range_last_days(safe_days)
    closed_rows = get_closed_pnl(start_ms=start_ms, end_ms=end_ms)
    closed_summary = summarize_closed_pnl(closed_rows)
    closed_by_symbol = closed_summary.get("by_symbol", {})

    groups: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        strategy = row.get("strategy") or "UNKNOWN"
        symbol = row.get("symbol") or "UNKNOWN"
        side = row.get("side") or "UNKNOWN"
        mode = row.get("mode") or "UNKNOWN"
        decision = row.get("decision") or "UNKNOWN"
        status = row.get("status") or "UNKNOWN"

        key = f"{strategy}|{symbol}|{side}|{mode}"

        if key not in groups:
            groups[key] = {
                "strategy": strategy,
                "symbol": symbol,
                "side": side,
                "mode": mode,
                "event_count": 0,
                "paper_logged": 0,
                "accepted_micro": 0,
                "accepted_live": 0,
                "rejected": 0,
                "order_sent": 0,
                "order_failed": 0,
                "runtime_paused": 0,
                "order_quality_rejected": 0,
                "price_deviation_rejected": 0,
                "duplicate_signal_rejected": 0,
                "duplicate_alert_rejected": 0,
                "exposure_rejected": 0,
                "trade_limit_rejected": 0,
                "decisions": {},
                "statuses": {},
                "latest_created_at": None,
            }

        group = groups[key]
        group["event_count"] += 1
        group["decisions"][decision] = group["decisions"].get(decision, 0) + 1
        group["statuses"][status] = group["statuses"].get(status, 0) + 1

        if decision == "PAPER_LOGGED":
            group["paper_logged"] += 1
        elif decision == "ACCEPTED_MICRO":
            group["accepted_micro"] += 1
        elif decision == "ACCEPTED_LIVE":
            group["accepted_live"] += 1
        elif decision == "REJECTED":
            group["rejected"] += 1
        elif decision == "ORDER_FAILED":
            group["order_failed"] += 1
        elif decision == "RUNTIME_PAUSED":
            group["runtime_paused"] += 1
        elif decision == "ORDER_QUALITY_REJECTED":
            group["order_quality_rejected"] += 1
        elif decision == "ORDER_PRICE_DEVIATION_REJECTED":
            group["price_deviation_rejected"] += 1
        elif decision == "DUPLICATE_SIGNAL_REJECTED":
            group["duplicate_signal_rejected"] += 1
        elif decision == "DUPLICATE_ALERT_REJECTED":
            group["duplicate_alert_rejected"] += 1
        elif decision == "EXPOSURE_REJECTED":
            group["exposure_rejected"] += 1
        elif decision == "TRADE_LIMIT_REJECTED":
            group["trade_limit_rejected"] += 1

        if status == "order_sent":
            group["order_sent"] += 1

        created_at = row.get("created_at")
        if created_at and group["latest_created_at"] is None:
            group["latest_created_at"] = created_at

    health_rows = []

    for key, group in groups.items():
        symbol = group["symbol"]
        symbol_pnl = closed_by_symbol.get(symbol, {})

        net_pnl = symbol_pnl.get("net_pnl")
        profit_factor = symbol_pnl.get("profit_factor")

        health = classify_health(
            event_count=int(group["event_count"]),
            paper_logged=int(group["paper_logged"]),
            order_sent=int(group["order_sent"]),
            rejected=int(group["rejected"]),
            order_failed=int(group["order_failed"]),
            order_quality_rejected=int(group["order_quality_rejected"]),
            price_deviation_rejected=int(group["price_deviation_rejected"]),
            duplicate_signal_rejected=int(group["duplicate_signal_rejected"]),
            duplicate_alert_rejected=int(group["duplicate_alert_rejected"]),
            exposure_rejected=int(group["exposure_rejected"]),
            trade_limit_rejected=int(group["trade_limit_rejected"]),
            net_pnl=net_pnl,
            profit_factor=profit_factor,
            mode=group["mode"],
        )

        health_rows.append({
            **group,
            "closed_pnl_by_symbol": {
                "net_pnl": net_pnl,
                "profit_factor": profit_factor,
                "trades": symbol_pnl.get("trades"),
                "win_rate": symbol_pnl.get("win_rate"),
            },
            "health": health,
        })

    health_rows.sort(
        key=lambda x: (
            x["health"]["status"],
            -int(x["event_count"]),
        )
    )

    status_counts: Dict[str, int] = {}

    for row in health_rows:
        status = row["health"]["status"]
        status_counts[status] = status_counts.get(status, 0) + 1

    return {
        "days": safe_days,
        "group_count": len(health_rows),
        "status_counts": status_counts,
        "items": health_rows,
        "bybit_closed_pnl_total": closed_summary,
        "open_risk": summarize_open_risk(),
    }


# ============================================================
# DASHBOARD HTML
# ============================================================

def badge_class(value: str) -> str:
    v = str(value).upper()
    if v in {
        "GOOD",
        "ON",
        "TRUE",
        "PAPER_LOGGED",
        "ACCEPTED_MICRO",
        "ACCEPTED_LIVE",
        "ORDER_SENT",
        "EMERGENCY_CLOSE_SENT",
        "CANCEL_ALL_ORDERS_SENT",
    }:
        return "good"
    if v in {
        "WATCH",
        "PAPER",
        "MICRO",
        "SYSTEM",
        "RUNTIME_PAUSED",
        "ORDER_QUALITY_REJECTED",
        "ORDER_PRICE_DEVIATION_REJECTED",
        "DUPLICATE_SIGNAL_REJECTED",
        "DUPLICATE_ALERT_REJECTED",
        "EXPOSURE_REJECTED",
        "PROTECTION_VERIFY_FAILED",
    }:
        return "watch"
    if v in {"BAD", "OFF", "FALSE", "REJECTED", "ORDER_FAILED", "ERROR"}:
        return "bad"
    return "neutral"


def html_badge(value: Any) -> str:
    text = h(value)
    css = badge_class(str(value))
    return f'<span class="badge {css}">{text}</span>'


def html_table(headers: list[str], rows: list[list[Any]]) -> str:
    thead = "".join(f"<th>{h(header)}</th>" for header in headers)
    body_rows = []

    for row in rows:
        cells = "".join(f"<td>{cell}</td>" for cell in row)
        body_rows.append(f"<tr>{cells}</tr>")

    tbody = "".join(body_rows) if body_rows else f'<tr><td colspan="{len(headers)}" class="muted">No data</td></tr>'

    return f"""
    <table>
        <thead><tr>{thead}</tr></thead>
        <tbody>{tbody}</tbody>
    </table>
    """


def build_dashboard_html(secret: str, days: int = 7) -> str:
    safe_days = max(1, min(days, 30))

    state_data = load_state()
    runtime_state = load_runtime_state()
    trading_paused = bool(runtime_state.get("trading_paused", False))

    open_risk = summarize_open_risk()

    closed_1_rows = get_closed_pnl(*utc_range_last_days(1))
    closed_7_rows = get_closed_pnl(*utc_range_last_days(7))
    closed_1 = summarize_closed_pnl(closed_1_rows)
    closed_7 = summarize_closed_pnl(closed_7_rows)

    performance = build_performance_report(days=safe_days) if supabase_enabled() else {}
    health = build_strategy_health(days=safe_days) if supabase_enabled() else {
        "items": [],
        "status_counts": {},
        "group_count": 0,
    }

    latest_logs = fetch_supabase_logs(limit=20) if supabase_enabled() else read_trade_log_rows(limit=20)

    closed_pnl_rows = [
        [
            "1 day",
            fmt_num(closed_1.get("net_pnl")),
            fmt_num(closed_1.get("gross_profit")),
            fmt_num(closed_1.get("gross_loss")),
            h(closed_1.get("trades")),
            fmt_num(closed_1.get("profit_factor"), 3),
            fmt_num(closed_1.get("win_rate"), 2),
        ],
        [
            "7 days",
            fmt_num(closed_7.get("net_pnl")),
            fmt_num(closed_7.get("gross_profit")),
            fmt_num(closed_7.get("gross_loss")),
            h(closed_7.get("trades")),
            fmt_num(closed_7.get("profit_factor"), 3),
            fmt_num(closed_7.get("win_rate"), 2),
        ],
    ]

    health_rows = []
    for item in health.get("items", []):
        health_data = item.get("health", {})
        closed_data = item.get("closed_pnl_by_symbol", {})
        health_rows.append([
            h(item.get("strategy")),
            h(item.get("symbol")),
            h(item.get("side")),
            html_badge(item.get("mode")),
            h(item.get("event_count")),
            h(item.get("paper_logged")),
            h(item.get("order_sent")),
            h(item.get("rejected")),
            h(item.get("order_failed")),
            h(item.get("runtime_paused", 0)),
            h(item.get("order_quality_rejected", 0)),
            h(item.get("price_deviation_rejected", 0)),
            h(item.get("duplicate_signal_rejected", 0)),
            h(item.get("duplicate_alert_rejected", 0)),
            h(item.get("exposure_rejected", 0)),
            h(item.get("trade_limit_rejected", 0)),
            fmt_num(closed_data.get("net_pnl")),
            fmt_num(closed_data.get("profit_factor"), 3),
            html_badge(health_data.get("status")),
            h(health_data.get("score")),
            h(", ".join(health_data.get("reasons", []))),
        ])

    latest_rows = []
    for row in latest_logs:
        latest_rows.append([
            h(row.get("created_at") or row.get("timestamp")),
            h(row.get("strategy")),
            h(row.get("symbol")),
            h(row.get("side")),
            html_badge(row.get("mode")),
            html_badge(row.get("decision")),
            h(row.get("decision_reason")),
            h(row.get("status")),
            h(row.get("order_id")),
        ])

    perf_orders = performance.get("orders", {}) if performance else {}
    perf_rows = [
        ["Event count", h(performance.get("event_count", 0) if performance else 0)],
        ["Paper logged", h(perf_orders.get("paper_logged", 0))],
        ["Accepted MICRO", h(perf_orders.get("accepted_micro", 0))],
        ["Accepted LIVE", h(perf_orders.get("accepted_live", 0))],
        ["Runtime paused", h(perf_orders.get("runtime_paused", 0))],
        ["Order quality rejected", h(perf_orders.get("order_quality_rejected", 0))],
        ["Price deviation rejected", h(perf_orders.get("price_deviation_rejected", 0))],
        ["Duplicate signal rejected", h(perf_orders.get("duplicate_signal_rejected", 0))],
        ["Duplicate alert rejected", h(perf_orders.get("duplicate_alert_rejected", 0))],
        ["Exposure rejected", h(perf_orders.get("exposure_rejected", 0))],
        ["Trade limit rejected", h(perf_orders.get("trade_limit_rejected", 0))],
        ["Order sent", h(perf_orders.get("order_sent", 0))],
        ["Order failed", h(perf_orders.get("order_failed", 0))],
        ["Rejected", h(perf_orders.get("rejected", 0))],
    ]

    open_positions_rows = []
    open_position_symbols = []

    for symbol, data in open_risk.get("by_symbol", {}).items():
        open_position_symbols.append(symbol)
        open_positions_rows.append([
            h(symbol),
            h(data.get("side")),
            fmt_num(data.get("size")),
            fmt_num(data.get("avg_price")),
            fmt_num(data.get("mark_price")),
            fmt_num(data.get("position_value")),
            fmt_num(data.get("unrealized_pnl")),
            h(data.get("leverage")),
            h(data.get("stop_loss")),
            h(data.get("take_profit")),
        ])

    health_counts = health.get("status_counts", {})

    per_symbol_close_buttons = ""
    for symbol in open_position_symbols:
        per_symbol_close_buttons += f"""
        <form method="post" action="/emergency_close_symbol?secret={h(secret)}&symbol={h(symbol)}"
              onsubmit="return confirm('Close {h(symbol)} position at market?');">
            <button class="danger" type="submit">Close {h(symbol)}</button>
        </form>
        """

    if trading_paused:
        pause_button = f"""
        <form method="post" action="/trading_pause_off?secret={h(secret)}"
              onsubmit="return confirm('Resume MICRO/LIVE order execution?');">
            <button class="goodbtn" type="submit">Resume Trading</button>
        </form>
        """
    else:
        pause_button = f"""
        <form method="post" action="/trading_pause_on?secret={h(secret)}&reason=Manual%20dashboard%20pause"
              onsubmit="return confirm('Pause MICRO/LIVE order execution? Signals will still be logged.');">
            <button class="warn" type="submit">Pause Trading</button>
        </form>
        """

    nav = f"""
    <div class="nav">
        <a href="/dashboard?secret={h(secret)}&days=1">Dashboard 1D</a>
        <a href="/dashboard?secret={h(secret)}&days=7">Dashboard 7D</a>
        <a href="/performance_report?secret={h(secret)}&days={safe_days}">JSON Performance</a>
        <a href="/strategy_health?secret={h(secret)}&days={safe_days}">JSON Health</a>
        <a href="/db_logs?secret={h(secret)}&limit=20">JSON DB Logs</a>
        <a href="/risk_status?secret={h(secret)}">Risk Status</a>
        <a href="/trading_pause_status?secret={h(secret)}">Pause Status</a>
        <a href="/order_quality_config?secret={h(secret)}">Guard Config</a>
    </div>
    """

    pause_card_class = "dangerbox" if trading_paused else "card"

    return f"""
    <!doctype html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>TV Bybit Risk Engine Dashboard</title>
        <style>
            body {{
                font-family: Arial, Helvetica, sans-serif;
                margin: 24px;
                background: #f6f8fb;
                color: #1f2937;
            }}
            h1, h2 {{
                margin-bottom: 8px;
            }}
            .grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                gap: 14px;
                margin: 16px 0 24px;
            }}
            .card {{
                background: white;
                border-radius: 12px;
                padding: 16px;
                box-shadow: 0 1px 4px rgba(0,0,0,0.08);
                border: 1px solid #e5e7eb;
            }}
            .metric {{
                font-size: 28px;
                font-weight: 700;
                margin-top: 6px;
            }}
            .label {{
                color: #6b7280;
                font-size: 13px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background: white;
                border-radius: 12px;
                overflow: hidden;
                box-shadow: 0 1px 4px rgba(0,0,0,0.08);
                margin-bottom: 24px;
            }}
            th, td {{
                padding: 10px 12px;
                border-bottom: 1px solid #e5e7eb;
                text-align: left;
                font-size: 13px;
                vertical-align: top;
            }}
            th {{
                background: #111827;
                color: white;
                font-weight: 600;
            }}
            tr:hover td {{
                background: #f9fafb;
            }}
            .badge {{
                display: inline-block;
                padding: 3px 8px;
                border-radius: 999px;
                font-size: 12px;
                font-weight: 700;
            }}
            .good {{
                background: #dcfce7;
                color: #166534;
            }}
            .watch {{
                background: #fef3c7;
                color: #92400e;
            }}
            .bad {{
                background: #fee2e2;
                color: #991b1b;
            }}
            .neutral {{
                background: #e5e7eb;
                color: #374151;
            }}
            .muted {{
                color: #6b7280;
            }}
            .nav {{
                margin: 14px 0 24px;
            }}
            .nav a {{
                display: inline-block;
                margin: 0 8px 8px 0;
                padding: 8px 12px;
                border-radius: 8px;
                background: #111827;
                color: white;
                text-decoration: none;
                font-size: 13px;
            }}
            .section {{
                margin-top: 26px;
            }}
            code {{
                background: #eef2ff;
                padding: 2px 6px;
                border-radius: 5px;
            }}
            .controls {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
                margin-top: 12px;
            }}
            form {{
                display: inline-block;
                margin: 0;
            }}
            button {{
                border: 0;
                border-radius: 8px;
                padding: 10px 14px;
                font-weight: 700;
                cursor: pointer;
            }}
            button.danger {{
                background: #991b1b;
                color: white;
            }}
            button.warn {{
                background: #92400e;
                color: white;
            }}
            button.secondary {{
                background: #111827;
                color: white;
            }}
            button.goodbtn {{
                background: #166534;
                color: white;
            }}
            .dangerbox {{
                border: 1px solid #fecaca;
                background: #fff1f2;
                border-radius: 12px;
                padding: 16px;
                box-shadow: 0 1px 4px rgba(0,0,0,0.08);
            }}
        </style>
    </head>
    <body>
        <h1>TV Webhook ↔ Bybit Risk Engine Dashboard</h1>
        <div class="muted">Version 3.5.0 · Generated at {h(now_iso())} · Window: {safe_days} day(s)</div>
        {nav}

        <div class="grid">
            <div class="card">
                <div class="label">Real orders master</div>
                <div class="metric">{html_badge(str(ENABLE_REAL_ORDERS))}</div>
            </div>
            <div class="{pause_card_class}">
                <div class="label">Runtime trading pause</div>
                <div class="metric">{html_badge(str(trading_paused))}</div>
                <div class="muted">Reason: {h(runtime_state.get("pause_reason"))}</div>
            </div>
            <div class="card">
                <div class="label">Supabase</div>
                <div class="metric">{html_badge(str(supabase_enabled()))}</div>
            </div>
            <div class="card">
                <div class="label">Open positions</div>
                <div class="metric">{h(open_risk.get("open_positions"))}</div>
            </div>
            <div class="card">
                <div class="label">Open position value</div>
                <div class="metric">{fmt_num(open_risk.get("total_position_value"))}</div>
            </div>
            <div class="card">
                <div class="label">Open unrealized PnL</div>
                <div class="metric">{fmt_num(open_risk.get("total_unrealized_pnl"))}</div>
            </div>
            <div class="card">
                <div class="label">Closed PnL 1D</div>
                <div class="metric">{fmt_num(closed_1.get("net_pnl"))}</div>
            </div>
            <div class="card">
                <div class="label">Closed PnL 7D</div>
                <div class="metric">{fmt_num(closed_7.get("net_pnl"))}</div>
            </div>
            <div class="card">
                <div class="label">Health WATCH / BAD</div>
                <div class="metric">{h(health_counts.get("WATCH", 0))} / {h(health_counts.get("BAD", 0))}</div>
            </div>
        </div>

        <div class="section card">
            <h2>Runtime Trading Control</h2>
            <div class="muted">
                Pause blocks MICRO/LIVE order execution immediately. TradingView signals are still accepted and logged.
            </div>
            <div class="controls">
                {pause_button}
            </div>
        </div>

        <div class="section card">
            <h2>Order Guards</h2>
            <div class="muted">
                Active before every MICRO/LIVE order.
                Min stop: {ORDER_MIN_STOP_DISTANCE_PCT}% · Max stop: {ORDER_MAX_STOP_DISTANCE_PCT}% ·
                Min TP1 RR: {ORDER_MIN_TP1_RR} · Min TP2 RR: {ORDER_MIN_TP2_RR} ·
                Max signal/live price deviation: {ORDER_MAX_SIGNAL_PRICE_DEVIATION_PCT}% ·
                Duplicate cooldown: {ORDER_SIGNAL_COOLDOWN_MINUTES} minutes ·
                Alert idempotency lookback: {ORDER_ALERT_IDEMPOTENCY_LOOKBACK_HOURS} hours
            </div>
        </div>

        <div class="section card">
            <h2>Exposure Guard</h2>
            <div class="muted">
                0 means disabled.
                Max total position value: {MAX_TOTAL_POSITION_VALUE_USDT} USDT ·
                Max symbol position value: {MAX_SYMBOL_POSITION_VALUE_USDT} USDT ·
                Max equity usage: {MAX_EQUITY_USAGE_PCT}% ·
                Max leverage exposure: {MAX_LEVERAGE_EXPOSURE_PCT}%
            </div>
        </div>

        <div class="section card">
            <h2>Post-order Protection Verification</h2>
            <div class="muted">
                Checks after real order execution that an open position has stop loss and at least one reduce-only take-profit order.
                Enabled: {POST_ORDER_VERIFY_ENABLED} · Retries: {POST_ORDER_VERIFY_RETRIES} · Sleep: {POST_ORDER_VERIFY_SLEEP_SEC}s ·
                Auto-close if protection is missing: {AUTO_CLOSE_ON_PROTECTION_MISSING}
            </div>
        </div>

        <div class="section card dangerbox">
            <h2>Emergency Controls</h2>
            <div class="muted">
                These actions send real Bybit requests. Use only if manual intervention is needed.
            </div>
            <div class="controls">
                <form method="post" action="/emergency_close_all?secret={h(secret)}"
                      onsubmit="return confirm('Close ALL open positions at market?');">
                    <button class="danger" type="submit">Close ALL Positions</button>
                </form>

                <form method="post" action="/cancel_all_orders?secret={h(secret)}"
                      onsubmit="return confirm('Cancel ALL open orders?');">
                    <button class="warn" type="submit">Cancel ALL Orders</button>
                </form>

                {per_symbol_close_buttons}
            </div>
        </div>

        <div class="section">
            <h2>Performance Summary</h2>
            {html_table(["Metric", "Value"], perf_rows)}
        </div>

        <div class="section">
            <h2>Closed PnL</h2>
            {html_table(["Window", "Net PnL", "Gross Profit", "Gross Loss", "Trades", "PF", "Win Rate %"], closed_pnl_rows)}
        </div>

        <div class="section">
            <h2>Open Positions / Risk</h2>
            {html_table(["Symbol", "Side", "Size", "Avg Price", "Mark Price", "Position Value", "Unrealized PnL", "Leverage", "SL", "TP"], open_positions_rows)}
        </div>

        <div class="section">
            <h2>Strategy Health</h2>
            {html_table(["Strategy", "Symbol", "Side", "Mode", "Events", "Paper", "Orders", "Rejects", "Failures", "Paused", "Quality Rejects", "Price Rejects", "Duplicate Rejects", "Alert Duplicates", "Exposure Rejects", "Trade Limit Rejects", "Closed PnL", "PF", "Health", "Score", "Reasons"], health_rows)}
        </div>

        <div class="section">
            <h2>Latest Events</h2>
            {html_table(["Created", "Strategy", "Symbol", "Side", "Mode", "Decision", "Reason", "Status", "Order ID"], latest_rows)}
        </div>

        <div class="section">
            <h2>Runtime State</h2>
            <pre class="card">{h(runtime_state)}</pre>
        </div>

        <div class="section">
            <h2>Configured State</h2>
            <pre class="card">{h(state_data)}</pre>
        </div>
    </body>
    </html>
    """


# ============================================================
# STRATEGY CONFIG
# ============================================================

def get_strategy_side_config(
    state: Dict[str, Any],
    strategy: str,
    symbol: str,
    side: str,
) -> Dict[str, Any]:
    global_cfg = state.get("global", {})
    strategies = state.get("strategies", {})

    if not global_cfg.get("enabled", False):
        return {
            "mode": "OFF",
            "risk_pct": 0.0,
            "reason": "GLOBAL_DISABLED",
        }

    strategy_cfg = strategies.get(strategy)
    if not strategy_cfg:
        return {
            "mode": "OFF",
            "risk_pct": 0.0,
            "reason": "UNKNOWN_STRATEGY",
        }

    if not strategy_cfg.get("enabled", False):
        return {
            "mode": "OFF",
            "risk_pct": 0.0,
            "reason": "STRATEGY_DISABLED",
        }

    symbols_cfg = strategy_cfg.get("symbols", {})
    symbol_cfg = symbols_cfg.get(symbol)

    if not symbol_cfg:
        if global_cfg.get("allow_unknown_symbols", False):
            return {
                "mode": global_cfg.get("default_mode", "OFF").upper(),
                "risk_pct": 0.0,
                "reason": "UNKNOWN_SYMBOL_DEFAULT_MODE",
            }
        return {
            "mode": "OFF",
            "risk_pct": 0.0,
            "reason": "UNKNOWN_SYMBOL",
        }

    side_cfg = symbol_cfg.get(side)
    if not side_cfg:
        return {
            "mode": "OFF",
            "risk_pct": 0.0,
            "reason": "SIDE_NOT_CONFIGURED",
        }

    return {
        "mode": str(side_cfg.get("mode", "OFF")).upper(),
        "risk_pct": float(side_cfg.get("risk_pct", 0.0)),
        "reason": "CONFIG_OK",
    }


# ============================================================
# BYBIT CORE CALL
# ============================================================

def bybit(method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = BYBIT_BASE + path
    ts = now_ms()

    if method.upper() == "GET":
        query = ""
        if params:
            items = sorted((key, str(value)) for key, value in params.items() if value is not None)
            query = "&".join([f"{key}={value}" for key, value in items])
            url = url + "?" + query

        sign = sign_v5(ts, API_KEY, RECV_WINDOW, query)
        headers = {
            "X-BAPI-API-KEY": API_KEY,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": RECV_WINDOW,
            "X-BAPI-SIGN": sign,
        }
        response = client.get(url, headers=headers)

    else:
        body = json.dumps(params or {}, separators=(",", ":"))
        sign = sign_v5(ts, API_KEY, RECV_WINDOW, body)
        headers = {
            "X-BAPI-API-KEY": API_KEY,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": RECV_WINDOW,
            "X-BAPI-SIGN": sign,
            "Content-Type": "application/json",
        }
        response = client.post(url, headers=headers, content=body)

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


# ============================================================
# BYBIT HELPERS
# ============================================================

def get_instrument(symbol: str) -> Tuple[float, float, float]:
    resp = bybit(
        "GET",
        "/v5/market/instruments-info",
        {
            "category": "linear",
            "symbol": symbol,
        },
    )

    instruments = (resp.get("result") or {}).get("list") or []
    if not instruments:
        raise HTTPException(400, f"Symbol not found: {symbol}")

    item = instruments[0]
    price_filter = item.get("priceFilter", {})
    lot_filter = item.get("lotSizeFilter", {})

    tick = float(price_filter.get("tickSize", "0.01"))
    step = float(lot_filter.get("qtyStep", "0.001"))
    min_qty = float(lot_filter.get("minOrderQty", "0.001"))

    return tick, step, min_qty


def get_ticker_last(symbol: str) -> float:
    resp = bybit(
        "GET",
        "/v5/market/tickers",
        {
            "category": "linear",
            "symbol": symbol,
        },
    )

    items = (resp.get("result") or {}).get("list") or []
    if not items:
        raise HTTPException(400, f"No ticker for {symbol}")

    return float(items[0]["lastPrice"])


def get_equity_usdt() -> float:
    resp = bybit(
        "GET",
        "/v5/account/wallet-balance",
        {
            "accountType": "UNIFIED",
            "coin": "USDT",
        },
    )

    accounts = (resp.get("result") or {}).get("list") or []
    if not accounts:
        return 0.0

    coins = accounts[0].get("coin", [])
    for coin in coins:
        if coin.get("coin") == "USDT":
            return float(coin.get("equity", "0") or 0)

    return 0.0


def get_position_linear(symbol: str) -> Dict[str, Any]:
    resp = bybit(
        "GET",
        "/v5/position/list",
        {
            "category": "linear",
            "symbol": symbol,
        },
    )

    positions = (resp.get("result") or {}).get("list") or []
    if not positions:
        return {"side": "", "size": "0"}

    best = None
    max_abs_size = 0.0

    for position in positions:
        size = abs(float(position.get("size", "0") or 0))
        if size > max_abs_size:
            best = position
            max_abs_size = size

    return best or positions[0]


def get_open_orders(symbol: str) -> list[Dict[str, Any]]:
    resp = bybit(
        "GET",
        "/v5/order/realtime",
        {
            "category": "linear",
            "symbol": normalize_symbol(symbol),
            "openOnly": 0,
            "limit": 50,
        },
    )

    orders = (resp.get("result") or {}).get("list") or []
    if not isinstance(orders, list):
        return []

    return orders


def get_all_open_positions() -> list[Dict[str, Any]]:
    resp = bybit(
        "GET",
        "/v5/position/list",
        {
            "category": "linear",
            "settleCoin": "USDT",
        },
    )

    positions = (resp.get("result") or {}).get("list") or []

    open_positions = []
    for position in positions:
        size = abs(float(position.get("size", "0") or 0.0))
        if size > 0:
            open_positions.append(position)

    return open_positions


def get_open_positions_count() -> int:
    return len(get_all_open_positions())


def has_open_position(symbol: str) -> bool:
    pos = get_position_linear(symbol)
    size = abs(float(pos.get("size", "0") or 0.0))
    return size > 0


def set_leverage(symbol: str, leverage: int) -> Dict[str, Any]:
    req = {
        "category": "linear",
        "symbol": symbol,
        "buyLeverage": str(leverage),
        "sellLeverage": str(leverage),
    }
    log(f"[REQ] set-leverage: {req}")
    resp = bybit("POST", "/v5/position/set-leverage", req)
    log(f"[RESP] set-leverage: {resp}")
    return resp


# ============================================================
# CLOSED PNL / OPEN RISK HELPERS
# ============================================================

def get_closed_pnl(
    start_ms: int,
    end_ms: int,
    symbol: Optional[str] = None,
    limit: int = 100,
) -> list[Dict[str, Any]]:
    all_rows: list[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        params: Dict[str, Any] = {
            "category": "linear",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }

        if symbol:
            params["symbol"] = normalize_symbol(symbol)

        if cursor:
            params["cursor"] = cursor

        resp = bybit("GET", "/v5/position/closed-pnl", params)
        result = resp.get("result") or {}
        rows = result.get("list") or []

        all_rows.extend(rows)

        cursor = result.get("nextPageCursor")
        if not cursor:
            break

        if len(all_rows) >= 1000:
            break

    return all_rows


def summarize_closed_pnl(rows: list[Dict[str, Any]]) -> Dict[str, Any]:
    total = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    wins = 0
    losses = 0

    by_symbol: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        symbol = row.get("symbol", "UNKNOWN")
        pnl = float(row.get("closedPnl", "0") or 0.0)

        total += pnl

        if pnl > 0:
            gross_profit += pnl
            wins += 1
        elif pnl < 0:
            gross_loss += abs(pnl)
            losses += 1

        if symbol not in by_symbol:
            by_symbol[symbol] = {
                "trades": 0,
                "net_pnl": 0.0,
                "wins": 0,
                "losses": 0,
                "gross_profit": 0.0,
                "gross_loss": 0.0,
            }

        by_symbol[symbol]["trades"] += 1
        by_symbol[symbol]["net_pnl"] += pnl

        if pnl > 0:
            by_symbol[symbol]["wins"] += 1
            by_symbol[symbol]["gross_profit"] += pnl
        elif pnl < 0:
            by_symbol[symbol]["losses"] += 1
            by_symbol[symbol]["gross_loss"] += abs(pnl)

    trade_count = len(rows)

    profit_factor = None
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss

    win_rate = None
    if trade_count > 0:
        win_rate = wins / trade_count * 100.0

    for symbol, data in by_symbol.items():
        symbol_pf = None
        if data["gross_loss"] > 0:
            symbol_pf = data["gross_profit"] / data["gross_loss"]

        symbol_wr = None
        if data["trades"] > 0:
            symbol_wr = data["wins"] / data["trades"] * 100.0

        data["profit_factor"] = symbol_pf
        data["win_rate"] = symbol_wr

    return {
        "trades": trade_count,
        "net_pnl": total,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "wins": wins,
        "losses": losses,
        "profit_factor": profit_factor,
        "win_rate": win_rate,
        "by_symbol": by_symbol,
    }


def summarize_open_risk() -> Dict[str, Any]:
    positions = get_all_open_positions()

    total_unrealized_pnl = 0.0
    total_position_value = 0.0
    by_symbol: Dict[str, Dict[str, Any]] = {}

    for position in positions:
        symbol = position.get("symbol", "UNKNOWN")
        side = position.get("side", "")
        size = float(position.get("size", "0") or 0.0)
        avg_price = float(position.get("avgPrice", "0") or 0.0)
        mark_price = float(position.get("markPrice", "0") or 0.0)
        position_value = float(position.get("positionValue", "0") or 0.0)
        unrealized = float(position.get("unrealisedPnl", "0") or 0.0)

        total_unrealized_pnl += unrealized
        total_position_value += position_value

        by_symbol[symbol] = {
            "symbol": symbol,
            "side": side,
            "size": size,
            "avg_price": avg_price,
            "mark_price": mark_price,
            "position_value": position_value,
            "unrealized_pnl": unrealized,
            "liq_price": position.get("liqPrice", ""),
            "leverage": position.get("leverage", ""),
            "take_profit": position.get("takeProfit", ""),
            "stop_loss": position.get("stopLoss", ""),
        }

    return {
        "open_positions": len(positions),
        "total_unrealized_pnl": total_unrealized_pnl,
        "total_position_value": total_position_value,
        "by_symbol": by_symbol,
    }


def check_closed_pnl_limits(state: Dict[str, Any]) -> Optional[str]:
    global_cfg = state.get("global", {})

    daily_limit = float(global_cfg.get("daily_loss_limit_usdt", 0.0) or 0.0)
    weekly_limit = float(global_cfg.get("weekly_loss_limit_usdt", 0.0) or 0.0)

    if daily_limit > 0:
        start_ms, end_ms = utc_range_last_days(1)
        rows = get_closed_pnl(start_ms=start_ms, end_ms=end_ms)
        summary = summarize_closed_pnl(rows)
        daily_net = float(summary.get("net_pnl", 0.0))

        if daily_net <= -abs(daily_limit):
            return f"DAILY_CLOSED_PNL_LIMIT_REACHED: {daily_net:.4f} USDT"

    if weekly_limit > 0:
        start_ms, end_ms = utc_range_last_days(7)
        rows = get_closed_pnl(start_ms=start_ms, end_ms=end_ms)
        summary = summarize_closed_pnl(rows)
        weekly_net = float(summary.get("net_pnl", 0.0))

        if weekly_net <= -abs(weekly_limit):
            return f"WEEKLY_CLOSED_PNL_LIMIT_REACHED: {weekly_net:.4f} USDT"

    return None


def check_open_unrealized_limits(state: Dict[str, Any], symbol: str) -> Optional[str]:
    global_cfg = state.get("global", {})

    total_limit = float(global_cfg.get("open_unrealized_loss_limit_usdt", 0.0) or 0.0)
    symbol_limit = float(global_cfg.get("symbol_unrealized_loss_limit_usdt", 0.0) or 0.0)

    if total_limit <= 0 and symbol_limit <= 0:
        return None

    summary = summarize_open_risk()
    total_unrealized = float(summary.get("total_unrealized_pnl", 0.0))

    if total_limit > 0 and total_unrealized <= -abs(total_limit):
        return f"OPEN_UNREALIZED_TOTAL_LIMIT_REACHED: {total_unrealized:.4f} USDT"

    if symbol_limit > 0:
        by_symbol = summary.get("by_symbol", {})
        symbol_data = by_symbol.get(symbol)
        if symbol_data:
            symbol_unrealized = float(symbol_data.get("unrealized_pnl", 0.0))
            if symbol_unrealized <= -abs(symbol_limit):
                return f"OPEN_UNREALIZED_SYMBOL_LIMIT_REACHED: {symbol} {symbol_unrealized:.4f} USDT"

    return None


# ============================================================
# POSITION LIMIT CHECKS
# ============================================================

def check_position_limits(state: Dict[str, Any], symbol: str) -> Optional[str]:
    global_cfg = state.get("global", {})
    max_open_positions = int(global_cfg.get("max_open_positions", 1))

    try:
        if has_open_position(symbol):
            return "SYMBOL_ALREADY_OPEN"
    except Exception as exc:
        return f"SYMBOL_POSITION_CHECK_ERROR: {exc}"

    try:
        open_count = get_open_positions_count()
    except Exception as exc:
        return f"OPEN_POSITION_CHECK_ERROR: {exc}"

    if open_count >= max_open_positions:
        return "MAX_OPEN_POSITIONS_REACHED"

    return None


def exposure_limits_enabled() -> bool:
    return any([
        MAX_TOTAL_POSITION_VALUE_USDT > 0,
        MAX_SYMBOL_POSITION_VALUE_USDT > 0,
        MAX_EQUITY_USAGE_PCT > 0,
        MAX_LEVERAGE_EXPOSURE_PCT > 0,
    ])


def estimate_new_order_exposure(body: Dict[str, Any], risk_pct_used: float) -> Dict[str, Any]:
    symbol = normalize_symbol(body.get("symbol", ""))
    side = normalize_side(body.get("side", ""))
    sl = to_float_or_none(body.get("sl"))
    qty_in = to_float_or_none(body.get("qty"))

    _, lot_step, min_qty = get_instrument(symbol)
    live_price = get_ticker_last(symbol)
    equity = get_equity_usdt()

    if qty_in is not None and qty_in > 0:
        qty_calc = qty_in
        sizing_method = "explicit_qty"
        risk_usd = None
        stop_distance = None
    else:
        if sl is None or sl <= 0:
            return {
                "ok": False,
                "reason": "EXPOSURE_ESTIMATE_FAILED_MISSING_SL",
                "details": {
                    "symbol": symbol,
                    "side": side,
                    "risk_pct_used": risk_pct_used,
                    "sl": sl,
                    "qty": qty_in,
                },
            }

        stop_distance = abs(live_price - sl)
        if stop_distance <= 0:
            return {
                "ok": False,
                "reason": "EXPOSURE_ESTIMATE_FAILED_INVALID_STOP_DISTANCE",
                "details": {
                    "symbol": symbol,
                    "side": side,
                    "live_price": live_price,
                    "sl": sl,
                    "stop_distance": stop_distance,
                },
            }

        risk_usd = equity * (risk_pct_used / 100.0)
        qty_calc = risk_usd / stop_distance
        sizing_method = "risk_pct"

    qty_rounded = max(round_step(float(qty_calc), lot_step), min_qty)
    estimated_new_position_value = qty_rounded * live_price

    open_risk = summarize_open_risk()
    current_total_position_value = float(open_risk.get("total_position_value", 0.0) or 0.0)

    current_symbol_position_value = 0.0
    by_symbol = open_risk.get("by_symbol", {})
    if symbol in by_symbol:
        current_symbol_position_value = float(by_symbol[symbol].get("position_value", 0.0) or 0.0)

    projected_total_position_value = current_total_position_value + estimated_new_position_value
    projected_symbol_position_value = current_symbol_position_value + estimated_new_position_value

    projected_equity_usage_pct = None
    projected_leverage_exposure_pct = None

    if equity > 0:
        projected_equity_usage_pct = projected_total_position_value / equity * 100.0
        projected_leverage_exposure_pct = projected_total_position_value / equity * 100.0

    return {
        "ok": True,
        "reason": "OK",
        "details": {
            "symbol": symbol,
            "side": side,
            "sizing_method": sizing_method,
            "risk_pct_used": risk_pct_used,
            "risk_usd": risk_usd,
            "stop_distance": stop_distance,
            "live_price": live_price,
            "equity": equity,
            "qty_calc": qty_calc,
            "qty_rounded": qty_rounded,
            "lot_step": lot_step,
            "min_qty": min_qty,
            "estimated_new_position_value": estimated_new_position_value,
            "current_total_position_value": current_total_position_value,
            "current_symbol_position_value": current_symbol_position_value,
            "projected_total_position_value": projected_total_position_value,
            "projected_symbol_position_value": projected_symbol_position_value,
            "projected_equity_usage_pct": projected_equity_usage_pct,
            "projected_leverage_exposure_pct": projected_leverage_exposure_pct,
            "limits": {
                "max_total_position_value_usdt": MAX_TOTAL_POSITION_VALUE_USDT,
                "max_symbol_position_value_usdt": MAX_SYMBOL_POSITION_VALUE_USDT,
                "max_equity_usage_pct": MAX_EQUITY_USAGE_PCT,
                "max_leverage_exposure_pct": MAX_LEVERAGE_EXPOSURE_PCT,
            "post_order_verify_enabled": POST_ORDER_VERIFY_ENABLED,
            "post_order_verify_retries": POST_ORDER_VERIFY_RETRIES,
            "post_order_verify_sleep_sec": POST_ORDER_VERIFY_SLEEP_SEC,
            "auto_close_on_protection_missing": AUTO_CLOSE_ON_PROTECTION_MISSING,
            "strategy_admin_enabled": STRATEGY_ADMIN_ENABLED,
            "max_daily_trades_global": MAX_DAILY_TRADES_GLOBAL,
            "max_daily_trades_per_symbol": MAX_DAILY_TRADES_PER_SYMBOL,
            "max_daily_losses_per_symbol": MAX_DAILY_LOSSES_PER_SYMBOL,
            "max_consecutive_losses": MAX_CONSECUTIVE_LOSSES,
            "auto_downgrade_enabled": AUTO_DOWNGRADE_ENABLED,
            "auto_downgrade_target_mode": AUTO_DOWNGRADE_TARGET_MODE,
            "telegram_enabled": TELEGRAM_ENABLED,
            "telegram_configured": telegram_configured(),
            },
        },
    }


def validate_pre_trade_exposure(body: Dict[str, Any], risk_pct_used: float) -> Dict[str, Any]:
    if not exposure_limits_enabled():
        return {
            "ok": True,
            "reason": "EXPOSURE_GUARD_DISABLED",
            "details": {
                "limits": {
                    "max_total_position_value_usdt": MAX_TOTAL_POSITION_VALUE_USDT,
                    "max_symbol_position_value_usdt": MAX_SYMBOL_POSITION_VALUE_USDT,
                    "max_equity_usage_pct": MAX_EQUITY_USAGE_PCT,
                    "max_leverage_exposure_pct": MAX_LEVERAGE_EXPOSURE_PCT,
            "post_order_verify_enabled": POST_ORDER_VERIFY_ENABLED,
            "post_order_verify_retries": POST_ORDER_VERIFY_RETRIES,
            "post_order_verify_sleep_sec": POST_ORDER_VERIFY_SLEEP_SEC,
            "auto_close_on_protection_missing": AUTO_CLOSE_ON_PROTECTION_MISSING,
                },
            },
        }

    estimate = estimate_new_order_exposure(body, risk_pct_used)
    if not estimate.get("ok"):
        return estimate

    details = estimate["details"]
    reasons = []

    projected_total = float(details.get("projected_total_position_value", 0.0) or 0.0)
    projected_symbol = float(details.get("projected_symbol_position_value", 0.0) or 0.0)
    projected_equity_usage_pct = details.get("projected_equity_usage_pct")
    projected_leverage_exposure_pct = details.get("projected_leverage_exposure_pct")

    if MAX_TOTAL_POSITION_VALUE_USDT > 0 and projected_total > MAX_TOTAL_POSITION_VALUE_USDT:
        reasons.append(
            f"MAX_TOTAL_POSITION_VALUE_EXCEEDED_{projected_total:.4f}_MAX_{MAX_TOTAL_POSITION_VALUE_USDT:.4f}"
        )

    if MAX_SYMBOL_POSITION_VALUE_USDT > 0 and projected_symbol > MAX_SYMBOL_POSITION_VALUE_USDT:
        reasons.append(
            f"MAX_SYMBOL_POSITION_VALUE_EXCEEDED_{projected_symbol:.4f}_MAX_{MAX_SYMBOL_POSITION_VALUE_USDT:.4f}"
        )

    if (
        MAX_EQUITY_USAGE_PCT > 0
        and projected_equity_usage_pct is not None
        and projected_equity_usage_pct > MAX_EQUITY_USAGE_PCT
    ):
        reasons.append(
            f"MAX_EQUITY_USAGE_EXCEEDED_{projected_equity_usage_pct:.4f}%_MAX_{MAX_EQUITY_USAGE_PCT:.4f}%"
        )

    if (
        MAX_LEVERAGE_EXPOSURE_PCT > 0
        and projected_leverage_exposure_pct is not None
        and projected_leverage_exposure_pct > MAX_LEVERAGE_EXPOSURE_PCT
    ):
        reasons.append(
            f"MAX_LEVERAGE_EXPOSURE_EXCEEDED_{projected_leverage_exposure_pct:.4f}%_MAX_{MAX_LEVERAGE_EXPOSURE_PCT:.4f}%"
        )

    if reasons:
        return {
            "ok": False,
            "reason": ";".join(reasons),
            "details": details,
        }

    return {
        "ok": True,
        "reason": "OK",
        "details": details,
    }


# ============================================================
# POST-ORDER PROTECTION VERIFICATION
# ============================================================

def _is_positive_number(value: Any) -> bool:
    try:
        return value is not None and value != "" and float(value) > 0
    except Exception:
        return False


def validate_post_order_protection(symbol: str) -> Dict[str, Any]:
    symbol = normalize_symbol(symbol)

    if not POST_ORDER_VERIFY_ENABLED:
        return {
            "ok": True,
            "reason": "POST_ORDER_VERIFY_DISABLED",
            "details": {
                "symbol": symbol,
                "enabled": POST_ORDER_VERIFY_ENABLED,
            },
        }

    attempts = max(1, POST_ORDER_VERIFY_RETRIES)
    sleep_sec = max(0.0, POST_ORDER_VERIFY_SLEEP_SEC)
    last_details: Dict[str, Any] = {}

    for attempt in range(1, attempts + 1):
        position = get_position_linear(symbol)
        size = abs(float(position.get("size", "0") or 0.0))
        side = position.get("side") or ""
        stop_loss = position.get("stopLoss", "")
        take_profit = position.get("takeProfit", "")

        open_orders = get_open_orders(symbol)
        active_orders = [
            order for order in open_orders
            if str(order.get("orderStatus", "")).lower() in {"new", "partiallyfilled", "untriggered"}
        ]
        reduce_only_orders = [
            order for order in active_orders
            if str(order.get("reduceOnly", "")).lower() == "true"
        ]
        reduce_only_limit_orders = [
            order for order in reduce_only_orders
            if str(order.get("orderType", "")).lower() == "limit"
        ]

        has_position = size > 0
        has_sl = _is_positive_number(stop_loss)
        has_tp = len(reduce_only_limit_orders) > 0 or _is_positive_number(take_profit)

        missing = []
        if not has_position:
            missing.append("NO_OPEN_POSITION_AFTER_ENTRY")
        if has_position and not has_sl:
            missing.append("MISSING_STOP_LOSS")
        if has_position and not has_tp:
            missing.append("MISSING_REDUCE_ONLY_TAKE_PROFIT_ORDER")

        last_details = {
            "symbol": symbol,
            "attempt": attempt,
            "attempts": attempts,
            "position": {
                "side": side,
                "size": size,
                "avgPrice": position.get("avgPrice", ""),
                "markPrice": position.get("markPrice", ""),
                "stopLoss": stop_loss,
                "takeProfit": take_profit,
            },
            "open_orders_count": len(open_orders),
            "active_orders_count": len(active_orders),
            "reduce_only_orders_count": len(reduce_only_orders),
            "reduce_only_limit_orders_count": len(reduce_only_limit_orders),
            "has_position": has_position,
            "has_sl": has_sl,
            "has_tp": has_tp,
            "missing": missing,
            "auto_close_on_protection_missing": AUTO_CLOSE_ON_PROTECTION_MISSING,
        }

        if has_position and has_sl and has_tp:
            return {
                "ok": True,
                "reason": "PROTECTION_VERIFIED",
                "details": last_details,
            }

        if attempt < attempts and sleep_sec > 0:
            time.sleep(sleep_sec)

    return {
        "ok": False,
        "reason": ";".join(last_details.get("missing", [])) or "PROTECTION_VERIFY_FAILED",
        "details": last_details,
    }



# ============================================================
# STRATEGY ADMIN / TRADE LIMITS / NOTIFICATIONS / LIFECYCLE / DASHBOARD V2
# ============================================================

def telegram_configured() -> bool:
    return bool(TELEGRAM_ENABLED and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def notify_event(title: str, message: str, important: bool = False) -> Dict[str, Any]:
    if not telegram_configured():
        return {"ok": False, "sent": False, "reason": "TELEGRAM_DISABLED_OR_NOT_CONFIGURED"}

    text = f"{title}\n{message}"
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text[:3900],
        "disable_web_page_preview": True,
    }

    try:
        resp = client.post(url, json=payload, timeout=10.0)
        if resp.status_code >= 400:
            log(f"[WARN] Telegram notify failed: {resp.status_code} {resp.text}")
            return {"ok": False, "sent": False, "reason": resp.text}
        return {"ok": True, "sent": True}
    except Exception as exc:
        log(f"[WARN] Telegram notify exception: {exc}")
        return {"ok": False, "sent": False, "reason": str(exc)}


def safe_notify_event(title: str, message: str, important: bool = False) -> Dict[str, Any]:
    """Best-effort Telegram notification. Never breaks trading flow."""
    try:
        return notify_event(title=title, message=message, important=important)
    except Exception as exc:
        log(f"[WARN] safe_notify_event failed: {exc}")
        return {"ok": False, "sent": False, "reason": str(exc)}


def short_event_line(strategy: str, symbol: str, side: str, mode: str = "", reason: str = "") -> str:
    parts = [str(strategy or "UNKNOWN"), normalize_symbol(symbol or ""), str(side or "").upper()]
    if mode:
        parts.append(f"mode={mode}")
    if reason:
        parts.append(f"reason={reason}")
    return " | ".join(parts)


def format_daily_report_message(days: int = 1) -> str:
    safe_days = max(1, min(days, 30))

    try:
        perf = build_performance_report(days=safe_days)
    except Exception as exc:
        perf = {"error": str(exc), "orders": {}, "event_count": 0, "by_decision": {}, "by_symbol": {}}

    try:
        health = build_strategy_health(days=safe_days)
    except Exception as exc:
        health = {"error": str(exc), "status_counts": {}, "items": []}

    orders = perf.get("orders", {}) or {}
    pnl = perf.get("bybit_closed_pnl", {}) or {}
    open_risk = perf.get("open_risk", {}) or {}

    lines = [
        f"Window: {safe_days} day(s)",
        f"Time: {now_iso()}",
        "",
        "Performance:",
        f"- events: {perf.get('event_count', 0)}",
        f"- order_sent: {orders.get('order_sent', 0)}",
        f"- paper_logged: {orders.get('paper_logged', 0)}",
        f"- order_failed: {orders.get('order_failed', 0)}",
        f"- rejected: {orders.get('rejected', 0)}",
        f"- quality_rejected: {orders.get('order_quality_rejected', 0)}",
        f"- price_deviation_rejected: {orders.get('price_deviation_rejected', 0)}",
        f"- exposure_rejected: {orders.get('exposure_rejected', 0)}",
        f"- duplicate_signal_rejected: {orders.get('duplicate_signal_rejected', 0)}",
        f"- duplicate_alert_rejected: {orders.get('duplicate_alert_rejected', 0)}",
        "",
        "Closed PnL:",
        f"- trades: {pnl.get('trades', 0)}",
        f"- net_pnl: {fmt_num(pnl.get('net_pnl'))}",
        f"- PF: {fmt_num(pnl.get('profit_factor'), 3)}",
        f"- win_rate: {fmt_num(pnl.get('win_rate'), 2)}%",
        "",
        "Open risk:",
        f"- open_positions: {open_risk.get('open_positions', 0)}",
        f"- open_value: {fmt_num(open_risk.get('total_position_value'))}",
        f"- unrealized_pnl: {fmt_num(open_risk.get('total_unrealized_pnl'))}",
        "",
        "Health:",
        f"- GOOD: {(health.get('status_counts') or {}).get('GOOD', 0)}",
        f"- WATCH: {(health.get('status_counts') or {}).get('WATCH', 0)}",
        f"- BAD: {(health.get('status_counts') or {}).get('BAD', 0)}",
    ]

    bad_watch_items = []
    for item in (health.get("items") or [])[:10]:
        hdata = item.get("health", {}) or {}
        status = hdata.get("status")
        if status in {"BAD", "WATCH"}:
            bad_watch_items.append(
                f"- {item.get('strategy')} {item.get('symbol')} {item.get('side')} {item.get('mode')}: {status} score={hdata.get('score')}"
            )

    if bad_watch_items:
        lines += ["", "Watchlist:"] + bad_watch_items[:5]

    if perf.get("error"):
        lines += ["", f"Performance report error: {perf.get('error')}"]
    if health.get("error"):
        lines += ["", f"Health report error: {health.get('error')}"]

    return "\n".join(lines)


def get_recent_trade_events(days: int = 1) -> list[Dict[str, Any]]:
    if supabase_enabled():
        try:
            return fetch_supabase_logs_since(days=days, limit=10000)
        except Exception as exc:
            log(f"[WARN] Supabase recent event fetch failed: {exc}")
    return read_trade_log_rows(limit=0)


def is_order_sent_event(row: Dict[str, Any]) -> bool:
    status = str(row.get("status") or "").lower()
    decision = str(row.get("decision") or "").upper()
    return status == "order_sent" or decision in {"ACCEPTED_MICRO", "ACCEPTED_LIVE"}


def count_daily_orders(symbol: Optional[str] = None, strategy: Optional[str] = None) -> int:
    rows = get_recent_trade_events(days=1)
    count = 0
    sym = normalize_symbol(symbol) if symbol else None
    for row in rows:
        if not is_order_sent_event(row):
            continue
        if sym and normalize_symbol(row.get("symbol", "")) != sym:
            continue
        if strategy and str(row.get("strategy")) != str(strategy):
            continue
        count += 1
    return count


def get_closed_pnl_losses(symbol: str, days: int = 1) -> Dict[str, Any]:
    start_ms, end_ms = utc_range_last_days(days)
    rows = get_closed_pnl(start_ms=start_ms, end_ms=end_ms, symbol=symbol)
    losses = []
    for row in rows:
        pnl = float(row.get("closedPnl", "0") or 0.0)
        if pnl < 0:
            losses.append(row)
    return {"rows": rows, "losses": losses, "loss_count": len(losses)}


def get_consecutive_losses(symbol: str, days: int = 7) -> int:
    start_ms, end_ms = utc_range_last_days(days)
    rows = get_closed_pnl(start_ms=start_ms, end_ms=end_ms, symbol=symbol)
    try:
        rows = sorted(rows, key=lambda r: int(r.get("updatedTime") or r.get("createdTime") or 0), reverse=True)
    except Exception:
        pass

    streak = 0
    for row in rows:
        pnl = float(row.get("closedPnl", "0") or 0.0)
        if pnl < 0:
            streak += 1
        elif pnl > 0:
            break
    return streak


def get_configured_trade_limits(strategy: str, symbol: str, side: str) -> Dict[str, Any]:
    state = load_state()
    side_cfg = get_side_config_copy(state, strategy, symbol, side)
    global_cfg = state.get("global", {})

    def int_limit(key: str, default: int) -> int:
        value = side_cfg.get(key, global_cfg.get(key, default))
        try:
            return int(value or 0)
        except Exception:
            return int(default or 0)

    return {
        "max_daily_trades_global": int(global_cfg.get("max_daily_trades_global", MAX_DAILY_TRADES_GLOBAL) or 0),
        "max_daily_trades_per_symbol": int_limit("max_daily_trades", MAX_DAILY_TRADES_PER_SYMBOL),
        "max_daily_losses_per_symbol": int_limit("max_daily_losses", MAX_DAILY_LOSSES_PER_SYMBOL),
        "max_consecutive_losses": int_limit("max_consecutive_losses", MAX_CONSECUTIVE_LOSSES),
    }


def validate_trade_limits(body: Dict[str, Any]) -> Dict[str, Any]:
    strategy = str(body.get("strategy", "UNKNOWN"))
    symbol = normalize_symbol(body.get("symbol", ""))
    side = normalize_side(body.get("side", ""))
    limits = get_configured_trade_limits(strategy, symbol, side)

    reasons = []
    daily_global = count_daily_orders()
    daily_symbol = count_daily_orders(symbol=symbol)
    losses_data = get_closed_pnl_losses(symbol=symbol, days=1)
    consecutive_losses = get_consecutive_losses(symbol=symbol, days=7)

    if limits["max_daily_trades_global"] > 0 and daily_global >= limits["max_daily_trades_global"]:
        reasons.append(f"MAX_DAILY_TRADES_GLOBAL_REACHED_{daily_global}_MAX_{limits['max_daily_trades_global']}")

    if limits["max_daily_trades_per_symbol"] > 0 and daily_symbol >= limits["max_daily_trades_per_symbol"]:
        reasons.append(f"MAX_DAILY_TRADES_SYMBOL_REACHED_{daily_symbol}_MAX_{limits['max_daily_trades_per_symbol']}")

    if limits["max_daily_losses_per_symbol"] > 0 and losses_data["loss_count"] >= limits["max_daily_losses_per_symbol"]:
        reasons.append(f"MAX_DAILY_LOSSES_SYMBOL_REACHED_{losses_data['loss_count']}_MAX_{limits['max_daily_losses_per_symbol']}")

    if limits["max_consecutive_losses"] > 0 and consecutive_losses >= limits["max_consecutive_losses"]:
        reasons.append(f"MAX_CONSECUTIVE_LOSSES_REACHED_{consecutive_losses}_MAX_{limits['max_consecutive_losses']}")

    return {
        "ok": len(reasons) == 0,
        "reason": "OK" if not reasons else ";".join(reasons),
        "details": {
            "strategy": strategy,
            "symbol": symbol,
            "side": side,
            "limits": limits,
            "daily_global_order_count": daily_global,
            "daily_symbol_order_count": daily_symbol,
            "daily_symbol_loss_count": losses_data["loss_count"],
            "consecutive_losses": consecutive_losses,
        },
    }


def auto_downgrade_strategy(
    strategy: str,
    symbol: str,
    side: str,
    trigger: str,
    reason: str,
) -> Dict[str, Any]:
    if not AUTO_DOWNGRADE_ENABLED:
        return {"ok": False, "changed": False, "reason": "AUTO_DOWNGRADE_DISABLED"}

    target_mode = AUTO_DOWNGRADE_TARGET_MODE.upper()
    if target_mode not in {"OFF", "PAPER", "MICRO", "LIVE"}:
        target_mode = "PAPER"

    try:
        state = load_state()
        current = get_side_config_copy(state, strategy, symbol, side)
        current_mode = str(current.get("mode", "OFF")).upper()
        if current_mode in {"OFF", target_mode}:
            return {"ok": True, "changed": False, "reason": "NO_CHANGE_NEEDED", "current_mode": current_mode}

        result = set_strategy_side_config(
            strategy=strategy,
            symbol=symbol,
            side=side,
            mode=target_mode,
            reason=f"auto_downgrade:{trigger}:{reason}",
        )

        if NOTIFY_AUTO_DOWNGRADE:
            safe_notify_event(
                "⚠️ Auto downgrade executed",
                f"{strategy} {normalize_symbol(symbol)} {normalize_side(side)}: {current_mode} → {target_mode}\nTrigger: {trigger}\nReason: {reason}",
                important=True,
            )

        return {"ok": True, "changed": True, "result": result}
    except Exception as exc:
        log(f"[WARN] auto downgrade failed: {exc}")
        return {"ok": False, "changed": False, "reason": str(exc)}


def build_order_lifecycle(symbol: Optional[str] = None, days: int = 7) -> Dict[str, Any]:
    safe_days = max(1, min(days, 30))
    sym = normalize_symbol(symbol) if symbol else None
    rows = get_recent_trade_events(days=safe_days)
    if sym:
        rows = [row for row in rows if normalize_symbol(row.get("symbol", "")) == sym]

    latest_rows = rows[:100]
    position = None
    open_orders = []
    protection = None
    if sym:
        try:
            position = get_position_linear(sym)
        except Exception as exc:
            position = {"error": str(exc)}
        try:
            open_orders = get_open_orders(sym)
        except Exception as exc:
            open_orders = [{"error": str(exc)}]
        try:
            protection = validate_post_order_protection(sym)
        except Exception as exc:
            protection = {"ok": False, "reason": str(exc)}

    return {
        "days": safe_days,
        "symbol": sym,
        "events_count": len(rows),
        "latest_events": latest_rows,
        "position": position,
        "open_orders": open_orders,
        "protection": protection,
        "summary": summarize_supabase_rows(rows) if rows else {},
    }


def build_dashboard_v2_html(secret: str, days: int = 7) -> str:
    state = load_state()
    safe_days = max(1, min(days, 30))
    health = build_strategy_health(days=safe_days) if supabase_enabled() else {"items": [], "status_counts": {}}
    open_risk = summarize_open_risk()

    control_rows = []
    strategies = state.get("strategies", {})
    for strategy, strategy_cfg in strategies.items():
        for symbol, symbol_cfg in strategy_cfg.get("symbols", {}).items():
            for side in ["LONG", "SHORT"]:
                side_cfg = symbol_cfg.get(side, {})
                if not side_cfg:
                    continue
                mode = str(side_cfg.get("mode", "OFF")).upper()
                risk_pct = side_cfg.get("risk_pct", 0.0)
                buttons = []
                for new_mode in ["OFF", "PAPER", "MICRO"]:
                    buttons.append(
                        f'<form method="get" action="/strategy_side_update_form?secret={h(secret)}&strategy={h(strategy)}&symbol={h(symbol)}&side={h(side)}&mode={new_mode}">'
                        f'<button class="secondary" type="submit">{new_mode}</button></form>'
                    )
                risk_form = (
                    f'<form method="get" action="/strategy_side_update_form?secret={h(secret)}&strategy={h(strategy)}&symbol={h(symbol)}&side={h(side)}">'
                    f'<input name="risk_pct" value="{h(risk_pct)}" style="width:70px;padding:6px;border:1px solid #ddd;border-radius:6px;">'
                    f'<button class="secondary" type="submit">Risk</button></form>'
                )
                control_rows.append([
                    h(strategy), h(symbol), h(side), html_badge(mode), h(risk_pct), " ".join(buttons), risk_form
                ])

    open_rows = []
    for symbol, data in open_risk.get("by_symbol", {}).items():
        prot = {"ok": None, "reason": ""}
        try:
            prot = validate_post_order_protection(symbol)
        except Exception as exc:
            prot = {"ok": False, "reason": str(exc)}
        open_rows.append([
            h(symbol), h(data.get("side")), fmt_num(data.get("size")), fmt_num(data.get("position_value")),
            fmt_num(data.get("unrealized_pnl")), html_badge("PROTECTED" if prot.get("ok") else "MISSING"), h(prot.get("reason")),
            f'<a href="/order_lifecycle?secret={h(secret)}&symbol={h(symbol)}&days={safe_days}">lifecycle</a>'
        ])

    health_rows = []
    for item in health.get("items", []):
        hd = item.get("health", {})
        health_rows.append([
            h(item.get("strategy")), h(item.get("symbol")), h(item.get("side")), html_badge(item.get("mode")),
            h(item.get("event_count")), h(item.get("order_sent")), h(item.get("order_failed")),
            h(item.get("exposure_rejected", 0)), h(item.get("trade_limit_rejected", 0)), html_badge(hd.get("status")), h(hd.get("score")),
            h(", ".join(hd.get("reasons", [])))
        ])

    return f"""
    <!doctype html>
    <html><head><meta charset="utf-8"><title>Trading Control Center v3.5.0</title>
    <style>
    body{{font-family:Arial,Helvetica,sans-serif;margin:24px;background:#f6f8fb;color:#1f2937}}
    table{{width:100%;border-collapse:collapse;background:white;border-radius:12px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:24px}}
    th,td{{padding:10px 12px;border-bottom:1px solid #e5e7eb;text-align:left;font-size:13px;vertical-align:top}}
    th{{background:#111827;color:white}} .card{{background:white;border-radius:12px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:18px}}
    .badge{{display:inline-block;padding:3px 8px;border-radius:999px;font-size:12px;font-weight:700}} .good{{background:#dcfce7;color:#166534}} .watch{{background:#fef3c7;color:#92400e}} .bad{{background:#fee2e2;color:#991b1b}} .neutral{{background:#e5e7eb;color:#374151}}
    button.secondary{{background:#111827;color:white;border:0;border-radius:8px;padding:7px 10px;font-weight:700;margin:2px;cursor:pointer}}
    .nav a{{display:inline-block;margin:0 8px 8px 0;padding:8px 12px;border-radius:8px;background:#111827;color:white;text-decoration:none;font-size:13px}}
    </style></head><body>
    <h1>Trading Control Center v3.5.0</h1>
    <div class="nav"><a href="/dashboard?secret={h(secret)}&days={safe_days}">Classic dashboard</a><a href="/risk_status?secret={h(secret)}">Risk JSON</a><a href="/strategy_state?secret={h(secret)}">Strategy JSON</a></div>
    <div class="card"><b>Runtime:</b> real_orders={h(ENABLE_REAL_ORDERS)} · telegram={h(telegram_configured())} · auto_downgrade={h(AUTO_DOWNGRADE_ENABLED)} · open_positions={h(open_risk.get('open_positions'))} · open_value={fmt_num(open_risk.get('total_position_value'))}</div>
    <h2>Strategy Controls</h2>{html_table(['Strategy','Symbol','Side','Mode','Risk %','Mode actions','Risk action'], control_rows)}
    <h2>Open Positions & Protection</h2>{html_table(['Symbol','Side','Size','Position Value','Unrealized PnL','Protection','Reason','Lifecycle'], open_rows)}
    <h2>Strategy Health</h2>{html_table(['Strategy','Symbol','Side','Mode','Events','Orders','Failures','Exposure Rejects','Trade Limit Rejects','Health','Score','Reasons'], health_rows)}
    </body></html>
    """

# ============================================================
# RISK ENGINE
# ============================================================

def risk_engine_decision(body: Dict[str, Any]) -> Dict[str, Any]:
    state = load_state()

    strategy = body.get("strategy")
    if not strategy:
        return {
            "allow_order": False,
            "mode": "OFF",
            "risk_pct_used": 0.0,
            "decision": "REJECTED",
            "reason": "MISSING_STRATEGY",
        }

    symbol = normalize_symbol(body.get("symbol", ""))
    side = normalize_side(body.get("side", ""))

    cfg = get_strategy_side_config(
        state=state,
        strategy=strategy,
        symbol=symbol,
        side=side,
    )

    mode = cfg["mode"]
    risk_pct_used = float(cfg["risk_pct"])
    reason = cfg["reason"]

    if mode == "OFF":
        return {
            "allow_order": False,
            "mode": "OFF",
            "risk_pct_used": 0.0,
            "decision": "REJECTED",
            "reason": reason,
        }

    if mode == "PAPER":
        return {
            "allow_order": False,
            "mode": "PAPER",
            "risk_pct_used": 0.0,
            "decision": "PAPER_LOGGED",
            "reason": "PAPER_MODE",
        }

    if mode in {"MICRO", "LIVE"}:
        closed_pnl_limit_reason = check_closed_pnl_limits(state)
        if closed_pnl_limit_reason:
            return {
                "allow_order": False,
                "mode": mode,
                "risk_pct_used": 0.0,
                "decision": "REJECTED",
                "reason": closed_pnl_limit_reason,
            }

        open_unrealized_limit_reason = check_open_unrealized_limits(state, symbol)
        if open_unrealized_limit_reason:
            return {
                "allow_order": False,
                "mode": mode,
                "risk_pct_used": 0.0,
                "decision": "REJECTED",
                "reason": open_unrealized_limit_reason,
            }

        position_limit_reason = check_position_limits(state, symbol)
        if position_limit_reason:
            return {
                "allow_order": False,
                "mode": mode,
                "risk_pct_used": 0.0,
                "decision": "REJECTED",
                "reason": position_limit_reason,
            }

        return {
            "allow_order": True,
            "mode": mode,
            "risk_pct_used": risk_pct_used,
            "decision": f"ACCEPTED_{mode}",
            "reason": "RISK_ENGINE_APPROVED",
        }

    return {
        "allow_order": False,
        "mode": mode,
        "risk_pct_used": 0.0,
        "decision": "REJECTED",
        "reason": f"UNKNOWN_MODE_{mode}",
    }


# ============================================================
# SECURITY
# ============================================================

def verify_secret(request: Request, body: Dict[str, Any]) -> None:
    header_secret = request.headers.get("x-alert-secret") or request.headers.get("X-Alert-Secret")
    body_secret = body.get("secret")

    if SHARED_SECRET and (header_secret == SHARED_SECRET or body_secret == SHARED_SECRET):
        return

    raise HTTPException(401, "Unauthorized")


# ============================================================
# DRAWDOWN GUARD
# ============================================================

def guard_check_block() -> bool:
    if not _guard["enabled"]:
        return False

    if _guard["baseline"] is None:
        _guard["baseline"] = get_equity_usdt()
        _guard["start_date"] = int(time.time())

    equity = get_equity_usdt()
    _guard["equity_now"] = equity

    drawdown_usd = _guard["baseline"] - equity
    drawdown_pct = (drawdown_usd / _guard["baseline"] * 100.0) if _guard["baseline"] else 0.0

    _guard["drawdown_usd"] = max(0.0, drawdown_usd)
    _guard["drawdown_pct"] = max(0.0, drawdown_pct)

    limit_hit = False

    if _guard["limit_pct"] is not None and drawdown_pct >= _guard["limit_pct"]:
        limit_hit = True

    if _guard["limit_usd"] is not None and drawdown_usd >= _guard["limit_usd"]:
        limit_hit = True

    _guard["block"] = limit_hit
    return limit_hit


# ============================================================
# EMERGENCY ACTIONS
# ============================================================

def cancel_all_orders_for_symbol(symbol: Optional[str] = None) -> Dict[str, Any]:
    req: Dict[str, Any] = {
        "category": "linear",
        "settleCoin": "USDT",
    }

    if symbol:
        req.pop("settleCoin", None)
        req["symbol"] = normalize_symbol(symbol)

    log(f"[REQ] order/cancel-all: {req}")
    resp = bybit("POST", "/v5/order/cancel-all", req)
    log(f"[RESP] order/cancel-all: {resp}")

    return resp


def close_position_market(position: Dict[str, Any]) -> Dict[str, Any]:
    symbol = normalize_symbol(position.get("symbol", ""))
    side = position.get("side", "")
    size = abs(float(position.get("size", "0") or 0.0))

    if not symbol or not side or size <= 0:
        raise HTTPException(400, f"Invalid open position data: {position}")

    _, lot_step, min_qty = get_instrument(symbol)

    close_side = opposite_bybit_side(side)
    qty_rounded = max(round_step(size, lot_step), min_qty)

    link_id = f"EMERG-CLOSE-{symbol}-{now_ms()}"

    req = {
        "category": "linear",
        "symbol": symbol,
        "side": close_side,
        "orderType": "Market",
        "qty": fmt_qty(qty_rounded),
        "timeInForce": "IOC",
        "reduceOnly": True,
        "orderLinkId": link_id,
    }

    log(f"[REQ] emergency close position: {req}")
    resp = bybit("POST", "/v5/order/create", req)
    log(f"[RESP] emergency close position: {resp}")

    order_id = ""
    try:
        order_id = resp.get("result", {}).get("orderId", "")
    except Exception:
        order_id = ""

    write_system_log(
        action="emergency_close_position",
        symbol=symbol,
        side=close_side,
        decision="EMERGENCY_CLOSE_SENT",
        reason=f"Closed {symbol} {side} size={size}",
        order_id=order_id,
        status="order_sent",
        extra={
            "position": position,
            "request": req,
            "response": resp,
        },
    )

    return {
        "symbol": symbol,
        "original_side": side,
        "close_side": close_side,
        "size": size,
        "qty_sent": qty_rounded,
        "order_id": order_id,
        "response": resp,
    }


def emergency_close_symbol_impl(symbol: str) -> Dict[str, Any]:
    symbol = normalize_symbol(symbol)

    try:
        cancel_resp = cancel_all_orders_for_symbol(symbol=symbol)
        write_system_log(
            action="cancel_orders_before_emergency_close_symbol",
            symbol=symbol,
            side="",
            decision="CANCEL_ALL_ORDERS_SENT",
            reason=f"Canceled open orders for {symbol} before emergency close",
            status="order_sent",
            extra={"response": cancel_resp},
        )
    except Exception as exc:
        cancel_resp = {"error": str(exc)}
        write_system_log(
            action="cancel_orders_before_emergency_close_symbol",
            symbol=symbol,
            side="",
            decision="ORDER_FAILED",
            reason=f"Cancel orders failed before emergency close: {exc}",
            status="error",
        )

    pos = get_position_linear(symbol)
    size = abs(float(pos.get("size", "0") or 0.0))

    if size <= 0:
        write_system_log(
            action="emergency_close_symbol",
            symbol=symbol,
            side="",
            decision="REJECTED",
            reason="NO_OPEN_POSITION",
            status="logged",
            extra={"position": pos},
        )
        return {
            "symbol": symbol,
            "closed": False,
            "reason": "NO_OPEN_POSITION",
            "cancel_orders_response": cancel_resp,
            "position": pos,
        }

    close_resp = close_position_market(pos)

    return {
        "symbol": symbol,
        "closed": True,
        "cancel_orders_response": cancel_resp,
        "close_response": close_resp,
    }


def emergency_close_all_impl() -> Dict[str, Any]:
    try:
        cancel_resp = cancel_all_orders_for_symbol(symbol=None)
        write_system_log(
            action="cancel_orders_before_emergency_close_all",
            symbol="SYSTEM",
            side="",
            decision="CANCEL_ALL_ORDERS_SENT",
            reason="Canceled all open orders before emergency close all",
            status="order_sent",
            extra={"response": cancel_resp},
        )
    except Exception as exc:
        cancel_resp = {"error": str(exc)}
        write_system_log(
            action="cancel_orders_before_emergency_close_all",
            symbol="SYSTEM",
            side="",
            decision="ORDER_FAILED",
            reason=f"Cancel all orders failed before emergency close all: {exc}",
            status="error",
        )

    positions = get_all_open_positions()
    results = []

    for position in positions:
        try:
            results.append(close_position_market(position))
        except Exception as exc:
            symbol = normalize_symbol(position.get("symbol", "UNKNOWN"))
            write_system_log(
                action="emergency_close_all_position_error",
                symbol=symbol,
                side=position.get("side", ""),
                decision="ORDER_FAILED",
                reason=str(exc),
                status="error",
                extra={"position": position},
            )
            results.append({
                "symbol": symbol,
                "error": str(exc),
                "position": position,
            })

    return {
        "cancel_orders_response": cancel_resp,
        "positions_found": len(positions),
        "close_results": results,
    }


# ============================================================
# ROUTES
# ============================================================

@app.get("/", response_class=HTMLResponse)
def root():
    runtime_state = load_runtime_state()
    return f"""
    <h3>TV Webhook ↔ Bybit Risk Engine: OK</h3>
    <p>version: 3.5.0</p>
    <p>real_orders_enabled: {ENABLE_REAL_ORDERS}</p>
    <p>trading_paused: {runtime_state.get("trading_paused")}</p>
    <p>supabase_enabled: {supabase_enabled()}</p>
    <p>telegram_enabled: {TELEGRAM_ENABLED}</p>
    <p>telegram_configured: {telegram_configured()}</p>
    <p>notify_daily_report: {NOTIFY_DAILY_REPORT}</p>
    <p>cooldown_minutes: {ORDER_SIGNAL_COOLDOWN_MINUTES}</p>
    <p>alert_idempotency_lookback_hours: {ORDER_ALERT_IDEMPOTENCY_LOOKBACK_HOURS}</p>
    <p>max_total_position_value_usdt: {MAX_TOTAL_POSITION_VALUE_USDT}</p>
    <p>max_symbol_position_value_usdt: {MAX_SYMBOL_POSITION_VALUE_USDT}</p>
    <p>max_equity_usage_pct: {MAX_EQUITY_USAGE_PCT}</p>
    <p>max_leverage_exposure_pct: {MAX_LEVERAGE_EXPOSURE_PCT}</p>
    <p>post_order_verify_enabled: {POST_ORDER_VERIFY_ENABLED}</p>
    <p>auto_close_on_protection_missing: {AUTO_CLOSE_ON_PROTECTION_MISSING}</p>
    <p>strategy_admin_enabled: {STRATEGY_ADMIN_ENABLED}</p>
    <p>auto_downgrade_enabled: {AUTO_DOWNGRADE_ENABLED}</p>
    <p>telegram_configured: {telegram_configured()}</p>
    <p><a href="/dashboard_v2?secret=REPLACE_WITH_SECRET&days=7">Dashboard v2</a></p>
    <p>time: {now_iso()}</p>
    <p><a href="/dashboard?secret=REPLACE_WITH_SECRET&days=7">Dashboard</a></p>
    """


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(secret: str, days: int = 7):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    return HTMLResponse(
        content=build_dashboard_html(secret=secret, days=days),
        media_type="text/html",
    )


@app.get("/order_quality_config")
def order_quality_config(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    return {
        "ok": True,
        "order_guards": {
            "min_stop_distance_pct": ORDER_MIN_STOP_DISTANCE_PCT,
            "max_stop_distance_pct": ORDER_MAX_STOP_DISTANCE_PCT,
            "min_tp1_rr": ORDER_MIN_TP1_RR,
            "min_tp2_rr": ORDER_MIN_TP2_RR,
            "max_signal_price_deviation_pct": ORDER_MAX_SIGNAL_PRICE_DEVIATION_PCT,
            "duplicate_signal_cooldown_minutes": ORDER_SIGNAL_COOLDOWN_MINUTES,
            "alert_idempotency_lookback_hours": ORDER_ALERT_IDEMPOTENCY_LOOKBACK_HOURS,
            "max_total_position_value_usdt": MAX_TOTAL_POSITION_VALUE_USDT,
            "max_symbol_position_value_usdt": MAX_SYMBOL_POSITION_VALUE_USDT,
            "max_equity_usage_pct": MAX_EQUITY_USAGE_PCT,
            "max_leverage_exposure_pct": MAX_LEVERAGE_EXPOSURE_PCT,
            "post_order_verify_enabled": POST_ORDER_VERIFY_ENABLED,
            "post_order_verify_retries": POST_ORDER_VERIFY_RETRIES,
            "post_order_verify_sleep_sec": POST_ORDER_VERIFY_SLEEP_SEC,
            "auto_close_on_protection_missing": AUTO_CLOSE_ON_PROTECTION_MISSING,
        },
    }


@app.post("/test_order_quality")
async def test_order_quality(request: Request):
    body = await request.json()
    verify_secret(request, body)

    return {
        "ok": True,
        "quality": validate_order_quality(body),
    }


@app.post("/test_price_deviation")
async def test_price_deviation(request: Request):
    body = await request.json()
    verify_secret(request, body)

    return {
        "ok": True,
        "price_deviation": validate_live_price_deviation(body),
    }


@app.post("/test_duplicate_signal")
async def test_duplicate_signal(request: Request):
    body = await request.json()
    verify_secret(request, body)

    return {
        "ok": True,
        "duplicate_signal": validate_duplicate_signal(body),
    }


@app.post("/test_alert_idempotency")
async def test_alert_idempotency(request: Request):
    body = await request.json()
    verify_secret(request, body)

    return {
        "ok": True,
        "alert_idempotency": validate_alert_idempotency(body),
    }


@app.post("/test_exposure")
async def test_exposure(request: Request):
    body = await request.json()
    verify_secret(request, body)

    risk_pct_used = to_float_or_none(body.get("riskPct"))
    if risk_pct_used is None:
        risk_pct_used = 0.0

    return {
        "ok": True,
        "exposure": validate_pre_trade_exposure(body, risk_pct_used),
    }


@app.get("/protection_status")
def protection_status(secret: str, symbol: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    return {
        "ok": True,
        "protection": validate_post_order_protection(symbol),
    }


@app.get("/dashboard_v2", response_class=HTMLResponse)
def dashboard_v2(secret: str, days: int = 7):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return HTMLResponse(content=build_dashboard_v2_html(secret=secret, days=days), media_type="text/html")


@app.get("/strategy_state")
def strategy_state_get(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "state": load_state()}


@app.post("/strategy_state_raw_update")
async def strategy_state_raw_update(request: Request):
    body = await request.json()
    verify_secret(request, body)
    require_strategy_admin()
    new_state = body.get("state")
    if not isinstance(new_state, dict):
        raise HTTPException(400, "Body must contain object field: state")
    before = load_state()
    save_state(new_state)
    write_system_log(
        action="strategy_state_raw_update",
        symbol="SYSTEM",
        decision="STRATEGY_STATE_UPDATED",
        reason="raw_state_update",
        status="logged",
        extra={"before": before, "after": new_state},
    )
    return {"ok": True, "state_saved": True, "state": new_state}


@app.post("/strategy_side_update")
async def strategy_side_update(request: Request):
    body = await request.json()
    verify_secret(request, body)
    result = set_strategy_side_config(
        strategy=body.get("strategy"),
        symbol=body.get("symbol"),
        side=body.get("side"),
        mode=body.get("mode"),
        risk_pct=to_float_or_none(body.get("risk_pct")) if "risk_pct" in body else None,
        extra_updates=body.get("extra") if isinstance(body.get("extra"), dict) else None,
        reason=body.get("reason", "api_update"),
    )
    return {"ok": True, "result": result}


@app.get("/strategy_side_update_form")
@app.post("/strategy_side_update_form")
def strategy_side_update_form(
    secret: str,
    strategy: str,
    symbol: str,
    side: str,
    mode: Optional[str] = None,
    risk_pct: Optional[float] = None,
):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    result = set_strategy_side_config(
        strategy=strategy,
        symbol=symbol,
        side=side,
        mode=mode,
        risk_pct=risk_pct,
        reason="dashboard_v2_form_update",
    )
    return HTMLResponse(
        content=f"<html><body><h3>Updated</h3><pre>{h(result)}</pre><p><a href='/dashboard_v2?secret={h(secret)}&days=7'>Back to dashboard v2</a></p></body></html>",
        media_type="text/html",
    )


@app.get("/trade_limits_status")
def trade_limits_status(secret: str, strategy: str, symbol: str, side: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    payload = {"strategy": strategy, "symbol": symbol, "side": side}
    return {"ok": True, "trade_limits": validate_trade_limits(payload)}


@app.get("/order_lifecycle")
def order_lifecycle(secret: str, symbol: Optional[str] = None, days: int = 7):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "lifecycle": build_order_lifecycle(symbol=symbol, days=days)}


@app.post("/notify_test")
async def notify_test(request: Request):
    body = await request.json()
    verify_secret(request, body)
    result = safe_notify_event("✅ Trading bot test notification", body.get("message", "Notification test OK"), important=True)
    return {"ok": True, "notify": result}


@app.get("/telegram_status")
def telegram_status(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {
        "ok": True,
        "telegram_enabled": TELEGRAM_ENABLED,
        "telegram_configured": telegram_configured(),
        "chat_id_set": bool(TELEGRAM_CHAT_ID),
        "bot_token_set": bool(TELEGRAM_BOT_TOKEN),
        "notify_order_sent": NOTIFY_ORDER_SENT,
        "notify_order_failed": NOTIFY_ORDER_FAILED,
        "notify_rejections": NOTIFY_REJECTIONS,
        "notify_protection_failed": NOTIFY_PROTECTION_FAILED,
        "notify_auto_downgrade": NOTIFY_AUTO_DOWNGRADE,
        "notify_trading_pause": NOTIFY_TRADING_PAUSE,
        "notify_emergency_actions": NOTIFY_EMERGENCY_ACTIONS,
        "notify_daily_report": NOTIFY_DAILY_REPORT,
        "notify_runtime_blocked": NOTIFY_RUNTIME_BLOCKED,
    }


@app.post("/telegram_daily_report")
def telegram_daily_report(secret: str, days: int = 1):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    message = format_daily_report_message(days=days)
    result = safe_notify_event("📊 Daily trading report", message, important=False)
    return {"ok": True, "days": max(1, min(days, 30)), "notify": result, "message": message}


@app.get("/telegram_daily_report")
def telegram_daily_report_get(secret: str, days: int = 1):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    message = format_daily_report_message(days=days)
    result = safe_notify_event("📊 Daily trading report", message, important=False)
    return {"ok": True, "days": max(1, min(days, 30)), "notify": result, "message": message}


@app.post("/trading_pause_on")
def trading_pause_on(secret: str, reason: Optional[str] = None):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    state = set_trading_paused(True, reason=reason or "Manual pause")

    write_system_log(
        action="trading_pause_on",
        symbol="SYSTEM",
        side="",
        decision="RUNTIME_PAUSED",
        reason=state.get("pause_reason") or "Manual pause",
        status="logged",
        extra={"runtime_state": state},
    )

    if NOTIFY_TRADING_PAUSE:
        safe_notify_event(
            "⏸️ Trading paused",
            f"Reason: {state.get('pause_reason') or 'Manual pause'}\nTime: {state.get('paused_at')}",
            important=True,
        )

    return {
        "ok": True,
        "action": "trading_pause_on",
        "runtime_state": state,
    }


@app.post("/trading_pause_off")
def trading_pause_off(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    state = set_trading_paused(False)

    write_system_log(
        action="trading_pause_off",
        symbol="SYSTEM",
        side="",
        decision="RUNTIME_RESUMED",
        reason="Manual resume",
        status="logged",
        extra={"runtime_state": state},
    )

    if NOTIFY_TRADING_PAUSE:
        safe_notify_event(
            "▶️ Trading resumed",
            f"Time: {state.get('resumed_at')}",
            important=True,
        )

    return {
        "ok": True,
        "action": "trading_pause_off",
        "runtime_state": state,
    }


@app.get("/trading_pause_status")
def trading_pause_status(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    return {
        "ok": True,
        "runtime_state": load_runtime_state(),
        "real_orders_enabled": ENABLE_REAL_ORDERS,
        "effective_real_order_execution_enabled": ENABLE_REAL_ORDERS and not is_trading_paused(),
    }


@app.post("/emergency_close_all")
def emergency_close_all(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    result = emergency_close_all_impl()

    if NOTIFY_EMERGENCY_ACTIONS:
        safe_notify_event(
            "🚨 Emergency close ALL executed",
            f"positions_found={result.get('positions_found')}\nTime: {now_iso()}",
            important=True,
        )

    return {
        "ok": True,
        "action": "emergency_close_all",
        "result": result,
    }


@app.post("/emergency_close_symbol")
def emergency_close_symbol(secret: str, symbol: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    result = emergency_close_symbol_impl(symbol)

    if NOTIFY_EMERGENCY_ACTIONS:
        safe_notify_event(
            "🚨 Emergency close symbol executed",
            f"symbol={normalize_symbol(symbol)}\nclosed={result.get('closed')}\nTime: {now_iso()}",
            important=True,
        )

    return {
        "ok": True,
        "action": "emergency_close_symbol",
        "symbol": normalize_symbol(symbol),
        "result": result,
    }


@app.post("/cancel_all_orders")
def cancel_all_orders(secret: str, symbol: Optional[str] = None):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    resp = cancel_all_orders_for_symbol(symbol=symbol)

    write_system_log(
        action="cancel_all_orders",
        symbol=normalize_symbol(symbol) if symbol else "SYSTEM",
        side="",
        decision="CANCEL_ALL_ORDERS_SENT",
        reason=f"Cancel all orders requested. symbol={symbol or 'ALL'}",
        status="order_sent",
        extra={"response": resp},
    )

    if NOTIFY_EMERGENCY_ACTIONS:
        safe_notify_event(
            "⚠️ Cancel all orders executed",
            f"symbol={normalize_symbol(symbol) if symbol else 'ALL'}\nTime: {now_iso()}",
            important=True,
        )

    return {
        "ok": True,
        "action": "cancel_all_orders",
        "symbol": normalize_symbol(symbol) if symbol else None,
        "result": resp,
    }


@app.get("/state")
def state(secret: Optional[str] = None):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return load_state()


@app.get("/logs")
def logs(secret: str, limit: int = 100):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    limit = max(1, min(limit, 1000))
    rows = read_trade_log_rows(limit=limit)

    return {
        "ok": True,
        "count": len(rows),
        "rows": rows,
    }


@app.get("/logs_summary")
def logs_summary(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    return {
        "ok": True,
        "summary": summarize_trade_log(),
    }


@app.get("/logs_csv")
def logs_csv(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    ensure_trade_log()

    with TRADE_LOG_FILE.open("r", encoding="utf-8") as file:
        content = file.read()

    return HTMLResponse(
        content=f"<pre>{content}</pre>",
        media_type="text/html",
    )


@app.get("/db_logs")
def db_logs(secret: str, limit: int = 100):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    if not supabase_enabled():
        return {
            "ok": True,
            "supabase_enabled": False,
            "count": 0,
            "rows": [],
        }

    rows = fetch_supabase_logs(limit=limit)

    return {
        "ok": True,
        "supabase_enabled": True,
        "count": len(rows),
        "rows": rows,
    }


@app.get("/db_logs_summary")
def db_logs_summary(secret: str, limit: int = 1000):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    if not supabase_enabled():
        return {
            "ok": True,
            "supabase_enabled": False,
            "summary": {},
        }

    rows = fetch_supabase_logs(limit=limit)

    return {
        "ok": True,
        "supabase_enabled": True,
        "summary": summarize_supabase_rows(rows),
    }


@app.get("/performance_report")
def performance_report(secret: str, days: int = 1):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    if not supabase_enabled():
        return {
            "ok": False,
            "error": "Supabase is not enabled",
        }

    return {
        "ok": True,
        "report": build_performance_report(days=days),
    }


@app.get("/strategy_health")
def strategy_health(secret: str, days: int = 7):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    if not supabase_enabled():
        return {
            "ok": False,
            "error": "Supabase is not enabled",
        }

    return {
        "ok": True,
        "health": build_strategy_health(days=days),
    }


@app.get("/closed_pnl_summary")
def closed_pnl_summary(secret: str, days: int = 1, symbol: Optional[str] = None):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    days = max(1, min(days, 30))
    start_ms, end_ms = utc_range_last_days(days)

    rows = get_closed_pnl(
        start_ms=start_ms,
        end_ms=end_ms,
        symbol=symbol,
    )

    summary = summarize_closed_pnl(rows)

    return {
        "ok": True,
        "days": days,
        "symbol": normalize_symbol(symbol) if symbol else None,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "summary": summary,
    }


@app.get("/open_risk_summary")
def open_risk_summary(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    return {
        "ok": True,
        "summary": summarize_open_risk(),
    }


@app.get("/risk_status")
def risk_status(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    state_data = load_state()

    closed_pnl_reason = check_closed_pnl_limits(state_data)
    open_unrealized_reason = check_open_unrealized_limits(state_data, symbol="")
    runtime_state = load_runtime_state()

    return {
        "ok": True,
        "real_orders_enabled": ENABLE_REAL_ORDERS,
        "trading_paused": runtime_state.get("trading_paused"),
        "effective_real_order_execution_enabled": ENABLE_REAL_ORDERS and not runtime_state.get("trading_paused"),
        "supabase_enabled": supabase_enabled(),
        "telegram": {
            "enabled": TELEGRAM_ENABLED,
            "configured": telegram_configured(),
            "notify_order_sent": NOTIFY_ORDER_SENT,
            "notify_order_failed": NOTIFY_ORDER_FAILED,
            "notify_rejections": NOTIFY_REJECTIONS,
            "notify_protection_failed": NOTIFY_PROTECTION_FAILED,
            "notify_auto_downgrade": NOTIFY_AUTO_DOWNGRADE,
            "notify_trading_pause": NOTIFY_TRADING_PAUSE,
            "notify_emergency_actions": NOTIFY_EMERGENCY_ACTIONS,
            "notify_daily_report": NOTIFY_DAILY_REPORT,
            "notify_runtime_blocked": NOTIFY_RUNTIME_BLOCKED,
        },
        "order_guards": {
            "min_stop_distance_pct": ORDER_MIN_STOP_DISTANCE_PCT,
            "max_stop_distance_pct": ORDER_MAX_STOP_DISTANCE_PCT,
            "min_tp1_rr": ORDER_MIN_TP1_RR,
            "min_tp2_rr": ORDER_MIN_TP2_RR,
            "max_signal_price_deviation_pct": ORDER_MAX_SIGNAL_PRICE_DEVIATION_PCT,
            "duplicate_signal_cooldown_minutes": ORDER_SIGNAL_COOLDOWN_MINUTES,
            "alert_idempotency_lookback_hours": ORDER_ALERT_IDEMPOTENCY_LOOKBACK_HOURS,
            "max_total_position_value_usdt": MAX_TOTAL_POSITION_VALUE_USDT,
            "max_symbol_position_value_usdt": MAX_SYMBOL_POSITION_VALUE_USDT,
            "max_equity_usage_pct": MAX_EQUITY_USAGE_PCT,
            "max_leverage_exposure_pct": MAX_LEVERAGE_EXPOSURE_PCT,
            "post_order_verify_enabled": POST_ORDER_VERIFY_ENABLED,
            "post_order_verify_retries": POST_ORDER_VERIFY_RETRIES,
            "post_order_verify_sleep_sec": POST_ORDER_VERIFY_SLEEP_SEC,
            "auto_close_on_protection_missing": AUTO_CLOSE_ON_PROTECTION_MISSING,
            "exposure_guard_enabled": exposure_limits_enabled(),
            "strategy_admin_enabled": STRATEGY_ADMIN_ENABLED,
            "max_daily_trades_global": MAX_DAILY_TRADES_GLOBAL,
            "max_daily_trades_per_symbol": MAX_DAILY_TRADES_PER_SYMBOL,
            "max_daily_losses_per_symbol": MAX_DAILY_LOSSES_PER_SYMBOL,
            "max_consecutive_losses": MAX_CONSECUTIVE_LOSSES,
            "auto_downgrade_enabled": AUTO_DOWNGRADE_ENABLED,
            "auto_downgrade_target_mode": AUTO_DOWNGRADE_TARGET_MODE,
            "telegram_enabled": TELEGRAM_ENABLED,
            "telegram_configured": telegram_configured(),
        },
        "closed_pnl_guard": {
            "blocked": closed_pnl_reason is not None,
            "reason": closed_pnl_reason,
        },
        "open_unrealized_guard": {
            "blocked": open_unrealized_reason is not None,
            "reason": open_unrealized_reason,
        },
        "open_risk": summarize_open_risk(),
    }


@app.get("/guard_status")
def guard_status(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "status": _guard}


@app.post("/guard")
async def guard_set(request: Request):
    body = await request.json()
    verify_secret(request, body)

    _guard["enabled"] = bool(body.get("enable", False))
    _guard["limit_pct"] = body.get("limit_pct")
    _guard["limit_usd"] = body.get("limit_usd")

    return ok({"msg": "guard updated"})


@app.get("/position")
def position(symbol: str, secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    symbol = normalize_symbol(symbol)
    pos = get_position_linear(symbol)

    return {"ok": True, "position": pos}


@app.get("/open_positions_count")
def open_positions_count(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    count = get_open_positions_count()

    return {"ok": True, "open_positions_count": count}


@app.post("/set_leverage")
async def set_lev(request: Request):
    body = await request.json()
    verify_secret(request, body)

    symbol = normalize_symbol(body["symbol"])
    leverage = int(body["leverage"])
    resp = set_leverage(symbol, leverage)

    return ok(resp)


# ============================================================
# ORDER EXECUTION
# ============================================================

def execute_bybit_trade(body: Dict[str, Any], risk_pct_used: float) -> Dict[str, Any]:
    exchange = body.get("exchange", "bybit").lower()
    if exchange != "bybit":
        raise HTTPException(400, "Only `bybit` exchange supported")

    symbol = normalize_symbol(body["symbol"])
    side_s = normalize_side(body["side"])

    order_type = body.get("orderType", "Market")
    if order_type != "Market":
        raise HTTPException(400, "Currently only Market entries are supported")

    tp1 = float(body.get("tp1")) if body.get("tp1") is not None else None
    tp2 = float(body.get("tp2")) if body.get("tp2") is not None else None
    sl = float(body.get("sl")) if body.get("sl") is not None else None
    qty_in = body.get("qty")

    tick, lot_step, min_qty = get_instrument(symbol)
    log(f"[INFO] {symbol} tick={tick} lot={lot_step} min_qty={min_qty}")

    if qty_in is not None:
        qty_calc = float(qty_in)
        qty_rounded = max(round_step(qty_calc, lot_step), min_qty)
        log(f"[INFO] sizing=explicit qty={qty_rounded}")
    else:
        if sl is None:
            raise HTTPException(400, "sl is required when qty is not provided")

        equity = get_equity_usdt()
        last_px = get_ticker_last(symbol)
        stop_dist = abs(last_px - sl)

        if stop_dist <= 0:
            raise HTTPException(400, "Invalid stop distance")

        risk_usd = equity * (risk_pct_used / 100.0)
        qty_calc = risk_usd / stop_dist
        qty_rounded = max(round_step(qty_calc, lot_step), min_qty)

        log(
            f"[INFO] sizing=risk engine: equity={equity:.4f} "
            f"riskPctUsed={risk_pct_used:.4f}% riskUsd={risk_usd:.4f} "
            f"lastPx={last_px:.4f} sl={sl:.4f} stopDist={stop_dist:.4f} "
            f"qty_calc={qty_calc:.6f} -> qty_rounded={qty_rounded}"
        )

    desired_side = bybit_order_side(side_s)

    pos = get_position_linear(symbol)
    current_side = pos.get("side") or ""
    current_size = float(pos.get("size", "0") or 0.0)

    actual_qty = qty_rounded

    if current_size > 0 and current_side and current_side != desired_side:
        flip_qty = current_size + qty_rounded
        actual_qty = max(round_step(flip_qty, lot_step), min_qty)
        log(
            f"[FLIP] Existing {current_side} {current_size} -> "
            f"desired {desired_side} {qty_rounded} => sending {desired_side} {actual_qty}"
        )

    link_id = f"TV-{symbol}-{now_ms()}"

    entry_req = {
        "category": "linear",
        "symbol": symbol,
        "side": desired_side,
        "orderType": "Market",
        "qty": fmt_qty(actual_qty),
        "timeInForce": "IOC",
        "reduceOnly": False,
        "orderLinkId": link_id,
    }

    log(f"[REQ] order/create ENTRY: {entry_req}")
    entry_resp = bybit("POST", "/v5/order/create", entry_req)
    log(f"[RESP] order/create ENTRY: {entry_resp}")

    order_id = ""
    try:
        order_id = entry_resp.get("result", {}).get("orderId", "")
    except Exception:
        order_id = ""

    size = 0.0
    side_now = ""

    for i in range(12):
        time.sleep(0.25)
        p = get_position_linear(symbol)
        side_now = p.get("side") or ""
        size = float(p.get("size", "0") or 0.0)
        log(f"[INFO] poll pos {i + 1}/12: side={side_now} size={size}")

        if size > 0.0 and side_now == desired_side:
            break

    if size <= 0.0 or side_now != desired_side:
        log("[WARN] No net position in desired direction after ENTRY; skipping TP/SL.")
        return {
            "msg": "entry ok, but no net position in desired direction; tp/sl skipped",
            "order_id": order_id,
            "entry_resp": entry_resp,
        }

    tp1_share_pct = 30.0
    tp1_qty = round_step(size * (tp1_share_pct / 100.0), lot_step)

    if tp1_qty < min_qty:
        tp1_qty = 0.0

    tp2_qty = round_step(size - tp1_qty, lot_step)

    if tp2_qty < min_qty:
        tp1_qty = 0.0
        tp2_qty = round_step(size, lot_step)

    log(f"[INFO] tp1_qty={tp1_qty} tp2_qty={tp2_qty}")

    if tp1_qty > 0 and tp1 is not None:
        tp1_req = {
            "category": "linear",
            "symbol": symbol,
            "side": opposite_bybit_side(desired_side),
            "orderType": "Limit",
            "price": fmt_price(tp1, tick),
            "qty": fmt_qty(tp1_qty),
            "timeInForce": "GTC",
            "reduceOnly": True,
            "orderLinkId": f"{link_id}-TP1",
        }

        log(f"[REQ] order/create TP1: {tp1_req}")
        try:
            tp1_resp = bybit("POST", "/v5/order/create", tp1_req)
            log(f"[RESP] order/create TP1: {tp1_resp}")
        except HTTPException as err:
            log(f"[ERR] order/create TP1 failed: {err.detail}")

    if tp2_qty > 0 and tp2 is not None:
        tp2_req = {
            "category": "linear",
            "symbol": symbol,
            "side": opposite_bybit_side(desired_side),
            "orderType": "Limit",
            "price": fmt_price(tp2, tick),
            "qty": fmt_qty(tp2_qty),
            "timeInForce": "GTC",
            "reduceOnly": True,
            "orderLinkId": f"{link_id}-TP2",
        }

        log(f"[REQ] order/create TP2: {tp2_req}")
        try:
            tp2_resp = bybit("POST", "/v5/order/create", tp2_req)
            log(f"[RESP] order/create TP2: {tp2_resp}")
        except HTTPException as err:
            log(f"[ERR] order/create TP2 failed: {err.detail}")

    if sl is not None:
        sl_req = {
            "category": "linear",
            "symbol": symbol,
            "stopLoss": fmt_price(sl, tick),
            "slTriggerBy": "MarkPrice",
            "tpslMode": "Full",
            "positionIdx": 0,
        }

        log(f"[REQ] position/trading-stop SL MarkPrice: {sl_req}")

        try:
            sl_resp = bybit("POST", "/v5/position/trading-stop", sl_req)
            log(f"[RESP] position/trading-stop SL: {sl_resp}")
        except HTTPException as err:
            log(f"[WARN] trading-stop MarkPrice failed: {err.detail}")

            sl_req_last = dict(sl_req)
            sl_req_last["slTriggerBy"] = "LastPrice"

            log(f"[REQ] position/trading-stop SL LastPrice: {sl_req_last}")

            try:
                sl_resp_last = bybit("POST", "/v5/position/trading-stop", sl_req_last)
                log(f"[RESP] position/trading-stop SL LastPrice: {sl_resp_last}")
            except HTTPException as err2:
                log(f"[ERR] trading-stop failed both triggers: {err2.detail}")

    return {
        "msg": "entry+tp/sl processed",
        "order_id": order_id,
        "entry_resp": entry_resp,
    }


# ============================================================
# CORE WEBHOOK
# ============================================================

@app.post("/tv")
async def tv_webhook(request: Request):
    raw = await request.body()

    try:
        body = json.loads(raw)
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    if isinstance(body, dict) and body.get("type") == "ping":
        log(f"INCOMING /tv RAW: {json.dumps(sanitize_payload(body))}")
        return ok({"msg": "pong"})

    try:
        safe_raw_for_log = json.dumps(sanitize_payload(json.loads(raw)), ensure_ascii=False)
    except Exception:
        safe_raw_for_log = "<unparseable payload>"

    log(f"INCOMING /tv RAW: {safe_raw_for_log}")

    verify_secret(request, body)

    strategy = body.get("strategy")
    symbol = normalize_symbol(body.get("symbol", ""))
    side = normalize_side(body.get("side", ""))

    body["symbol"] = symbol
    body["side"] = side

    if not strategy:
        write_trade_log(
            body=body,
            mode="OFF",
            risk_pct_used=0.0,
            decision="REJECTED",
            decision_reason="MISSING_STRATEGY",
            status="rejected",
        )
        raise HTTPException(400, "Missing strategy field in payload")

    if guard_check_block():
        write_trade_log(
            body=body,
            mode="OFF",
            risk_pct_used=0.0,
            decision="REJECTED",
            decision_reason="GUARD_DAILY_LOSS_LIMIT",
            status="rejected",
        )
        raise HTTPException(400, "Guard: daily loss limit reached, blocking new orders")

    decision = risk_engine_decision(body)

    mode = decision["mode"]
    risk_pct_used = float(decision["risk_pct_used"])

    if not decision["allow_order"]:
        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision=decision["decision"],
            decision_reason=decision["reason"],
            status="logged",
        )

        return ok(
            {
                "order_sent": False,
                "decision": decision,
            }
        )

    quality = validate_order_quality(body)
    if not quality["ok"]:
        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision="ORDER_QUALITY_REJECTED",
            decision_reason=quality["reason"],
            status="rejected_by_order_quality_guard",
        )

        if NOTIFY_REJECTIONS:
            safe_notify_event(
                "🚫 Order quality rejected signal",
                short_event_line(strategy, symbol, side, mode, quality["reason"]),
                important=False,
            )

        return ok(
            {
                "order_sent": False,
                "decision": {
                    **decision,
                    "allow_order": False,
                    "decision": "ORDER_QUALITY_REJECTED",
                    "reason": quality["reason"],
                },
                "quality": quality,
                "msg": "Risk engine approved, but order quality guard rejected the signal.",
            }
        )

    price_deviation = validate_live_price_deviation(body)
    if not price_deviation["ok"]:
        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision="ORDER_PRICE_DEVIATION_REJECTED",
            decision_reason=price_deviation["reason"],
            status="rejected_by_price_deviation_guard",
        )

        if NOTIFY_REJECTIONS:
            safe_notify_event(
                "🚫 Price deviation rejected signal",
                short_event_line(strategy, symbol, side, mode, price_deviation["reason"]),
                important=False,
            )

        return ok(
            {
                "order_sent": False,
                "decision": {
                    **decision,
                    "allow_order": False,
                    "decision": "ORDER_PRICE_DEVIATION_REJECTED",
                    "reason": price_deviation["reason"],
                },
                "quality": quality,
                "price_deviation": price_deviation,
                "msg": "Risk engine approved, but live price deviation guard rejected the signal.",
            }
        )

    duplicate_signal = validate_duplicate_signal(body)
    if not duplicate_signal["ok"]:
        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision="DUPLICATE_SIGNAL_REJECTED",
            decision_reason=duplicate_signal["reason"],
            status="rejected_by_duplicate_signal_guard",
        )

        if NOTIFY_REJECTIONS:
            safe_notify_event(
                "🚫 Duplicate signal rejected",
                short_event_line(strategy, symbol, side, mode, duplicate_signal["reason"]),
                important=False,
            )

        return ok(
            {
                "order_sent": False,
                "decision": {
                    **decision,
                    "allow_order": False,
                    "decision": "DUPLICATE_SIGNAL_REJECTED",
                    "reason": duplicate_signal["reason"],
                },
                "quality": quality,
                "price_deviation": price_deviation,
                "duplicate_signal": duplicate_signal,
                "msg": "Risk engine approved, but duplicate signal cooldown guard rejected the signal.",
            }
        )

    alert_idempotency = validate_alert_idempotency(body)
    if not alert_idempotency["ok"]:
        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision="DUPLICATE_ALERT_REJECTED",
            decision_reason=alert_idempotency["reason"],
            status="rejected_by_alert_idempotency_guard",
        )

        if NOTIFY_REJECTIONS:
            safe_notify_event(
                "🚫 Duplicate alert rejected",
                short_event_line(strategy, symbol, side, mode, alert_idempotency["reason"]),
                important=False,
            )

        return ok(
            {
                "order_sent": False,
                "decision": {
                    **decision,
                    "allow_order": False,
                    "decision": "DUPLICATE_ALERT_REJECTED",
                    "reason": alert_idempotency["reason"],
                },
                "quality": quality,
                "price_deviation": price_deviation,
                "duplicate_signal": duplicate_signal,
                "alert_idempotency": alert_idempotency,
                "msg": "Risk engine approved, but alert idempotency guard rejected the duplicate barTime alert.",
            }
        )

    exposure = validate_pre_trade_exposure(body, risk_pct_used)
    if not exposure["ok"]:
        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision="EXPOSURE_REJECTED",
            decision_reason=exposure["reason"],
            status="rejected_by_exposure_guard",
        )

        if NOTIFY_REJECTIONS:
            safe_notify_event(
                "🚫 Exposure rejected signal",
                short_event_line(strategy, symbol, side, mode, exposure["reason"]),
                important=True,
            )

        return ok(
            {
                "order_sent": False,
                "decision": {
                    **decision,
                    "allow_order": False,
                    "decision": "EXPOSURE_REJECTED",
                    "reason": exposure["reason"],
                },
                "quality": quality,
                "price_deviation": price_deviation,
                "duplicate_signal": duplicate_signal,
                "alert_idempotency": alert_idempotency,
                "exposure": exposure,
                "msg": "Risk engine approved, but pre-trade exposure guard rejected the signal.",
            }
        )

    trade_limits = validate_trade_limits(body)
    if not trade_limits["ok"]:
        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision="TRADE_LIMIT_REJECTED",
            decision_reason=trade_limits["reason"],
            status="rejected_by_trade_limit_guard",
        )

        if NOTIFY_REJECTIONS:
            safe_notify_event(
                "🚫 Trade limit rejected signal",
                f"{strategy} {symbol} {side}\nReason: {trade_limits['reason']}",
                important=True,
            )

        if AUTO_DOWNGRADE_ON_DAILY_LIMIT:
            auto_downgrade_strategy(strategy, symbol, side, "trade_limit", trade_limits["reason"])

        return ok(
            {
                "order_sent": False,
                "decision": {
                    **decision,
                    "allow_order": False,
                    "decision": "TRADE_LIMIT_REJECTED",
                    "reason": trade_limits["reason"],
                },
                "quality": quality,
                "price_deviation": price_deviation,
                "duplicate_signal": duplicate_signal,
                "alert_idempotency": alert_idempotency,
                "exposure": exposure,
                "trade_limits": trade_limits,
                "msg": "Risk engine approved, but daily/symbol trade limit guard rejected the signal.",
            }
        )

    if is_trading_paused():
        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision="RUNTIME_PAUSED",
            decision_reason="Trading paused by runtime kill switch",
            status="blocked_by_runtime_pause",
        )

        if NOTIFY_RUNTIME_BLOCKED:
            safe_notify_event(
                "⏸️ Signal blocked by runtime pause",
                short_event_line(strategy, symbol, side, mode, "Trading paused by runtime kill switch"),
                important=False,
            )

        return ok(
            {
                "order_sent": False,
                "decision": {
                    **decision,
                    "allow_order": False,
                    "decision": "RUNTIME_PAUSED",
                    "reason": "Trading paused by runtime kill switch",
                },
                "quality": quality,
                "price_deviation": price_deviation,
                "duplicate_signal": duplicate_signal,
                "alert_idempotency": alert_idempotency,
                "exposure": exposure,
                "trade_limits": trade_limits,
                "msg": "Risk engine approved, but runtime trading pause is active.",
            }
        )

    if not ENABLE_REAL_ORDERS:
        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision=decision["decision"],
            decision_reason="REAL_ORDERS_DISABLED",
            status="blocked_by_master_switch",
        )

        return ok(
            {
                "order_sent": False,
                "decision": decision,
                "quality": quality,
                "price_deviation": price_deviation,
                "duplicate_signal": duplicate_signal,
                "alert_idempotency": alert_idempotency,
                "exposure": exposure,
                "trade_limits": trade_limits,
                "msg": "Risk engine approved, but ENABLE_REAL_ORDERS=false",
            }
        )

    try:
        result = execute_bybit_trade(body, risk_pct_used)
        order_id = result.get("order_id", "")

        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision=decision["decision"],
            decision_reason=decision["reason"],
            order_id=order_id,
            status="order_sent",
        )

        if NOTIFY_ORDER_SENT:
            safe_notify_event(
                "✅ Order sent",
                f"{strategy} {symbol} {side} mode={mode} risk={risk_pct_used}% order_id={order_id}",
                important=False,
            )

        post_order_protection = validate_post_order_protection(symbol)
        protection_recovery = None

        if not post_order_protection["ok"]:
            write_system_log(
                action="post_order_protection_verify_failed",
                symbol=symbol,
                side=side,
                decision="PROTECTION_VERIFY_FAILED",
                reason=post_order_protection["reason"],
                order_id=order_id,
                status="warning",
                extra={"post_order_protection": post_order_protection},
            )

            if NOTIFY_PROTECTION_FAILED:
                safe_notify_event(
                    "⚠️ Protection verification failed",
                    f"{strategy} {symbol} {side} order_id={order_id}\nReason: {post_order_protection['reason']}",
                    important=True,
                )

            if AUTO_DOWNGRADE_ON_PROTECTION_FAILED:
                auto_downgrade_strategy(strategy, symbol, side, "protection_failed", post_order_protection["reason"])

            if AUTO_CLOSE_ON_PROTECTION_MISSING:
                try:
                    protection_recovery = emergency_close_symbol_impl(symbol)
                except Exception as close_exc:
                    protection_recovery = {"error": str(close_exc)}
                    write_system_log(
                        action="post_order_auto_close_failed",
                        symbol=symbol,
                        side=side,
                        decision="ORDER_FAILED",
                        reason=str(close_exc),
                        order_id=order_id,
                        status="error",
                        extra={"post_order_protection": post_order_protection},
                    )

        return ok(
            {
                "order_sent": True,
                "decision": decision,
                "quality": quality,
                "price_deviation": price_deviation,
                "duplicate_signal": duplicate_signal,
                "alert_idempotency": alert_idempotency,
                "exposure": exposure,
                "trade_limits": trade_limits,
                "post_order_protection": post_order_protection,
                "protection_recovery": protection_recovery,
                "result": result,
            }
        )

    except Exception as err:
        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision="ORDER_FAILED",
            decision_reason=str(err),
            status="error",
        )
        if NOTIFY_ORDER_FAILED:
            safe_notify_event(
                "❌ Order failed",
                f"{strategy} {symbol} {side} mode={mode}\nError: {err}",
                important=True,
            )
        if AUTO_DOWNGRADE_ON_ORDER_FAILED:
            auto_downgrade_strategy(strategy, symbol, side, "order_failed", str(err))
        raise


# ============================================================
# ADJUST ENDPOINT
# ============================================================

@app.post("/adjust")
async def adjust(request: Request):
    body = await request.json()
    verify_secret(request, body)

    symbol = normalize_symbol(body["symbol"])
    action = body["action"]

    pos = get_position_linear(symbol)
    size = float(pos.get("size", "0") or 0.0)
    side = pos.get("side") or ""

    if size <= 0.0 or not side:
        raise HTTPException(400, "No open position")

    tick, _, _ = get_instrument(symbol)

    if action == "be":
        be_offset_bp = int(body.get("be_offset_bp", 0))
        entry = float(pos.get("avgPrice", "0") or 0.0)

        if entry <= 0:
            raise HTTPException(400, "avgPrice missing")

        be_px = entry * (1.0 + (be_offset_bp / 10000.0)) if side == "Buy" else entry * (1.0 - (be_offset_bp / 10000.0))

        req = {
            "category": "linear",
            "symbol": symbol,
            "tpslMode": "Full",
            "stopLoss": fmt_price(be_px, tick),
            "slTriggerBy": "MarkPrice",
            "positionIdx": 0,
        }

        log(f"[REQ] trading-stop BE: {req}")
        resp = bybit("POST", "/v5/position/trading-stop", req)
        log(f"[RESP] trading-stop BE: {resp}")

        return ok({"msg": "be set"})

    if action == "trail":
        trail_dist = float(body["trail_dist"])

        req = {
            "category": "linear",
            "symbol": symbol,
            "tpslMode": "Full",
            "trailingStop": fmt_qty(trail_dist),
            "positionIdx": 0,
        }

        log(f"[REQ] trading-stop trail: {req}")
        resp = bybit("POST", "/v5/position/trading-stop", req)
        log(f"[RESP] trading-stop trail: {resp}")

        return ok({"msg": "trail set"})

    if action == "cancel_trail":
        req = {
            "category": "linear",
            "symbol": symbol,
            "tpslMode": "Full",
            "trailingStop": "0",
            "positionIdx": 0,
        }

        log(f"[REQ] trading-stop cancel trail: {req}")
        resp = bybit("POST", "/v5/position/trading-stop", req)
        log(f"[RESP] trading-stop cancel trail: {resp}")

        return ok({"msg": "trail canceled"})

    if action == "set_sl":
        sl = float(body["sl"])

        req = {
            "category": "linear",
            "symbol": symbol,
            "tpslMode": "Full",
            "stopLoss": fmt_price(sl, tick),
            "slTriggerBy": "MarkPrice",
            "positionIdx": 0,
        }

        log(f"[REQ] trading-stop set_sl: {req}")
        resp = bybit("POST", "/v5/position/trading-stop", req)
        log(f"[RESP] trading-stop set_sl: {resp}")

        return ok({"msg": "sl set"})

    raise HTTPException(400, f"Unknown action: {action}")
