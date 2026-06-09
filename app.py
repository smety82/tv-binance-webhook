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
from urllib.parse import quote

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

# v4.x operational controls.
# CRON_SECRET is optional; if empty, SHARED_SECRET is accepted for cron endpoints.
CRON_SECRET = os.getenv("CRON_SECRET", "")
BACKTEST_STORAGE_ENABLED = os.getenv("BACKTEST_STORAGE_ENABLED", "true").lower() == "true"


# v5.x modules except multi-account/testnet-live split.
SUPABASE_SPLIT_TABLES_ENABLED = os.getenv("SUPABASE_SPLIT_TABLES_ENABLED", "false").lower() == "true"
SUPABASE_ORDERS_TABLE = os.getenv("SUPABASE_ORDERS_TABLE", "orders")
SUPABASE_POSITIONS_TABLE = os.getenv("SUPABASE_POSITIONS_TABLE", "positions")
SUPABASE_SYSTEM_EVENTS_TABLE = os.getenv("SUPABASE_SYSTEM_EVENTS_TABLE", "system_events")
SUPABASE_STRATEGY_HISTORY_TABLE = os.getenv("SUPABASE_STRATEGY_HISTORY_TABLE", "strategy_state_history")
SUPABASE_DAILY_REPORTS_TABLE = os.getenv("SUPABASE_DAILY_REPORTS_TABLE", "daily_reports")
SUPABASE_TELEGRAM_TABLE = os.getenv("SUPABASE_TELEGRAM_TABLE", "telegram_notifications")
SUPABASE_BACKTEST_TABLE = os.getenv("SUPABASE_BACKTEST_TABLE", "backtest_results")

RECONCILIATION_LOOKBACK_DAYS = int(os.getenv("RECONCILIATION_LOOKBACK_DAYS", "7"))
RECOVERY_AUTO_PAUSE_ON_UNKNOWN_POSITION = os.getenv("RECOVERY_AUTO_PAUSE_ON_UNKNOWN_POSITION", "false").lower() == "true"
RECOVERY_NOTIFY_ON_STARTUP_ISSUES = os.getenv("RECOVERY_NOTIFY_ON_STARTUP_ISSUES", "true").lower() == "true"

TELEGRAM_COMMANDS_ENABLED = os.getenv("TELEGRAM_COMMANDS_ENABLED", "true").lower() == "true"
TELEGRAM_COMMANDS_ALLOW_TRADING_ACTIONS = os.getenv("TELEGRAM_COMMANDS_ALLOW_TRADING_ACTIONS", "false").lower() == "true"

PROMOTION_MIN_PAPER_EVENTS = int(os.getenv("PROMOTION_MIN_PAPER_EVENTS", "20"))
PROMOTION_MIN_MICRO_ORDERS = int(os.getenv("PROMOTION_MIN_MICRO_ORDERS", "10"))
PROMOTION_MIN_PROFIT_FACTOR = float(os.getenv("PROMOTION_MIN_PROFIT_FACTOR", "1.2"))
PROMOTION_MAX_REJECTION_RATE = float(os.getenv("PROMOTION_MAX_REJECTION_RATE", "0.25"))
PROMOTION_MAX_PROTECTION_FAILURES = int(os.getenv("PROMOTION_MAX_PROTECTION_FAILURES", "0"))

PAYLOAD_SCHEMA_VALIDATION_ENABLED = os.getenv("PAYLOAD_SCHEMA_VALIDATION_ENABLED", "true").lower() == "true"
PAYLOAD_SCHEMA_REQUIRE_VERSION = os.getenv("PAYLOAD_SCHEMA_REQUIRE_VERSION", "false").lower() == "true"
SUPPORTED_PAYLOAD_VERSIONS = [x.strip() for x in os.getenv("SUPPORTED_PAYLOAD_VERSIONS", "1.0").split(",") if x.strip()]

EXECUTION_QUALITY_ENABLED = os.getenv("EXECUTION_QUALITY_ENABLED", "true").lower() == "true"
MAX_ALLOWED_SLIPPAGE_PCT = float(os.getenv("MAX_ALLOWED_SLIPPAGE_PCT", "0"))

CAPITAL_ALLOCATION_ENABLED = os.getenv("CAPITAL_ALLOCATION_ENABLED", "true").lower() == "true"
MAX_STRATEGY_EXPOSURE_PCT = float(os.getenv("MAX_STRATEGY_EXPOSURE_PCT", "0"))
MAX_STRATEGY_POSITION_VALUE_USDT = float(os.getenv("MAX_STRATEGY_POSITION_VALUE_USDT", "0"))
MAX_GROUP_EXPOSURE_PCT = float(os.getenv("MAX_GROUP_EXPOSURE_PCT", "0"))

STRATEGY_REVIEW_LOOKBACK_DAYS = int(os.getenv("STRATEGY_REVIEW_LOOKBACK_DAYS", "30"))

# v6.7.0 PAPER trade outcome tracker.
# Computes whether a PAPER signal would have hit TP1/TP2/SL using Bybit kline data.
PAPER_OUTCOME_ENABLED = os.getenv("PAPER_OUTCOME_ENABLED", "true").lower() == "true"
PAPER_OUTCOME_DEFAULT_DAYS = int(os.getenv("PAPER_OUTCOME_DEFAULT_DAYS", "7"))
PAPER_OUTCOME_MAX_EVENTS = int(os.getenv("PAPER_OUTCOME_MAX_EVENTS", "300"))
PAPER_OUTCOME_TP1_QTY_PCT = float(os.getenv("PAPER_OUTCOME_TP1_QTY_PCT", "50"))
PAPER_OUTCOME_SAME_CANDLE_RULE = os.getenv("PAPER_OUTCOME_SAME_CANDLE_RULE", "SL_FIRST").upper()
PAPER_OUTCOME_DEFAULT_INTERVAL = os.getenv("PAPER_OUTCOME_DEFAULT_INTERVAL", "15")

# v6.8.0 PAPER candidate decision / monitoring automation.
PAPER_DECISION_MIN_SAMPLE_REJECT = int(os.getenv("PAPER_DECISION_MIN_SAMPLE_REJECT", "10"))
PAPER_DECISION_MIN_SAMPLE_PROMOTE = int(os.getenv("PAPER_DECISION_MIN_SAMPLE_PROMOTE", "20"))
PAPER_DECISION_MIN_SAMPLE_KEEP = int(os.getenv("PAPER_DECISION_MIN_SAMPLE_KEEP", "8"))
PAPER_DECISION_REJECT_AVG_R = float(os.getenv("PAPER_DECISION_REJECT_AVG_R", "-0.30"))
PAPER_DECISION_KEEP_AVG_R = float(os.getenv("PAPER_DECISION_KEEP_AVG_R", "0.05"))
PAPER_DECISION_PROMOTE_AVG_R = float(os.getenv("PAPER_DECISION_PROMOTE_AVG_R", "0.15"))
PAPER_DECISION_PROMOTE_BACKTEST_PF = float(os.getenv("PAPER_DECISION_PROMOTE_BACKTEST_PF", "1.15"))
PAPER_MONITOR_REPORT_MIN_HOURS = float(os.getenv("PAPER_MONITOR_REPORT_MIN_HOURS", "12"))

# v6.9.0 PAPER strategy guard / automation.
# Default is warning-only: it reports candidates that should be rejected, but does not switch them OFF.
PAPER_STRATEGY_GUARD_ENABLED = os.getenv("PAPER_STRATEGY_GUARD_ENABLED", "true").lower() == "true"
PAPER_STRATEGY_GUARD_MODE = os.getenv("PAPER_STRATEGY_GUARD_MODE", "WARNING").upper()  # WARNING or OFF
PAPER_STRATEGY_GUARD_MIN_CLOSED = int(os.getenv("PAPER_STRATEGY_GUARD_MIN_CLOSED", str(PAPER_DECISION_MIN_SAMPLE_REJECT)))
PAPER_STRATEGY_GUARD_REJECT_AVG_R = float(os.getenv("PAPER_STRATEGY_GUARD_REJECT_AVG_R", str(PAPER_DECISION_REJECT_AVG_R)))
PAPER_STRATEGY_GUARD_NOTIFY = os.getenv("PAPER_STRATEGY_GUARD_NOTIFY", "true").lower() == "true"
PAPER_STRATEGY_GUARD_COOLDOWN_HOURS = float(os.getenv("PAPER_STRATEGY_GUARD_COOLDOWN_HOURS", "12"))

HTTP_TIMEOUT = 15.0

APP_DIR = Path(__file__).resolve().parent
PAPER_MONITOR_STATE_FILE = APP_DIR / "paper_monitor_state.json"
PAPER_STRATEGY_GUARD_STATE_FILE = APP_DIR / "paper_strategy_guard_state.json"
STATE_FILE = APP_DIR / "strategy_state.json"
TRADE_LOG_FILE = APP_DIR / "trade_log.csv"
RUNTIME_STATE_FILE = APP_DIR / "runtime_state.json"
BACKTEST_FILE = APP_DIR / "backtest_results.json"
DAILY_REPORT_STATE_FILE = APP_DIR / "daily_report_state.json"

app = FastAPI(title="TradingView Bybit Risk Engine", version="9.1.3")
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

    try:
        write_extended_data_model_event(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision=decision,
            decision_reason=decision_reason,
            order_id=order_id,
            status=status,
        )
    except Exception as exc:
        log(f"[WARN] extended data model write failed: {exc}")


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


# Supabase trade-event reporting state.
# Trade-event reports must remain available even if Supabase has a transient
# DNS/network outage or the SUPABASE_URL environment variable is malformed.
# The local CSV log is always written first and acts as a degraded-mode cache.
_supabase_trade_log_state: Dict[str, Any] = {
    "last_error": None,
    "last_error_at": 0.0,
    "last_success_at": None,
    "last_success_operation": None,
    "last_source": "local_csv",
}


def _trade_log_cloud_fail(operation: str, exc: Any) -> None:
    msg = f"{operation}: {exc}"
    _supabase_trade_log_state["last_error"] = msg
    _supabase_trade_log_state["last_error_at"] = time.time()
    _supabase_trade_log_state["last_source"] = "local_csv_fallback"
    log(f"[WARN] Supabase trade-log fallback: {msg}")


def _trade_log_cloud_ok(operation: str) -> None:
    _supabase_trade_log_state["last_error"] = None
    _supabase_trade_log_state["last_error_at"] = 0.0
    _supabase_trade_log_state["last_success_at"] = now_iso()
    _supabase_trade_log_state["last_success_operation"] = operation
    _supabase_trade_log_state["last_source"] = "supabase"


def _local_trade_log_rows_for_reporting(limit: int = 100, days: Optional[int] = None) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 10000))
    rows = read_trade_log_rows(limit=0)

    if days is not None:
        safe_days = max(1, min(int(days), 90))
        cutoff = time.time() - safe_days * 24 * 60 * 60
        filtered: list[Dict[str, Any]] = []
        for row in rows:
            raw_ts = row.get("created_at") or row.get("timestamp_utc") or row.get("timestamp")
            if not raw_ts:
                filtered.append(row)
                continue
            try:
                import datetime as _dt
                cleaned = str(raw_ts).replace("Z", "+00:00")
                parsed = _dt.datetime.fromisoformat(cleaned)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=_dt.timezone.utc)
                if parsed.timestamp() >= cutoff:
                    filtered.append(row)
            except Exception:
                # Keep malformed legacy rows visible rather than making reports fail.
                filtered.append(row)
        rows = filtered

    # Supabase queries return created_at.desc. Mirror that order in degraded mode.
    return list(reversed(rows[-safe_limit:]))


def fetch_supabase_logs(limit: int = 100) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 1000))

    if not supabase_enabled():
        _supabase_trade_log_state["last_source"] = "local_csv_supabase_disabled"
        return _local_trade_log_rows_for_reporting(limit=safe_limit)

    params = {
        "select": "*",
        "order": "created_at.desc",
        "limit": str(safe_limit),
    }

    try:
        resp = client.get(
            supabase_table_url(),
            headers=supabase_headers(prefer=""),
            params=params,
        )
        if resp.status_code >= 400:
            _trade_log_cloud_fail("fetch_recent", f"HTTP_{resp.status_code} {resp.text[:240]}")
            return _local_trade_log_rows_for_reporting(limit=safe_limit)

        rows = resp.json()
        if not isinstance(rows, list):
            _trade_log_cloud_fail("fetch_recent", "INVALID_JSON_RESPONSE_NOT_LIST")
            return _local_trade_log_rows_for_reporting(limit=safe_limit)

        _trade_log_cloud_ok("fetch_recent")
        return rows
    except Exception as exc:
        _trade_log_cloud_fail("fetch_recent", exc)
        return _local_trade_log_rows_for_reporting(limit=safe_limit)


def fetch_supabase_logs_since(days: int = 1, limit: int = 5000) -> list[Dict[str, Any]]:
    safe_days = max(1, min(int(days), 90))
    safe_limit = max(1, min(int(limit), 10000))

    if not supabase_enabled():
        _supabase_trade_log_state["last_source"] = "local_csv_supabase_disabled"
        return _local_trade_log_rows_for_reporting(limit=safe_limit, days=safe_days)

    start_s = int(time.time()) - safe_days * 24 * 60 * 60
    start_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_s))

    params = {
        "select": "*",
        "created_at": f"gte.{start_iso}",
        "order": "created_at.desc",
        "limit": str(safe_limit),
    }

    try:
        resp = client.get(
            supabase_table_url(),
            headers=supabase_headers(prefer=""),
            params=params,
        )
        if resp.status_code >= 400:
            _trade_log_cloud_fail("fetch_since", f"HTTP_{resp.status_code} {resp.text[:240]}")
            return _local_trade_log_rows_for_reporting(limit=safe_limit, days=safe_days)

        rows = resp.json()
        if not isinstance(rows, list):
            _trade_log_cloud_fail("fetch_since", "INVALID_JSON_RESPONSE_NOT_LIST")
            return _local_trade_log_rows_for_reporting(limit=safe_limit, days=safe_days)

        _trade_log_cloud_ok("fetch_since")
        return rows
    except Exception as exc:
        _trade_log_cloud_fail("fetch_since", exc)
        return _local_trade_log_rows_for_reporting(limit=safe_limit, days=safe_days)


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


def verify_cron_secret(secret: str) -> None:
    expected = CRON_SECRET or SHARED_SECRET
    if secret != expected:
        raise HTTPException(401, "Unauthorized")


def load_daily_report_state() -> Dict[str, Any]:
    if not DAILY_REPORT_STATE_FILE.exists():
        return {}
    try:
        with DAILY_REPORT_STATE_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_daily_report_state(state: Dict[str, Any]) -> None:
    try:
        with DAILY_REPORT_STATE_FILE.open("w", encoding="utf-8") as file:
            json.dump(state, file, ensure_ascii=False, indent=2)
    except Exception as exc:
        log(f"[WARN] daily report state save failed: {exc}")


def send_daily_report_once(days: int = 1, force: bool = False) -> Dict[str, Any]:
    today_key = time.strftime("%Y-%m-%d", time.gmtime())
    state = load_daily_report_state()
    last_sent = state.get("last_sent_date")

    if not force and last_sent == today_key:
        return {
            "ok": True,
            "sent": False,
            "reason": "ALREADY_SENT_TODAY",
            "last_sent_date": last_sent,
        }

    message = format_daily_report_message(days=days)
    notify = safe_notify_event("📊 Daily trading report", message, important=False)

    if notify.get("sent"):
        state["last_sent_date"] = today_key
        state["last_sent_at"] = now_iso()
        state["last_days"] = max(1, min(days, 30))
        save_daily_report_state(state)

    return {
        "ok": True,
        "sent": bool(notify.get("sent")),
        "notify": notify,
        "date": today_key,
        "message": message,
    }


def load_backtest_results() -> list[Dict[str, Any]]:
    if not BACKTEST_FILE.exists():
        return []
    try:
        with BACKTEST_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)
        if isinstance(data, dict):
            data = data.get("rows", [])
        return data if isinstance(data, list) else []
    except Exception as exc:
        log(f"[WARN] backtest load failed: {exc}")
        return []


def save_backtest_results(rows: list[Dict[str, Any]]) -> None:
    if not BACKTEST_STORAGE_ENABLED:
        raise HTTPException(400, "Backtest storage is disabled")
    payload = {"updated_at": now_iso(), "rows": rows}
    with BACKTEST_FILE.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def normalize_backtest_row(row: Dict[str, Any]) -> Dict[str, Any]:
    strategy = str(row.get("strategy") or row.get("Strategy") or "UNKNOWN")
    symbol = normalize_symbol(row.get("symbol") or row.get("Symbol") or "")
    side = str(row.get("side") or row.get("Side") or "BOTH").upper()

    def f(*names: str) -> Optional[float]:
        for name in names:
            if name in row:
                return to_float_or_none(row.get(name))
        return None

    return {
        "strategy": strategy,
        "symbol": symbol,
        "side": side,
        "trades": f("trades", "Trades", "closed_trades"),
        "net_pnl": f("net_pnl", "Net PnL", "net_profit", "Net Profit"),
        "profit_factor": f("profit_factor", "PF", "Profit Factor"),
        "win_rate": f("win_rate", "Win Rate", "win_rate_pct"),
        "max_drawdown": f("max_drawdown", "Max Drawdown"),
        "raw": row,
    }


def backtest_key(strategy: str, symbol: str, side: str) -> str:
    return f"{strategy}|{normalize_symbol(symbol)}|{str(side).upper()}"


def merge_backtest_rows(existing_rows: list[Dict[str, Any]], incoming_rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    """Upsert backtest benchmarks by strategy|symbol|side.

    This intentionally keeps the file-backed registry simple and Render-friendly.
    The newest incoming row wins for the same strategy/symbol/side.
    """
    index: Dict[str, Dict[str, Any]] = {}
    order: list[str] = []

    for raw in existing_rows:
        row = normalize_backtest_row(raw if isinstance(raw, dict) else {"raw": raw})
        key = backtest_key(row.get("strategy", "UNKNOWN"), row.get("symbol", ""), row.get("side", "BOTH"))
        if key not in index:
            order.append(key)
        index[key] = row

    for raw in incoming_rows:
        row = normalize_backtest_row(raw if isinstance(raw, dict) else {"raw": raw})
        row["updated_at"] = now_iso()
        key = backtest_key(row.get("strategy", "UNKNOWN"), row.get("symbol", ""), row.get("side", "BOTH"))
        if key not in index:
            order.append(key)
        index[key] = row

    return [index[k] for k in order if k in index]


def build_backtest_registry() -> Dict[str, Any]:
    rows = [normalize_backtest_row(r if isinstance(r, dict) else {"raw": r}) for r in load_backtest_results()]
    rows_sorted = sorted(rows, key=lambda r: (str(r.get("strategy")), str(r.get("symbol")), str(r.get("side"))))
    return {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "count": len(rows_sorted),
        "file": str(BACKTEST_FILE),
        "rows": rows_sorted,
    }


def paper_vs_backtest_status(avg_r: Optional[float], closed: int, bt_pf: Optional[float]) -> str:
    if closed < PAPER_DECISION_MIN_SAMPLE_KEEP:
        return "TOO_EARLY"
    if bt_pf is None:
        return "NO_BACKTEST"
    if avg_r is None:
        return "NO_PAPER_R"
    if bt_pf >= PAPER_DECISION_PROMOTE_BACKTEST_PF and avg_r < 0:
        return "UNDERPERFORMING"
    if bt_pf >= PAPER_DECISION_PROMOTE_BACKTEST_PF and avg_r >= PAPER_DECISION_KEEP_AVG_R:
        return "ALIGNED_OK"
    if bt_pf < 1.0:
        return "BACKTEST_WEAK"
    return "WATCH"


def build_default_candidate_backtest_rows() -> list[Dict[str, Any]]:
    """Manual benchmark seed based on currently selected TradingView tester results.

    These are not used automatically unless imported through /backtest_manual_import
    or /backtest_seed_known_candidates.
    """
    return [
        {"strategy": "trend_continuation_avax_v11", "symbol": "AVAXUSDT", "side": "LONG", "profit_factor": 1.49, "trades": 66, "win_rate": 54.55, "source": "manual_tradingview"},
        {"strategy": "trend_continuation_movr_v11", "symbol": "MOVRUSDT", "side": "LONG", "profit_factor": 1.85, "trades": 80, "win_rate": 51.25, "source": "manual_tradingview"},
        {"strategy": "momentum_breakout_sol_v11", "symbol": "SOLUSDT", "side": "LONG", "profit_factor": 1.41, "trades": 38, "win_rate": 55.26, "source": "manual_tradingview"},
        {"strategy": "intraday_trend_pullback_icp_v13", "symbol": "ICPUSDT", "side": "LONG", "profit_factor": 1.38, "trades": 30, "win_rate": 50.00, "source": "manual_tradingview"},
    ]


def build_live_performance_index(days: int = 30) -> Dict[str, Dict[str, Any]]:
    health = build_strategy_health(days=days) if supabase_enabled() else {"items": []}
    index: Dict[str, Dict[str, Any]] = {}
    for item in health.get("items", []):
        key = backtest_key(item.get("strategy", "UNKNOWN"), item.get("symbol", ""), item.get("side", "BOTH"))
        closed = item.get("closed_pnl_by_symbol", {}) or {}
        index[key] = {
            "strategy": item.get("strategy"),
            "symbol": item.get("symbol"),
            "side": item.get("side"),
            "mode": item.get("mode"),
            "events": item.get("event_count"),
            "orders": item.get("order_sent"),
            "failures": item.get("order_failed"),
            "net_pnl": closed.get("net_pnl"),
            "profit_factor": closed.get("profit_factor"),
            "trades": closed.get("trades"),
            "win_rate": closed.get("win_rate"),
            "health": item.get("health", {}),
        }
    return index


def build_backtest_vs_live_report(days: int = 30) -> Dict[str, Any]:
    safe_days = max(1, min(days, 90))
    backtest_rows = [normalize_backtest_row(r) for r in load_backtest_results()]
    live_index = build_live_performance_index(days=safe_days)
    comparisons = []

    for bt in backtest_rows:
        key = backtest_key(bt["strategy"], bt["symbol"], bt["side"] if bt["side"] != "BOTH" else "LONG")
        live = live_index.get(key)
        if live is None and bt["side"] == "BOTH":
            live_candidates = [item for item in live_index.values() if str(item.get("strategy")) == bt["strategy"] and normalize_symbol(item.get("symbol", "")) == bt["symbol"]]
            if live_candidates:
                live = {
                    "strategy": bt["strategy"],
                    "symbol": bt["symbol"],
                    "side": "BOTH",
                    "mode": ",".join(sorted(set(str(x.get("mode")) for x in live_candidates))),
                    "events": sum(int(x.get("events") or 0) for x in live_candidates),
                    "orders": sum(int(x.get("orders") or 0) for x in live_candidates),
                    "failures": sum(int(x.get("failures") or 0) for x in live_candidates),
                    "net_pnl": sum(float(x.get("net_pnl") or 0) for x in live_candidates),
                    "profit_factor": None,
                    "trades": sum(int(x.get("trades") or 0) for x in live_candidates),
                    "win_rate": None,
                    "health": {},
                }

        bt_pf = bt.get("profit_factor")
        live_pf = live.get("profit_factor") if live else None
        bt_pnl = bt.get("net_pnl")
        live_pnl = live.get("net_pnl") if live else None

        comparisons.append({
            "strategy": bt["strategy"],
            "symbol": bt["symbol"],
            "side": bt["side"],
            "backtest": bt,
            "live": live,
            "deltas": {
                "net_pnl_delta": (float(live_pnl) - float(bt_pnl)) if live_pnl is not None and bt_pnl is not None else None,
                "profit_factor_delta": (float(live_pf) - float(bt_pf)) if live_pf is not None and bt_pf is not None else None,
            },
            "status": "NO_LIVE_DATA" if live is None else "MATCHED",
        })

    return {"ok": True, "days": safe_days, "backtest_rows": len(backtest_rows), "live_groups": len(live_index), "comparisons": comparisons}


def build_data_model_export(days: int = 30) -> Dict[str, Any]:
    rows = get_recent_trade_events(days=max(1, min(days, 90)))
    orders = []
    system_events = []
    notifications = []
    for row in rows:
        strategy = str(row.get("strategy") or "")
        decision = str(row.get("decision") or "")
        status = str(row.get("status") or "")
        if status == "order_sent" or decision in {"ACCEPTED_MICRO", "ACCEPTED_LIVE", "ORDER_FAILED"}:
            orders.append(row)
        if strategy.startswith("SYSTEM") or decision in {"RUNTIME_PAUSED", "RUNTIME_RESUMED", "EMERGENCY_CLOSE_SENT", "CANCEL_ALL_ORDERS_SENT"}:
            system_events.append(row)
        if "TELEGRAM" in decision.upper() or "notify" in str(row).lower():
            notifications.append(row)
    return {
        "ok": True,
        "note": "Logical export derived from trade_events. Physical Supabase split tables can be added later without breaking compatibility.",
        "trade_events": rows,
        "orders": orders,
        "system_events": system_events,
        "telegram_notifications": notifications,
        "positions_snapshot": summarize_open_risk(),
        "strategy_state_snapshot": load_state(),
    }


def build_dashboard_charts_html(secret: str, days: int = 30) -> str:
    safe_days = max(1, min(days, 90))
    perf = build_performance_report(days=min(safe_days, 30)) if supabase_enabled() else {"orders": {}, "by_symbol": {}, "by_decision": {}}
    orders = perf.get("orders", {}) or {}
    by_symbol = perf.get("by_symbol", {}) or {}
    by_decision = perf.get("by_decision", {}) or {}

    def bars(title: str, data: Dict[str, Any]) -> str:
        if not data:
            return f"<div class='card'><h2>{h(title)}</h2><p>No data</p></div>"
        max_val = max(float(v or 0) for v in data.values()) or 1.0
        rows = []
        for key, value in sorted(data.items(), key=lambda kv: float(kv[1] or 0), reverse=True):
            width = max(2.0, float(value or 0) / max_val * 100.0)
            rows.append(f"<div class='barrow'><div class='barlabel'>{h(key)}</div><div class='barwrap'><div class='bar' style='width:{width:.2f}%'></div></div><div class='barvalue'>{h(value)}</div></div>")
        return f"<div class='card'><h2>{h(title)}</h2>{''.join(rows)}</div>"

    return f"""
    <!doctype html><html><head><meta charset='utf-8'><title>Trading Charts v4.3.0</title>
    <style>body{{font-family:Arial,Helvetica,sans-serif;margin:24px;background:#f6f8fb;color:#111827}}.card{{background:white;border-radius:12px;padding:16px;margin-bottom:18px;box-shadow:0 1px 4px rgba(0,0,0,.08)}}.barrow{{display:grid;grid-template-columns:220px 1fr 80px;gap:10px;align-items:center;margin:8px 0}}.barwrap{{height:18px;background:#e5e7eb;border-radius:999px;overflow:hidden}}.bar{{height:18px;background:#111827;border-radius:999px}}.barlabel,.barvalue{{font-size:13px}}a{{display:inline-block;margin:0 8px 8px 0;padding:8px 12px;border-radius:8px;background:#111827;color:white;text-decoration:none;font-size:13px}}</style>
    </head><body><h1>Trading Dashboard Charts v4.3.0</h1>
    <p><a href='/dashboard_v2?secret={h(secret)}&days=7'>Back to dashboard v2</a></p>
    {bars('Order counters', orders)}
    {bars('Decisions', by_decision)}
    {bars('Symbols', by_symbol)}
    </body></html>
    """


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
    <html><head><meta charset="utf-8"><title>Trading Control Center v5.3.0</title>
    <style>
    body{{font-family:Arial,Helvetica,sans-serif;margin:24px;background:#f6f8fb;color:#1f2937}}
    table{{width:100%;border-collapse:collapse;background:white;border-radius:12px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:24px}}
    th,td{{padding:10px 12px;border-bottom:1px solid #e5e7eb;text-align:left;font-size:13px;vertical-align:top}}
    th{{background:#111827;color:white}} .card{{background:white;border-radius:12px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:18px}}
    .badge{{display:inline-block;padding:3px 8px;border-radius:999px;font-size:12px;font-weight:700}} .good{{background:#dcfce7;color:#166534}} .watch{{background:#fef3c7;color:#92400e}} .bad{{background:#fee2e2;color:#991b1b}} .neutral{{background:#e5e7eb;color:#374151}}
    button.secondary{{background:#111827;color:white;border:0;border-radius:8px;padding:7px 10px;font-weight:700;margin:2px;cursor:pointer}}
    .nav a{{display:inline-block;margin:0 8px 8px 0;padding:8px 12px;border-radius:8px;background:#111827;color:white;text-decoration:none;font-size:13px}}
    </style></head><body>
    <h1>Trading Control Center v5.3.0</h1>
    <div class="nav"><a href="/dashboard?secret={h(secret)}&days={safe_days}">Classic dashboard</a><a href="/dashboard_charts?secret={h(secret)}&days={safe_days}">Charts</a><a href="/backtest_vs_live?secret={h(secret)}&days={safe_days}">Backtest vs Live</a><a href="/risk_status?secret={h(secret)}">Risk JSON</a><a href="/strategy_state?secret={h(secret)}">Strategy JSON</a></div>
    <div class="card"><b>Runtime:</b> real_orders={h(ENABLE_REAL_ORDERS)} · telegram={h(telegram_configured())} · auto_downgrade={h(AUTO_DOWNGRADE_ENABLED)} · open_positions={h(open_risk.get('open_positions'))} · open_value={fmt_num(open_risk.get('total_position_value'))}</div>
    <div class="card"><b>v5.3 modules:</b> daily Telegram cron · trade limits/loss streak · per-symbol limits · lifecycle · auto downgrade · backtest vs live · charts</div>
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
    <p>version: 6.8.0</p>
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
    <p>supabase_split_tables_enabled: {SUPABASE_SPLIT_TABLES_ENABLED}</p>
    <p>payload_schema_validation_enabled: {PAYLOAD_SCHEMA_VALIDATION_ENABLED}</p>
    <p>telegram_commands_enabled: {TELEGRAM_COMMANDS_ENABLED}</p>
    <p>capital_allocation_enabled: {CAPITAL_ALLOCATION_ENABLED}</p>
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


@app.get("/cron_daily_report")
def cron_daily_report(secret: str, days: int = 1, force: bool = False):
    verify_cron_secret(secret)
    return send_daily_report_once(days=days, force=force)


@app.post("/cron_daily_report")
def cron_daily_report_post(secret: str, days: int = 1, force: bool = False):
    verify_cron_secret(secret)
    return send_daily_report_once(days=days, force=force)


@app.post("/backtest_import")
async def backtest_import(request: Request):
    body = await request.json()
    verify_secret(request, body)
    rows = body.get("rows")
    if rows is None and isinstance(body.get("data"), list):
        rows = body.get("data")
    if not isinstance(rows, list):
        raise HTTPException(400, "Expected JSON body with rows: [...]")
    normalized = [normalize_backtest_row(row if isinstance(row, dict) else {"raw": row}) for row in rows]
    save_backtest_results(normalized)
    return {"ok": True, "count": len(normalized), "rows": normalized[:10]}


@app.get("/backtest_registry")
def backtest_registry(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_backtest_registry()


@app.post("/backtest_manual_import")
async def backtest_manual_import(request: Request):
    body = await request.json()
    verify_secret(request, body)
    rows = body.get("items") or body.get("rows") or body.get("data")
    if not isinstance(rows, list):
        raise HTTPException(400, "Expected JSON body with items/rows: [...]")

    mode = str(body.get("mode", "upsert")).lower()
    normalized = [normalize_backtest_row(row if isinstance(row, dict) else {"raw": row}) for row in rows]
    for row in normalized:
        row["updated_at"] = now_iso()
        row["source"] = row.get("source") or "manual"

    if mode == "replace":
        final_rows = normalized
    elif mode == "upsert":
        final_rows = merge_backtest_rows(load_backtest_results(), normalized)
    else:
        raise HTTPException(400, "mode must be 'upsert' or 'replace'")

    save_backtest_results(final_rows)
    return {
        "ok": True,
        "mode": mode,
        "imported": len(normalized),
        "total_registry_rows": len(final_rows),
        "rows": normalized,
        "registry": final_rows,
    }


@app.post("/backtest_seed_known_candidates")
async def backtest_seed_known_candidates(request: Request):
    body = await request.json()
    verify_secret(request, body)
    rows = build_default_candidate_backtest_rows()
    final_rows = merge_backtest_rows(load_backtest_results(), rows)
    save_backtest_results(final_rows)
    return {
        "ok": True,
        "seeded": len(rows),
        "total_registry_rows": len(final_rows),
        "rows": rows,
    }


@app.post("/backtest_registry_clear")
async def backtest_registry_clear(request: Request):
    body = await request.json()
    verify_secret(request, body)
    confirm = str(body.get("confirm", "")).upper()
    if confirm != "CLEAR_BACKTEST_REGISTRY":
        raise HTTPException(400, "confirm must be CLEAR_BACKTEST_REGISTRY")
    save_backtest_results([])
    return {"ok": True, "cleared": True, "count": 0}


@app.get("/backtest_vs_live")
def backtest_vs_live(secret: str, days: int = 30):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_backtest_vs_live_report(days=days)


@app.get("/data_model_export")
def data_model_export(secret: str, days: int = 30):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_data_model_export(days=days)


@app.get("/dashboard_charts", response_class=HTMLResponse)
def dashboard_charts(secret: str, days: int = 30):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return HTMLResponse(content=build_dashboard_charts_html(secret=secret, days=days), media_type="text/html")


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

    payload_validation = validate_payload_schema(body)
    if not payload_validation["ok"]:
        write_trade_log(
            body=body if isinstance(body, dict) else {},
            mode="OFF",
            risk_pct_used=0.0,
            decision="PAYLOAD_SCHEMA_REJECTED",
            decision_reason=payload_validation["reason"],
            status="rejected_by_payload_schema",
        )
        return ok({
            "order_sent": False,
            "decision": {
                "allow_order": False,
                "mode": "OFF",
                "risk_pct_used": 0.0,
                "decision": "PAYLOAD_SCHEMA_REJECTED",
                "reason": payload_validation["reason"],
            },
            "payload_validation": payload_validation,
            "msg": "Payload schema validation rejected the alert.",
        })

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

    capital_allocation = validate_capital_allocation(body, risk_pct_used)
    if not capital_allocation["ok"]:
        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision="CAPITAL_ALLOCATION_REJECTED",
            decision_reason=capital_allocation["reason"],
            status="rejected_by_capital_allocation_guard",
        )

        if NOTIFY_REJECTIONS:
            safe_notify_event(
                "🚫 Capital allocation rejected signal",
                short_event_line(strategy, symbol, side, mode, capital_allocation["reason"]),
                important=True,
            )

        return ok(
            {
                "order_sent": False,
                "decision": {
                    **decision,
                    "allow_order": False,
                    "decision": "CAPITAL_ALLOCATION_REJECTED",
                    "reason": capital_allocation["reason"],
                },
                "quality": quality,
                "price_deviation": price_deviation,
                "duplicate_signal": duplicate_signal,
                "alert_idempotency": alert_idempotency,
                "exposure": exposure,
                "trade_limits": trade_limits,
                "capital_allocation": capital_allocation,
                "msg": "Risk engine approved, but capital allocation guard rejected the signal.",
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
                "capital_allocation": capital_allocation,
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
                "capital_allocation": capital_allocation,
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

        execution_quality = assess_execution_quality_after_order(body, result, order_id)

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
                "capital_allocation": capital_allocation,
                "post_order_protection": post_order_protection,
                "execution_quality": execution_quality,
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
# v6.7.0 PAPER TRADE OUTCOME TRACKER
# ============================================================

def interval_to_ms(interval: Any) -> int:
    """Convert Bybit/TradingView interval to milliseconds."""
    text = str(interval or PAPER_OUTCOME_DEFAULT_INTERVAL).strip().upper()
    mapping = {
        "1": 60_000,
        "3": 3 * 60_000,
        "5": 5 * 60_000,
        "15": 15 * 60_000,
        "30": 30 * 60_000,
        "60": 60 * 60_000,
        "120": 2 * 60 * 60_000,
        "240": 4 * 60 * 60_000,
        "360": 6 * 60 * 60_000,
        "720": 12 * 60 * 60_000,
        "D": 24 * 60 * 60_000,
        "1D": 24 * 60 * 60_000,
        "W": 7 * 24 * 60 * 60_000,
        "1W": 7 * 24 * 60 * 60_000,
    }
    if text in mapping:
        return mapping[text]
    try:
        return int(float(text)) * 60_000
    except Exception:
        return int(PAPER_OUTCOME_DEFAULT_INTERVAL) * 60_000


def normalize_interval_for_bybit(interval: Any) -> str:
    text = str(interval or PAPER_OUTCOME_DEFAULT_INTERVAL).strip().upper()
    if text in {"1D", "D"}:
        return "D"
    if text in {"1W", "W"}:
        return "W"
    if text.endswith("M") and text[:-1].isdigit():
        return text[:-1]
    if text.endswith("H") and text[:-1].isdigit():
        return str(int(text[:-1]) * 60)
    return text


def parse_raw_payload(raw_payload: Any) -> Dict[str, Any]:
    if isinstance(raw_payload, dict):
        return raw_payload
    if isinstance(raw_payload, str) and raw_payload.strip():
        try:
            parsed = json.loads(raw_payload)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def event_numeric(row: Dict[str, Any], key: str, raw_payload: Dict[str, Any], payload_key: Optional[str] = None) -> Optional[float]:
    candidates = []
    if key in row:
        candidates.append(row.get(key))
    if payload_key and payload_key in raw_payload:
        candidates.append(raw_payload.get(payload_key))
    if key in raw_payload:
        candidates.append(raw_payload.get(key))
    for value in candidates:
        converted = to_float_or_none(value)
        if converted is not None:
            return converted
    return None


def get_public_klines(symbol: str, interval: Any, start_ms: int, end_ms: int, limit: int = 1000) -> list[Dict[str, Any]]:
    """Fetch public Bybit linear klines and return ascending candles."""
    symbol = normalize_symbol(symbol)
    bybit_interval = normalize_interval_for_bybit(interval)
    safe_limit = max(1, min(int(limit), 1000))
    all_rows: list[Dict[str, Any]] = []
    current_start = int(start_ms)
    hard_end = int(end_ms)
    interval_ms = interval_to_ms(bybit_interval)
    max_pages = 20

    for _ in range(max_pages):
        if current_start >= hard_end:
            break
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": bybit_interval,
            "start": current_start,
            "end": hard_end,
            "limit": safe_limit,
        }
        resp = bybit("GET", "/v5/market/kline", params)
        rows = (resp.get("result") or {}).get("list") or []
        if not rows:
            break
        parsed_rows = []
        for row in rows:
            try:
                parsed_rows.append({
                    "start_ms": int(row[0]),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]) if len(row) > 5 and row[5] not in (None, "") else None,
                    "turnover": float(row[6]) if len(row) > 6 and row[6] not in (None, "") else None,
                })
            except Exception:
                continue
        parsed_rows.sort(key=lambda x: x["start_ms"])
        if not parsed_rows:
            break
        all_rows.extend(parsed_rows)
        next_start = parsed_rows[-1]["start_ms"] + interval_ms
        if next_start <= current_start:
            break
        current_start = next_start
        if len(parsed_rows) < safe_limit:
            break

    # Deduplicate by candle start.
    dedup: Dict[int, Dict[str, Any]] = {}
    for row in all_rows:
        if start_ms <= row["start_ms"] <= end_ms:
            dedup[row["start_ms"]] = row
    return [dedup[k] for k in sorted(dedup)]


def extract_paper_event(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw = parse_raw_payload(row.get("raw_payload"))
    strategy = row.get("strategy") or raw.get("strategy")
    symbol = normalize_symbol(row.get("symbol") or raw.get("symbol") or "")
    side = str(row.get("side") or raw.get("side") or "").upper()
    mode = str(row.get("mode") or "").upper()
    decision = str(row.get("decision") or "").upper()
    status = str(row.get("status") or "")

    if mode != "PAPER" or decision != "PAPER_LOGGED":
        return None
    if side not in {"LONG", "SHORT"} or not symbol:
        return None

    signal_price = event_numeric(row, "signal_price", raw, "signalPrice")
    sl = event_numeric(row, "sl", raw, "sl")
    tp1 = event_numeric(row, "tp1", raw, "tp1")
    tp2 = event_numeric(row, "tp2", raw, "tp2")
    if signal_price is None or sl is None or tp1 is None or tp2 is None:
        return None

    tf = raw.get("tf") or raw.get("timeframe") or PAPER_OUTCOME_DEFAULT_INTERVAL
    bar_time_raw = raw.get("barTime") or raw.get("bar_time")
    bar_time_ms = None
    if bar_time_raw is not None:
        try:
            bar_time_ms = int(float(bar_time_raw))
        except Exception:
            bar_time_ms = None
    if bar_time_ms is None:
        created_at = row.get("created_at") or row.get("timestamp_utc") or row.get("timestamp")
        try:
            # ISO UTC string parse without external dependencies.
            cleaned = str(created_at).replace("Z", "+00:00")
            import datetime as _dt
            bar_time_ms = int(_dt.datetime.fromisoformat(cleaned).timestamp() * 1000)
        except Exception:
            bar_time_ms = int(time.time() * 1000)

    return {
        "id": row.get("id"),
        "created_at": row.get("created_at") or row.get("timestamp_utc") or row.get("timestamp"),
        "strategy": strategy,
        "symbol": symbol,
        "side": side,
        "mode": mode,
        "status": status,
        "decision": decision,
        "signal_price": float(signal_price),
        "sl": float(sl),
        "tp1": float(tp1),
        "tp2": float(tp2),
        "tf": str(tf),
        "bar_time_ms": int(bar_time_ms),
        "raw_payload": raw,
    }


def evaluate_paper_trade(event: Dict[str, Any], end_ms: Optional[int] = None) -> Dict[str, Any]:
    """Evaluate if a PAPER signal would have hit TP/SL using Bybit candles.

    Assumptions:
    - Entry is approximated by signal_price.
    - Evaluation starts after the signal bar closes.
    - TP1 and TP2 are partial exits using PAPER_OUTCOME_TP1_QTY_PCT.
    - If SL and target are hit in the same candle, PAPER_OUTCOME_SAME_CANDLE_RULE decides.
    """
    if not PAPER_OUTCOME_ENABLED:
        return {"ok": False, "status": "DISABLED", "reason": "PAPER_OUTCOME_DISABLED", "event": event}

    symbol = event["symbol"]
    side = event["side"]
    interval = event.get("tf") or PAPER_OUTCOME_DEFAULT_INTERVAL
    interval_ms = interval_to_ms(interval)
    start_ms = int(event["bar_time_ms"]) + interval_ms
    actual_end_ms = int(end_ms or time.time() * 1000)

    if actual_end_ms <= start_ms:
        return {"ok": True, "status": "OPEN", "reason": "NO_CANDLES_AFTER_SIGNAL", "event": event, "candles_checked": 0}

    try:
        candles = get_public_klines(symbol, interval, start_ms, actual_end_ms, limit=1000)
    except Exception as exc:
        return {"ok": False, "status": "ERROR", "reason": f"KLINE_FETCH_FAILED: {exc}", "event": event}

    entry = float(event["signal_price"])
    sl = float(event["sl"])
    tp1 = float(event["tp1"])
    tp2 = float(event["tp2"])
    q1 = max(0.0, min(PAPER_OUTCOME_TP1_QTY_PCT / 100.0, 0.99))
    q2 = 1.0 - q1

    if side == "LONG":
        risk = entry - sl
        tp1_r = (tp1 - entry) / risk if risk > 0 else None
        tp2_r = (tp2 - entry) / risk if risk > 0 else None
    else:
        risk = sl - entry
        tp1_r = (entry - tp1) / risk if risk > 0 else None
        tp2_r = (entry - tp2) / risk if risk > 0 else None

    if risk <= 0 or tp1_r is None or tp2_r is None:
        return {"ok": False, "status": "INVALID", "reason": "INVALID_RISK_OR_TARGET_STRUCTURE", "event": event}

    tp1_hit = False
    tp1_hit_at = None
    terminal_status = "OPEN"
    terminal_reason = "NO_TP_OR_SL_HIT_YET"
    terminal_at = None
    terminal_candle = None
    estimated_r = None

    for candle in candles:
        high = float(candle["high"])
        low = float(candle["low"])
        start = int(candle["start_ms"])

        if side == "LONG":
            hit_sl = low <= sl
            hit_tp1 = high >= tp1
            hit_tp2 = high >= tp2
        else:
            hit_sl = high >= sl
            hit_tp1 = low <= tp1
            hit_tp2 = low <= tp2

        if not tp1_hit:
            if hit_sl and (hit_tp1 or hit_tp2):
                if PAPER_OUTCOME_SAME_CANDLE_RULE == "TARGET_FIRST":
                    if hit_tp2:
                        tp1_hit = True
                        tp1_hit_at = start
                        terminal_status = "WIN_TP2"
                        terminal_reason = "TP2_AND_SL_SAME_CANDLE_TARGET_FIRST"
                        estimated_r = q1 * tp1_r + q2 * tp2_r
                    else:
                        tp1_hit = True
                        tp1_hit_at = start
                        terminal_status = "PARTIAL_TP1_THEN_SL"
                        terminal_reason = "TP1_AND_SL_SAME_CANDLE_TARGET_FIRST"
                        estimated_r = q1 * tp1_r + q2 * (-1.0)
                else:
                    terminal_status = "LOSS_SL"
                    terminal_reason = "SL_AND_TARGET_SAME_CANDLE_SL_FIRST"
                    estimated_r = -1.0
                terminal_at = start
                terminal_candle = candle
                break
            if hit_sl:
                terminal_status = "LOSS_SL"
                terminal_reason = "SL_HIT_BEFORE_TP1"
                estimated_r = -1.0
                terminal_at = start
                terminal_candle = candle
                break
            if hit_tp2:
                tp1_hit = True
                tp1_hit_at = start
                terminal_status = "WIN_TP2"
                terminal_reason = "TP2_HIT_BEFORE_SL"
                estimated_r = q1 * tp1_r + q2 * tp2_r
                terminal_at = start
                terminal_candle = candle
                break
            if hit_tp1:
                tp1_hit = True
                tp1_hit_at = start
                continue
        else:
            if hit_sl and hit_tp2:
                if PAPER_OUTCOME_SAME_CANDLE_RULE == "TARGET_FIRST":
                    terminal_status = "WIN_TP2"
                    terminal_reason = "AFTER_TP1_TP2_AND_SL_SAME_CANDLE_TARGET_FIRST"
                    estimated_r = q1 * tp1_r + q2 * tp2_r
                else:
                    terminal_status = "PARTIAL_TP1_THEN_SL"
                    terminal_reason = "AFTER_TP1_TP2_AND_SL_SAME_CANDLE_SL_FIRST"
                    estimated_r = q1 * tp1_r + q2 * (-1.0)
                terminal_at = start
                terminal_candle = candle
                break
            if hit_tp2:
                terminal_status = "WIN_TP2"
                terminal_reason = "TP2_HIT_AFTER_TP1"
                estimated_r = q1 * tp1_r + q2 * tp2_r
                terminal_at = start
                terminal_candle = candle
                break
            if hit_sl:
                terminal_status = "PARTIAL_TP1_THEN_SL"
                terminal_reason = "SL_HIT_AFTER_TP1"
                estimated_r = q1 * tp1_r + q2 * (-1.0)
                terminal_at = start
                terminal_candle = candle
                break

    if terminal_status == "OPEN" and tp1_hit:
        terminal_status = "OPEN_AFTER_TP1"
        terminal_reason = "TP1_HIT_BUT_TP2_OR_SL_NOT_YET"
        estimated_r = q1 * tp1_r

    estimated_pnl_usdt = None
    if estimated_r is not None:
        risk_pct_requested = event_numeric({}, "riskPct", event.get("raw_payload", {}), "riskPct") or 0.0
        # Only an estimate: uses current equity fetched live. This keeps the endpoint lightweight.
        try:
            equity = get_equity_usdt()
            estimated_pnl_usdt = equity * (risk_pct_requested / 100.0) * estimated_r
        except Exception:
            estimated_pnl_usdt = None

    return {
        "ok": True,
        "status": terminal_status,
        "reason": terminal_reason,
        "event": event,
        "candles_checked": len(candles),
        "tp1_hit": tp1_hit,
        "tp1_hit_at_ms": tp1_hit_at,
        "terminal_at_ms": terminal_at,
        "terminal_candle": terminal_candle,
        "r_multiple": estimated_r,
        "estimated_pnl_usdt": estimated_pnl_usdt,
        "assumptions": {
            "entry_price": entry,
            "risk_distance": risk,
            "tp1_r": tp1_r,
            "tp2_r": tp2_r,
            "tp1_qty_pct": PAPER_OUTCOME_TP1_QTY_PCT,
            "same_candle_rule": PAPER_OUTCOME_SAME_CANDLE_RULE,
            "interval": interval,
            "evaluation_start_ms": start_ms,
            "evaluation_end_ms": actual_end_ms,
        },
    }


def fetch_paper_events_for_outcome(days: int, limit: int) -> list[Dict[str, Any]]:
    safe_days = max(1, min(int(days), 60))
    safe_limit = max(1, min(int(limit), max(1, PAPER_OUTCOME_MAX_EVENTS)))
    if supabase_enabled():
        rows = fetch_supabase_logs_since(days=safe_days, limit=safe_limit * 3)
    else:
        rows = read_trade_log_rows(limit=safe_limit * 3)
    events = []
    for row in rows:
        event = extract_paper_event(row)
        if event:
            events.append(event)
        if len(events) >= safe_limit:
            break
    return events


def summarize_paper_outcomes(outcomes: list[Dict[str, Any]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "total": len(outcomes),
        "by_status": {},
        "by_strategy_symbol": {},
        "closed_count": 0,
        "open_count": 0,
        "wins": 0,
        "losses": 0,
        "partial_then_sl": 0,
        "total_r": 0.0,
        "average_r_closed": None,
        "estimated_pnl_usdt": 0.0,
    }
    closed_r_count = 0
    for item in outcomes:
        status = item.get("status", "UNKNOWN")
        summary["by_status"][status] = summary["by_status"].get(status, 0) + 1
        event = item.get("event") or {}
        key = f"{event.get('strategy')}|{event.get('symbol')}|{event.get('side')}"
        if key not in summary["by_strategy_symbol"]:
            summary["by_strategy_symbol"][key] = {"strategy": event.get("strategy"), "symbol": event.get("symbol"), "side": event.get("side"), "count": 0, "by_status": {}, "total_r": 0.0, "closed_count": 0, "average_r_closed": None}
        group = summary["by_strategy_symbol"][key]
        group["count"] += 1
        group["by_status"][status] = group["by_status"].get(status, 0) + 1
        if status in {"OPEN", "OPEN_AFTER_TP1"}:
            summary["open_count"] += 1
        else:
            summary["closed_count"] += 1
        if status == "WIN_TP2":
            summary["wins"] += 1
        elif status == "LOSS_SL":
            summary["losses"] += 1
        elif status == "PARTIAL_TP1_THEN_SL":
            summary["partial_then_sl"] += 1
        r_val = item.get("r_multiple")
        if r_val is not None and status not in {"OPEN"}:
            try:
                rv = float(r_val)
                summary["total_r"] += rv
                group["total_r"] += rv
                closed_r_count += 1
                group["closed_count"] += 1
            except Exception:
                pass
        pnl = item.get("estimated_pnl_usdt")
        if pnl is not None:
            try:
                summary["estimated_pnl_usdt"] += float(pnl)
            except Exception:
                pass
    if closed_r_count:
        summary["average_r_closed"] = summary["total_r"] / closed_r_count
    for group in summary["by_strategy_symbol"].values():
        if group["closed_count"]:
            group["average_r_closed"] = group["total_r"] / group["closed_count"]
    return summary


@app.get("/paper_outcome_scan")
def paper_outcome_scan(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = 100):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    events = fetch_paper_events_for_outcome(days=days, limit=limit)
    outcomes = [evaluate_paper_trade(event) for event in events]
    return {"ok": True, "days": days, "count": len(outcomes), "summary": summarize_paper_outcomes(outcomes), "outcomes": outcomes}


@app.get("/paper_outcome_summary")
def paper_outcome_summary(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = 300):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    events = fetch_paper_events_for_outcome(days=days, limit=limit)
    outcomes = [evaluate_paper_trade(event) for event in events]
    return {"ok": True, "days": days, "count": len(outcomes), "summary": summarize_paper_outcomes(outcomes)}


@app.get("/paper_outcome_open")
def paper_outcome_open(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = 300):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    events = fetch_paper_events_for_outcome(days=days, limit=limit)
    outcomes = [evaluate_paper_trade(event) for event in events]
    open_items = [x for x in outcomes if x.get("status") in {"OPEN", "OPEN_AFTER_TP1"}]
    return {"ok": True, "days": days, "count": len(open_items), "open_outcomes": open_items}


@app.get("/paper_outcome_event")
def paper_outcome_event(secret: str, event_id: int, days: int = 30):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    events = fetch_paper_events_for_outcome(days=days, limit=PAPER_OUTCOME_MAX_EVENTS)
    for event in events:
        try:
            if int(event.get("id")) == int(event_id):
                return {"ok": True, "outcome": evaluate_paper_trade(event)}
        except Exception:
            continue
    return {"ok": False, "error": "paper event not found in selected window", "event_id": event_id, "days": days}


@app.get("/paper_outcome_config")
def paper_outcome_config(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {
        "ok": True,
        "paper_outcome": {
            "enabled": PAPER_OUTCOME_ENABLED,
            "default_days": PAPER_OUTCOME_DEFAULT_DAYS,
            "max_events": PAPER_OUTCOME_MAX_EVENTS,
            "tp1_qty_pct": PAPER_OUTCOME_TP1_QTY_PCT,
            "same_candle_rule": PAPER_OUTCOME_SAME_CANDLE_RULE,
            "default_interval": PAPER_OUTCOME_DEFAULT_INTERVAL,
        },
        "decision_layer": {
            "min_sample_reject": PAPER_DECISION_MIN_SAMPLE_REJECT,
            "min_sample_keep": PAPER_DECISION_MIN_SAMPLE_KEEP,
            "min_sample_promote": PAPER_DECISION_MIN_SAMPLE_PROMOTE,
            "reject_avg_r": PAPER_DECISION_REJECT_AVG_R,
            "keep_avg_r": PAPER_DECISION_KEEP_AVG_R,
            "promote_avg_r": PAPER_DECISION_PROMOTE_AVG_R,
            "promote_backtest_pf": PAPER_DECISION_PROMOTE_BACKTEST_PF,
        },
    }


# ============================================================
# v6.8.0 PAPER OUTCOME DECISION LAYER / CANDIDATE MONITOR
# ============================================================

def read_json_file(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as exc:
        log(f"[WARN] read_json_file failed for {path}: {exc}")
    return default


def write_json_file(path: Path, data: Any) -> None:
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        log(f"[WARN] write_json_file failed for {path}: {exc}")


def paper_event_risk_pct(event: Dict[str, Any]) -> float:
    raw = event.get("raw_payload") or {}
    value = event_numeric({}, "riskPct", raw, "riskPct")
    if value is None:
        value = event_numeric({}, "risk_pct", raw, "risk_pct")
    return float(value or 0.0)


def enrich_paper_outcome(item: Dict[str, Any], equity: Optional[float] = None) -> Dict[str, Any]:
    """Add robust estimated risk/PnL fields without changing the original outcome contract."""
    result = dict(item)
    event = result.get("event") or {}
    r_val = result.get("r_multiple")
    risk_pct = paper_event_risk_pct(event)
    risk_usd = None
    pnl_usdt = None
    if equity is None:
        try:
            equity = get_equity_usdt()
        except Exception:
            equity = None
    if equity is not None and risk_pct > 0:
        risk_usd = float(equity) * (risk_pct / 100.0)
        if r_val is not None:
            try:
                pnl_usdt = risk_usd * float(r_val)
            except Exception:
                pnl_usdt = None
    result["risk_pct_requested"] = risk_pct
    result["estimated_risk_usdt"] = risk_usd
    result["estimated_pnl_usdt_v2"] = pnl_usdt
    if result.get("estimated_pnl_usdt") in (None, 0, 0.0, -0.0) and pnl_usdt is not None:
        result["estimated_pnl_usdt"] = pnl_usdt
    return result


def backtest_index_by_candidate() -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for row in load_backtest_results():
        bt = normalize_backtest_row(row if isinstance(row, dict) else {"raw": row})
        side = bt.get("side") if bt.get("side") != "BOTH" else "LONG"
        key = backtest_key(bt.get("strategy", "UNKNOWN"), bt.get("symbol", ""), side or "LONG")
        index[key] = bt
    return index


def classify_paper_candidate(group: Dict[str, Any], backtest: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    closed = int(group.get("closed_count") or 0)
    count = int(group.get("count") or 0)
    avg_r = group.get("average_r_closed")
    total_r = float(group.get("total_r") or 0.0)
    wins = int(group.get("by_status", {}).get("WIN_TP2", 0))
    losses = int(group.get("by_status", {}).get("LOSS_SL", 0))
    partial = int(group.get("by_status", {}).get("PARTIAL_TP1_THEN_SL", 0))
    open_count = int(group.get("by_status", {}).get("OPEN", 0)) + int(group.get("by_status", {}).get("OPEN_AFTER_TP1", 0))
    bt_pf = to_float_or_none((backtest or {}).get("profit_factor"))
    bt_trades = to_float_or_none((backtest or {}).get("trades"))

    status = "WATCH"
    reasons = []
    action = "Collect more PAPER data"

    if closed == 0:
        reasons.append("No closed PAPER outcomes yet")
    if closed < PAPER_DECISION_MIN_SAMPLE_REJECT:
        reasons.append(f"Low sample size: {closed} closed < {PAPER_DECISION_MIN_SAMPLE_REJECT}")

    if avg_r is not None:
        avg_r_float = float(avg_r)
        if closed >= PAPER_DECISION_MIN_SAMPLE_REJECT and avg_r_float <= PAPER_DECISION_REJECT_AVG_R:
            status = "REJECT"
            action = "Set to OFF or re-optimize before further use"
            reasons.append(f"Average R {avg_r_float:.3f} <= reject threshold {PAPER_DECISION_REJECT_AVG_R:.3f}")
        elif closed >= PAPER_DECISION_MIN_SAMPLE_PROMOTE and avg_r_float >= PAPER_DECISION_PROMOTE_AVG_R and (bt_pf is None or bt_pf >= PAPER_DECISION_PROMOTE_BACKTEST_PF):
            status = "PROMOTE_CANDIDATE"
            action = "Eligible for cautious MICRO review"
            reasons.append(f"Average R {avg_r_float:.3f} >= promote threshold {PAPER_DECISION_PROMOTE_AVG_R:.3f}")
            if bt_pf is not None:
                reasons.append(f"Backtest PF {bt_pf:.3f} supports promotion")
        elif closed >= PAPER_DECISION_MIN_SAMPLE_KEEP and avg_r_float >= PAPER_DECISION_KEEP_AVG_R:
            status = "KEEP"
            action = "Keep PAPER running; not enough for MICRO yet"
            reasons.append(f"Average R {avg_r_float:.3f} >= keep threshold {PAPER_DECISION_KEEP_AVG_R:.3f}")
        elif closed >= PAPER_DECISION_MIN_SAMPLE_REJECT:
            status = "WATCH_NEGATIVE" if avg_r_float < 0 else "WATCH"
            action = "Keep PAPER only; review after more samples"
            reasons.append(f"Average R {avg_r_float:.3f} is not strong enough")

    if bt_pf is None:
        reasons.append("No imported backtest benchmark for this strategy/symbol/side")
    elif bt_pf < 1.0:
        reasons.append(f"Backtest PF {bt_pf:.3f} is below 1.0")
        if status not in {"REJECT"} and closed >= PAPER_DECISION_MIN_SAMPLE_REJECT:
            status = "WATCH_BACKTEST_WEAK"
    else:
        reasons.append(f"Backtest PF {bt_pf:.3f} available")

    win_rate_closed = (wins / closed * 100.0) if closed else None
    return {
        "status": status,
        "action": action,
        "reasons": reasons,
        "metrics": {
            "count": count,
            "closed_count": closed,
            "open_count": open_count,
            "wins_tp2": wins,
            "losses_sl": losses,
            "partial_tp1_then_sl": partial,
            "win_rate_closed_pct": win_rate_closed,
            "total_r": total_r,
            "average_r_closed": avg_r,
            "backtest_profit_factor": bt_pf,
            "backtest_trades": bt_trades,
        },
    }


def build_paper_outcome_decision_report(days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS, include_outcomes: bool = False) -> Dict[str, Any]:
    safe_days = max(1, min(int(days), 60))
    safe_limit = max(1, min(int(limit), PAPER_OUTCOME_MAX_EVENTS))
    events = fetch_paper_events_for_outcome(days=safe_days, limit=safe_limit)
    try:
        equity = get_equity_usdt()
    except Exception:
        equity = None
    outcomes = [enrich_paper_outcome(evaluate_paper_trade(event), equity=equity) for event in events]
    summary = summarize_paper_outcomes(outcomes)
    bt_index = backtest_index_by_candidate()

    decisions = []
    for key, group in sorted((summary.get("by_strategy_symbol") or {}).items()):
        bt = bt_index.get(key)
        decision = classify_paper_candidate(group, bt)
        metrics = decision.get("metrics", {}) if isinstance(decision, dict) else {}
        alignment_status = paper_vs_backtest_status(
            to_float_or_none(metrics.get("average_r_closed")),
            int(metrics.get("closed_count") or 0),
            to_float_or_none(metrics.get("backtest_profit_factor")),
        )
        decision["backtest_alignment_status"] = alignment_status
        decisions.append({
            "key": key,
            "strategy": group.get("strategy"),
            "symbol": group.get("symbol"),
            "side": group.get("side"),
            "decision": decision,
            "backtest": bt,
            "paper": group,
        })

    status_counts: Dict[str, int] = {}
    for item in decisions:
        st = item.get("decision", {}).get("status", "UNKNOWN")
        status_counts[st] = status_counts.get(st, 0) + 1

    result = {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "days": safe_days,
        "count": len(outcomes),
        "equity_for_pnl_estimate": equity,
        "summary": summary,
        "decision_thresholds": {
            "min_sample_reject": PAPER_DECISION_MIN_SAMPLE_REJECT,
            "min_sample_keep": PAPER_DECISION_MIN_SAMPLE_KEEP,
            "min_sample_promote": PAPER_DECISION_MIN_SAMPLE_PROMOTE,
            "reject_avg_r": PAPER_DECISION_REJECT_AVG_R,
            "keep_avg_r": PAPER_DECISION_KEEP_AVG_R,
            "promote_avg_r": PAPER_DECISION_PROMOTE_AVG_R,
            "promote_backtest_pf": PAPER_DECISION_PROMOTE_BACKTEST_PF,
        },
        "status_counts": status_counts,
        "decisions": decisions,
    }
    if include_outcomes:
        result["outcomes"] = outcomes
    return result


def format_paper_decision_report_message(report: Dict[str, Any]) -> str:
    lines = [
        f"📄 PAPER outcome monitor — {report.get('days')}d",
        f"Signals: {report.get('count')} | Total R: {fmt_num((report.get('summary') or {}).get('total_r'))} | Avg R: {fmt_num((report.get('summary') or {}).get('average_r_closed'))}",
    ]
    for item in report.get("decisions", [])[:12]:
        d = item.get("decision", {})
        m = d.get("metrics", {})
        lines.append(
            f"{d.get('status')}: {item.get('strategy')} {item.get('symbol')} {item.get('side')} "
            f"closed={m.get('closed_count')} avgR={fmt_num(m.get('average_r_closed'))} btPF={fmt_num(m.get('backtest_profit_factor'))}"
        )
    return "\n".join(lines)


@app.get("/paper_outcome_decisions")
def paper_outcome_decisions(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS, include_outcomes: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_paper_outcome_decision_report(days=days, limit=limit, include_outcomes=include_outcomes)


@app.get("/candidate_monitor")
def candidate_monitor(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_paper_outcome_decision_report(days=days, limit=limit, include_outcomes=False)


@app.get("/candidate_monitor_dashboard", response_class=HTMLResponse)
def candidate_monitor_dashboard(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    report = build_paper_outcome_decision_report(days=days, limit=limit, include_outcomes=False)
    rows = []
    badge_class = {"PROMOTE_CANDIDATE": "good", "KEEP": "good", "WATCH": "watch", "WATCH_NEGATIVE": "watch", "WATCH_BACKTEST_WEAK": "watch", "REJECT": "bad"}
    for item in report.get("decisions", []):
        d = item.get("decision", {})
        m = d.get("metrics", {})
        cls = badge_class.get(d.get("status"), "watch")
        rows.append(f"""
        <tr>
          <td>{h(item.get('strategy'))}</td><td>{h(item.get('symbol'))}</td><td>{h(item.get('side'))}</td>
          <td><span class='{cls}'>{h(d.get('status'))}</span></td>
          <td>{h(m.get('closed_count'))}</td><td>{fmt_num(m.get('average_r_closed'))}</td><td>{fmt_num(m.get('total_r'))}</td>
          <td>{fmt_num(m.get('win_rate_closed_pct'))}%</td><td>{fmt_num(m.get('backtest_profit_factor'))}</td><td>{h(d.get('backtest_alignment_status'))}</td>
          <td>{h(d.get('action'))}</td>
        </tr>
        """)
    if not rows:
        rows.append("<tr><td colspan='11'>No PAPER candidates found in selected window.</td></tr>")
    return HTMLResponse(f"""
    <html><head><title>Candidate Monitor</title><style>
    body{{font-family:Arial;margin:24px;background:#f6f8fb;color:#111827}}
    table{{border-collapse:collapse;width:100%;background:white;border-radius:10px;overflow:hidden}}
    th,td{{border-bottom:1px solid #e5e7eb;padding:9px;text-align:left;font-size:14px}}
    th{{background:#111827;color:white;position:sticky;top:0}} .good{{color:#166534;font-weight:700}} .watch{{color:#92400e;font-weight:700}} .bad{{color:#991b1b;font-weight:700}}
    .card{{background:white;border-radius:12px;padding:14px;margin-bottom:14px;box-shadow:0 2px 8px rgba(15,23,42,.08)}} a{{color:#2563eb}}
    </style></head><body>
    <h1>Candidate Strategy Monitor · Platform v9.1.3</h1>
    <div class='card'>Signals: {h(report.get('count'))} | Total R: {fmt_num((report.get('summary') or {}).get('total_r'))} | Average R: {fmt_num((report.get('summary') or {}).get('average_r_closed'))} | Status counts: {h(report.get('status_counts'))}</div>
    <table><tr><th>Strategy</th><th>Symbol</th><th>Side</th><th>Decision</th><th>Closed</th><th>Avg R</th><th>Total R</th><th>Win %</th><th>BT PF</th><th>BT Align</th><th>Action</th></tr>{''.join(rows)}</table>
    <p><a href='/paper_outcome_decisions?secret={h(secret)}&days={days}&limit={limit}'>JSON report</a> · <a href='/backtest_registry?secret={h(secret)}'>Backtest registry</a> · <a href='/dashboard_v2?secret={h(secret)}&days={days}'>Dashboard</a></p>
    </body></html>
    """)


@app.get("/paper_backtest_alignment")
def paper_backtest_alignment(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    report = build_paper_outcome_decision_report(days=days, limit=limit, include_outcomes=False)
    return {
        "ok": True,
        "days": report.get("days"),
        "backtest_rows": len(load_backtest_results()),
        "alignment": [
            {
                "strategy": x.get("strategy"),
                "symbol": x.get("symbol"),
                "side": x.get("side"),
                "paper_avg_r": x.get("decision", {}).get("metrics", {}).get("average_r_closed"),
                "paper_closed": x.get("decision", {}).get("metrics", {}).get("closed_count"),
                "backtest_profit_factor": x.get("decision", {}).get("metrics", {}).get("backtest_profit_factor"),
                "decision_status": x.get("decision", {}).get("status"),
                "backtest_alignment_status": x.get("decision", {}).get("backtest_alignment_status"),
            }
            for x in report.get("decisions", [])
        ],
    }


@app.get("/candidate_backtest_template")
def candidate_backtest_template(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    state = load_state()
    rows = []
    for strategy, cfg in (state.get("strategies") or {}).items():
        for symbol, sym_cfg in ((cfg or {}).get("symbols") or {}).items():
            for side in ["LONG", "SHORT"]:
                sc = (sym_cfg or {}).get(side) or {}
                if str(sc.get("mode", "OFF")).upper() != "OFF":
                    rows.append({
                        "strategy": strategy,
                        "symbol": normalize_symbol(symbol),
                        "side": side,
                        "trades": None,
                        "profit_factor": None,
                        "win_rate": None,
                        "net_pnl": None,
                        "max_drawdown": None,
                    })
    return {"ok": True, "rows": rows, "import_endpoint": "/backtest_import", "method": "POST", "note": "Fill the metrics from TradingView Strategy Tester exports and POST as {'secret':'...','rows':[...]}"}


@app.get("/cron_paper_outcome_report")
def cron_paper_outcome_report(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, force: bool = False):
    verify_cron_secret(secret)
    state = read_json_file(PAPER_MONITOR_STATE_FILE, {})
    now_ts = time.time()
    last_sent = float(state.get("last_sent_ts") or 0)
    min_seconds = PAPER_MONITOR_REPORT_MIN_HOURS * 3600.0
    if not force and last_sent and now_ts - last_sent < min_seconds:
        return {"ok": True, "sent": False, "reason": "TOO_SOON", "last_sent_at": state.get("last_sent_at"), "min_hours": PAPER_MONITOR_REPORT_MIN_HOURS}
    report = build_paper_outcome_decision_report(days=days, limit=PAPER_OUTCOME_MAX_EVENTS, include_outcomes=False)
    message = format_paper_decision_report_message(report)
    notify = safe_notify_event("📄 PAPER candidate monitor", message, important=False)
    if notify.get("sent"):
        state.update({"last_sent_ts": now_ts, "last_sent_at": now_iso(), "last_days": days})
        write_json_file(PAPER_MONITOR_STATE_FILE, state)
    return {"ok": True, "sent": bool(notify.get("sent")), "notify": notify, "report": report}


@app.get("/telegram_candidate_monitor_report")
def telegram_candidate_monitor_report(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, force: bool = False):
    """Alias endpoint for manual/cron Telegram candidate monitoring."""
    return cron_paper_outcome_report(secret=secret, days=days, force=force)


def _current_strategy_side_mode(strategy: str, symbol: str, side: str) -> str:
    try:
        state = load_state()
        cfg = get_side_config_copy(state, strategy, symbol, side)
        return str(cfg.get("mode", "OFF")).upper()
    except Exception:
        return "UNKNOWN"


def build_paper_strategy_guard_plan(days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS) -> Dict[str, Any]:
    report = build_paper_outcome_decision_report(days=days, limit=limit, include_outcomes=False)
    actions = []
    for item in report.get("decisions", []):
        strategy = str(item.get("strategy") or "")
        symbol = normalize_symbol(item.get("symbol") or "")
        side = normalize_side(item.get("side") or "LONG")
        decision = item.get("decision") or {}
        metrics = decision.get("metrics") or {}
        closed = int(metrics.get("closed_count") or 0)
        avg_r = to_float_or_none(metrics.get("average_r_closed"))
        current_mode = _current_strategy_side_mode(strategy, symbol, side)
        qualifies = (
            PAPER_STRATEGY_GUARD_ENABLED
            and current_mode == "PAPER"
            and closed >= PAPER_STRATEGY_GUARD_MIN_CLOSED
            and avg_r is not None
            and float(avg_r) <= PAPER_STRATEGY_GUARD_REJECT_AVG_R
        )
        action_type = "NONE"
        recommended_mode = current_mode
        reason = "No guard action needed"
        if qualifies:
            action_type = "WARN_ONLY" if PAPER_STRATEGY_GUARD_MODE != "OFF" else "SET_OFF"
            recommended_mode = "OFF"
            reason = f"PAPER guard triggered: closed={closed}, avgR={float(avg_r):.4f} <= {PAPER_STRATEGY_GUARD_REJECT_AVG_R:.4f}"
        actions.append({
            "strategy": strategy,
            "symbol": symbol,
            "side": side,
            "current_mode": current_mode,
            "decision_status": decision.get("status"),
            "closed_count": closed,
            "average_r_closed": avg_r,
            "total_r": metrics.get("total_r"),
            "backtest_profit_factor": metrics.get("backtest_profit_factor"),
            "backtest_alignment_status": decision.get("backtest_alignment_status"),
            "qualifies": qualifies,
            "action_type": action_type,
            "recommended_mode": recommended_mode,
            "reason": reason,
        })
    triggered = [x for x in actions if x.get("qualifies")]
    return {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "enabled": PAPER_STRATEGY_GUARD_ENABLED,
        "mode": PAPER_STRATEGY_GUARD_MODE,
        "days": report.get("days"),
        "thresholds": {
            "min_closed": PAPER_STRATEGY_GUARD_MIN_CLOSED,
            "reject_avg_r": PAPER_STRATEGY_GUARD_REJECT_AVG_R,
            "notify": PAPER_STRATEGY_GUARD_NOTIFY,
            "cooldown_hours": PAPER_STRATEGY_GUARD_COOLDOWN_HOURS,
        },
        "triggered_count": len(triggered),
        "actions": actions,
        "triggered": triggered,
        "source_report": {
            "count": report.get("count"),
            "summary": report.get("summary"),
            "status_counts": report.get("status_counts"),
        },
    }


def format_paper_strategy_guard_message(plan: Dict[str, Any]) -> str:
    lines = [
        f"🛡️ PAPER strategy guard — {plan.get('days')}d",
        f"Mode: {plan.get('mode')} | Triggered: {plan.get('triggered_count')} | Threshold: closed≥{(plan.get('thresholds') or {}).get('min_closed')}, avgR≤{fmt_num((plan.get('thresholds') or {}).get('reject_avg_r'))}",
    ]
    triggered = plan.get("triggered") or []
    if not triggered:
        lines.append("No strategy meets reject/downgrade criteria.")
    for item in triggered[:10]:
        lines.append(
            f"{item.get('action_type')}: {item.get('strategy')} {item.get('symbol')} {item.get('side')} "
            f"closed={item.get('closed_count')} avgR={fmt_num(item.get('average_r_closed'))} mode={item.get('current_mode')} → {item.get('recommended_mode')}"
        )
    return "\n".join(lines)


@app.get("/paper_strategy_guard_config")
def paper_strategy_guard_config(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {
        "ok": True,
        "paper_strategy_guard": {
            "enabled": PAPER_STRATEGY_GUARD_ENABLED,
            "mode": PAPER_STRATEGY_GUARD_MODE,
            "min_closed": PAPER_STRATEGY_GUARD_MIN_CLOSED,
            "reject_avg_r": PAPER_STRATEGY_GUARD_REJECT_AVG_R,
            "notify": PAPER_STRATEGY_GUARD_NOTIFY,
            "cooldown_hours": PAPER_STRATEGY_GUARD_COOLDOWN_HOURS,
        }
    }


@app.get("/paper_strategy_guard_plan")
def paper_strategy_guard_plan(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_paper_strategy_guard_plan(days=days, limit=limit)


@app.post("/paper_strategy_guard_run")
async def paper_strategy_guard_run(request: Request):
    body = await request.json()
    verify_secret(request, body)
    days = int(body.get("days", PAPER_OUTCOME_DEFAULT_DAYS))
    limit = int(body.get("limit", PAPER_OUTCOME_MAX_EVENTS))
    force_notify = bool(body.get("force_notify", False))
    apply_off = bool(body.get("apply_off", False))
    plan = build_paper_strategy_guard_plan(days=days, limit=limit)

    notify_result = {"sent": False, "skipped": True}
    state = read_json_file(PAPER_STRATEGY_GUARD_STATE_FILE, {})
    now_ts = time.time()
    last_sent = float(state.get("last_sent_ts") or 0)
    min_seconds = PAPER_STRATEGY_GUARD_COOLDOWN_HOURS * 3600.0
    should_notify = PAPER_STRATEGY_GUARD_NOTIFY and (force_notify or not last_sent or now_ts - last_sent >= min_seconds)
    if should_notify:
        notify_result = safe_notify_event("🛡️ PAPER strategy guard", format_paper_strategy_guard_message(plan), important=bool(plan.get("triggered_count")))
        if notify_result.get("sent"):
            state.update({"last_sent_ts": now_ts, "last_sent_at": now_iso(), "last_days": days, "last_triggered_count": plan.get("triggered_count")})
            write_json_file(PAPER_STRATEGY_GUARD_STATE_FILE, state)

    applied = []
    if apply_off:
        require_strategy_admin()
        for item in plan.get("triggered", []):
            if item.get("current_mode") == "PAPER":
                result = set_strategy_side_config(
                    strategy=item.get("strategy"),
                    symbol=item.get("symbol"),
                    side=item.get("side"),
                    mode="OFF",
                    risk_pct=0.0,
                    reason="paper_strategy_guard_reject",
                )
                applied.append(result)
    return {"ok": True, "plan": plan, "notify": notify_result, "applied_count": len(applied), "applied": applied, "apply_off_requested": apply_off}


@app.get("/cron_paper_strategy_guard")
def cron_paper_strategy_guard(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, apply_off: bool = False, force_notify: bool = False):
    verify_cron_secret(secret)
    plan = build_paper_strategy_guard_plan(days=days, limit=PAPER_OUTCOME_MAX_EVENTS)
    state = read_json_file(PAPER_STRATEGY_GUARD_STATE_FILE, {})
    now_ts = time.time()
    last_sent = float(state.get("last_sent_ts") or 0)
    min_seconds = PAPER_STRATEGY_GUARD_COOLDOWN_HOURS * 3600.0
    notify_result = {"sent": False, "skipped": True}
    if PAPER_STRATEGY_GUARD_NOTIFY and (force_notify or not last_sent or now_ts - last_sent >= min_seconds):
        notify_result = safe_notify_event("🛡️ PAPER strategy guard", format_paper_strategy_guard_message(plan), important=bool(plan.get("triggered_count")))
        if notify_result.get("sent"):
            state.update({"last_sent_ts": now_ts, "last_sent_at": now_iso(), "last_days": days, "last_triggered_count": plan.get("triggered_count")})
            write_json_file(PAPER_STRATEGY_GUARD_STATE_FILE, state)
    applied = []
    if apply_off:
        require_strategy_admin()
        for item in plan.get("triggered", []):
            if item.get("current_mode") == "PAPER":
                applied.append(set_strategy_side_config(item.get("strategy"), item.get("symbol"), item.get("side"), mode="OFF", risk_pct=0.0, reason="cron_paper_strategy_guard"))
    return {"ok": True, "plan": plan, "notify": notify_result, "applied_count": len(applied), "applied": applied}


# ============================================================
# v5.3 EXTENSIONS: SPLIT DATA MODEL, RECONCILIATION, RECOVERY,
# TELEGRAM COMMANDS, PROMOTION, EXECUTION QUALITY, PAYLOAD SCHEMA,
# CAPITAL ALLOCATION, STRATEGY REVIEW
# ============================================================

def supabase_url_for_table(table_name: str) -> str:
    return f"{SUPABASE_URL}/rest/v1/{table_name}"


def supabase_insert_optional(table_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not (supabase_enabled() and SUPABASE_SPLIT_TABLES_ENABLED):
        return {"ok": False, "skipped": True, "reason": "split_tables_disabled"}
    try:
        resp = client.post(
            supabase_url_for_table(table_name),
            headers=supabase_headers(),
            json=payload,
        )
        if resp.status_code >= 400:
            # Optional physical split tables are allowed to be absent during migration.
            log(f"[WARN] Optional Supabase table insert failed {table_name}: {resp.status_code} {resp.text}")
            return {"ok": False, "status_code": resp.status_code, "reason": resp.text[:500]}
        return {"ok": True, "status_code": resp.status_code}
    except Exception as exc:
        log(f"[WARN] Optional Supabase table insert exception {table_name}: {exc}")
        return {"ok": False, "reason": str(exc)}


def write_extended_data_model_event(
    body: Dict[str, Any],
    mode: str,
    risk_pct_used: float,
    decision: str,
    decision_reason: str,
    order_id: str = "",
    status: str = "logged",
) -> None:
    """Best-effort writes to optional normalized Supabase tables.

    This keeps backward compatibility with trade_events. If the physical tables do
    not exist yet, writes are ignored and the main trading flow is not affected.
    """
    if not SUPABASE_SPLIT_TABLES_ENABLED:
        return

    symbol = normalize_symbol(body.get("symbol", "")) if body.get("symbol") else "SYSTEM"
    side = str(body.get("side", "")).upper()
    strategy = str(body.get("strategy", "UNKNOWN"))
    event_payload = {
        "timestamp_utc": now_iso(),
        "strategy": strategy,
        "symbol": symbol,
        "side": side,
        "mode": mode,
        "decision": decision,
        "decision_reason": decision_reason,
        "order_id": order_id,
        "status": status,
        "raw_payload": sanitize_payload(body),
    }

    if strategy == "SYSTEM_EMERGENCY" or decision.startswith("SYSTEM") or decision in {
        "RUNTIME_PAUSED", "RUNTIME_RESUMED", "STRATEGY_STATE_UPDATED",
        "EMERGENCY_CLOSE_SENT", "CANCEL_ALL_ORDERS_SENT", "PROTECTION_VERIFY_FAILED",
    }:
        supabase_insert_optional(SUPABASE_SYSTEM_EVENTS_TABLE, event_payload)

    if decision in {"ACCEPTED_MICRO", "ACCEPTED_LIVE", "ORDER_FAILED"} or order_id:
        order_payload = dict(event_payload)
        order_payload.update({
            "signal_price": to_float_or_none(body.get("signalPrice")),
            "sl": to_float_or_none(body.get("sl")),
            "tp1": to_float_or_none(body.get("tp1")),
            "tp2": to_float_or_none(body.get("tp2")),
            "risk_pct_used": risk_pct_used,
        })
        supabase_insert_optional(SUPABASE_ORDERS_TABLE, order_payload)

    if decision == "STRATEGY_STATE_UPDATED":
        supabase_insert_optional(SUPABASE_STRATEGY_HISTORY_TABLE, event_payload)


def validate_payload_schema(body: Dict[str, Any]) -> Dict[str, Any]:
    if not PAYLOAD_SCHEMA_VALIDATION_ENABLED:
        return {"ok": True, "reason": "PAYLOAD_SCHEMA_VALIDATION_DISABLED", "details": {}}

    if not isinstance(body, dict):
        return {"ok": False, "reason": "PAYLOAD_NOT_OBJECT", "details": {}}

    if body.get("type") == "ping":
        return {"ok": True, "reason": "PING_PAYLOAD", "details": {}}

    payload_version = body.get("payload_version") or body.get("version")
    warnings = []
    errors = []

    if PAYLOAD_SCHEMA_REQUIRE_VERSION and not payload_version:
        errors.append("MISSING_PAYLOAD_VERSION")

    if payload_version and str(payload_version) not in SUPPORTED_PAYLOAD_VERSIONS:
        errors.append(f"UNSUPPORTED_PAYLOAD_VERSION_{payload_version}")

    required = ["strategy", "symbol", "side", "orderType", "signalPrice", "sl", "tp1", "tp2", "riskPct", "barTime"]
    for key in required:
        if key not in body or body.get(key) in (None, ""):
            errors.append(f"MISSING_{key}")

    if "side" in body and str(body.get("side", "")).upper() not in {"LONG", "SHORT"}:
        errors.append("INVALID_SIDE")

    if "orderType" in body and str(body.get("orderType", "Market")) != "Market":
        errors.append("UNSUPPORTED_ORDER_TYPE")

    numeric_fields = ["signalPrice", "sl", "tp1", "tp2", "riskPct"]
    for key in numeric_fields:
        if key in body and body.get(key) not in (None, "") and to_float_or_none(body.get(key)) is None:
            errors.append(f"INVALID_NUMERIC_{key}")

    if not payload_version:
        warnings.append("NO_PAYLOAD_VERSION_PROVIDED_COMPAT_MODE")

    return {
        "ok": len(errors) == 0,
        "reason": "OK" if not errors else ";".join(errors),
        "details": {
            "payload_version": payload_version,
            "supported_versions": SUPPORTED_PAYLOAD_VERSIONS,
            "warnings": warnings,
            "errors": errors,
            "required_fields": required,
        },
    }


def estimate_strategy_exposure(strategy: str) -> Dict[str, Any]:
    open_risk = summarize_open_risk()
    # Current Bybit positions do not carry strategy tags, so this is conservative.
    # The strategy-specific allocation checks use global open value as current exposure
    # unless future normalized positions table provides exact strategy attribution.
    return {
        "strategy": strategy,
        "current_strategy_position_value": float(open_risk.get("total_position_value", 0.0) or 0.0),
        "current_total_position_value": float(open_risk.get("total_position_value", 0.0) or 0.0),
        "open_risk": open_risk,
    }


def validate_capital_allocation(body: Dict[str, Any], risk_pct_used: float) -> Dict[str, Any]:
    if not CAPITAL_ALLOCATION_ENABLED:
        return {"ok": True, "reason": "CAPITAL_ALLOCATION_DISABLED", "details": {}}

    strategy = str(body.get("strategy", "UNKNOWN"))
    symbol = normalize_symbol(body.get("symbol", ""))
    side = normalize_side(body.get("side", ""))
    state = load_state()

    strategy_cfg = state.get("strategies", {}).get(strategy, {})
    side_cfg = get_side_config_copy(state, strategy, symbol, side)
    global_cfg = state.get("global", {})

    def limit_value(*keys: str, default: float = 0.0) -> float:
        for source in (side_cfg, strategy_cfg, global_cfg):
            for key in keys:
                if key in source and source.get(key) not in (None, ""):
                    try:
                        return float(source.get(key))
                    except Exception:
                        continue
        return float(default)

    max_strategy_value = limit_value("max_strategy_position_value_usdt", "max_strategy_exposure_usdt", default=MAX_STRATEGY_POSITION_VALUE_USDT)
    max_strategy_pct = limit_value("max_strategy_exposure_pct", default=MAX_STRATEGY_EXPOSURE_PCT)
    max_group_pct = limit_value("max_group_exposure_pct", default=MAX_GROUP_EXPOSURE_PCT)

    if max_strategy_value <= 0 and max_strategy_pct <= 0 and max_group_pct <= 0:
        return {"ok": True, "reason": "CAPITAL_ALLOCATION_LIMITS_DISABLED", "details": {"strategy": strategy, "symbol": symbol}}

    exposure_estimate = estimate_new_order_exposure(body, risk_pct_used)
    if not exposure_estimate.get("ok"):
        return exposure_estimate

    allocation = estimate_strategy_exposure(strategy)
    details = dict(exposure_estimate.get("details", {}))
    equity = float(details.get("equity", 0.0) or 0.0)
    new_value = float(details.get("estimated_new_position_value", 0.0) or 0.0)
    current_strategy_value = float(allocation.get("current_strategy_position_value", 0.0) or 0.0)
    projected_strategy_value = current_strategy_value + new_value
    projected_strategy_pct = (projected_strategy_value / equity * 100.0) if equity > 0 else None

    reasons = []
    if max_strategy_value > 0 and projected_strategy_value > max_strategy_value:
        reasons.append(f"MAX_STRATEGY_POSITION_VALUE_EXCEEDED_{projected_strategy_value:.4f}_MAX_{max_strategy_value:.4f}")
    if max_strategy_pct > 0 and projected_strategy_pct is not None and projected_strategy_pct > max_strategy_pct:
        reasons.append(f"MAX_STRATEGY_EXPOSURE_EXCEEDED_{projected_strategy_pct:.4f}%_MAX_{max_strategy_pct:.4f}%")
    if max_group_pct > 0:
        projected_group_pct = details.get("projected_equity_usage_pct")
        if projected_group_pct is not None and projected_group_pct > max_group_pct:
            reasons.append(f"MAX_GROUP_EXPOSURE_EXCEEDED_{projected_group_pct:.4f}%_MAX_{max_group_pct:.4f}%")

    details.update({
        "strategy": strategy,
        "current_strategy_position_value": current_strategy_value,
        "projected_strategy_position_value": projected_strategy_value,
        "projected_strategy_exposure_pct": projected_strategy_pct,
        "capital_allocation_limits": {
            "max_strategy_position_value_usdt": max_strategy_value,
            "max_strategy_exposure_pct": max_strategy_pct,
            "max_group_exposure_pct": max_group_pct,
        },
        "allocation_note": "Strategy attribution is conservative until normalized positions table is fully populated.",
    })

    return {"ok": len(reasons) == 0, "reason": "OK" if not reasons else ";".join(reasons), "details": details}


def list_open_orders(symbol: Optional[str] = None) -> list[Dict[str, Any]]:
    params: Dict[str, Any] = {"category": "linear", "settleCoin": "USDT"}
    if symbol:
        params.pop("settleCoin", None)
        params["symbol"] = normalize_symbol(symbol)
    try:
        resp = bybit("GET", "/v5/order/realtime", params)
        return (resp.get("result") or {}).get("list") or []
    except Exception as exc:
        log(f"[WARN] list_open_orders failed: {exc}")
        return []


def build_reconciliation_report(days: int = RECONCILIATION_LOOKBACK_DAYS) -> Dict[str, Any]:
    safe_days = max(1, min(int(days), 30))
    open_positions = get_all_open_positions()
    open_orders = list_open_orders()
    rows = fetch_supabase_logs_since(days=safe_days, limit=10000) if supabase_enabled() else []

    known_order_symbols = {normalize_symbol(r.get("symbol", "")) for r in rows if r.get("status") == "order_sent" or r.get("order_id")}
    open_position_symbols = {normalize_symbol(p.get("symbol", "")) for p in open_positions}

    unknown_positions = []
    protection_warnings = []
    for pos in open_positions:
        symbol = normalize_symbol(pos.get("symbol", ""))
        if symbol and symbol not in known_order_symbols:
            unknown_positions.append(pos)
        protection = validate_post_order_protection(symbol)
        if not protection.get("ok"):
            protection_warnings.append({"symbol": symbol, "protection": protection})

    reduce_only_orders = [o for o in open_orders if str(o.get("reduceOnly", "")).lower() == "true" or o.get("reduceOnly") is True]
    orphan_reduce_only_orders = []
    for order in reduce_only_orders:
        symbol = normalize_symbol(order.get("symbol", ""))
        if symbol and symbol not in open_position_symbols:
            orphan_reduce_only_orders.append(order)

    issues = []
    if unknown_positions:
        issues.append("UNKNOWN_BYBIT_OPEN_POSITION")
    if protection_warnings:
        issues.append("OPEN_POSITION_PROTECTION_WARNING")
    if orphan_reduce_only_orders:
        issues.append("ORPHAN_REDUCE_ONLY_ORDERS")

    return {
        "ok": len(issues) == 0,
        "days": safe_days,
        "issues": issues,
        "open_positions_count": len(open_positions),
        "open_orders_count": len(open_orders),
        "known_order_symbols": sorted(x for x in known_order_symbols if x),
        "open_position_symbols": sorted(x for x in open_position_symbols if x),
        "unknown_positions": unknown_positions,
        "protection_warnings": protection_warnings,
        "orphan_reduce_only_orders": orphan_reduce_only_orders,
        "summary": {
            "unknown_positions": len(unknown_positions),
            "protection_warnings": len(protection_warnings),
            "orphan_reduce_only_orders": len(orphan_reduce_only_orders),
        },
    }


def build_recovery_status(days: int = RECONCILIATION_LOOKBACK_DAYS) -> Dict[str, Any]:
    report = build_reconciliation_report(days=days)
    actions = []
    if report["summary"]["unknown_positions"] > 0:
        actions.append("Review unknown Bybit positions; consider manual pause or emergency close.")
    if report["summary"]["protection_warnings"] > 0:
        actions.append("Review missing TP/SL protection immediately.")
    if report["summary"]["orphan_reduce_only_orders"] > 0:
        actions.append("Review/cancel orphan reduce-only orders.")
    if not actions:
        actions.append("No recovery action required.")
    return {"ok": report["ok"], "reconciliation": report, "recommended_actions": actions}


def run_recovery_scan(days: int = RECONCILIATION_LOOKBACK_DAYS, notify: bool = True) -> Dict[str, Any]:
    status = build_recovery_status(days=days)
    report = status["reconciliation"]
    if not status["ok"]:
        if notify and RECOVERY_NOTIFY_ON_STARTUP_ISSUES:
            safe_notify_event(
                "⚠️ Recovery scan warning",
                f"Issues: {', '.join(report.get('issues', []))}\nSummary: {json.dumps(report.get('summary', {}), ensure_ascii=False)}",
                important=True,
            )
        if RECOVERY_AUTO_PAUSE_ON_UNKNOWN_POSITION and report["summary"].get("unknown_positions", 0) > 0:
            set_trading_paused(True, reason="Recovery scan detected unknown Bybit position")
    return status


def assess_order_execution_quality(body: Dict[str, Any], result: Dict[str, Any], order_id: str = "") -> Dict[str, Any]:
    if not EXECUTION_QUALITY_ENABLED:
        return {"ok": True, "reason": "EXECUTION_QUALITY_DISABLED", "details": {}}
    symbol = normalize_symbol(body.get("symbol", ""))
    signal_price = to_float_or_none(body.get("signalPrice"))
    if signal_price is None or signal_price <= 0:
        return {"ok": False, "reason": "MISSING_SIGNAL_PRICE", "details": {"symbol": symbol, "order_id": order_id}}
    details: Dict[str, Any] = {"symbol": symbol, "order_id": order_id, "signal_price": signal_price}
    try:
        pos = get_position_linear(symbol)
        avg_price = to_float_or_none(pos.get("avgPrice"))
        mark_price = to_float_or_none(pos.get("markPrice"))
        details.update({"avg_price": avg_price, "mark_price": mark_price, "position": pos})
        ref_price = avg_price or mark_price
        if ref_price and ref_price > 0:
            slippage_pct = abs(ref_price - signal_price) / signal_price * 100.0
            details["execution_ref_price"] = ref_price
            details["slippage_pct"] = slippage_pct
            details["max_allowed_slippage_pct"] = MAX_ALLOWED_SLIPPAGE_PCT
            ok_flag = MAX_ALLOWED_SLIPPAGE_PCT <= 0 or slippage_pct <= MAX_ALLOWED_SLIPPAGE_PCT
            reason = "OK" if ok_flag else f"SLIPPAGE_TOO_HIGH_{slippage_pct:.4f}%_MAX_{MAX_ALLOWED_SLIPPAGE_PCT:.4f}%"
            return {"ok": ok_flag, "reason": reason, "details": details}
        return {"ok": False, "reason": "NO_EXECUTION_REFERENCE_PRICE", "details": details}
    except Exception as exc:
        details["error"] = str(exc)
        return {"ok": False, "reason": "EXECUTION_QUALITY_CHECK_FAILED", "details": details}


def assess_execution_quality_after_order(body: Dict[str, Any], result: Dict[str, Any], order_id: str = "") -> Dict[str, Any]:
    quality = assess_order_execution_quality(body, result, order_id)
    try:
        write_system_log(
            action="execution_quality_assessed",
            symbol=normalize_symbol(body.get("symbol", "")),
            side=str(body.get("side", "")).upper(),
            decision="EXECUTION_QUALITY_OK" if quality.get("ok") else "EXECUTION_QUALITY_WARNING",
            reason=quality.get("reason", "UNKNOWN"),
            order_id=order_id,
            status="logged" if quality.get("ok") else "warning",
            extra={"execution_quality": quality},
        )
    except Exception as exc:
        log(f"[WARN] execution quality log failed: {exc}")
    return quality


def compute_promotion_status(strategy: str, symbol: str, side: str, days: int = 30) -> Dict[str, Any]:
    symbol = normalize_symbol(symbol)
    side = normalize_side(side)
    safe_days = max(1, min(days, 90))
    rows = fetch_supabase_logs_since(days=safe_days, limit=10000) if supabase_enabled() else []
    matched = [r for r in rows if r.get("strategy") == strategy and normalize_symbol(r.get("symbol", "")) == symbol and str(r.get("side", "")).upper() == side]
    event_count = len(matched)
    paper_events = sum(1 for r in matched if r.get("decision") == "PAPER_LOGGED")
    orders = sum(1 for r in matched if r.get("status") == "order_sent")
    rejected = sum(1 for r in matched if str(r.get("decision", "")).endswith("REJECTED") or r.get("decision") == "REJECTED")
    protection_failures = sum(1 for r in matched if r.get("decision") == "PROTECTION_VERIFY_FAILED")
    rejection_rate = rejected / event_count if event_count else 0.0

    start_ms, end_ms = utc_range_last_days(safe_days)
    pnl_summary = summarize_closed_pnl(get_closed_pnl(start_ms, end_ms, symbol=symbol))
    pf = pnl_summary.get("profit_factor")

    ready_for_micro = paper_events >= PROMOTION_MIN_PAPER_EVENTS and rejection_rate <= PROMOTION_MAX_REJECTION_RATE and protection_failures <= PROMOTION_MAX_PROTECTION_FAILURES
    ready_for_live = orders >= PROMOTION_MIN_MICRO_ORDERS and (pf is None or pf >= PROMOTION_MIN_PROFIT_FACTOR) and rejection_rate <= PROMOTION_MAX_REJECTION_RATE and protection_failures <= PROMOTION_MAX_PROTECTION_FAILURES

    if ready_for_live:
        status = "READY_FOR_LIVE"
    elif ready_for_micro:
        status = "READY_FOR_MICRO"
    elif event_count >= max(3, PROMOTION_MIN_PAPER_EVENTS // 2):
        status = "WATCH"
    else:
        status = "NOT_READY"

    reasons = []
    if paper_events < PROMOTION_MIN_PAPER_EVENTS:
        reasons.append(f"paper_events {paper_events}/{PROMOTION_MIN_PAPER_EVENTS}")
    if orders < PROMOTION_MIN_MICRO_ORDERS:
        reasons.append(f"micro_orders {orders}/{PROMOTION_MIN_MICRO_ORDERS}")
    if pf is not None and pf < PROMOTION_MIN_PROFIT_FACTOR:
        reasons.append(f"profit_factor {pf:.2f} < {PROMOTION_MIN_PROFIT_FACTOR:.2f}")
    if rejection_rate > PROMOTION_MAX_REJECTION_RATE:
        reasons.append(f"rejection_rate {rejection_rate:.1%} > {PROMOTION_MAX_REJECTION_RATE:.1%}")
    if protection_failures > PROMOTION_MAX_PROTECTION_FAILURES:
        reasons.append(f"protection_failures {protection_failures} > {PROMOTION_MAX_PROTECTION_FAILURES}")

    return {
        "strategy": strategy,
        "symbol": symbol,
        "side": side,
        "days": safe_days,
        "status": status,
        "ready_for_micro": ready_for_micro,
        "ready_for_live": ready_for_live,
        "metrics": {
            "event_count": event_count,
            "paper_events": paper_events,
            "orders": orders,
            "rejected": rejected,
            "rejection_rate": rejection_rate,
            "protection_failures": protection_failures,
            "profit_factor": pf,
            "closed_pnl": pnl_summary,
        },
        "reasons": reasons or ["Promotion criteria satisfied for current status."],
    }


def build_all_promotion_status(days: int = 30) -> Dict[str, Any]:
    state = load_state()
    items = []
    for strategy, scfg in state.get("strategies", {}).items():
        for symbol, symcfg in scfg.get("symbols", {}).items():
            for side in ("LONG", "SHORT"):
                if side in symcfg:
                    items.append(compute_promotion_status(strategy, symbol, side, days=days))
    counts: Dict[str, int] = {}
    for item in items:
        counts[item["status"]] = counts.get(item["status"], 0) + 1
    return {"ok": True, "days": days, "counts": counts, "items": items}


def build_strategy_review_report(days: int = STRATEGY_REVIEW_LOOKBACK_DAYS) -> Dict[str, Any]:
    safe_days = max(1, min(days, 90))
    health = build_strategy_health(days=min(safe_days, 30)) if supabase_enabled() else {"items": []}
    promotions = build_all_promotion_status(days=safe_days)
    backtest = build_backtest_vs_live_report(days=min(safe_days, 30))
    recommendations = []

    promotion_index = {(p["strategy"], p["symbol"], p["side"]): p for p in promotions.get("items", [])}
    for item in health.get("items", []):
        key = (item.get("strategy"), item.get("symbol"), item.get("side"))
        promo = promotion_index.get(key, {})
        hdata = item.get("health", {})
        status = hdata.get("status")
        mode = str(item.get("mode", "")).upper()
        rec = "KEEP_CURRENT_MODE"
        if status == "BAD" and mode in {"MICRO", "LIVE"}:
            rec = "DOWNGRADE_TO_PAPER_OR_OFF"
        elif promo.get("status") == "READY_FOR_MICRO" and mode == "PAPER":
            rec = "CONSIDER_PROMOTION_TO_MICRO"
        elif promo.get("status") == "READY_FOR_LIVE" and mode == "MICRO":
            rec = "CONSIDER_PROMOTION_TO_LIVE_WITH_SMALL_RISK"
        elif item.get("price_deviation_rejected", 0) or item.get("order_quality_rejected", 0):
            rec = "REVIEW_ALERT_PARAMETERS_OR_LIMITS"
        recommendations.append({
            "strategy": item.get("strategy"),
            "symbol": item.get("symbol"),
            "side": item.get("side"),
            "mode": mode,
            "health": hdata,
            "promotion": promo,
            "recommendation": rec,
            "reasons": hdata.get("reasons", []),
        })

    return {
        "ok": True,
        "days": safe_days,
        "generated_at": now_iso(),
        "summary": {
            "health_groups": len(health.get("items", [])),
            "promotion_counts": promotions.get("counts", {}),
            "backtest_comparisons": len(backtest.get("comparisons", [])) if isinstance(backtest, dict) else 0,
        },
        "recommendations": recommendations,
        "promotion_status": promotions,
        "backtest_vs_live": backtest,
    }


def handle_telegram_command_text(text: str) -> Dict[str, Any]:
    if not TELEGRAM_COMMANDS_ENABLED:
        return {"ok": False, "response": "Telegram commands are disabled."}
    cmd = (text or "").strip()
    cmd_lower = cmd.lower()
    try:
        if cmd_lower in {"/status", "status"}:
            risk = summarize_open_risk()
            runtime = load_runtime_state()
            response = f"Status\nreal_orders={ENABLE_REAL_ORDERS}\npaused={runtime.get('trading_paused')}\nopen_positions={risk.get('open_positions')}\nopen_value={risk.get('total_position_value'):.4f}\nunrealized={risk.get('total_unrealized_pnl'):.4f}"
        elif cmd_lower in {"/positions", "positions"}:
            risk = summarize_open_risk()
            if not risk.get("by_symbol"):
                response = "No open positions."
            else:
                lines = ["Open positions:"]
                for sym, data in risk.get("by_symbol", {}).items():
                    lines.append(f"{sym} {data.get('side')} size={data.get('size')} value={data.get('position_value'):.4f} pnl={data.get('unrealized_pnl'):.4f}")
                response = "\n".join(lines)
        elif cmd_lower.startswith("/report") or cmd_lower == "report":
            rep = build_performance_report(days=1)
            orders = rep.get("orders", {})
            pnl = rep.get("bybit_closed_pnl", {})
            response = f"Daily report\nevents={rep.get('event_count')}\norders_sent={orders.get('order_sent')}\nfailed={orders.get('order_failed')}\nrejected={orders.get('rejected')}\nnet_pnl={pnl.get('net_pnl')}"
        elif cmd_lower.startswith("/reconcile"):
            report = build_reconciliation_report(days=RECONCILIATION_LOOKBACK_DAYS)
            response = f"Reconciliation ok={report.get('ok')} issues={report.get('issues')} summary={report.get('summary')}"
        elif cmd_lower in {"/pause", "pause"}:
            if not TELEGRAM_COMMANDS_ALLOW_TRADING_ACTIONS:
                response = "Trading commands are disabled. Set TELEGRAM_COMMANDS_ALLOW_TRADING_ACTIONS=true to enable."
            else:
                set_trading_paused(True, reason="Telegram command pause")
                response = "Trading paused."
        elif cmd_lower in {"/resume", "resume"}:
            if not TELEGRAM_COMMANDS_ALLOW_TRADING_ACTIONS:
                response = "Trading commands are disabled. Set TELEGRAM_COMMANDS_ALLOW_TRADING_ACTIONS=true to enable."
            else:
                set_trading_paused(False)
                response = "Trading resumed."
        else:
            response = "Supported commands: /status, /positions, /report, /reconcile. Optional trading commands: /pause, /resume."
        return {"ok": True, "response": response}
    except Exception as exc:
        return {"ok": False, "response": f"Command failed: {exc}"}


@app.get("/payload_schema")
def payload_schema(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {
        "ok": True,
        "schema_validation_enabled": PAYLOAD_SCHEMA_VALIDATION_ENABLED,
        "require_version": PAYLOAD_SCHEMA_REQUIRE_VERSION,
        "supported_versions": SUPPORTED_PAYLOAD_VERSIONS,
        "example": {
            "payload_version": "1.0",
            "secret": "***",
            "exchange": "bybit",
            "strategy": "structure_swing_v134",
            "symbol": "SOLUSDT",
            "side": "LONG",
            "orderType": "Market",
            "signalPrice": 84.47,
            "sl": 83.2,
            "tp1": 86.5,
            "tp2": 88.0,
            "riskPct": 0.1,
            "tf": "15",
            "barTime": 1779051154922,
        },
    }


@app.post("/validate_payload")
async def validate_payload_endpoint(request: Request):
    body = await request.json()
    verify_secret(request, body)
    return {"ok": True, "payload_validation": validate_payload_schema(body)}


@app.get("/reconciliation_report")
def reconciliation_report(secret: str, days: int = RECONCILIATION_LOOKBACK_DAYS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "report": build_reconciliation_report(days=days)}


@app.get("/recovery_status")
def recovery_status(secret: str, days: int = RECONCILIATION_LOOKBACK_DAYS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "recovery": build_recovery_status(days=days)}


@app.post("/recovery_scan")
def recovery_scan(secret: str, days: int = RECONCILIATION_LOOKBACK_DAYS, notify: bool = True):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "recovery": run_recovery_scan(days=days, notify=notify)}


@app.get("/execution_quality")
def execution_quality(secret: str, symbol: str, signal_price: Optional[float] = None):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    body = {"symbol": symbol, "side": "LONG", "signalPrice": signal_price or get_ticker_last(normalize_symbol(symbol))}
    return {"ok": True, "execution_quality": assess_order_execution_quality(body, {}, "manual_check")}


@app.get("/capital_allocation_status")
def capital_allocation_status(secret: str, strategy: Optional[str] = None):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    open_risk = summarize_open_risk()
    state = load_state()
    strategies = []
    for sname in state.get("strategies", {}).keys():
        if strategy and sname != strategy:
            continue
        strategies.append(estimate_strategy_exposure(sname))
    return {
        "ok": True,
        "capital_allocation_enabled": CAPITAL_ALLOCATION_ENABLED,
        "global_limits": {
            "max_strategy_exposure_pct": MAX_STRATEGY_EXPOSURE_PCT,
            "max_strategy_position_value_usdt": MAX_STRATEGY_POSITION_VALUE_USDT,
            "max_group_exposure_pct": MAX_GROUP_EXPOSURE_PCT,
        },
        "open_risk": open_risk,
        "strategies": strategies,
    }


@app.get("/promotion_status")
def promotion_status(secret: str, strategy: str, symbol: str, side: str, days: int = 30):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "promotion": compute_promotion_status(strategy, symbol, side, days=days)}


@app.get("/promotion_all")
def promotion_all(secret: str, days: int = 30):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_all_promotion_status(days=days)


@app.post("/telegram_command")
async def telegram_command(request: Request):
    body = await request.json()
    if body.get("secret"):
        verify_secret(request, body)
        source_chat_id = str(body.get("chat_id") or TELEGRAM_CHAT_ID)
    else:
        source_chat_id = str((body.get("message") or {}).get("chat", {}).get("id", ""))
        if not telegram_user_allowed(source_chat_id):
            raise HTTPException(401, "Unauthorized Telegram user")
    text = body.get("text") or (body.get("message") or {}).get("text") or ""
    result = handle_telegram_command_text_secure(text, source_chat_id)
    if TELEGRAM_ENABLED and result.get("response"):
        safe_notify_event("🤖 Command response", result["response"], important=False)
    return result


@app.get("/telegram_command")
def telegram_command_get(secret: str, text: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    result = handle_telegram_command_text_secure(text, str(TELEGRAM_CHAT_ID))
    if TELEGRAM_ENABLED and result.get("response"):
        safe_notify_event("🤖 Command response", result["response"], important=False)
    return result


@app.get("/strategy_review_report")
def strategy_review_report(secret: str, days: int = STRATEGY_REVIEW_LOOKBACK_DAYS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_strategy_review_report(days=days)


@app.get("/supabase_split_model")
def supabase_split_model(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {
        "ok": True,
        "enabled": SUPABASE_SPLIT_TABLES_ENABLED,
        "tables": {
            "orders": SUPABASE_ORDERS_TABLE,
            "positions": SUPABASE_POSITIONS_TABLE,
            "system_events": SUPABASE_SYSTEM_EVENTS_TABLE,
            "strategy_state_history": SUPABASE_STRATEGY_HISTORY_TABLE,
            "daily_reports": SUPABASE_DAILY_REPORTS_TABLE,
            "telegram_notifications": SUPABASE_TELEGRAM_TABLE,
            "backtest_results": SUPABASE_BACKTEST_TABLE,
        },
        "note": "The application writes these tables best-effort when SUPABASE_SPLIT_TABLES_ENABLED=true. Missing tables do not break trading.",
        "minimal_sql_hint": "Create nullable columns matching the JSON payloads, or keep SUPABASE_SPLIT_TABLES_ENABLED=false until schema migration is ready.",
    }

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

# ============================================================
# v6.5.0 / v6.7.0 EXTENSIONS
# Order hardening, safer auto-close, audit/compliance, replay,
# portfolio/correlation risk, monitoring, config validation,
# and lightweight control panel.
# ============================================================

import uuid

APP_FEATURE_LEVEL = "9.1.3"

SUPABASE_ORDERS_TABLE = os.getenv("SUPABASE_ORDERS_TABLE", "orders")
SUPABASE_POSITIONS_TABLE = os.getenv("SUPABASE_POSITIONS_TABLE", "positions")
SUPABASE_SYSTEM_EVENTS_TABLE = os.getenv("SUPABASE_SYSTEM_EVENTS_TABLE", "system_events")
SUPABASE_STATE_HISTORY_TABLE = os.getenv("SUPABASE_STATE_HISTORY_TABLE", "strategy_state_history")
SUPABASE_DAILY_REPORTS_TABLE = os.getenv("SUPABASE_DAILY_REPORTS_TABLE", "daily_reports")
SUPABASE_TELEGRAM_TABLE = os.getenv("SUPABASE_TELEGRAM_TABLE", "telegram_notifications")
SUPABASE_BACKTEST_TABLE = os.getenv("SUPABASE_BACKTEST_TABLE", "backtest_results")
SUPABASE_SPLIT_WRITE_ENABLED = os.getenv("SUPABASE_SPLIT_WRITE_ENABLED", "false").lower() == "true"
AUDIT_LOG_ENABLED = os.getenv("AUDIT_LOG_ENABLED", "true").lower() == "true"
AUDIT_PAYLOAD_HASH_ENABLED = os.getenv("AUDIT_PAYLOAD_HASH_ENABLED", "true").lower() == "true"

BYBIT_RETRY_ENABLED = os.getenv("BYBIT_RETRY_ENABLED", "true").lower() == "true"
BYBIT_RETRY_ATTEMPTS = int(os.getenv("BYBIT_RETRY_ATTEMPTS", "3"))
BYBIT_RETRY_SLEEP_SEC = float(os.getenv("BYBIT_RETRY_SLEEP_SEC", "0.35"))
ORDER_VERIFY_AFTER_ENTRY = os.getenv("ORDER_VERIFY_AFTER_ENTRY", "true").lower() == "true"
ORDER_VERIFY_SLEEP_SEC = float(os.getenv("ORDER_VERIFY_SLEEP_SEC", "0.5"))
ORDER_VERIFY_RETRIES = int(os.getenv("ORDER_VERIFY_RETRIES", "4"))

SAFE_AUTO_CLOSE_ENABLED = os.getenv("SAFE_AUTO_CLOSE_ENABLED", "false").lower() == "true"
SAFE_AUTO_CLOSE_REQUIRE_PROTECTION_MISSING = os.getenv("SAFE_AUTO_CLOSE_REQUIRE_PROTECTION_MISSING", "true").lower() == "true"
SAFE_AUTO_CLOSE_MAX_POSITION_VALUE_USDT = float(os.getenv("SAFE_AUTO_CLOSE_MAX_POSITION_VALUE_USDT", "50"))
SAFE_AUTO_CLOSE_DOUBLE_CHECK_SLEEP_SEC = float(os.getenv("SAFE_AUTO_CLOSE_DOUBLE_CHECK_SLEEP_SEC", "1.0"))

TELEGRAM_ALLOWED_USER_IDS = [x.strip() for x in os.getenv("TELEGRAM_ALLOWED_USER_IDS", str(TELEGRAM_CHAT_ID)).split(",") if x.strip()]
TELEGRAM_CONFIRM_TTL_SEC = int(os.getenv("TELEGRAM_CONFIRM_TTL_SEC", "180"))
TELEGRAM_COMMAND_RATE_LIMIT_SEC = float(os.getenv("TELEGRAM_COMMAND_RATE_LIMIT_SEC", "1.5"))
_pending_telegram_confirms: Dict[str, Dict[str, Any]] = {}
_last_telegram_command_at: Dict[str, float] = {}

REPLAY_MAX_EVENTS = int(os.getenv("REPLAY_MAX_EVENTS", "250"))

CORRELATION_GUARD_ENABLED = os.getenv("CORRELATION_GUARD_ENABLED", "false").lower() == "true"
CORRELATION_GROUPS_JSON = os.getenv("CORRELATION_GROUPS_JSON", "{}")

MARKET_REGIME_FILTER_ENABLED = os.getenv("MARKET_REGIME_FILTER_ENABLED", "false").lower() == "true"
MARKET_MAX_1M_MOVE_PCT = float(os.getenv("MARKET_MAX_1M_MOVE_PCT", "3.0"))
MARKET_MAX_SPREAD_PCT = float(os.getenv("MARKET_MAX_SPREAD_PCT", "0.35"))
MARKET_NEWS_BLACKOUT_UTC = os.getenv("MARKET_NEWS_BLACKOUT_UTC", "")

SIGNAL_STALE_HOURS = float(os.getenv("SIGNAL_STALE_HOURS", "24"))


def json_hash(data: Any) -> str:
    try:
        raw = json.dumps(data, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        raw = str(data)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def supabase_url_for_table(table: str) -> str:
    return f"{SUPABASE_URL}/rest/v1/{table}"


def supabase_optional_insert(table: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not supabase_enabled():
        return {"ok": False, "reason": "SUPABASE_DISABLED"}
    try:
        resp = client.post(supabase_url_for_table(table), headers=supabase_headers(), json=payload)
        if resp.status_code >= 400:
            log(f"[WARN] optional Supabase insert failed table={table}: {resp.status_code} {resp.text}")
            return {"ok": False, "status_code": resp.status_code, "reason": resp.text}
        return {"ok": True, "table": table}
    except Exception as exc:
        log(f"[WARN] optional Supabase insert exception table={table}: {exc}")
        return {"ok": False, "table": table, "reason": str(exc)}


def write_audit_event(event_type: str, payload: Dict[str, Any], status: str = "logged") -> Dict[str, Any]:
    if not AUDIT_LOG_ENABLED:
        return {"ok": False, "reason": "AUDIT_DISABLED"}
    record = {
        "timestamp_utc": now_iso(),
        "event_type": event_type,
        "status": status,
        "payload_hash": json_hash(payload) if AUDIT_PAYLOAD_HASH_ENABLED else None,
        "payload": sanitize_payload(payload) if isinstance(payload, dict) else {"value": str(payload)},
    }
    return supabase_optional_insert(SUPABASE_SYSTEM_EVENTS_TABLE, record)


_V65_BASE_BYBIT = bybit

def bybit(method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    attempts = max(1, BYBIT_RETRY_ATTEMPTS if BYBIT_RETRY_ENABLED else 1)
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            resp = _V65_BASE_BYBIT(method, path, params)
            ret_code = resp.get("retCode")
            if ret_code in {10000, 10002, 10006, 10016} and attempt < attempts:
                log(f"[WARN] Bybit transient retCode={ret_code}, retry {attempt}/{attempts}")
                time.sleep(BYBIT_RETRY_SLEEP_SEC * attempt)
                continue
            return resp
        except HTTPException as exc:
            last_exc = exc
            if attempt >= attempts or exc.status_code not in {408, 409, 425, 429, 500, 502, 503, 504}:
                raise
            log(f"[WARN] Bybit HTTPException retry {attempt}/{attempts}: {exc.detail}")
            time.sleep(BYBIT_RETRY_SLEEP_SEC * attempt)
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts:
                raise
            log(f"[WARN] Bybit exception retry {attempt}/{attempts}: {exc}")
            time.sleep(BYBIT_RETRY_SLEEP_SEC * attempt)
    if last_exc:
        raise last_exc
    raise RuntimeError("Bybit request failed without exception")


_V65_BASE_WRITE_TRADE_LOG = write_trade_log

def write_trade_log(body: Dict[str, Any], mode: str, risk_pct_used: float, decision: str, decision_reason: str, order_id: str = "", status: str = "logged") -> None:
    _V65_BASE_WRITE_TRADE_LOG(body, mode, risk_pct_used, decision, decision_reason, order_id, status)
    try:
        audit_payload = {"body": sanitize_payload(body), "mode": mode, "risk_pct_used": risk_pct_used, "decision": decision, "decision_reason": decision_reason, "order_id": order_id, "status": status}
        write_audit_event("trade_decision", audit_payload, status=status)
        if SUPABASE_SPLIT_WRITE_ENABLED:
            if order_id:
                supabase_optional_insert(SUPABASE_ORDERS_TABLE, {"timestamp_utc": now_iso(), "strategy": body.get("strategy", "UNKNOWN"), "symbol": normalize_symbol(body.get("symbol", "")), "side": str(body.get("side", "")).upper(), "mode": mode, "order_id": order_id, "decision": decision, "status": status, "raw_payload": sanitize_payload(body)})
            if str(body.get("strategy", "")).startswith("SYSTEM") or mode == "SYSTEM":
                supabase_optional_insert(SUPABASE_SYSTEM_EVENTS_TABLE, {"timestamp_utc": now_iso(), "event_type": decision, "status": status, "payload": audit_payload})
    except Exception as exc:
        log(f"[WARN] extended write_trade_log failed: {exc}")


def strategy_state_snapshot(reason: str, source: str = "system", before: Optional[Dict[str, Any]] = None, after: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    before_state = before if before is not None else load_state()
    after_state = after if after is not None else before_state
    version_id = str(uuid.uuid4())
    payload = {"timestamp_utc": now_iso(), "version_id": version_id, "source": source, "reason": reason, "before_hash": json_hash(before_state), "after_hash": json_hash(after_state), "before_state": before_state, "after_state": after_state}
    supabase_optional_insert(SUPABASE_STATE_HISTORY_TABLE, payload)
    write_audit_event("strategy_state_snapshot", payload)
    return {"ok": True, "version_id": version_id, "before_hash": payload["before_hash"], "after_hash": payload["after_hash"]}


_V65_BASE_SET_STRATEGY_SIDE_CONFIG = set_strategy_side_config

def set_strategy_side_config(strategy: str, symbol: str, side: str, mode: Optional[str] = None, risk_pct: Optional[float] = None, extra_updates: Optional[Dict[str, Any]] = None, reason: str = "api_update") -> Dict[str, Any]:
    before = load_state()
    result = _V65_BASE_SET_STRATEGY_SIDE_CONFIG(strategy, symbol, side, mode, risk_pct, extra_updates, reason)
    try:
        after = load_state()
        result["state_version"] = strategy_state_snapshot(reason=reason, source="strategy_side_update", before=before, after=after)
    except Exception as exc:
        result["state_version_error"] = str(exc)
    return result


def fetch_strategy_state_history(limit: int = 20) -> list[Dict[str, Any]]:
    if not supabase_enabled():
        return []
    params = {"select": "*", "order": "timestamp_utc.desc", "limit": str(max(1, min(limit, 200)))}
    resp = client.get(supabase_url_for_table(SUPABASE_STATE_HISTORY_TABLE), headers=supabase_headers(prefer=""), params=params)
    if resp.status_code >= 400:
        return []
    rows = resp.json()
    return rows if isinstance(rows, list) else []


@app.get("/strategy_state_history")
def strategy_state_history(secret: str, limit: int = 20):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "history": fetch_strategy_state_history(limit=limit)}


@app.post("/strategy_state_rollback")
async def strategy_state_rollback(request: Request):
    body = await request.json()
    verify_secret(request, body)
    require_strategy_admin()
    version_id = str(body.get("version_id", ""))
    if not version_id:
        raise HTTPException(400, "version_id required")
    selected = next((r for r in fetch_strategy_state_history(limit=200) if str(r.get("version_id")) == version_id), None)
    if not selected:
        raise HTTPException(404, "version_id not found in recent history")
    target_state = selected.get("before_state") or selected.get("after_state")
    if not isinstance(target_state, dict):
        raise HTTPException(400, "Selected history row does not contain a usable state")
    before = load_state()
    save_state(target_state)
    snap = strategy_state_snapshot("rollback", source="strategy_state_rollback", before=before, after=target_state)
    safe_notify_event("↩️ Strategy state rollback", f"Rolled back to version {version_id}\nnew_version={snap.get('version_id')}", important=True)
    return {"ok": True, "rolled_back_to": version_id, "new_version": snap, "state": target_state}


def load_correlation_groups() -> Dict[str, Any]:
    try:
        data = json.loads(CORRELATION_GROUPS_JSON or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def validate_portfolio_correlation_guard(symbol: str, projected_total_position_value: Optional[float] = None, equity: Optional[float] = None) -> Dict[str, Any]:
    if not CORRELATION_GUARD_ENABLED:
        return {"ok": True, "reason": "CORRELATION_GUARD_DISABLED", "details": {"groups": load_correlation_groups()}}
    symbol = normalize_symbol(symbol)
    groups = load_correlation_groups()
    open_risk = summarize_open_risk()
    by_symbol = open_risk.get("by_symbol", {})
    equity = equity if equity is not None else get_equity_usdt()
    reasons = []
    details = {}
    for group_name, cfg in groups.items():
        symbols = [normalize_symbol(s) for s in cfg.get("symbols", [])]
        if symbol not in symbols:
            continue
        open_symbols = [s for s in symbols if s in by_symbol]
        group_value = sum(float(by_symbol.get(s, {}).get("position_value", 0.0) or 0.0) for s in symbols)
        if projected_total_position_value is not None and symbol not in by_symbol:
            current_total = float(open_risk.get("total_position_value", 0.0) or 0.0)
            estimated_new = max(0.0, projected_total_position_value - current_total)
            group_value += estimated_new
            open_symbols.append(symbol)
        group_exposure_pct = (group_value / equity * 100.0) if equity and equity > 0 else None
        max_open = int(cfg.get("max_open_positions", 999) or 999)
        max_pct = float(cfg.get("max_group_exposure_pct", 0) or 0)
        details[group_name] = {"symbols": symbols, "open_symbols": open_symbols, "group_value": group_value, "group_exposure_pct": group_exposure_pct, "max_open_positions": max_open, "max_group_exposure_pct": max_pct}
        if len(set(open_symbols)) > max_open:
            reasons.append(f"CORRELATION_MAX_OPEN_EXCEEDED_{group_name}_{len(set(open_symbols))}_MAX_{max_open}")
        if max_pct > 0 and group_exposure_pct is not None and group_exposure_pct > max_pct:
            reasons.append(f"CORRELATION_GROUP_EXPOSURE_EXCEEDED_{group_name}_{group_exposure_pct:.4f}%_MAX_{max_pct:.4f}%")
    return {"ok": not reasons, "reason": "OK" if not reasons else ";".join(reasons), "details": details}


def validate_market_regime(symbol: str) -> Dict[str, Any]:
    if not MARKET_REGIME_FILTER_ENABLED:
        return {"ok": True, "reason": "MARKET_REGIME_FILTER_DISABLED"}
    symbol = normalize_symbol(symbol)
    reasons = []
    details: Dict[str, Any] = {"symbol": symbol}
    try:
        ticker = bybit("GET", "/v5/market/tickers", {"category": "linear", "symbol": symbol})
        items = (ticker.get("result") or {}).get("list") or []
        item = items[0] if items else {}
        bid = to_float_or_none(item.get("bid1Price"))
        ask = to_float_or_none(item.get("ask1Price"))
        last = to_float_or_none(item.get("lastPrice"))
        if bid and ask and last and last > 0:
            spread_pct = (ask - bid) / last * 100.0
            details["spread_pct"] = spread_pct
            if spread_pct > MARKET_MAX_SPREAD_PCT:
                reasons.append(f"MARKET_SPREAD_TOO_WIDE_{spread_pct:.4f}%_MAX_{MARKET_MAX_SPREAD_PCT:.4f}%")
        k = bybit("GET", "/v5/market/kline", {"category": "linear", "symbol": symbol, "interval": "1", "limit": 2})
        klines = (k.get("result") or {}).get("list") or []
        if len(klines) >= 2:
            newest = klines[0]
            older = klines[1]
            new_close = float(newest[4])
            old_close = float(older[4])
            if old_close > 0:
                move_pct = abs(new_close - old_close) / old_close * 100.0
                details["move_1m_pct"] = move_pct
                if move_pct > MARKET_MAX_1M_MOVE_PCT:
                    reasons.append(f"MARKET_1M_MOVE_TOO_HIGH_{move_pct:.4f}%_MAX_{MARKET_MAX_1M_MOVE_PCT:.4f}%")
    except Exception as exc:
        details["error"] = str(exc)
    return {"ok": not reasons, "reason": "OK" if not reasons else ";".join(reasons), "details": details}


_V65_BASE_VALIDATE_EXPOSURE = validate_pre_trade_exposure

def validate_pre_trade_exposure(body: Dict[str, Any], risk_pct_used: float) -> Dict[str, Any]:
    base = _V65_BASE_VALIDATE_EXPOSURE(body, risk_pct_used)
    if not base.get("ok"):
        return base
    symbol = normalize_symbol(body.get("symbol", ""))
    details = base.get("details") or {}
    market = validate_market_regime(symbol)
    corr = validate_portfolio_correlation_guard(symbol, details.get("projected_total_position_value"), details.get("equity"))
    base["market_regime"] = market
    base["correlation_guard"] = corr
    if not market.get("ok"):
        return {"ok": False, "reason": market.get("reason", "MARKET_REGIME_REJECTED"), "details": details, "market_regime": market, "correlation_guard": corr}
    if not corr.get("ok"):
        return {"ok": False, "reason": corr.get("reason", "CORRELATION_GUARD_REJECTED"), "details": details, "market_regime": market, "correlation_guard": corr}
    return base


def get_order_status(symbol: str, order_id: str = "", order_link_id: str = "") -> Dict[str, Any]:
    symbol = normalize_symbol(symbol)
    params: Dict[str, Any] = {"category": "linear", "symbol": symbol}
    if order_id:
        params["orderId"] = order_id
    if order_link_id:
        params["orderLinkId"] = order_link_id
    try:
        return bybit("GET", "/v5/order/realtime", params)
    except Exception as exc:
        return {"retCode": -1, "retMsg": str(exc), "result": {"list": []}}


_V65_BASE_EXECUTE_BYBIT_TRADE = execute_bybit_trade

def execute_bybit_trade(body: Dict[str, Any], risk_pct_used: float) -> Dict[str, Any]:
    exec_id = str(uuid.uuid4())
    write_audit_event("execution_started", {"exec_id": exec_id, "body": sanitize_payload(body), "risk_pct_used": risk_pct_used})
    last_exc = None
    attempts = max(1, BYBIT_RETRY_ATTEMPTS if BYBIT_RETRY_ENABLED else 1)
    for attempt in range(1, attempts + 1):
        try:
            result = _V65_BASE_EXECUTE_BYBIT_TRADE(body, risk_pct_used)
            result["execution_id"] = exec_id
            result["execution_attempt"] = attempt
            symbol = normalize_symbol(body.get("symbol", ""))
            order_id = result.get("order_id", "")
            if ORDER_VERIFY_AFTER_ENTRY and order_id:
                status = {}
                for _ in range(max(1, ORDER_VERIFY_RETRIES)):
                    time.sleep(ORDER_VERIFY_SLEEP_SEC)
                    status = get_order_status(symbol=symbol, order_id=order_id)
                    rows = (status.get("result") or {}).get("list") or []
                    if rows:
                        break
                result["entry_order_status"] = status
            write_audit_event("execution_completed", {"exec_id": exec_id, "result": result}, status="order_sent")
            return result
        except Exception as exc:
            last_exc = exc
            write_audit_event("execution_attempt_failed", {"exec_id": exec_id, "attempt": attempt, "error": str(exc)}, status="error")
            if attempt >= attempts:
                break
            time.sleep(BYBIT_RETRY_SLEEP_SEC * attempt)
    write_audit_event("execution_failed", {"exec_id": exec_id, "error": str(last_exc)}, status="error")
    raise last_exc if last_exc else RuntimeError("execution failed")


def safer_auto_close_check(symbol: str) -> Dict[str, Any]:
    symbol = normalize_symbol(symbol)
    pos1 = get_position_linear(symbol)
    size1 = abs(float(pos1.get("size", "0") or 0.0))
    value1 = float(pos1.get("positionValue", "0") or 0.0)
    if size1 <= 0:
        return {"ok": False, "allowed": False, "reason": "NO_OPEN_POSITION", "position": pos1}
    if SAFE_AUTO_CLOSE_MAX_POSITION_VALUE_USDT > 0 and value1 > SAFE_AUTO_CLOSE_MAX_POSITION_VALUE_USDT:
        return {"ok": False, "allowed": False, "reason": f"POSITION_VALUE_TOO_LARGE_FOR_SAFE_AUTO_CLOSE_{value1:.4f}_MAX_{SAFE_AUTO_CLOSE_MAX_POSITION_VALUE_USDT:.4f}", "position": pos1}
    protection = validate_post_order_protection(symbol)
    if SAFE_AUTO_CLOSE_REQUIRE_PROTECTION_MISSING and protection.get("ok"):
        return {"ok": False, "allowed": False, "reason": "PROTECTION_PRESENT_AUTO_CLOSE_NOT_ALLOWED", "protection": protection}
    time.sleep(SAFE_AUTO_CLOSE_DOUBLE_CHECK_SLEEP_SEC)
    pos2 = get_position_linear(symbol)
    size2 = abs(float(pos2.get("size", "0") or 0.0))
    if size2 <= 0:
        return {"ok": False, "allowed": False, "reason": "POSITION_CLOSED_DURING_DOUBLE_CHECK", "position": pos2}
    if abs(size2 - size1) > max(0.000001, size1 * 0.05):
        return {"ok": False, "allowed": False, "reason": "POSITION_SIZE_CHANGED_DURING_DOUBLE_CHECK", "before": pos1, "after": pos2}
    return {"ok": True, "allowed": True, "reason": "SAFE_AUTO_CLOSE_ALLOWED", "position": pos2, "protection": protection}


@app.post("/safe_auto_close_symbol")
def safe_auto_close_symbol(secret: str, symbol: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    if not SAFE_AUTO_CLOSE_ENABLED:
        raise HTTPException(400, "SAFE_AUTO_CLOSE_ENABLED=false")
    check = safer_auto_close_check(symbol)
    if not check.get("allowed"):
        safe_notify_event("⚠️ Safe auto-close blocked", f"{normalize_symbol(symbol)}: {check.get('reason')}", important=True)
        return {"ok": False, "check": check}
    result = emergency_close_symbol_impl(symbol)
    post = get_position_linear(symbol)
    safe_notify_event("🚨 Safe auto-close executed", f"{normalize_symbol(symbol)} closed. result={result.get('closed')} remaining_size={post.get('size')}", important=True)
    return {"ok": True, "check": check, "result": result, "post_position": post}


def telegram_user_allowed(chat_id: Any) -> bool:
    cid = str(chat_id or "")
    return cid and (cid in TELEGRAM_ALLOWED_USER_IDS or cid == str(TELEGRAM_CHAT_ID))


def telegram_rate_limited(chat_id: str) -> bool:
    now = time.time()
    last = _last_telegram_command_at.get(chat_id, 0)
    if now - last < TELEGRAM_COMMAND_RATE_LIMIT_SEC:
        return True
    _last_telegram_command_at[chat_id] = now
    return False


def create_telegram_confirm(chat_id: str, action: str, payload: Dict[str, Any]) -> str:
    token = str(uuid.uuid4())[:8]
    _pending_telegram_confirms[token] = {"chat_id": chat_id, "action": action, "payload": payload, "expires_at": time.time() + TELEGRAM_CONFIRM_TTL_SEC}
    return token


def handle_telegram_command_text_secure(text: str, chat_id: str) -> Dict[str, Any]:
    if not telegram_user_allowed(chat_id):
        return {"ok": False, "response": "Unauthorized Telegram user."}
    if telegram_rate_limited(str(chat_id)):
        return {"ok": False, "response": "Rate limit: wait a moment before sending another command."}
    cmd = (text or "").strip()
    cmd_lower = cmd.lower()
    write_audit_event("telegram_command", {"chat_id": chat_id, "text": cmd})
    if cmd_lower.startswith("/confirm"):
        parts = cmd.split()
        token = parts[1] if len(parts) > 1 else ""
        item = _pending_telegram_confirms.get(token)
        if not item or item.get("chat_id") != str(chat_id) or item.get("expires_at", 0) < time.time():
            return {"ok": False, "response": "Invalid or expired confirm token."}
        action = item.get("action")
        payload = item.get("payload") or {}
        del _pending_telegram_confirms[token]
        if action == "pause":
            set_trading_paused(True, reason="Telegram confirmed pause")
            return {"ok": True, "response": "Trading paused."}
        if action == "resume":
            set_trading_paused(False)
            return {"ok": True, "response": "Trading resumed."}
        if action == "close_symbol":
            result = emergency_close_symbol_impl(payload.get("symbol", ""))
            return {"ok": True, "response": f"Close executed for {payload.get('symbol')}: {result.get('closed')}", "result": result}
        return {"ok": False, "response": f"Unknown confirmation action: {action}"}
    if cmd_lower in {"/pause", "pause"}:
        token = create_telegram_confirm(str(chat_id), "pause", {})
        return {"ok": True, "response": f"Confirm trading pause with: /confirm {token}"}
    if cmd_lower in {"/resume", "resume"}:
        token = create_telegram_confirm(str(chat_id), "resume", {})
        return {"ok": True, "response": f"Confirm trading resume with: /confirm {token}"}
    if cmd_lower.startswith("/close"):
        parts = cmd.split()
        if len(parts) < 2:
            return {"ok": False, "response": "Usage: /close SYMBOL"}
        symbol = normalize_symbol(parts[1])
        token = create_telegram_confirm(str(chat_id), "close_symbol", {"symbol": symbol})
        return {"ok": True, "response": f"Confirm emergency close {symbol} with: /confirm {token}"}
    return handle_telegram_command_text(cmd)


@app.get("/telegram_security_status")
def telegram_security_status(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "allowed_user_ids": TELEGRAM_ALLOWED_USER_IDS, "pending_confirms": len(_pending_telegram_confirms), "rate_limit_sec": TELEGRAM_COMMAND_RATE_LIMIT_SEC}


def evaluate_payload_without_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    body = dict(payload)
    if "symbol" in body:
        body["symbol"] = normalize_symbol(body.get("symbol", ""))
    if "side" in body:
        try:
            body["side"] = normalize_side(body.get("side", ""))
        except Exception as exc:
            return {"ok": False, "stage": "normalize", "reason": str(exc)}
    validation = validate_payload_schema(body) if "validate_payload_schema" in globals() else {"ok": True}
    if not validation.get("ok", True):
        return {"ok": False, "stage": "payload_schema", "payload_validation": validation}
    try:
        decision = risk_engine_decision(body)
        result: Dict[str, Any] = {"ok": True, "decision": decision}
        if decision.get("allow_order"):
            risk_pct_used = float(decision.get("risk_pct_used") or 0.0)
            result["quality"] = validate_order_quality(body)
            if result["quality"].get("ok"):
                result["price_deviation"] = validate_live_price_deviation(body)
            if result.get("price_deviation", {}).get("ok"):
                result["duplicate_signal"] = validate_duplicate_signal(body)
            if result.get("duplicate_signal", {}).get("ok"):
                result["alert_idempotency"] = validate_alert_idempotency(body)
            if result.get("alert_idempotency", {}).get("ok"):
                result["exposure"] = validate_pre_trade_exposure(body, risk_pct_used)
        return result
    except Exception as exc:
        return {"ok": False, "stage": "simulation", "reason": str(exc)}


@app.post("/simulation_replay")
async def simulation_replay(request: Request):
    body = await request.json()
    verify_secret(request, body)
    payloads = body.get("payloads")
    if not isinstance(payloads, list):
        raise HTTPException(400, "payloads list required")
    out = [evaluate_payload_without_order(p) for p in payloads[:REPLAY_MAX_EVENTS] if isinstance(p, dict)]
    return {"ok": True, "count": len(out), "results": out}


@app.get("/simulation_replay_recent")
def simulation_replay_recent(secret: str, days: int = 30, limit: int = 100):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    rows = fetch_supabase_logs_since(days=max(1, min(days, 90)), limit=max(1, min(limit, REPLAY_MAX_EVENTS))) if supabase_enabled() else []
    payloads = []
    for row in rows:
        raw = row.get("raw_payload") or {}
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = {}
        if isinstance(raw, dict) and raw.get("strategy"):
            raw["secret"] = SHARED_SECRET
            payloads.append(raw)
    results = [evaluate_payload_without_order(p) for p in payloads]
    summary = {"would_allow": 0, "blocked": 0, "paper": 0}
    for r in results:
        d = r.get("decision", {})
        if d.get("allow_order"):
            summary["would_allow"] += 1
        elif d.get("mode") == "PAPER":
            summary["paper"] += 1
        else:
            summary["blocked"] += 1
    return {"ok": True, "days": days, "count": len(results), "summary": summary, "results": results[:100]}


def check_supabase_health() -> Dict[str, Any]:
    if not supabase_enabled():
        return {"ok": False, "reason": "SUPABASE_DISABLED"}
    try:
        rows = fetch_supabase_logs(limit=1)
        return {"ok": True, "sample_count": len(rows)}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}


def check_bybit_health() -> Dict[str, Any]:
    try:
        server = _V65_BASE_BYBIT("GET", "/v5/market/time", {})
        wallet_ok = True
        equity = None
        try:
            equity = get_equity_usdt()
        except Exception:
            wallet_ok = False
        return {"ok": True, "server_time": server, "wallet_ok": wallet_ok, "equity": equity}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}


def last_signal_age_hours() -> Optional[float]:
    try:
        rows = fetch_supabase_logs(limit=100) if supabase_enabled() else []
        for row in rows:
            if row.get("strategy") not in {None, "SYSTEM", "SYSTEM_EMERGENCY"}:
                ts = row.get("created_at") or row.get("timestamp_utc")
                if ts:
                    import datetime as _dt
                    dt = _dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    return (time.time() - dt.timestamp()) / 3600.0
    except Exception:
        pass
    return None


@app.get("/production_health")
def production_health(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    signal_age = last_signal_age_hours()
    stale = signal_age is not None and signal_age > SIGNAL_STALE_HOURS
    health = {"ok": True, "version": APP_FEATURE_LEVEL, "bybit": check_bybit_health(), "supabase": check_supabase_health(), "telegram": {"configured": telegram_configured(), "enabled": TELEGRAM_ENABLED}, "runtime": load_runtime_state(), "last_signal_age_hours": signal_age, "signal_stale": stale, "threshold_hours": SIGNAL_STALE_HOURS}
    health["ok"] = bool(health["bybit"].get("ok")) and (not supabase_enabled() or bool(health["supabase"].get("ok")))
    return health


@app.post("/production_health_notify")
def production_health_notify(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    health = production_health(secret)
    if not health.get("ok") or health.get("signal_stale"):
        safe_notify_event("⚠️ Production health warning", json.dumps(health, ensure_ascii=False)[:3000], important=True)
    return {"ok": True, "health": health}


def validate_runtime_config() -> Dict[str, Any]:
    issues = []
    warnings = []
    state = load_state()
    global_cfg = state.get("global", {})
    if not global_cfg.get("enabled", False):
        warnings.append("GLOBAL_DISABLED")
    for st_name, st_cfg in (state.get("strategies") or {}).items():
        if not isinstance(st_cfg, dict):
            issues.append(f"STRATEGY_CONFIG_NOT_OBJECT_{st_name}")
            continue
        symbols = st_cfg.get("symbols") or {}
        if not symbols:
            warnings.append(f"STRATEGY_HAS_NO_SYMBOLS_{st_name}")
        for sym, sym_cfg in symbols.items():
            nsym = normalize_symbol(sym)
            if nsym != sym:
                warnings.append(f"SYMBOL_NOT_NORMALIZED_{sym}_SHOULD_BE_{nsym}")
            for side in ["LONG", "SHORT"]:
                sc = (sym_cfg or {}).get(side)
                if not sc:
                    warnings.append(f"SIDE_MISSING_{st_name}_{sym}_{side}")
                    continue
                mode = str(sc.get("mode", "OFF")).upper()
                risk_pct = float(sc.get("risk_pct", 0) or 0)
                if mode not in {"OFF", "PAPER", "MICRO", "LIVE"}:
                    issues.append(f"INVALID_MODE_{st_name}_{sym}_{side}_{mode}")
                if mode in {"MICRO", "LIVE"} and risk_pct <= 0:
                    issues.append(f"ACTIVE_MODE_WITH_ZERO_RISK_{st_name}_{sym}_{side}")
                if risk_pct > 1.0:
                    warnings.append(f"RISK_PCT_HIGH_{st_name}_{sym}_{side}_{risk_pct}")
    env_checks = {"ENABLE_REAL_ORDERS": ENABLE_REAL_ORDERS, "SUPABASE_ENABLED": supabase_enabled(), "TELEGRAM_CONFIGURED": telegram_configured(), "EXPOSURE_LIMITS_ENABLED": exposure_limits_enabled(), "CORRELATION_GUARD_ENABLED": CORRELATION_GUARD_ENABLED, "MARKET_REGIME_FILTER_ENABLED": MARKET_REGIME_FILTER_ENABLED}
    if ENABLE_REAL_ORDERS and not supabase_enabled():
        warnings.append("REAL_ORDERS_WITHOUT_SUPABASE")
    if TELEGRAM_ENABLED and not telegram_configured():
        issues.append("TELEGRAM_ENABLED_BUT_NOT_CONFIGURED")
    return {"ok": not issues, "issues": issues, "warnings": warnings, "env": env_checks}


@app.get("/config_validation")
def config_validation(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return validate_runtime_config()


@app.get("/config_validation_dashboard", response_class=HTMLResponse)
def config_validation_dashboard(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = validate_runtime_config()
    rows = []
    for issue in data.get("issues", []):
        rows.append(f"<tr><td><span class='bad'>ISSUE</span></td><td>{h(issue)}</td></tr>")
    for warn in data.get("warnings", []):
        rows.append(f"<tr><td><span class='watch'>WARNING</span></td><td>{h(warn)}</td></tr>")
    if not rows:
        rows.append("<tr><td><span class='good'>OK</span></td><td>No blocking configuration issues found.</td></tr>")
    return HTMLResponse(f"""
    <html><head><title>Config Validation</title><style>
    body{{font-family:Arial;margin:24px;background:#f6f8fb;color:#111827}} table{{border-collapse:collapse;width:100%;background:white}} td,th{{border-bottom:1px solid #e5e7eb;padding:10px}} .bad{{color:#991b1b;font-weight:700}} .watch{{color:#92400e;font-weight:700}} .good{{color:#166534;font-weight:700}} pre{{background:white;padding:12px;border-radius:8px}}
    </style></head><body><h1>Configuration Validation v6.8.0</h1><table><tr><th>Level</th><th>Message</th></tr>{''.join(rows)}</table><h2>Environment</h2><pre>{h(data.get('env'))}</pre><p><a href='/dashboard_v2?secret={h(secret)}&days=7'>Back to dashboard</a></p></body></html>
    """)


@app.get("/control_panel", response_class=HTMLResponse)
def control_panel(secret: str, days: int = 7):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    risk = summarize_open_risk()
    runtime = load_runtime_state()
    config = validate_runtime_config()
    prod = production_health(secret)
    return HTMLResponse(f"""
    <html><head><title>Trading Control Panel</title><style>
    body{{font-family:Arial;margin:0;background:#0f172a;color:#e5e7eb}} header{{padding:20px;background:#111827}} main{{padding:20px;display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px}} .card{{background:#1f2937;border:1px solid #374151;border-radius:14px;padding:16px;box-shadow:0 4px 14px rgba(0,0,0,.25)}} a,button{{display:inline-block;margin:4px;padding:8px 10px;border-radius:8px;background:#2563eb;color:white;text-decoration:none;border:0}} .danger{{background:#991b1b}} .warn{{background:#92400e}} .good{{background:#166534}} .muted{{color:#9ca3af}} pre{{white-space:pre-wrap;background:#111827;padding:10px;border-radius:8px}}
    </style></head><body><header><h1>Trading Control Panel v6.8.0</h1><div class='muted'>Lightweight frontend separated from the main dashboard logic.</div></header><main>
    <section class='card'><h2>Runtime</h2><p>real_orders={ENABLE_REAL_ORDERS}</p><p>paused={h(runtime.get('trading_paused'))}</p><a class='warn' href='/trading_pause_on?secret={h(secret)}&reason=Control%20panel%20pause'>Pause</a><a class='good' href='/trading_pause_off?secret={h(secret)}'>Resume</a></section>
    <section class='card'><h2>Open Risk</h2><p>positions={h(risk.get('open_positions'))}</p><p>value={fmt_num(risk.get('total_position_value'))}</p><p>unrealized={fmt_num(risk.get('total_unrealized_pnl'))}</p><a href='/open_risk_summary?secret={h(secret)}'>JSON</a></section>
    <section class='card'><h2>Health</h2><p>production_ok={h(prod.get('ok'))}</p><p>config_ok={h(config.get('ok'))}</p><a href='/production_health?secret={h(secret)}'>Production health</a><a href='/config_validation_dashboard?secret={h(secret)}'>Config validation</a></section>
    <section class='card'><h2>Dashboards</h2><a href='/dashboard_v2?secret={h(secret)}&days={days}'>Dashboard v2</a><a href='/dashboard_charts?secret={h(secret)}&days=30'>Charts</a><a href='/strategy_review_report?secret={h(secret)}&days=30'>Strategy review</a></section>
    <section class='card'><h2>Emergency</h2><a class='danger' href='/emergency_close_all?secret={h(secret)}'>Close all positions</a><a class='warn' href='/cancel_all_orders?secret={h(secret)}'>Cancel all orders</a></section>
    </main></body></html>
    """)


@app.get("/supabase_physical_schema_sql")
def supabase_physical_schema_sql(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    sql = """
-- Optional v6.8.0 physical split tables. Run manually in Supabase SQL editor if needed.
create table if not exists orders (id bigserial primary key, created_at timestamptz default now(), timestamp_utc text, strategy text, symbol text, side text, mode text, order_id text, decision text, status text, raw_payload jsonb);
create table if not exists positions (id bigserial primary key, created_at timestamptz default now(), timestamp_utc text, symbol text, side text, size numeric, position_value numeric, unrealized_pnl numeric, raw_position jsonb);
create table if not exists system_events (id bigserial primary key, created_at timestamptz default now(), timestamp_utc text, event_type text, status text, payload_hash text, payload jsonb);
create table if not exists strategy_state_history (id bigserial primary key, created_at timestamptz default now(), timestamp_utc text, version_id text, source text, reason text, before_hash text, after_hash text, before_state jsonb, after_state jsonb);
create table if not exists daily_reports (id bigserial primary key, created_at timestamptz default now(), timestamp_utc text, days integer, report jsonb);
create table if not exists telegram_notifications (id bigserial primary key, created_at timestamptz default now(), timestamp_utc text, title text, message text, status text, response jsonb);
create table if not exists backtest_results (id bigserial primary key, created_at timestamptz default now(), timestamp_utc text, strategy text, symbol text, side text, source text, metrics jsonb, raw_payload jsonb);
"""
    return {"ok": True, "sql": sql}

# ============================================================
# v7.0.0 - v7.6.0 STRATEGY PROMOTION / AI ANALYST / APPROVAL AUTOMATION
# ============================================================

# These modules intentionally use deterministic, auditable logic by default.
# The term "AI" here means an analyst/supervisor layer that synthesizes system state
# into recommendations. It never bypasses the hard risk engine.

PROMOTION_MANAGER_ENABLED = os.getenv("PROMOTION_MANAGER_ENABLED", "true").lower() == "true"
PROMOTION_MANAGER_MIN_CLOSED = int(os.getenv("PROMOTION_MANAGER_MIN_CLOSED", str(PAPER_DECISION_MIN_SAMPLE_PROMOTE)))
PROMOTION_MANAGER_PROMOTE_AVG_R = float(os.getenv("PROMOTION_MANAGER_PROMOTE_AVG_R", str(PAPER_DECISION_PROMOTE_AVG_R)))
PROMOTION_MANAGER_REJECT_AVG_R = float(os.getenv("PROMOTION_MANAGER_REJECT_AVG_R", str(PAPER_DECISION_REJECT_AVG_R)))
PROMOTION_MANAGER_MIN_BACKTEST_PF = float(os.getenv("PROMOTION_MANAGER_MIN_BACKTEST_PF", str(PAPER_DECISION_PROMOTE_BACKTEST_PF)))
PROMOTION_MANAGER_PROMOTE_TARGET_MODE = os.getenv("PROMOTION_MANAGER_PROMOTE_TARGET_MODE", "MICRO").upper()
PROMOTION_MANAGER_DEMOTE_TARGET_MODE = os.getenv("PROMOTION_MANAGER_DEMOTE_TARGET_MODE", "PAPER").upper()
PROMOTION_MANAGER_DEFAULT_MICRO_RISK_PCT = float(os.getenv("PROMOTION_MANAGER_DEFAULT_MICRO_RISK_PCT", "0.05"))
PROMOTION_MANAGER_REQUIRE_APPROVAL = os.getenv("PROMOTION_MANAGER_REQUIRE_APPROVAL", "true").lower() == "true"

AI_ANALYST_ENABLED = os.getenv("AI_ANALYST_ENABLED", "true").lower() == "true"
AI_RISK_SUPERVISOR_ENABLED = os.getenv("AI_RISK_SUPERVISOR_ENABLED", "true").lower() == "true"
AI_RISK_HIGH_AVG_R = float(os.getenv("AI_RISK_HIGH_AVG_R", "-0.50"))
AI_RISK_ELEVATED_AVG_R = float(os.getenv("AI_RISK_ELEVATED_AVG_R", "-0.20"))
AI_RISK_MAX_ACTIVE_MICRO_LIVE = int(os.getenv("AI_RISK_MAX_ACTIVE_MICRO_LIVE", "3"))
AI_RISK_ALT_LONG_CONCENTRATION_WARN = int(os.getenv("AI_RISK_ALT_LONG_CONCENTRATION_WARN", "4"))
AI_RISK_CAN_BLOCK_PROMOTION = os.getenv("AI_RISK_CAN_BLOCK_PROMOTION", "true").lower() == "true"

BACKTEST_IMPORT_DEFAULT_SOURCE = os.getenv("BACKTEST_IMPORT_DEFAULT_SOURCE", "manual_table_import")

APPROVAL_WORKFLOW_ENABLED = os.getenv("APPROVAL_WORKFLOW_ENABLED", "true").lower() == "true"
APPROVAL_TTL_HOURS = float(os.getenv("APPROVAL_TTL_HOURS", "24"))
APPROVAL_STATE_FILE = APP_DIR / "approval_workflow_state.json"


def _candidate_key(strategy: str, symbol: str, side: str) -> str:
    return backtest_key(strategy, symbol, side)


def _active_strategy_rows(include_off: bool = False) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    try:
        state = load_state()
    except Exception:
        return rows
    for strategy, st_cfg in (state.get("strategies") or {}).items():
        if not isinstance(st_cfg, dict) or not st_cfg.get("enabled", True):
            continue
        for symbol, sym_cfg in (st_cfg.get("symbols") or {}).items():
            if not isinstance(sym_cfg, dict):
                continue
            for side, side_cfg in sym_cfg.items():
                if not isinstance(side_cfg, dict):
                    continue
                mode = str(side_cfg.get("mode", "OFF")).upper()
                if not include_off and mode == "OFF":
                    continue
                rows.append({
                    "strategy": strategy,
                    "symbol": normalize_symbol(symbol),
                    "side": normalize_side(side),
                    "mode": mode,
                    "risk_pct": to_float_or_none(side_cfg.get("risk_pct")) or 0.0,
                    "config": side_cfg,
                })
    return rows


def _decision_items(days: int, limit: int) -> list[Dict[str, Any]]:
    report = build_paper_outcome_decision_report(days=days, limit=limit, include_outcomes=False)
    return report.get("decisions") or []


def _candidate_metrics_from_decision(item: Dict[str, Any]) -> Dict[str, Any]:
    decision = item.get("decision") or {}
    metrics = decision.get("metrics") or {}
    return {
        "status": decision.get("status"),
        "action": decision.get("action"),
        "reasons": decision.get("reasons") or [],
        "closed_count": int(metrics.get("closed_count") or 0),
        "average_r_closed": to_float_or_none(metrics.get("average_r_closed")),
        "total_r": to_float_or_none(metrics.get("total_r")),
        "win_rate_closed_pct": to_float_or_none(metrics.get("win_rate_closed_pct")),
        "backtest_profit_factor": to_float_or_none(metrics.get("backtest_profit_factor")),
        "backtest_trades": to_float_or_none(metrics.get("backtest_trades")),
        "backtest_alignment_status": decision.get("backtest_alignment_status"),
    }


def build_strategy_promotion_plan(days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS) -> Dict[str, Any]:
    safe_days = max(1, min(int(days), 90))
    safe_limit = max(1, min(int(limit), max(PAPER_OUTCOME_MAX_EVENTS, 1)))
    decisions = _decision_items(safe_days, safe_limit)
    open_risk = summarize_open_risk()
    ai_risk = build_ai_risk_supervisor_report(days=safe_days, limit=safe_limit, include_plan=False)

    actions: list[Dict[str, Any]] = []
    for item in decisions:
        strategy = str(item.get("strategy") or "")
        symbol = normalize_symbol(item.get("symbol") or "")
        side = normalize_side(item.get("side") or "LONG")
        current_mode = _current_strategy_side_mode(strategy, symbol, side)
        metrics = _candidate_metrics_from_decision(item)
        closed = metrics["closed_count"]
        avg_r = metrics["average_r_closed"]
        bt_pf = metrics["backtest_profit_factor"]

        proposed_action = "KEEP_PAPER"
        target_mode = current_mode
        target_risk_pct = None
        requires_approval = False
        block_reason = None
        reasons = []

        if current_mode == "PAPER":
            if closed >= PROMOTION_MANAGER_MIN_CLOSED and avg_r is not None and avg_r <= PROMOTION_MANAGER_REJECT_AVG_R:
                proposed_action = "REJECT_TO_OFF"
                target_mode = "OFF"
                reasons.append(f"closed={closed}, avgR={avg_r:.4f} <= reject threshold {PROMOTION_MANAGER_REJECT_AVG_R:.4f}")
                requires_approval = PROMOTION_MANAGER_REQUIRE_APPROVAL
            elif (
                closed >= PROMOTION_MANAGER_MIN_CLOSED
                and avg_r is not None
                and avg_r >= PROMOTION_MANAGER_PROMOTE_AVG_R
                and bt_pf is not None
                and bt_pf >= PROMOTION_MANAGER_MIN_BACKTEST_PF
            ):
                proposed_action = f"PROMOTE_TO_{PROMOTION_MANAGER_PROMOTE_TARGET_MODE}"
                target_mode = PROMOTION_MANAGER_PROMOTE_TARGET_MODE
                target_risk_pct = PROMOTION_MANAGER_DEFAULT_MICRO_RISK_PCT
                reasons.append(f"closed={closed}, avgR={avg_r:.4f} >= promote threshold {PROMOTION_MANAGER_PROMOTE_AVG_R:.4f}")
                reasons.append(f"backtest PF={bt_pf:.4f} >= {PROMOTION_MANAGER_MIN_BACKTEST_PF:.4f}")
                requires_approval = True
            else:
                reasons.append("No promotion/rejection threshold met; collect more PAPER data")
        elif current_mode in {"MICRO", "LIVE"}:
            if closed >= PAPER_STRATEGY_GUARD_MIN_CLOSED and avg_r is not None and avg_r <= PAPER_STRATEGY_GUARD_REJECT_AVG_R:
                proposed_action = f"DEMOTE_TO_{PROMOTION_MANAGER_DEMOTE_TARGET_MODE}"
                target_mode = PROMOTION_MANAGER_DEMOTE_TARGET_MODE
                reasons.append(f"active strategy underperforming: avgR={avg_r:.4f}")
                requires_approval = PROMOTION_MANAGER_REQUIRE_APPROVAL
            else:
                proposed_action = "KEEP_ACTIVE"
                target_mode = current_mode
                reasons.append("No demotion threshold met")
        else:
            proposed_action = "NO_ACTION"
            reasons.append(f"Current mode is {current_mode}")

        if proposed_action.startswith("PROMOTE") and AI_RISK_CAN_BLOCK_PROMOTION:
            risk_level = str((ai_risk.get("risk") or {}).get("level", "NORMAL"))
            if risk_level in {"HIGH", "CRITICAL"}:
                block_reason = f"AI risk supervisor level {risk_level} blocks promotion"
                proposed_action = "PROMOTION_BLOCKED_BY_RISK"
                target_mode = current_mode
                target_risk_pct = None
                requires_approval = False
                reasons.append(block_reason)

        actions.append({
            "strategy": strategy,
            "symbol": symbol,
            "side": side,
            "current_mode": current_mode,
            "proposed_action": proposed_action,
            "target_mode": target_mode,
            "target_risk_pct": target_risk_pct,
            "requires_approval": requires_approval,
            "block_reason": block_reason,
            "reasons": reasons,
            "metrics": metrics,
        })

    counts: Dict[str, int] = {}
    for a in actions:
        key = str(a.get("proposed_action"))
        counts[key] = counts.get(key, 0) + 1

    return {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "enabled": PROMOTION_MANAGER_ENABLED,
        "days": safe_days,
        "thresholds": {
            "min_closed": PROMOTION_MANAGER_MIN_CLOSED,
            "promote_avg_r": PROMOTION_MANAGER_PROMOTE_AVG_R,
            "reject_avg_r": PROMOTION_MANAGER_REJECT_AVG_R,
            "min_backtest_pf": PROMOTION_MANAGER_MIN_BACKTEST_PF,
            "promote_target_mode": PROMOTION_MANAGER_PROMOTE_TARGET_MODE,
            "default_micro_risk_pct": PROMOTION_MANAGER_DEFAULT_MICRO_RISK_PCT,
            "require_approval": PROMOTION_MANAGER_REQUIRE_APPROVAL,
        },
        "open_risk": open_risk,
        "ai_risk_level": (ai_risk.get("risk") or {}).get("level"),
        "action_counts": counts,
        "actions": actions,
    }


def _apply_strategy_action(action: Dict[str, Any], reason_prefix: str = "promotion_manager") -> Dict[str, Any]:
    target_mode = str(action.get("target_mode") or "").upper()
    if target_mode not in {"OFF", "PAPER", "MICRO", "LIVE"}:
        return {"ok": False, "reason": "NO_VALID_TARGET_MODE", "action": action}
    current_mode = str(action.get("current_mode") or "").upper()
    if target_mode == current_mode:
        return {"ok": True, "changed": False, "reason": "ALREADY_IN_TARGET_MODE", "action": action}
    risk_pct = action.get("target_risk_pct")
    if target_mode == "OFF":
        risk_pct = 0.0
    result = set_strategy_side_config(
        strategy=action.get("strategy"),
        symbol=action.get("symbol"),
        side=action.get("side"),
        mode=target_mode,
        risk_pct=risk_pct if risk_pct is not None else None,
        reason=f"{reason_prefix}:{action.get('proposed_action')}",
    )
    return {"ok": True, "changed": True, "result": result, "action": action}


@app.get("/strategy_promotion_plan")
def strategy_promotion_plan(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_strategy_promotion_plan(days=days, limit=limit)


@app.get("/strategy_promotion_dashboard", response_class=HTMLResponse)
def strategy_promotion_dashboard(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    plan = build_strategy_promotion_plan(days=days, limit=limit)
    rows = []
    for a in plan.get("actions") or []:
        m = a.get("metrics") or {}
        rows.append(f"""
        <tr><td>{h(a.get('strategy'))}</td><td>{h(a.get('symbol'))}</td><td>{h(a.get('side'))}</td><td>{h(a.get('current_mode'))}</td>
        <td><b>{h(a.get('proposed_action'))}</b></td><td>{h(a.get('target_mode'))}</td><td>{h(m.get('closed_count'))}</td>
        <td>{fmt_num(m.get('average_r_closed'))}</td><td>{fmt_num(m.get('backtest_profit_factor'))}</td><td>{h('; '.join(a.get('reasons') or []))}</td></tr>
        """)
    return HTMLResponse(f"""
    <html><head><title>Strategy Promotion Manager</title><style>
    body{{font-family:Arial;margin:24px;background:#f6f8fb;color:#111827}} table{{border-collapse:collapse;width:100%;background:white;border-radius:10px;overflow:hidden}} th{{background:#111827;color:white}} td,th{{border-bottom:1px solid #e5e7eb;padding:9px;text-align:left}} .card{{background:white;border-radius:12px;padding:14px;margin-bottom:14px;box-shadow:0 1px 6px #d1d5db}} a{{margin-right:10px}}
    </style></head><body><h1>Strategy Promotion Manager v7.0.0</h1>
    <div class='card'>AI risk level: <b>{h(plan.get('ai_risk_level'))}</b> | Action counts: {h(plan.get('action_counts'))}</div>
    <table><tr><th>Strategy</th><th>Symbol</th><th>Side</th><th>Mode</th><th>Action</th><th>Target</th><th>Closed</th><th>Avg R</th><th>BT PF</th><th>Reasons</th></tr>{''.join(rows)}</table>
    <p><a href='/strategy_promotion_plan?secret={h(secret)}&days={days}&limit={limit}'>JSON plan</a><a href='/candidate_monitor_dashboard?secret={h(secret)}&days={days}&limit={limit}'>Candidate monitor</a><a href='/dashboard_v2?secret={h(secret)}&days={days}'>Dashboard</a></p>
    </body></html>
    """)


@app.post("/strategy_promotion_run")
async def strategy_promotion_run(request: Request):
    body = await request.json()
    verify_secret(request, body)
    if not PROMOTION_MANAGER_ENABLED:
        raise HTTPException(403, "Promotion manager is disabled")
    apply_changes = bool(body.get("apply", False))
    allow_promotions = bool(body.get("allow_promotions", False))
    allow_rejections = bool(body.get("allow_rejections", True))
    days = int(body.get("days", PAPER_OUTCOME_DEFAULT_DAYS))
    limit = int(body.get("limit", PAPER_OUTCOME_MAX_EVENTS))
    plan = build_strategy_promotion_plan(days=days, limit=limit)
    results = []
    for action in plan.get("actions") or []:
        proposed = str(action.get("proposed_action"))
        should_apply = False
        if proposed in {"REJECT_TO_OFF", "DEMOTE_TO_PAPER"} and allow_rejections:
            should_apply = True
        if proposed.startswith("PROMOTE_TO_") and allow_promotions:
            should_apply = True
        if not apply_changes or not should_apply:
            results.append({"ok": True, "changed": False, "reason": "DRY_RUN_OR_NOT_ALLOWED", "action": action})
            continue
        if action.get("requires_approval") and not body.get("approval_token"):
            results.append({"ok": False, "changed": False, "reason": "APPROVAL_REQUIRED", "action": action})
            continue
        results.append(_apply_strategy_action(action, reason_prefix="strategy_promotion_run"))
    return {"ok": True, "apply": apply_changes, "results": results, "plan": plan}


def build_ai_strategy_analyst_report(days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS) -> Dict[str, Any]:
    safe_days = max(1, min(int(days), 90))
    decision_report = build_paper_outcome_decision_report(days=safe_days, limit=limit, include_outcomes=False)
    promotion = build_strategy_promotion_plan(days=safe_days, limit=limit)
    risk = build_ai_risk_supervisor_report(days=safe_days, limit=limit, include_plan=False)
    items = []
    for d in decision_report.get("decisions") or []:
        decision = d.get("decision") or {}
        m = decision.get("metrics") or {}
        avg_r = to_float_or_none(m.get("average_r_closed"))
        closed = int(m.get("closed_count") or 0)
        bt_pf = to_float_or_none(m.get("backtest_profit_factor"))
        if closed < PAPER_DECISION_MIN_SAMPLE_REJECT:
            recommendation = "KEEP_PAPER_COLLECT_DATA"
            confidence = "low"
        elif avg_r is not None and avg_r <= PAPER_DECISION_REJECT_AVG_R:
            recommendation = "REJECT_OR_REOPTIMIZE"
            confidence = "medium"
        elif avg_r is not None and avg_r >= PAPER_DECISION_PROMOTE_AVG_R and bt_pf is not None and bt_pf >= PAPER_DECISION_PROMOTE_BACKTEST_PF:
            recommendation = "PROMOTION_REVIEW"
            confidence = "medium"
        elif avg_r is not None and avg_r >= 0:
            recommendation = "KEEP_PAPER"
            confidence = "medium"
        else:
            recommendation = "WATCH_NEGATIVE"
            confidence = "medium"
        items.append({
            "strategy": d.get("strategy"),
            "symbol": d.get("symbol"),
            "side": d.get("side"),
            "recommendation": recommendation,
            "confidence": confidence,
            "closed_count": closed,
            "average_r_closed": avg_r,
            "total_r": m.get("total_r"),
            "backtest_profit_factor": bt_pf,
            "backtest_alignment_status": decision.get("backtest_alignment_status"),
            "reasons": decision.get("reasons") or [],
        })
    portfolio_status = "NORMAL"
    avg_total = to_float_or_none((decision_report.get("summary") or {}).get("average_r_closed"))
    if str((risk.get("risk") or {}).get("level")) in {"HIGH", "CRITICAL"}:
        portfolio_status = "CAUTIOUS"
    elif avg_total is not None and avg_total < 0:
        portfolio_status = "CAUTIOUS"
    summary_text = build_ai_strategy_summary_text(items, risk)
    return {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "mode": "deterministic_ai_analyst",
        "portfolio_status": portfolio_status,
        "summary": summary_text,
        "days": safe_days,
        "items": items,
        "risk_supervisor": risk.get("risk"),
        "promotion_action_counts": promotion.get("action_counts"),
        "do_not_promote": str((risk.get("risk") or {}).get("level")) in {"HIGH", "CRITICAL"},
        "source_reports": {
            "paper_decision_summary": decision_report.get("summary"),
            "status_counts": decision_report.get("status_counts"),
        },
    }


def build_ai_strategy_summary_text(items: list[Dict[str, Any]], risk: Dict[str, Any]) -> str:
    if not items:
        return "No PAPER outcomes available yet. Keep collecting data."
    best = sorted(items, key=lambda x: (to_float_or_none(x.get("average_r_closed")) if to_float_or_none(x.get("average_r_closed")) is not None else -999), reverse=True)[:1]
    worst = sorted(items, key=lambda x: (to_float_or_none(x.get("average_r_closed")) if to_float_or_none(x.get("average_r_closed")) is not None else 999))[:1]
    risk_level = (risk.get("risk") or {}).get("level") if isinstance(risk, dict) else None
    return (
        f"Portfolio risk is {risk_level or 'UNKNOWN'}. "
        f"Best current candidate: {best[0].get('strategy')} {best[0].get('symbol')} avgR={fmt_num(best[0].get('average_r_closed'))}. "
        f"Weakest current candidate: {worst[0].get('strategy')} {worst[0].get('symbol')} avgR={fmt_num(worst[0].get('average_r_closed'))}. "
        "No AI recommendation may bypass the deterministic risk engine."
    )


@app.get("/ai_strategy_analyst_report")
def ai_strategy_analyst_report(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_ai_strategy_analyst_report(days=days, limit=limit)


def format_ai_strategy_analyst_message(report: Dict[str, Any]) -> str:
    lines = [
        f"🤖 AI Strategy Analyst — {report.get('days')}d",
        f"Portfolio: {report.get('portfolio_status')} | Risk: {(report.get('risk_supervisor') or {}).get('level')} | Do not promote: {report.get('do_not_promote')}",
        str(report.get("summary") or ""),
    ]
    for item in (report.get("items") or [])[:8]:
        lines.append(
            f"{item.get('recommendation')}: {item.get('strategy')} {item.get('symbol')} {item.get('side')} "
            f"closed={item.get('closed_count')} avgR={fmt_num(item.get('average_r_closed'))} btPF={fmt_num(item.get('backtest_profit_factor'))}"
        )
    return "\n".join(lines)


@app.get("/telegram_ai_strategy_analyst_report")
def telegram_ai_strategy_analyst_report(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS, force: bool = True):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    report = build_ai_strategy_analyst_report(days=days, limit=limit)
    message = format_ai_strategy_analyst_message(report)
    notify = safe_notify_event("🤖 AI Strategy Analyst", message, important=False)
    return {"ok": True, "sent": bool(notify.get("sent")), "notify": notify, "message": message, "report": report}


def build_ai_risk_supervisor_report(days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS, include_plan: bool = True) -> Dict[str, Any]:
    safe_days = max(1, min(int(days), 90))
    decision_report = build_paper_outcome_decision_report(days=safe_days, limit=limit, include_outcomes=False)
    summary = decision_report.get("summary") or {}
    avg_r = to_float_or_none(summary.get("average_r_closed"))
    total_r = to_float_or_none(summary.get("total_r")) or 0.0
    closed = int(summary.get("closed_count") or 0)
    open_risk = summarize_open_risk()
    active_rows = _active_strategy_rows(include_off=False)
    active_micro_live = [x for x in active_rows if x.get("mode") in {"MICRO", "LIVE"}]
    active_paper = [x for x in active_rows if x.get("mode") == "PAPER"]
    long_alt_count = len([x for x in active_rows if x.get("side") == "LONG" and x.get("symbol") not in {"BTCUSDT", "ETHUSDT"}])
    reasons = []
    level = "NORMAL"
    if closed == 0:
        level = "UNKNOWN"
        reasons.append("No closed PAPER outcomes yet")
    elif avg_r is not None and avg_r <= AI_RISK_HIGH_AVG_R:
        level = "HIGH"
        reasons.append(f"Average PAPER R {avg_r:.4f} <= high-risk threshold {AI_RISK_HIGH_AVG_R:.4f}")
    elif avg_r is not None and avg_r <= AI_RISK_ELEVATED_AVG_R:
        level = "ELEVATED"
        reasons.append(f"Average PAPER R {avg_r:.4f} <= elevated threshold {AI_RISK_ELEVATED_AVG_R:.4f}")
    if len(active_micro_live) > AI_RISK_MAX_ACTIVE_MICRO_LIVE:
        level = "HIGH"
        reasons.append(f"Too many active MICRO/LIVE strategies: {len(active_micro_live)}")
    if long_alt_count >= AI_RISK_ALT_LONG_CONCENTRATION_WARN:
        if level == "NORMAL":
            level = "ELEVATED"
        reasons.append(f"Altcoin LONG concentration: {long_alt_count} active long alt strategies")
    if not reasons:
        reasons.append("No elevated risk condition detected")
    recommendation = "ALLOW_MONITORING"
    if level in {"HIGH", "CRITICAL"}:
        recommendation = "BLOCK_PROMOTIONS_AND_REVIEW"
    elif level == "ELEVATED":
        recommendation = "NO_PROMOTION_UNTIL_IMPROVEMENT"
    elif level == "UNKNOWN":
        recommendation = "COLLECT_MORE_DATA"
    plan = build_strategy_promotion_plan(days=safe_days, limit=limit) if include_plan else None
    return {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "risk": {
            "level": level,
            "recommendation": recommendation,
            "reasons": reasons,
            "closed_count": closed,
            "average_r_closed": avg_r,
            "total_r": total_r,
            "active_paper_count": len(active_paper),
            "active_micro_live_count": len(active_micro_live),
            "long_alt_count": long_alt_count,
        },
        "open_risk": open_risk,
        "active_strategies": active_rows,
        "promotion_plan": plan,
    }


@app.get("/ai_risk_supervisor")
def ai_risk_supervisor(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_ai_risk_supervisor_report(days=days, limit=limit, include_plan=True)


def format_ai_risk_supervisor_message(report: Dict[str, Any]) -> str:
    risk = report.get("risk") or {}
    lines = [
        f"🧠 AI Risk Supervisor — level {risk.get('level')}",
        f"Recommendation: {risk.get('recommendation')}",
        f"closed={risk.get('closed_count')} avgR={fmt_num(risk.get('average_r_closed'))} totalR={fmt_num(risk.get('total_r'))}",
    ]
    for reason in (risk.get("reasons") or [])[:8]:
        lines.append(f"- {reason}")
    return "\n".join(lines)


@app.get("/telegram_ai_risk_supervisor")
def telegram_ai_risk_supervisor(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    report = build_ai_risk_supervisor_report(days=days, limit=limit, include_plan=False)
    message = format_ai_risk_supervisor_message(report)
    notify = safe_notify_event("🧠 AI Risk Supervisor", message, important=True)
    return {"ok": True, "sent": bool(notify.get("sent")), "notify": notify, "message": message, "report": report}


def parse_backtest_table_text(text: str, default_source: str = BACKTEST_IMPORT_DEFAULT_SOURCE) -> list[Dict[str, Any]]:
    cleaned = (text or "").strip()
    if not cleaned:
        return []
    dialect = csv.excel_tab if "\t" in cleaned.splitlines()[0] else csv.excel
    rows = list(csv.DictReader(cleaned.splitlines(), dialect=dialect))
    normalized = []
    for row in rows:
        lowered = {str(k).strip().lower().replace(" ", "_"): v for k, v in row.items()}
        item = {
            "strategy": lowered.get("strategy") or lowered.get("strategy_name") or lowered.get("name"),
            "symbol": lowered.get("symbol") or lowered.get("ticker") or lowered.get("pair"),
            "side": lowered.get("side") or "LONG",
            "profit_factor": lowered.get("profit_factor") or lowered.get("pf") or lowered.get("bt_pf"),
            "trades": lowered.get("trades") or lowered.get("total_trades") or lowered.get("closed_trades"),
            "win_rate": lowered.get("win_rate") or lowered.get("win_%") or lowered.get("percent_profitable") or lowered.get("profitable_trades"),
            "max_drawdown": lowered.get("max_drawdown") or lowered.get("max_dd"),
            "net_profit": lowered.get("net_profit") or lowered.get("total_pnl") or lowered.get("total_p&l"),
            "timeframe": lowered.get("timeframe") or lowered.get("tf"),
            "date_from": lowered.get("date_from") or lowered.get("from"),
            "date_to": lowered.get("date_to") or lowered.get("to"),
            "source": lowered.get("source") or default_source,
            "raw": row,
        }
        normalized.append(normalize_backtest_row(item))
    return normalized


@app.post("/backtest_table_import")
async def backtest_table_import(request: Request):
    body = await request.json()
    verify_secret(request, body)
    mode = str(body.get("mode", "upsert")).lower()
    rows = body.get("rows") or body.get("items")
    if isinstance(rows, list):
        normalized = [normalize_backtest_row(r if isinstance(r, dict) else {"raw": r}) for r in rows]
    else:
        csv_text = body.get("csv_text") or body.get("tsv_text") or body.get("text") or ""
        normalized = parse_backtest_table_text(str(csv_text), default_source=str(body.get("source") or BACKTEST_IMPORT_DEFAULT_SOURCE))
    for row in normalized:
        row["updated_at"] = now_iso()
        row["source"] = row.get("source") or BACKTEST_IMPORT_DEFAULT_SOURCE
    if not normalized:
        raise HTTPException(400, "No valid backtest rows found. Provide rows/items or csv_text/tsv_text.")
    if mode == "replace":
        final_rows = normalized
    elif mode == "upsert":
        final_rows = merge_backtest_rows(load_backtest_results(), normalized)
    else:
        raise HTTPException(400, "mode must be 'upsert' or 'replace'")
    save_backtest_results(final_rows)
    return {"ok": True, "mode": mode, "imported": len(normalized), "total_registry_rows": len(final_rows), "rows": normalized, "registry": final_rows}


@app.get("/backtest_table_import_template")
def backtest_table_import_template(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    csv_template = "strategy,symbol,side,timeframe,date_from,date_to,profit_factor,trades,win_rate,max_drawdown,net_profit,source\ntrend_continuation_avax_v11,AVAXUSDT,LONG,15,2026-01-01,2026-05-20,1.49,66,54.55,,,manual_tradingview"
    return {"ok": True, "csv_template": csv_template, "endpoint": "/backtest_table_import", "method": "POST", "body_example": {"secret": "...", "mode": "upsert", "csv_text": csv_template}}


def load_approval_state() -> Dict[str, Any]:
    data = read_json_file(APPROVAL_STATE_FILE, {"items": {}})
    if not isinstance(data, dict):
        data = {"items": {}}
    data.setdefault("items", {})
    return data


def save_approval_state(data: Dict[str, Any]) -> None:
    write_json_file(APPROVAL_STATE_FILE, data)


def create_approval_item(action: str, payload: Dict[str, Any], title: str = "Strategy approval") -> Dict[str, Any]:
    token_raw = f"{time.time()}:{action}:{json.dumps(payload, sort_keys=True, default=str)}"
    token = hashlib.sha256(token_raw.encode()).hexdigest()[:12]
    state = load_approval_state()
    item = {
        "token": token,
        "action": action,
        "title": title,
        "payload": payload,
        "status": "PENDING",
        "created_at": now_iso(),
        "expires_at_ts": time.time() + APPROVAL_TTL_HOURS * 3600,
        "decided_at": None,
        "decision": None,
    }
    state.setdefault("items", {})[token] = item
    save_approval_state(state)
    return item


def get_approval_item(token: str) -> Optional[Dict[str, Any]]:
    return (load_approval_state().get("items") or {}).get(str(token))


def set_approval_decision(token: str, decision: str) -> Dict[str, Any]:
    state = load_approval_state()
    item = (state.get("items") or {}).get(token)
    if not item:
        raise HTTPException(404, "Approval token not found")
    if item.get("status") != "PENDING":
        return {"ok": False, "reason": "ALREADY_DECIDED", "item": item}
    if time.time() > float(item.get("expires_at_ts") or 0):
        item["status"] = "EXPIRED"
        save_approval_state(state)
        return {"ok": False, "reason": "EXPIRED", "item": item}
    decision_up = str(decision).upper()
    if decision_up not in {"APPROVE", "REJECT"}:
        raise HTTPException(400, "decision must be APPROVE or REJECT")
    item["status"] = "APPROVED" if decision_up == "APPROVE" else "REJECTED"
    item["decision"] = decision_up
    item["decided_at"] = now_iso()
    save_approval_state(state)
    if decision_up == "APPROVE":
        payload = item.get("payload") or {}
        action_payload = payload.get("strategy_action") if isinstance(payload, dict) else None
        if isinstance(action_payload, dict):
            result = _apply_strategy_action(action_payload, reason_prefix="telegram_approval")
            item["execution_result"] = result
            state["items"][token] = item
            save_approval_state(state)
    return {"ok": True, "item": item}


@app.post("/approval_create")
async def approval_create(request: Request):
    body = await request.json()
    verify_secret(request, body)
    if not APPROVAL_WORKFLOW_ENABLED:
        raise HTTPException(403, "Approval workflow disabled")
    item = create_approval_item(str(body.get("action", "manual_action")), body.get("payload") or {}, str(body.get("title", "Strategy approval")))
    if body.get("notify", True):
        approve_url = f"/approval_decide?secret=***&token={item['token']}&decision=APPROVE"
        reject_url = f"/approval_decide?secret=***&token={item['token']}&decision=REJECT"
        safe_notify_event("✅ Approval requested", f"{item.get('title')}\nToken: {item['token']}\nApprove: {approve_url}\nReject: {reject_url}", important=True)
    return {"ok": True, "approval": item}


@app.get("/approval_list")
def approval_list(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "state": load_approval_state()}


@app.get("/approval_decide")
def approval_decide(secret: str, token: str, decision: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    result = set_approval_decision(token, decision)
    safe_notify_event("✅ Approval decision", f"token={token}\ndecision={decision}\nok={result.get('ok')}", important=True)
    return result


@app.post("/approval_decide")
async def approval_decide_post(request: Request):
    body = await request.json()
    verify_secret(request, body)
    result = set_approval_decision(str(body.get("token")), str(body.get("decision")))
    safe_notify_event("✅ Approval decision", f"token={body.get('token')}\ndecision={body.get('decision')}\nok={result.get('ok')}", important=True)
    return result


@app.post("/promotion_approval_create")
async def promotion_approval_create(request: Request):
    body = await request.json()
    verify_secret(request, body)
    plan = build_strategy_promotion_plan(days=int(body.get("days", PAPER_OUTCOME_DEFAULT_DAYS)), limit=int(body.get("limit", PAPER_OUTCOME_MAX_EVENTS)))
    created = []
    for a in plan.get("actions") or []:
        if str(a.get("proposed_action", "")).startswith("PROMOTE_TO_") or a.get("proposed_action") in {"REJECT_TO_OFF", "DEMOTE_TO_PAPER"}:
            item = create_approval_item("strategy_state_action", {"strategy_action": a}, title=f"{a.get('proposed_action')} {a.get('strategy')} {a.get('symbol')}")
            created.append(item)
    if created:
        safe_notify_event("✅ Promotion approvals created", "\n".join([f"{x['token']}: {x['title']}" for x in created[:10]]), important=True)
    return {"ok": True, "created_count": len(created), "created": created, "plan": plan}


def build_portfolio_exposure_ai_summary(days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS) -> Dict[str, Any]:
    active = _active_strategy_rows(include_off=False)
    open_risk = summarize_open_risk()
    risk = build_ai_risk_supervisor_report(days=days, limit=limit, include_plan=False)
    by_mode: Dict[str, int] = {}
    by_symbol: Dict[str, int] = {}
    by_side: Dict[str, int] = {}
    for x in active:
        by_mode[x["mode"]] = by_mode.get(x["mode"], 0) + 1
        by_symbol[x["symbol"]] = by_symbol.get(x["symbol"], 0) + 1
        by_side[x["side"]] = by_side.get(x["side"], 0) + 1
    concentration = []
    for sym, cnt in sorted(by_symbol.items(), key=lambda kv: kv[1], reverse=True):
        if cnt > 1:
            concentration.append({"symbol": sym, "active_strategy_count": cnt})
    text = (
        f"Active strategies: {len(active)}. Modes: {by_mode}. Sides: {by_side}. "
        f"Open positions: {open_risk.get('open_positions')}, open value: {fmt_num(open_risk.get('total_position_value'))}. "
        f"AI risk level: {(risk.get('risk') or {}).get('level')}."
    )
    return {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "summary": text,
        "active_count": len(active),
        "by_mode": by_mode,
        "by_symbol": by_symbol,
        "by_side": by_side,
        "concentration": concentration,
        "open_risk": open_risk,
        "ai_risk": risk.get("risk"),
        "active_strategies": active,
    }


@app.get("/portfolio_exposure_ai_summary")
def portfolio_exposure_ai_summary(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_portfolio_exposure_ai_summary(days=days, limit=limit)


@app.get("/telegram_portfolio_exposure_ai_summary")
def telegram_portfolio_exposure_ai_summary(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    report = build_portfolio_exposure_ai_summary(days=days, limit=limit)
    lines = ["📌 Portfolio Exposure AI Summary", report.get("summary", "")]
    for item in report.get("concentration") or []:
        lines.append(f"Concentration: {item.get('symbol')} active strategies={item.get('active_strategy_count')}")
    notify = safe_notify_event("📌 Portfolio exposure summary", "\n".join(lines), important=False)
    return {"ok": True, "sent": bool(notify.get("sent")), "notify": notify, "report": report}


@app.get("/v7_control_center", response_class=HTMLResponse)
def v7_control_center(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    risk = build_ai_risk_supervisor_report(days=days, limit=limit, include_plan=False)
    analyst = build_ai_strategy_analyst_report(days=days, limit=limit)
    promo = build_strategy_promotion_plan(days=days, limit=limit)
    port = build_portfolio_exposure_ai_summary(days=days, limit=limit)
    return HTMLResponse(f"""
    <html><head><title>v7 Trading AI Control Center</title><style>
    body{{font-family:Arial;margin:24px;background:#f6f8fb;color:#111827}} .card{{background:white;border-radius:12px;padding:14px;margin-bottom:14px;box-shadow:0 1px 6px #d1d5db}} a{{display:inline-block;margin:4px 8px 4px 0}} code{{background:#eef2ff;padding:2px 5px;border-radius:4px}}
    </style></head><body><h1>v7 Trading AI Control Center</h1>
    <div class='card'><h2>AI Risk Supervisor</h2><p>Level: <b>{h((risk.get('risk') or {}).get('level'))}</b></p><p>{h((risk.get('risk') or {}).get('recommendation'))}</p></div>
    <div class='card'><h2>AI Strategy Analyst</h2><p>Status: <b>{h(analyst.get('portfolio_status'))}</b></p><p>{h(analyst.get('summary'))}</p></div>
    <div class='card'><h2>Promotion Manager</h2><p>Action counts: {h(promo.get('action_counts'))}</p></div>
    <div class='card'><h2>Portfolio</h2><p>{h(port.get('summary'))}</p></div>
    <div class='card'><h2>Links</h2>
    <a href='/strategy_promotion_dashboard?secret={h(secret)}&days={days}&limit={limit}'>Promotion dashboard</a>
    <a href='/ai_strategy_analyst_report?secret={h(secret)}&days={days}&limit={limit}'>AI analyst JSON</a>
    <a href='/ai_risk_supervisor?secret={h(secret)}&days={days}&limit={limit}'>AI risk JSON</a>
    <a href='/portfolio_exposure_ai_summary?secret={h(secret)}&days={days}&limit={limit}'>Portfolio JSON</a>
    <a href='/candidate_monitor_dashboard?secret={h(secret)}&days={days}&limit={limit}'>Candidate monitor</a>
    <a href='/dashboard_v2?secret={h(secret)}&days={days}'>Dashboard</a>
    </div></body></html>
    """)



@app.get("/supabase_trade_log_health")
def supabase_trade_log_health(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    base_url = str(SUPABASE_URL or "")
    safe_url = ""
    if base_url:
        try:
            from urllib.parse import urlsplit
            parsed = urlsplit(base_url)
            safe_url = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else base_url
        except Exception:
            safe_url = base_url

    probe_rows = fetch_supabase_logs(limit=1)
    return {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "supabase_enabled": supabase_enabled(),
        "configured_base_url": safe_url,
        "trade_event_table": SUPABASE_TABLE,
        "state": dict(_supabase_trade_log_state),
        "probe_row_count": len(probe_rows),
        "effective_source": _supabase_trade_log_state.get("last_source"),
        "fallback": "local_csv",
    }


@app.get("/version")
def version(secret: Optional[str] = None):
    if secret is not None and secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "version": APP_FEATURE_LEVEL, "base": "5.3.0", "features": ["order_hardening", "safe_auto_close", "telegram_command_security", "strategy_state_rollback", "audit_log", "simulation_replay", "portfolio_correlation_guard", "market_regime_filter", "production_monitoring", "config_validation", "control_panel", "paper_trade_outcome_tracker", "paper_outcome_decision_layer", "candidate_monitor", "paper_backtest_alignment", "backtest_manual_import", "backtest_registry", "cron_paper_outcome_report", "telegram_candidate_monitor_report", "paper_strategy_guard", "paper_auto_reject_warning", "strategy_promotion_manager", "ai_strategy_analyst", "ai_risk_supervisor", "backtest_table_import", "telegram_approval_workflow", "portfolio_exposure_ai_summary", "v7_control_center", "bybit_universe_scanner", "multi_symbol_strategy_scanner", "python_mini_backtest_engine", "auto_paper_candidate_onboarding_plan", "ai_market_opportunity_analyst", "discovery_candidate_plan", "near_miss_analysis", "discovery_validation_registry", "discovery_quality_calibration", "discovery_ranking_quality_fix", "v9_multi_market_research_framework", "crypto_higher_timeframe_research", "external_market_backtest_registry", "market_regime_gate", "combined_research_dashboard", "external_market_yahoo_fallback", "external_market_data_diagnostics", "persistent_supabase_registry", "universal_strategy_instance_layer", "promotion_history_registry", "early_warning_rules", "registry_bootstrap", "market_regime_gate_helper_fix"]}


# ============================================================
# v8.0.0 - v8.4.0 BYBIT UNIVERSE SCANNER + MINI BACKTEST ENGINE
# ============================================================
# Purpose:
# - discover opportunities across many Bybit symbols without manually creating TV alerts
# - run lightweight Python strategy scans and mini-backtests
# - produce watchlists and AI-style opportunity summaries
# - keep execution safe: no direct orders, no automatic promotion without approval workflow

UNIVERSE_SCANNER_ENABLED = os.getenv("UNIVERSE_SCANNER_ENABLED", "true").lower() == "true"
UNIVERSE_CATEGORY = os.getenv("UNIVERSE_CATEGORY", "linear")
UNIVERSE_QUOTE = os.getenv("UNIVERSE_QUOTE", "USDT")
UNIVERSE_MIN_TURNOVER_24H = float(os.getenv("UNIVERSE_MIN_TURNOVER_24H", "1000000"))
UNIVERSE_MIN_VOLUME_24H = float(os.getenv("UNIVERSE_MIN_VOLUME_24H", "0"))
UNIVERSE_MAX_SYMBOLS = int(os.getenv("UNIVERSE_MAX_SYMBOLS", "80"))
UNIVERSE_EXCLUDE_SYMBOLS = {x.strip().upper() for x in os.getenv("UNIVERSE_EXCLUDE_SYMBOLS", "").split(",") if x.strip()}
UNIVERSE_CACHE_TTL_SEC = int(os.getenv("UNIVERSE_CACHE_TTL_SEC", "900"))

SCANNER_DEFAULT_INTERVAL = os.getenv("SCANNER_DEFAULT_INTERVAL", "15")
SCANNER_KLINE_LIMIT = int(os.getenv("SCANNER_KLINE_LIMIT", "300"))
SCANNER_TOP_N = int(os.getenv("SCANNER_TOP_N", "40"))
SCANNER_MIN_OPPORTUNITY_SCORE = float(os.getenv("SCANNER_MIN_OPPORTUNITY_SCORE", "60"))

MINI_BACKTEST_ENABLED = os.getenv("MINI_BACKTEST_ENABLED", "true").lower() == "true"
MINI_BACKTEST_DEFAULT_DAYS = int(os.getenv("MINI_BACKTEST_DEFAULT_DAYS", "60"))
MINI_BACKTEST_MAX_SYMBOLS = int(os.getenv("MINI_BACKTEST_MAX_SYMBOLS", "30"))
MINI_BACKTEST_KLINE_LIMIT = int(os.getenv("MINI_BACKTEST_KLINE_LIMIT", "1000"))
MINI_BACKTEST_MIN_TRADES = int(os.getenv("MINI_BACKTEST_MIN_TRADES", "15"))
MINI_BACKTEST_MIN_PF = float(os.getenv("MINI_BACKTEST_MIN_PF", "1.15"))
MINI_BACKTEST_TOP_N = int(os.getenv("MINI_BACKTEST_TOP_N", "20"))

AUTO_PAPER_CANDIDATE_MIN_SCORE = float(os.getenv("AUTO_PAPER_CANDIDATE_MIN_SCORE", "75"))
AUTO_PAPER_CANDIDATE_MIN_PF = float(os.getenv("AUTO_PAPER_CANDIDATE_MIN_PF", "1.20"))
AUTO_PAPER_CANDIDATE_MIN_TRADES = int(os.getenv("AUTO_PAPER_CANDIDATE_MIN_TRADES", "20"))
AUTO_PAPER_CANDIDATE_REQUIRE_APPROVAL = os.getenv("AUTO_PAPER_CANDIDATE_REQUIRE_APPROVAL", "true").lower() == "true"

UNIVERSE_CACHE_FILE = APP_DIR / "bybit_universe_cache.json"
SCANNER_RESULTS_FILE = APP_DIR / "multi_symbol_scanner_results.json"
MINI_BACKTEST_RESULTS_FILE = APP_DIR / "mini_backtest_results.json"
OPPORTUNITY_WATCHLIST_FILE = APP_DIR / "opportunity_watchlist.json"

V8_STRATEGY_FAMILIES = [
    "trend_continuation",
    "momentum_breakout",
    "trend_pullback",
]


def v8_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def v8_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


def v8_now_ts() -> int:
    return int(time.time())


def v8_interval_to_ms(interval: str) -> int:
    s = str(interval).strip().lower()
    if s.endswith("m"):
        return int(float(s[:-1]) * 60_000)
    if s.endswith("h"):
        return int(float(s[:-1]) * 3_600_000)
    if s.endswith("d"):
        return int(float(s[:-1]) * 86_400_000)
    return int(float(s) * 60_000)


def v8_public_bybit_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    # Public endpoints do not require signing; this reduces risk of signature/permission noise.
    url = BYBIT_BASE + path
    try:
        resp = client.get(url, params={k: v for k, v in (params or {}).items() if v is not None})
        if resp.status_code >= 400:
            return {"retCode": resp.status_code, "retMsg": resp.text, "result": {}}
        return resp.json()
    except Exception as exc:
        return {"retCode": -1, "retMsg": str(exc), "result": {}}


def bybit_get_all_linear_instruments() -> Dict[str, Dict[str, Any]]:
    cursor = None
    out: Dict[str, Dict[str, Any]] = {}
    for _ in range(20):
        params = {"category": UNIVERSE_CATEGORY, "limit": 1000, "cursor": cursor}
        resp = v8_public_bybit_get("/v5/market/instruments-info", params)
        if resp.get("retCode") != 0:
            break
        result = resp.get("result") or {}
        for item in result.get("list") or []:
            sym = str(item.get("symbol", "")).upper()
            if sym:
                out[sym] = item
        cursor = result.get("nextPageCursor")
        if not cursor:
            break
    return out


def bybit_get_all_linear_tickers() -> Dict[str, Dict[str, Any]]:
    resp = v8_public_bybit_get("/v5/market/tickers", {"category": UNIVERSE_CATEGORY})
    out: Dict[str, Dict[str, Any]] = {}
    if resp.get("retCode") != 0:
        return out
    for item in ((resp.get("result") or {}).get("list") or []):
        sym = str(item.get("symbol", "")).upper()
        if sym:
            out[sym] = item
    return out


def build_bybit_universe(force: bool = False, max_symbols: int = UNIVERSE_MAX_SYMBOLS) -> Dict[str, Any]:
    if not UNIVERSE_SCANNER_ENABLED:
        return {"ok": False, "reason": "UNIVERSE_SCANNER_DISABLED", "items": []}
    if not force:
        cached = read_json_file(UNIVERSE_CACHE_FILE, {})
        if cached and (v8_now_ts() - v8_int(cached.get("created_ts"), 0)) <= UNIVERSE_CACHE_TTL_SEC:
            return cached

    instruments = bybit_get_all_linear_instruments()
    tickers = bybit_get_all_linear_tickers()
    items = []
    for sym, inst in instruments.items():
        if not sym.endswith(UNIVERSE_QUOTE):
            continue
        if sym in UNIVERSE_EXCLUDE_SYMBOLS:
            continue
        status = str(inst.get("status", "")).lower()
        if status not in {"trading", ""}:
            continue
        ticker = tickers.get(sym) or {}
        turnover24h = v8_float(ticker.get("turnover24h"), 0.0)
        volume24h = v8_float(ticker.get("volume24h"), 0.0)
        last_price = v8_float(ticker.get("lastPrice"), 0.0)
        bid1 = v8_float(ticker.get("bid1Price"), 0.0)
        ask1 = v8_float(ticker.get("ask1Price"), 0.0)
        if turnover24h < UNIVERSE_MIN_TURNOVER_24H:
            continue
        if volume24h < UNIVERSE_MIN_VOLUME_24H:
            continue
        spread_pct = None
        if bid1 > 0 and ask1 > 0 and last_price > 0:
            spread_pct = abs(ask1 - bid1) / last_price * 100.0
        lot = inst.get("lotSizeFilter") or {}
        pricef = inst.get("priceFilter") or {}
        liquidity_score = min(100.0, 20.0 + math.log10(max(turnover24h, 1.0)) * 10.0)
        spread_penalty = min(25.0, (spread_pct or 0.0) * 50.0)
        score = max(0.0, min(100.0, liquidity_score - spread_penalty))
        items.append({
            "symbol": sym,
            "status": inst.get("status"),
            "last_price": last_price,
            "turnover24h": turnover24h,
            "volume24h": volume24h,
            "price_change_pct_24h": v8_float(ticker.get("price24hPcnt"), 0.0) * 100.0,
            "spread_pct": spread_pct,
            "min_qty": v8_float(lot.get("minOrderQty"), 0.0),
            "qty_step": v8_float(lot.get("qtyStep"), 0.0),
            "tick_size": v8_float(pricef.get("tickSize"), 0.0),
            "liquidity_score": round(score, 2),
        })
    items.sort(key=lambda x: (x.get("liquidity_score", 0), x.get("turnover24h", 0)), reverse=True)
    items = items[:max_symbols]
    result = {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "created_at": now_iso(),
        "created_ts": v8_now_ts(),
        "category": UNIVERSE_CATEGORY,
        "quote": UNIVERSE_QUOTE,
        "min_turnover_24h": UNIVERSE_MIN_TURNOVER_24H,
        "count": len(items),
        "items": items,
    }
    write_json_file(UNIVERSE_CACHE_FILE, result)
    return result


def fetch_bybit_klines(symbol: str, interval: str = SCANNER_DEFAULT_INTERVAL, limit: int = SCANNER_KLINE_LIMIT, start_ms: Optional[int] = None) -> list:
    params: Dict[str, Any] = {"category": UNIVERSE_CATEGORY, "symbol": symbol.upper(), "interval": str(interval), "limit": limit}
    if start_ms:
        params["start"] = start_ms
    resp = v8_public_bybit_get("/v5/market/kline", params)
    if resp.get("retCode") != 0:
        return []
    raw = (resp.get("result") or {}).get("list") or []
    candles = []
    for row in raw:
        try:
            candles.append({
                "start_ms": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "turnover": float(row[6]) if len(row) > 6 else 0.0,
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["start_ms"])
    return candles


def v8_ema(values: list, length: int) -> list:
    if not values:
        return []
    alpha = 2.0 / (length + 1.0)
    out = []
    ema = float(values[0])
    for v in values:
        ema = alpha * float(v) + (1.0 - alpha) * ema
        out.append(ema)
    return out


def v8_sma(values: list, length: int) -> list:
    out = []
    s = 0.0
    q = []
    for v in values:
        v = float(v)
        q.append(v)
        s += v
        if len(q) > length:
            s -= q.pop(0)
        out.append(s / len(q))
    return out


def v8_rsi(values: list, length: int = 14) -> list:
    if not values:
        return []
    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(values)):
        ch = float(values[i]) - float(values[i - 1])
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_gain = v8_ema(gains, length)
    avg_loss = v8_ema(losses, length)
    out = []
    for g, l in zip(avg_gain, avg_loss):
        if l == 0:
            out.append(100.0 if g > 0 else 50.0)
        else:
            rs = g / l
            out.append(100.0 - 100.0 / (1.0 + rs))
    return out


def v8_atr(candles: list, length: int = 14) -> list:
    if not candles:
        return []
    tr = []
    prev_close = candles[0]["close"]
    for c in candles:
        trv = max(c["high"] - c["low"], abs(c["high"] - prev_close), abs(c["low"] - prev_close))
        tr.append(trv)
        prev_close = c["close"]
    return v8_ema(tr, length)


def v8_adx(candles: list, length: int = 14) -> Dict[str, list]:
    if not candles:
        return {"adx": [], "plus_di": [], "minus_di": []}
    plus_dm = [0.0]
    minus_dm = [0.0]
    tr = [candles[0]["high"] - candles[0]["low"]]
    for i in range(1, len(candles)):
        up = candles[i]["high"] - candles[i - 1]["high"]
        down = candles[i - 1]["low"] - candles[i]["low"]
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        tr.append(max(candles[i]["high"] - candles[i]["low"], abs(candles[i]["high"] - candles[i - 1]["close"]), abs(candles[i]["low"] - candles[i - 1]["close"])))
    tr_sm = v8_ema(tr, length)
    plus_sm = v8_ema(plus_dm, length)
    minus_sm = v8_ema(minus_dm, length)
    plus_di, minus_di, dx = [], [], []
    for t, p, m in zip(tr_sm, plus_sm, minus_sm):
        if t <= 0:
            plus_di.append(0.0); minus_di.append(0.0); dx.append(0.0)
        else:
            pd = 100.0 * p / t
            md = 100.0 * m / t
            plus_di.append(pd); minus_di.append(md)
            dx.append(100.0 * abs(pd - md) / (pd + md) if (pd + md) > 0 else 0.0)
    return {"adx": v8_ema(dx, length), "plus_di": plus_di, "minus_di": minus_di}


def prepare_indicators(candles: list) -> Dict[str, list]:
    closes = [c["close"] for c in candles]
    vols = [c["volume"] for c in candles]
    adx_pack = v8_adx(candles, 14)
    return {
        "close": closes,
        "volume": vols,
        "ema20": v8_ema(closes, 20),
        "ema50": v8_ema(closes, 50),
        "ema200": v8_ema(closes, 200),
        "vol20": v8_sma(vols, 20),
        "rsi14": v8_rsi(closes, 14),
        "atr14": v8_atr(candles, 14),
        "adx14": adx_pack["adx"],
        "plus_di": adx_pack["plus_di"],
        "minus_di": adx_pack["minus_di"],
    }


def candle_body_pct(c: Dict[str, Any]) -> float:
    rng = c["high"] - c["low"]
    if rng <= 0:
        return 0.0
    return abs(c["close"] - c["open"]) / rng * 100.0


def v8_signal_for_family(candles: list, ind: Dict[str, list], i: int, family: str) -> Optional[Dict[str, Any]]:
    if i < 220 or i >= len(candles):
        return None
    c = candles[i]
    close = c["close"]
    atr = ind["atr14"][i]
    if close <= 0 or atr <= 0:
        return None
    atr_pct = atr / close * 100.0
    if atr_pct < 0.10 or atr_pct > 8.0:
        return None
    ema20 = ind["ema20"][i]
    ema50 = ind["ema50"][i]
    ema200 = ind["ema200"][i]
    rsi = ind["rsi14"][i]
    adx = ind["adx14"][i]
    plus_di = ind["plus_di"][i]
    minus_di = ind["minus_di"][i]
    vol20 = ind["vol20"][i]
    vol = c["volume"]
    body = candle_body_pct(c)
    is_green = c["close"] > c["open"]
    recent_low8 = min(x["low"] for x in candles[max(0, i - 8): i + 1])
    recent_high20 = max(x["high"] for x in candles[max(0, i - 20): i]) if i >= 20 else 0.0
    prev_high = candles[i - 1]["high"] if i > 0 else c["high"]

    if family == "trend_continuation":
        ok = (
            close > ema200 and ema20 > ema50 and ema50 > ind["ema50"][max(0, i - 5)]
            and adx >= 20.0 and plus_di >= minus_di
            and vol >= vol20 * 1.35
            and (recent_low8 <= ema20 or recent_low8 <= ema50)
            and close > ema20 and close > candles[i - 1]["close"]
            and 50.0 <= rsi <= 72.0
            and is_green and body >= 35.0
        )
        if not ok:
            return None
        sl = close - atr * 1.5
        return {"family": family, "side": "LONG", "entry": close, "sl": sl, "tp1": close + (close - sl), "tp2": close + (close - sl) * 2.1}

    if family == "momentum_breakout":
        break_level = recent_high20 * 1.0003
        ok = (
            close > ema50 and ema20 > ema50 and close > break_level
            and vol >= vol20 * 1.20
            and 55.0 <= rsi <= 78.0
            and is_green and body >= 45.0
            and abs(close - ema20) / close * 100.0 <= 1.20
        )
        if not ok:
            return None
        sl = close - atr * 1.4
        return {"family": family, "side": "LONG", "entry": close, "sl": sl, "tp1": close + (close - sl), "tp2": close + (close - sl) * 2.4}

    if family == "trend_pullback":
        ok = (
            close > ema50 and ema20 > ema50 and close > ema200
            and adx >= 20.0 and plus_di >= minus_di
            and vol >= vol20 * 1.10
            and (c["low"] <= ema20 or c["low"] <= ema50 or recent_low8 <= ema20)
            and close > ema20 and close > prev_high
            and 45.0 <= rsi <= 75.0
            and is_green and body >= 35.0
        )
        if not ok:
            return None
        sl = close - atr * 1.6
        return {"family": family, "side": "LONG", "entry": close, "sl": sl, "tp1": close + (close - sl), "tp2": close + (close - sl) * 2.1}
    return None


def score_current_opportunity(candles: list, family: str) -> Dict[str, Any]:
    if len(candles) < 220:
        return {"ok": False, "reason": "NOT_ENOUGH_CANDLES", "score": 0}
    ind = prepare_indicators(candles)
    i = len(candles) - 1
    c = candles[i]
    close = c["close"]
    ema20, ema50, ema200 = ind["ema20"][i], ind["ema50"][i], ind["ema200"][i]
    rsi, atr, adx = ind["rsi14"][i], ind["atr14"][i], ind["adx14"][i]
    vol, vol20 = c["volume"], ind["vol20"][i]
    atr_pct = atr / close * 100.0 if close else 0.0
    trend_score = 0.0
    if close > ema200: trend_score += 25
    if ema20 > ema50: trend_score += 25
    if ema50 > ind["ema50"][max(0, i - 5)]: trend_score += 20
    if ind["plus_di"][i] >= ind["minus_di"][i]: trend_score += 10
    trend_score += min(20.0, max(0.0, adx))
    momentum_score = max(0.0, 100.0 - abs(rsi - 60.0) * 2.5)
    volume_score = min(100.0, (vol / max(vol20, 1e-9)) * 55.0)
    vol_score = 100.0 if 0.10 <= atr_pct <= 6.0 else max(0.0, 70.0 - abs(atr_pct - 3.0) * 10.0)
    signal = v8_signal_for_family(candles, ind, i, family)
    trigger_bonus = 20.0 if signal else 0.0
    score = min(100.0, trend_score * 0.30 + momentum_score * 0.20 + volume_score * 0.20 + vol_score * 0.15 + trigger_bonus)
    if score >= 90:
        rec = "MICRO_REVIEW_CANDIDATE"
    elif score >= 75:
        rec = "STRONG_PAPER_CANDIDATE"
    elif score >= 60:
        rec = "PAPER_CANDIDATE"
    elif score >= 40:
        rec = "WATCH"
    else:
        rec = "IGNORE"
    return {
        "ok": True,
        "family": family,
        "score": round(score, 2),
        "recommendation": rec,
        "signal_now": bool(signal),
        "signal": signal,
        "details": {
            "close": close,
            "ema20": ema20,
            "ema50": ema50,
            "ema200": ema200,
            "rsi": round(rsi, 2),
            "adx": round(adx, 2),
            "atr_pct": round(atr_pct, 3),
            "volume_ratio": round(vol / max(vol20, 1e-9), 3),
            "trend_score": round(trend_score, 2),
            "momentum_score": round(momentum_score, 2),
            "volume_score": round(volume_score, 2),
            "volatility_score": round(vol_score, 2),
        },
    }


def run_strategy_mini_backtest(candles: list, family: str, same_candle_rule: str = "SL_FIRST") -> Dict[str, Any]:
    if len(candles) < 230:
        return {"ok": False, "reason": "NOT_ENOUGH_CANDLES", "family": family}
    ind = prepare_indicators(candles)
    trades = []
    i = 220
    while i < len(candles) - 2:
        sig = v8_signal_for_family(candles, ind, i, family)
        if not sig:
            i += 1
            continue
        entry, sl, tp1, tp2 = sig["entry"], sig["sl"], sig["tp1"], sig["tp2"]
        risk = entry - sl
        if risk <= 0:
            i += 1
            continue
        tp1_hit = False
        terminal = None
        terminal_i = None
        for j in range(i + 1, len(candles)):
            cj = candles[j]
            sl_hit = cj["low"] <= sl
            tp1_now = cj["high"] >= tp1
            tp2_now = cj["high"] >= tp2
            if not tp1_hit:
                if sl_hit and tp1_now and same_candle_rule == "SL_FIRST":
                    terminal = "LOSS_SL"; terminal_i = j; break
                if sl_hit:
                    terminal = "LOSS_SL"; terminal_i = j; break
                if tp2_now:
                    terminal = "WIN_TP2"; terminal_i = j; break
                if tp1_now:
                    tp1_hit = True
                    continue
            else:
                if sl_hit and tp2_now and same_candle_rule == "SL_FIRST":
                    terminal = "PARTIAL_TP1_THEN_SL"; terminal_i = j; break
                if tp2_now:
                    terminal = "WIN_TP2"; terminal_i = j; break
                if sl_hit:
                    terminal = "PARTIAL_TP1_THEN_SL"; terminal_i = j; break
        if terminal is None:
            terminal = "OPEN"
            terminal_i = len(candles) - 1
            r = 0.0
        elif terminal == "LOSS_SL":
            r = -1.0
        elif terminal == "WIN_TP2":
            tp1_r = (tp1 - entry) / risk
            tp2_r = (tp2 - entry) / risk
            r = 0.5 * tp1_r + 0.5 * tp2_r
        else:
            r = 0.0  # 50% at +1R and 50% at -1R = approx 0R
        trades.append({
            "entry_i": i,
            "exit_i": terminal_i,
            "entry_time": candles[i]["start_ms"],
            "exit_time": candles[terminal_i]["start_ms"] if terminal_i is not None else None,
            "status": terminal,
            "r": r,
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
        })
        i = max(i + 1, (terminal_i or i) + 1)
    closed = [t for t in trades if t["status"] != "OPEN"]
    wins = [t for t in closed if t["r"] > 0]
    losses = [t for t in closed if t["r"] < 0]
    gross_profit = sum(t["r"] for t in wins)
    gross_loss = abs(sum(t["r"] for t in losses))
    pf = gross_profit / gross_loss if gross_loss > 0 else (None if gross_profit == 0 else 999.0)
    total_r = sum(t["r"] for t in closed)
    avg_r = total_r / len(closed) if closed else None
    return {
        "ok": True,
        "family": family,
        "trade_count": len(closed),
        "open_count": len(trades) - len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(closed) * 100.0 if closed else None,
        "profit_factor": pf,
        "total_r": total_r,
        "average_r": avg_r,
        "by_status": {s: sum(1 for t in trades if t["status"] == s) for s in sorted({t["status"] for t in trades})},
        "latest_trades": trades[-10:],
    }


def run_multi_symbol_strategy_scan(max_symbols: int = SCANNER_TOP_N, interval: str = SCANNER_DEFAULT_INTERVAL, families: Optional[list] = None) -> Dict[str, Any]:
    universe = build_bybit_universe(force=False, max_symbols=max_symbols)
    families = families or V8_STRATEGY_FAMILIES
    rows = []
    for item in universe.get("items") or []:
        sym = item.get("symbol")
        candles = fetch_bybit_klines(sym, interval=interval, limit=SCANNER_KLINE_LIMIT)
        if len(candles) < 220:
            continue
        for fam in families:
            res = score_current_opportunity(candles, fam)
            if not res.get("ok"):
                continue
            row = {
                "symbol": sym,
                "interval": interval,
                "family": fam,
                "score": res.get("score"),
                "recommendation": res.get("recommendation"),
                "signal_now": res.get("signal_now"),
                "liquidity_score": item.get("liquidity_score"),
                "turnover24h": item.get("turnover24h"),
                "details": res.get("details"),
                "signal": res.get("signal"),
            }
            rows.append(row)
    rows.sort(key=lambda x: (x.get("signal_now") is True, x.get("score") or 0, x.get("turnover24h") or 0), reverse=True)
    result = {"ok": True, "version": APP_FEATURE_LEVEL, "created_at": now_iso(), "interval": interval, "count": len(rows), "top": rows[:100]}
    write_json_file(SCANNER_RESULTS_FILE, result)
    return result


def run_python_mini_backtests(max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, families: Optional[list] = None, kline_limit: int = MINI_BACKTEST_KLINE_LIMIT) -> Dict[str, Any]:
    if not MINI_BACKTEST_ENABLED:
        return {"ok": False, "reason": "MINI_BACKTEST_DISABLED"}
    universe = build_bybit_universe(force=False, max_symbols=max_symbols)
    families = families or V8_STRATEGY_FAMILIES
    rows = []
    for item in universe.get("items") or []:
        sym = item.get("symbol")
        candles = fetch_bybit_klines(sym, interval=interval, limit=kline_limit)
        if len(candles) < 230:
            continue
        for fam in families:
            bt = run_strategy_mini_backtest(candles, fam)
            if not bt.get("ok"):
                continue
            pf = bt.get("profit_factor")
            trades = bt.get("trade_count") or 0
            score_current = score_current_opportunity(candles, fam)
            candidate = bool(pf is not None and pf >= MINI_BACKTEST_MIN_PF and trades >= MINI_BACKTEST_MIN_TRADES)
            rows.append({
                "symbol": sym,
                "interval": interval,
                "family": fam,
                "candidate": candidate,
                "profit_factor": pf,
                "trade_count": trades,
                "win_rate": bt.get("win_rate"),
                "total_r": bt.get("total_r"),
                "average_r": bt.get("average_r"),
                "current_score": score_current.get("score") if score_current.get("ok") else None,
                "current_recommendation": score_current.get("recommendation") if score_current.get("ok") else None,
                "signal_now": score_current.get("signal_now") if score_current.get("ok") else False,
                "liquidity_score": item.get("liquidity_score"),
                "turnover24h": item.get("turnover24h"),
                "by_status": bt.get("by_status"),
            })
    rows.sort(key=lambda x: (x.get("candidate") is True, x.get("profit_factor") or 0, x.get("current_score") or 0), reverse=True)
    result = {"ok": True, "version": APP_FEATURE_LEVEL, "created_at": now_iso(), "interval": interval, "count": len(rows), "top": rows[:MINI_BACKTEST_TOP_N], "rows": rows}
    write_json_file(MINI_BACKTEST_RESULTS_FILE, result)
    return result


def build_auto_paper_candidate_plan(max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, force_backtest: bool = False) -> Dict[str, Any]:
    data = read_json_file(MINI_BACKTEST_RESULTS_FILE, {})
    if force_backtest or not data or not data.get("rows"):
        data = run_python_mini_backtests(max_symbols=max_symbols, interval=interval)
    state = load_state()
    existing = set()
    for strat, scfg in (state.get("strategies") or {}).items():
        for sym, symcfg in (scfg.get("symbols") or {}).items():
            for side, sidecfg in (symcfg or {}).items():
                if str(sidecfg.get("mode", "OFF")).upper() != "OFF":
                    existing.add((strat, sym.upper(), side.upper()))
    plans = []
    for row in data.get("rows") or []:
        pf = row.get("profit_factor")
        trades = row.get("trade_count") or 0
        score = row.get("current_score") or 0
        if pf is None or pf < AUTO_PAPER_CANDIDATE_MIN_PF or trades < AUTO_PAPER_CANDIDATE_MIN_TRADES or score < AUTO_PAPER_CANDIDATE_MIN_SCORE:
            continue
        strategy_name = f"auto_{row.get('family')}_{str(row.get('symbol')).lower()}_v1"
        key = (strategy_name, str(row.get("symbol")).upper(), "LONG")
        action = "ALREADY_ACTIVE" if key in existing else "ADD_PAPER_CANDIDATE"
        plans.append({
            "action": action,
            "strategy": strategy_name,
            "symbol": row.get("symbol"),
            "side": "LONG",
            "target_mode": "PAPER",
            "risk_pct": 0.05,
            "family": row.get("family"),
            "profit_factor": pf,
            "trade_count": trades,
            "win_rate": row.get("win_rate"),
            "current_score": score,
            "current_recommendation": row.get("current_recommendation"),
            "requires_approval": AUTO_PAPER_CANDIDATE_REQUIRE_APPROVAL,
        })
    result = {"ok": True, "version": APP_FEATURE_LEVEL, "created_at": now_iso(), "count": len(plans), "plans": plans[:50]}
    write_json_file(OPPORTUNITY_WATCHLIST_FILE, result)
    return result


def build_ai_market_opportunity_analyst(max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, force_backtest: bool = False) -> Dict[str, Any]:
    bt = read_json_file(MINI_BACKTEST_RESULTS_FILE, {})
    if force_backtest or not bt or not bt.get("rows"):
        bt = run_python_mini_backtests(max_symbols=max_symbols, interval=interval)
    plan = build_auto_paper_candidate_plan(max_symbols=max_symbols, interval=interval, force_backtest=False)
    top_candidates = plan.get("plans") or []
    risk = build_ai_risk_supervisor_report(days=PAPER_OUTCOME_DEFAULT_DAYS, limit=PAPER_OUTCOME_MAX_EVENTS, include_plan=False).get("risk") or {}
    summary = "No strong new paper candidates found."
    if top_candidates:
        summary = f"Found {len(top_candidates)} potential PAPER candidates. Top: {top_candidates[0].get('symbol')} {top_candidates[0].get('family')} PF={fmt_num(top_candidates[0].get('profit_factor'))}, score={fmt_num(top_candidates[0].get('current_score'))}."
    if risk.get("level") in {"HIGH", "ELEVATED"}:
        summary += f" Portfolio risk is {risk.get('level')}; do not promote to MICRO without review."
    return {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "created_at": now_iso(),
        "portfolio_risk": risk,
        "summary": summary,
        "top_candidates": top_candidates[:20],
        "mini_backtest_top": (bt.get("top") or [])[:20],
        "recommended_next_action": "Review top candidates, add only to PAPER with approval. No direct AI order execution.",
    }


@app.get("/bybit_universe")
def bybit_universe(secret: str, force: bool = False, max_symbols: int = UNIVERSE_MAX_SYMBOLS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_bybit_universe(force=force, max_symbols=max_symbols)


@app.get("/bybit_universe_dashboard", response_class=HTMLResponse)
def bybit_universe_dashboard(secret: str, force: bool = False, max_symbols: int = UNIVERSE_MAX_SYMBOLS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = build_bybit_universe(force=force, max_symbols=max_symbols)
    rows = "".join([f"<tr><td>{h(x.get('symbol'))}</td><td>{fmt_num(x.get('last_price'))}</td><td>{fmt_num(x.get('turnover24h'))}</td><td>{fmt_num(x.get('spread_pct'))}</td><td>{fmt_num(x.get('liquidity_score'))}</td></tr>" for x in data.get("items", [])[:100]])
    return HTMLResponse(f"""
    <html><head><title>Bybit Universe Scanner</title><style>body{{font-family:Arial;margin:20px;background:#f6f8fb}} table{{border-collapse:collapse;width:100%;background:white}} th{{background:#111827;color:white}} td,th{{padding:7px;border-bottom:1px solid #ddd;text-align:left}}</style></head>
    <body><h1>Bybit Universe Scanner v8.0</h1><p>Count: {data.get('count')} | Min turnover: {fmt_num(data.get('min_turnover_24h'))}</p><table><tr><th>Symbol</th><th>Last</th><th>Turnover 24h</th><th>Spread %</th><th>Liquidity score</th></tr>{rows}</table>
    <p><a href='/multi_symbol_strategy_scan_dashboard?secret={h(secret)}&max_symbols=40'>Strategy scanner</a> · <a href='/mini_backtest_dashboard?secret={h(secret)}&max_symbols=25'>Mini backtest</a></p></body></html>
    """)


@app.get("/multi_symbol_strategy_scan")
def multi_symbol_strategy_scan(secret: str, max_symbols: int = SCANNER_TOP_N, interval: str = SCANNER_DEFAULT_INTERVAL):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return run_multi_symbol_strategy_scan(max_symbols=max_symbols, interval=interval)


@app.get("/multi_symbol_strategy_scan_dashboard", response_class=HTMLResponse)
def multi_symbol_strategy_scan_dashboard(secret: str, max_symbols: int = SCANNER_TOP_N, interval: str = SCANNER_DEFAULT_INTERVAL):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = run_multi_symbol_strategy_scan(max_symbols=max_symbols, interval=interval)
    rows = "".join([f"<tr><td>{h(x.get('symbol'))}</td><td>{h(x.get('family'))}</td><td>{fmt_num(x.get('score'))}</td><td>{h(x.get('recommendation'))}</td><td>{'YES' if x.get('signal_now') else 'NO'}</td><td>{fmt_num(x.get('turnover24h'))}</td></tr>" for x in data.get("top", [])[:100]])
    return HTMLResponse(f"""
    <html><head><title>Multi-Symbol Strategy Scanner</title><style>body{{font-family:Arial;margin:20px;background:#f6f8fb}} table{{border-collapse:collapse;width:100%;background:white}} th{{background:#111827;color:white}} td,th{{padding:7px;border-bottom:1px solid #ddd;text-align:left}}</style></head>
    <body><h1>Multi-Symbol Strategy Scanner v8.1</h1><p>Interval: {h(interval)} | Rows: {data.get('count')}</p><table><tr><th>Symbol</th><th>Family</th><th>Score</th><th>Recommendation</th><th>Signal now</th><th>Turnover 24h</th></tr>{rows}</table>
    <p><a href='/mini_backtest_dashboard?secret={h(secret)}&max_symbols=25'>Mini backtest</a> · <a href='/ai_market_opportunity_analyst?secret={h(secret)}'>AI opportunity analyst JSON</a></p></body></html>
    """)


@app.get("/mini_backtest_run")
def mini_backtest_run(secret: str, max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, kline_limit: int = MINI_BACKTEST_KLINE_LIMIT):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return run_python_mini_backtests(max_symbols=max_symbols, interval=interval, kline_limit=kline_limit)


@app.get("/mini_backtest_dashboard", response_class=HTMLResponse)
def mini_backtest_dashboard(secret: str, max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = run_python_mini_backtests(max_symbols=max_symbols, interval=interval)
    rows = "".join([f"<tr><td>{h(x.get('symbol'))}</td><td>{h(x.get('family'))}</td><td>{fmt_num(x.get('profit_factor'))}</td><td>{x.get('trade_count')}</td><td>{fmt_num(x.get('win_rate'))}</td><td>{fmt_num(x.get('average_r'))}</td><td>{fmt_num(x.get('current_score'))}</td><td>{'YES' if x.get('candidate') else 'NO'}</td></tr>" for x in data.get("rows", [])[:100]])
    return HTMLResponse(f"""
    <html><head><title>Python Mini Backtest Engine</title><style>body{{font-family:Arial;margin:20px;background:#f6f8fb}} table{{border-collapse:collapse;width:100%;background:white}} th{{background:#111827;color:white}} td,th{{padding:7px;border-bottom:1px solid #ddd;text-align:left}}</style></head>
    <body><h1>Python Mini Backtest Engine v8.3</h1><p>Interval: {h(interval)} | Rows: {data.get('count')}</p><table><tr><th>Symbol</th><th>Family</th><th>PF</th><th>Trades</th><th>Win %</th><th>Avg R</th><th>Current score</th><th>Candidate</th></tr>{rows}</table>
    <p><a href='/auto_paper_candidate_plan?secret={h(secret)}'>Auto paper candidate plan</a> · <a href='/ai_market_opportunity_dashboard?secret={h(secret)}'>AI opportunity dashboard</a></p></body></html>
    """)


@app.get("/auto_paper_candidate_plan")
def auto_paper_candidate_plan(secret: str, max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, force_backtest: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_auto_paper_candidate_plan(max_symbols=max_symbols, interval=interval, force_backtest=force_backtest)


@app.get("/ai_market_opportunity_analyst")
def ai_market_opportunity_analyst(secret: str, max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, force_backtest: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_ai_market_opportunity_analyst(max_symbols=max_symbols, interval=interval, force_backtest=force_backtest)


@app.get("/ai_market_opportunity_dashboard", response_class=HTMLResponse)
def ai_market_opportunity_dashboard(secret: str, max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = build_ai_market_opportunity_analyst(max_symbols=max_symbols, interval=interval, force_backtest=False)
    rows = "".join([f"<tr><td>{h(x.get('symbol'))}</td><td>{h(x.get('family'))}</td><td>{fmt_num(x.get('profit_factor'))}</td><td>{x.get('trade_count')}</td><td>{fmt_num(x.get('current_score'))}</td><td>{h(x.get('action'))}</td></tr>" for x in data.get("top_candidates", [])[:50]])
    return HTMLResponse(f"""
    <html><head><title>AI Market Opportunity Analyst</title><style>body{{font-family:Arial;margin:20px;background:#f6f8fb}} .card{{background:white;border-radius:12px;padding:14px;margin-bottom:14px;box-shadow:0 1px 6px #d1d5db}} table{{border-collapse:collapse;width:100%;background:white}} th{{background:#111827;color:white}} td,th{{padding:7px;border-bottom:1px solid #ddd;text-align:left}}</style></head>
    <body><h1>AI Market Opportunity Analyst v8.4</h1><div class='card'><b>Summary:</b> {h(data.get('summary'))}<br><b>Recommended action:</b> {h(data.get('recommended_next_action'))}</div><table><tr><th>Symbol</th><th>Family</th><th>PF</th><th>Trades</th><th>Score</th><th>Action</th></tr>{rows}</table>
    <p><a href='/bybit_universe_dashboard?secret={h(secret)}'>Universe</a> · <a href='/mini_backtest_dashboard?secret={h(secret)}'>Mini backtest</a> · <a href='/v7_control_center?secret={h(secret)}'>v7 Control Center</a></p></body></html>
    """)


@app.get("/telegram_market_opportunity_report")
def telegram_market_opportunity_report(secret: str, max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, force_backtest: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = build_ai_market_opportunity_analyst(max_symbols=max_symbols, interval=interval, force_backtest=force_backtest)
    lines = ["🧠 Market Opportunity Analyst", data.get("summary", "")]
    for x in (data.get("top_candidates") or [])[:10]:
        lines.append(f"{x.get('symbol')} {x.get('family')} PF={fmt_num(x.get('profit_factor'))} trades={x.get('trade_count')} score={fmt_num(x.get('current_score'))} action={x.get('action')}")
    notify = safe_notify_event("🧠 Market opportunity report", "\n".join(lines), important=False)
    return {"ok": True, "sent": bool(notify.get("sent")), "notify": notify, "report": data}



# ============================================================
# v8.4.2 - DISCOVERY MODE / RELAXED CANDIDATE PLAN
# ============================================================
# Purpose:
# - show not only strict PAPER candidates, but also near misses
# - explain why a symbol/strategy was rejected by thresholds
# - support discovery without weakening the actual auto paper onboarding rules

DISCOVERY_MIN_SCORE = float(os.getenv("DISCOVERY_MIN_SCORE", "55"))
DISCOVERY_MIN_PF = float(os.getenv("DISCOVERY_MIN_PF", "1.00"))
DISCOVERY_MIN_TRADES = int(os.getenv("DISCOVERY_MIN_TRADES", "8"))
DISCOVERY_TOP_N = int(os.getenv("DISCOVERY_TOP_N", "50"))
DISCOVERY_STRONG_SCORE = float(os.getenv("DISCOVERY_STRONG_SCORE", "75"))
DISCOVERY_STRONG_PF = float(os.getenv("DISCOVERY_STRONG_PF", "1.20"))
DISCOVERY_STRONG_TRADES = int(os.getenv("DISCOVERY_STRONG_TRADES", "20"))


def classify_discovery_row(row: Dict[str, Any]) -> Dict[str, Any]:
    pf_raw = row.get("profit_factor")
    pf = None if pf_raw is None else v8_float(pf_raw, 0.0)
    trades = v8_int(row.get("trade_count"), 0)
    score = v8_float(row.get("current_score"), 0.0)
    candidate_strict = bool(
        pf is not None
        and pf >= AUTO_PAPER_CANDIDATE_MIN_PF
        and trades >= AUTO_PAPER_CANDIDATE_MIN_TRADES
        and score >= AUTO_PAPER_CANDIDATE_MIN_SCORE
    )

    failures = []
    if pf is None:
        failures.append("NO_PF")
    elif pf < AUTO_PAPER_CANDIDATE_MIN_PF:
        failures.append(f"PF_BELOW_STRICT_{pf:.2f}_LT_{AUTO_PAPER_CANDIDATE_MIN_PF:.2f}")
    if trades < AUTO_PAPER_CANDIDATE_MIN_TRADES:
        failures.append(f"TRADES_BELOW_STRICT_{trades}_LT_{AUTO_PAPER_CANDIDATE_MIN_TRADES}")
    if score < AUTO_PAPER_CANDIDATE_MIN_SCORE:
        failures.append(f"SCORE_BELOW_STRICT_{score:.1f}_LT_{AUTO_PAPER_CANDIDATE_MIN_SCORE:.1f}")

    if candidate_strict:
        bucket = "STRONG_CANDIDATE"
        action = "ADD_PAPER_CANDIDATE_REVIEW"
    elif pf is not None and pf >= DISCOVERY_STRONG_PF and trades >= DISCOVERY_MIN_TRADES and score >= DISCOVERY_MIN_SCORE:
        bucket = "CANDIDATE"
        action = "REVIEW_FOR_PAPER"
    elif pf is not None and pf >= DISCOVERY_MIN_PF and trades >= DISCOVERY_MIN_TRADES and score >= DISCOVERY_MIN_SCORE:
        bucket = "WATCHLIST"
        action = "WATCH_AND_RETEST"
    elif pf is not None and (pf >= DISCOVERY_MIN_PF or score >= DISCOVERY_MIN_SCORE or trades >= DISCOVERY_MIN_TRADES):
        bucket = "NEAR_MISS"
        action = "PARAMETER_REVIEW"
    else:
        bucket = "REJECTED"
        action = "IGNORE"

    if pf is None:
        reject_reason = "NO_PROFIT_FACTOR"
    elif trades < DISCOVERY_MIN_TRADES:
        reject_reason = "REJECTED_BY_TRADES"
    elif pf < DISCOVERY_MIN_PF:
        reject_reason = "REJECTED_BY_PF"
    elif score < DISCOVERY_MIN_SCORE:
        reject_reason = "REJECTED_BY_SCORE"
    else:
        reject_reason = "PASSED_DISCOVERY"

    return {
        "bucket": bucket,
        "action": action,
        "reject_reason": reject_reason,
        "strict_candidate": candidate_strict,
        "strict_failures": failures,
    }


def build_discovery_candidate_plan(max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, force_backtest: bool = False, include_rejected: bool = False) -> Dict[str, Any]:
    data = read_json_file(MINI_BACKTEST_RESULTS_FILE, {})
    if force_backtest or not data or not data.get("rows"):
        data = run_python_mini_backtests(max_symbols=max_symbols, interval=interval)

    state = load_state()
    active_symbols = set()
    active_strategy_symbol = set()
    for strat, scfg in (state.get("strategies") or {}).items():
        for sym, symcfg in (scfg.get("symbols") or {}).items():
            for side, sidecfg in (symcfg or {}).items():
                if str((sidecfg or {}).get("mode", "OFF")).upper() != "OFF":
                    active_symbols.add(str(sym).upper())
                    active_strategy_symbol.add((strat, str(sym).upper(), str(side).upper()))

    rows = []
    counts: Dict[str, int] = {}
    for row in data.get("rows") or []:
        cls = classify_discovery_row(row)
        bucket = cls["bucket"]
        counts[bucket] = counts.get(bucket, 0) + 1
        if bucket == "REJECTED" and not include_rejected:
            continue
        sym = str(row.get("symbol", "")).upper()
        fam = str(row.get("family", ""))
        strategy_name = f"auto_{fam}_{sym.lower()}_v1"
        is_active = (strategy_name, sym, "LONG") in active_strategy_symbol or sym in active_symbols
        rows.append({
            "symbol": sym,
            "family": fam,
            "strategy_suggestion": strategy_name,
            "side": "LONG",
            "bucket": bucket,
            "action": "ALREADY_ACTIVE_OR_SYMBOL_ACTIVE" if is_active and bucket in {"STRONG_CANDIDATE", "CANDIDATE", "WATCHLIST"} else cls["action"],
            "reject_reason": cls["reject_reason"],
            "strict_candidate": cls["strict_candidate"],
            "strict_failures": cls["strict_failures"],
            "profit_factor": row.get("profit_factor"),
            "trade_count": row.get("trade_count"),
            "win_rate": row.get("win_rate"),
            "average_r": row.get("average_r"),
            "total_r": row.get("total_r"),
            "current_score": row.get("current_score"),
            "current_recommendation": row.get("current_recommendation"),
            "signal_now": row.get("signal_now"),
            "turnover24h": row.get("turnover24h"),
            "liquidity_score": row.get("liquidity_score"),
            "already_active_symbol": sym in active_symbols,
            "requires_approval": True,
        })

    bucket_rank = {"STRONG_CANDIDATE": 5, "CANDIDATE": 4, "WATCHLIST": 3, "NEAR_MISS": 2, "REJECTED": 1}
    rows.sort(key=lambda x: (bucket_rank.get(x.get("bucket"), 0), v8_float(x.get("profit_factor"), 0), v8_float(x.get("current_score"), 0), v8_int(x.get("trade_count"), 0)), reverse=True)
    summary = {
        "strict_thresholds": {
            "min_score": AUTO_PAPER_CANDIDATE_MIN_SCORE,
            "min_pf": AUTO_PAPER_CANDIDATE_MIN_PF,
            "min_trades": AUTO_PAPER_CANDIDATE_MIN_TRADES,
        },
        "discovery_thresholds": {
            "min_score": DISCOVERY_MIN_SCORE,
            "min_pf": DISCOVERY_MIN_PF,
            "min_trades": DISCOVERY_MIN_TRADES,
        },
        "bucket_counts": counts,
        "strong_or_candidate": counts.get("STRONG_CANDIDATE", 0) + counts.get("CANDIDATE", 0),
        "watchlist_or_near_miss": counts.get("WATCHLIST", 0) + counts.get("NEAR_MISS", 0),
    }
    return {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "created_at": now_iso(),
        "interval": interval,
        "max_symbols": max_symbols,
        "count": len(rows[:DISCOVERY_TOP_N]),
        "total_rows_evaluated": len(data.get("rows") or []),
        "summary": summary,
        "items": rows[:DISCOVERY_TOP_N],
    }


@app.get("/discovery_candidate_plan")
def discovery_candidate_plan(secret: str, max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, force_backtest: bool = False, include_rejected: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return build_discovery_candidate_plan(max_symbols=max_symbols, interval=interval, force_backtest=force_backtest, include_rejected=include_rejected)


@app.get("/discovery_candidate_dashboard", response_class=HTMLResponse)
def discovery_candidate_dashboard(secret: str, max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, force_backtest: bool = False, include_rejected: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = build_discovery_candidate_plan(max_symbols=max_symbols, interval=interval, force_backtest=force_backtest, include_rejected=include_rejected)
    rows = "".join([
        f"<tr><td>{h(x.get('bucket'))}</td><td>{h(x.get('symbol'))}</td><td>{h(x.get('family'))}</td><td>{fmt_num(x.get('profit_factor'))}</td><td>{x.get('trade_count')}</td><td>{fmt_num(x.get('win_rate'))}</td><td>{fmt_num(x.get('average_r'))}</td><td>{fmt_num(x.get('current_score'))}</td><td>{h(x.get('reject_reason'))}</td><td>{h('; '.join(x.get('strict_failures') or []))}</td><td>{h(x.get('action'))}</td></tr>"
        for x in data.get("items", [])
    ])
    bc = data.get("summary", {}).get("bucket_counts", {})
    return HTMLResponse(f"""
    <html><head><title>Discovery Candidate Dashboard</title><style>body{{font-family:Arial;margin:20px;background:#f6f8fb}} .card{{background:white;border-radius:12px;padding:14px;margin-bottom:14px;box-shadow:0 1px 6px #d1d5db}} table{{border-collapse:collapse;width:100%;background:white;font-size:13px}} th{{background:#111827;color:white}} td,th{{padding:7px;border-bottom:1px solid #ddd;text-align:left}} .STRONG_CANDIDATE{{color:#047857;font-weight:bold}} .CANDIDATE{{color:#0369a1;font-weight:bold}} .WATCHLIST{{color:#92400e;font-weight:bold}} .NEAR_MISS{{color:#7c2d12;font-weight:bold}}</style></head>
    <body><h1>Discovery Candidate Dashboard · Platform v9.1.0</h1><div class='card'><b>Interval:</b> {h(interval)} | <b>Rows:</b> {data.get('total_rows_evaluated')} | <b>Shown:</b> {data.get('count')}<br><b>Bucket counts:</b> {h(json.dumps(bc, ensure_ascii=False))}</div>
    <table><tr><th>Bucket</th><th>Symbol</th><th>Family</th><th>PF</th><th>Trades</th><th>Win %</th><th>Avg R</th><th>Score</th><th>Discovery reason</th><th>Strict failures</th><th>Action</th></tr>{rows}</table>
    <p><a href='/discovery_candidate_plan?secret={h(secret)}&max_symbols={max_symbols}&interval={h(interval)}&include_rejected={str(include_rejected).lower()}'>JSON</a> · <a href='/auto_paper_candidate_plan?secret={h(secret)}&max_symbols={max_symbols}&interval={h(interval)}'>Strict auto plan</a> · <a href='/mini_backtest_dashboard?secret={h(secret)}&max_symbols={max_symbols}&interval={h(interval)}'>Mini backtest</a> · <a href='/ai_market_opportunity_dashboard?secret={h(secret)}&max_symbols={max_symbols}&interval={h(interval)}'>AI opportunity</a></p></body></html>
    """)


@app.get("/telegram_discovery_candidate_report")
def telegram_discovery_candidate_report(secret: str, max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, force_backtest: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = build_discovery_candidate_plan(max_symbols=max_symbols, interval=interval, force_backtest=force_backtest, include_rejected=False)
    bc = data.get("summary", {}).get("bucket_counts", {})
    lines = ["🔎 Discovery Candidate Report", f"interval={interval} max_symbols={max_symbols}", f"buckets={json.dumps(bc, ensure_ascii=False)}"]
    for x in (data.get("items") or [])[:10]:
        lines.append(f"{x.get('bucket')}: {x.get('symbol')} {x.get('family')} PF={fmt_num(x.get('profit_factor'))} trades={x.get('trade_count')} score={fmt_num(x.get('current_score'))} action={x.get('action')}")
    notify = safe_notify_event("🔎 Discovery candidate report", "\n".join(lines), important=False)
    return {"ok": True, "sent": bool(notify.get("sent")), "notify": notify, "report": data}


# ============================================================
# v8.4.3 / v8.4.4 DISCOVERY VALIDATION REGISTRY + QUALITY FIX
# ============================================================

DISCOVERY_VALIDATION_REGISTRY_FILE = APP_DIR / "discovery_validation_registry.json"
DISCOVERY_HIDE_TV_REJECTED_DEFAULT = os.getenv("DISCOVERY_HIDE_TV_REJECTED_DEFAULT", "false").lower() == "true"
DISCOVERY_TV_CONFIRM_PF = float(os.getenv("DISCOVERY_TV_CONFIRM_PF", "1.40"))
DISCOVERY_TV_WATCH_PF = float(os.getenv("DISCOVERY_TV_WATCH_PF", "1.20"))
DISCOVERY_TV_MIN_TRADES = int(os.getenv("DISCOVERY_TV_MIN_TRADES", "20"))
DISCOVERY_THIN_SAMPLE_TRADES = int(os.getenv("DISCOVERY_THIN_SAMPLE_TRADES", "10"))
DISCOVERY_NO_LOSS_PF_SENTINEL = float(os.getenv("DISCOVERY_NO_LOSS_PF_SENTINEL", "900"))


def discovery_key(symbol: str, family: str, side: str = "LONG", interval: str = "15") -> str:
    return f"{str(symbol).upper()}|{str(family)}|{str(side).upper()}|{str(interval)}"


def load_discovery_validations() -> Dict[str, Any]:
    data = read_json_file(DISCOVERY_VALIDATION_REGISTRY_FILE, {"rows": []})
    if isinstance(data, list):
        data = {"rows": data}
    rows = data.get("rows") or []
    out = {}
    for r in rows:
        key = r.get("key") or discovery_key(r.get("symbol", ""), r.get("family", ""), r.get("side", "LONG"), r.get("interval", "15"))
        if key.strip("|"):
            r["key"] = key
            out[key] = r
    return {"rows": list(out.values()), "by_key": out}


def save_discovery_validations(rows: list) -> None:
    unique = {}
    for r in rows:
        key = r.get("key") or discovery_key(r.get("symbol", ""), r.get("family", ""), r.get("side", "LONG"), r.get("interval", "15"))
        if key.strip("|"):
            r["key"] = key
            unique[key] = r
    write_json_file(DISCOVERY_VALIDATION_REGISTRY_FILE, {"rows": list(unique.values()), "updated_at": now_iso()})


def normalize_discovery_validation_row(item: Dict[str, Any]) -> Dict[str, Any]:
    symbol = str(item.get("symbol") or item.get("pair") or "").upper().strip()
    family = str(item.get("family") or item.get("strategy_family") or "").strip()
    side = str(item.get("side") or "LONG").upper().strip()
    interval = str(item.get("interval") or item.get("timeframe") or item.get("tf") or "15").strip()
    tv_pf = item.get("tv_pf", item.get("tradingview_pf", item.get("profit_factor", item.get("pf"))))
    tv_trades = item.get("tv_trades", item.get("trades", item.get("total_trades")))
    tv_win_rate = item.get("tv_win_rate", item.get("win_rate", item.get("profitable_trades")))
    tv_max_dd = item.get("tv_max_drawdown", item.get("max_drawdown", item.get("max_dd")))
    tv_net_pnl = item.get("tv_net_pnl", item.get("net_pnl", item.get("net_profit")))
    decision = str(item.get("decision") or "").upper().strip()
    pf = v8_float(tv_pf, None)
    trades = v8_int(tv_trades, 0)
    if not decision:
        if pf is None:
            decision = "UNVALIDATED"
        elif trades < DISCOVERY_TV_MIN_TRADES:
            decision = "TV_THIN_SAMPLE"
        elif pf >= DISCOVERY_TV_CONFIRM_PF:
            decision = "TV_CONFIRMED"
        elif pf >= DISCOVERY_TV_WATCH_PF:
            decision = "TV_WATCH"
        else:
            decision = "TV_REJECTED"
    row = {
        "key": discovery_key(symbol, family, side, interval),
        "symbol": symbol,
        "family": family,
        "side": side,
        "interval": interval,
        "tv_pf": pf,
        "tv_trades": trades,
        "tv_win_rate": v8_float(tv_win_rate, None),
        "tv_max_drawdown": v8_float(tv_max_dd, None),
        "tv_net_pnl": v8_float(tv_net_pnl, None),
        "decision": decision,
        "reason": str(item.get("reason") or ""),
        "source": str(item.get("source") or "manual_tradingview"),
        "updated_at": now_iso(),
        "raw": item.get("raw", item),
    }
    return row


def discovery_quality_for_row(row: Dict[str, Any], validation: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    pf = v8_float(row.get("profit_factor"), None)
    trades = v8_int(row.get("trade_count"), 0)
    score = v8_float(row.get("current_score"), 0.0)
    avg_r = v8_float(row.get("average_r"), 0.0)
    no_loss_sample = bool(pf is not None and pf >= DISCOVERY_NO_LOSS_PF_SENTINEL)

    quality = "GOOD"
    flags = []
    if no_loss_sample and trades < DISCOVERY_THIN_SAMPLE_TRADES:
        quality = "UNRELIABLE"
        flags.append("NO_LOSS_SAMPLE_TOO_THIN")
    elif trades < DISCOVERY_THIN_SAMPLE_TRADES:
        quality = "THIN_SAMPLE"
        flags.append(f"THIN_SAMPLE_{trades}_LT_{DISCOVERY_THIN_SAMPLE_TRADES}")
    elif pf is not None and pf >= 1.20 and trades >= 20 and score >= 65:
        quality = "STRONG"
    elif pf is not None and pf >= 1.05 and trades >= 10:
        quality = "GOOD"
    else:
        quality = "WEAK"

    tv_status = "UNVALIDATED"
    tv_pf = None
    tv_trades = None
    if validation:
        tv_status = str(validation.get("decision") or "UNVALIDATED")
        tv_pf = validation.get("tv_pf")
        tv_trades = validation.get("tv_trades")
        if tv_status == "TV_REJECTED":
            quality = "TV_REJECTED"
            flags.append("TV_VALIDATION_REJECTED")
        elif tv_status == "TV_CONFIRMED":
            quality = "TV_CONFIRMED"
            flags.append("TV_VALIDATION_CONFIRMED")
        elif tv_status == "TV_WATCH":
            quality = "TV_WATCH"
            flags.append("TV_VALIDATION_WATCH")
        elif tv_status == "TV_THIN_SAMPLE":
            flags.append("TV_THIN_SAMPLE")

    # Conservative ranking score: caps distorted PF and penalizes thin/no-loss samples.
    pf_for_score = 0.0 if pf is None else min(float(pf), 5.0)
    trade_factor = min(trades / 20.0, 1.5)
    rank_score = (pf_for_score * 18.0) + (score * 0.8) + (avg_r * 20.0) + (trade_factor * 12.0)
    if no_loss_sample:
        rank_score -= 40.0
    if trades < DISCOVERY_THIN_SAMPLE_TRADES:
        rank_score -= 25.0
    if quality == "TV_REJECTED":
        rank_score -= 100.0
    if quality == "TV_CONFIRMED":
        rank_score += 40.0
    if quality == "TV_WATCH":
        rank_score += 15.0

    return {
        "quality": quality,
        "quality_flags": flags,
        "no_loss_sample": no_loss_sample,
        "rank_score": round(rank_score, 4),
        "tv_status": tv_status,
        "tv_pf": tv_pf,
        "tv_trades": tv_trades,
    }


# Override v8.4.2 builder with calibrated v8.4.4 ranking/validation-aware builder.
def build_discovery_candidate_plan(max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, force_backtest: bool = False, include_rejected: bool = False) -> Dict[str, Any]:
    data = read_json_file(MINI_BACKTEST_RESULTS_FILE, {})
    if force_backtest or not data or not data.get("rows"):
        data = run_python_mini_backtests(max_symbols=max_symbols, interval=interval)

    validations = load_discovery_validations().get("by_key", {})
    state = load_state()
    active_symbols = set()
    active_strategy_symbol = set()
    for strat, scfg in (state.get("strategies") or {}).items():
        for sym, symcfg in (scfg.get("symbols") or {}).items():
            for side, sidecfg in (symcfg or {}).items():
                if str((sidecfg or {}).get("mode", "OFF")).upper() != "OFF":
                    active_symbols.add(str(sym).upper())
                    active_strategy_symbol.add((strat, str(sym).upper(), str(side).upper()))

    rows = []
    counts: Dict[str, int] = {}
    quality_counts: Dict[str, int] = {}
    tv_counts: Dict[str, int] = {}
    for row in data.get("rows") or []:
        sym = str(row.get("symbol", "")).upper()
        fam = str(row.get("family", ""))
        val = validations.get(discovery_key(sym, fam, "LONG", interval)) or validations.get(discovery_key(sym, fam, "LONG", "15"))
        q = discovery_quality_for_row(row, val)
        cls = classify_discovery_row(row)
        bucket = cls["bucket"]

        if q["quality"] == "UNRELIABLE" and bucket in {"CANDIDATE", "STRONG_CANDIDATE"}:
            bucket = "NEAR_MISS"
            cls["action"] = "IGNORE_UNTIL_MORE_TRADES"
            cls["reject_reason"] = "NO_LOSS_SAMPLE_THIN_SAMPLE"
        if q["quality"] == "TV_REJECTED":
            bucket = "TV_REJECTED"
            cls["action"] = "DO_NOT_ADD_TO_PAPER"
            cls["reject_reason"] = "REJECTED_BY_TRADINGVIEW_VALIDATION"
        elif q["quality"] == "TV_CONFIRMED":
            bucket = "TV_CONFIRMED"
            cls["action"] = "REVIEW_FOR_PAPER_OR_CANDIDATE_SCRIPT"
            cls["reject_reason"] = "CONFIRMED_BY_TRADINGVIEW"
        elif q["quality"] == "TV_WATCH" and bucket in {"CANDIDATE", "WATCHLIST", "NEAR_MISS"}:
            bucket = "TV_WATCH"
            cls["action"] = "WATCH_AND_RETEST"
            cls["reject_reason"] = "WATCH_BY_TRADINGVIEW_VALIDATION"

        counts[bucket] = counts.get(bucket, 0) + 1
        quality_counts[q["quality"]] = quality_counts.get(q["quality"], 0) + 1
        tv_counts[q["tv_status"]] = tv_counts.get(q["tv_status"], 0) + 1
        if bucket in {"REJECTED", "TV_REJECTED"} and not include_rejected:
            # Keep TV_REJECTED out of the main dashboard by default; visible through registry/top endpoint.
            continue

        strategy_name = f"auto_{fam}_{sym.lower()}_v1"
        is_active = (strategy_name, sym, "LONG") in active_strategy_symbol or sym in active_symbols
        rows.append({
            "symbol": sym,
            "family": fam,
            "strategy_suggestion": strategy_name,
            "side": "LONG",
            "bucket": bucket,
            "quality": q["quality"],
            "quality_flags": q["quality_flags"],
            "rank_score": q["rank_score"],
            "tv_status": q["tv_status"],
            "tv_pf": q["tv_pf"],
            "tv_trades": q["tv_trades"],
            "action": "ALREADY_ACTIVE_OR_SYMBOL_ACTIVE" if is_active and bucket in {"STRONG_CANDIDATE", "CANDIDATE", "WATCHLIST", "TV_CONFIRMED", "TV_WATCH"} else cls["action"],
            "reject_reason": cls["reject_reason"],
            "strict_candidate": cls["strict_candidate"],
            "strict_failures": cls["strict_failures"],
            "profit_factor": row.get("profit_factor"),
            "trade_count": row.get("trade_count"),
            "win_rate": row.get("win_rate"),
            "average_r": row.get("average_r"),
            "total_r": row.get("total_r"),
            "current_score": row.get("current_score"),
            "current_recommendation": row.get("current_recommendation"),
            "signal_now": row.get("signal_now"),
            "turnover24h": row.get("turnover24h"),
            "liquidity_score": row.get("liquidity_score"),
            "already_active_symbol": sym in active_symbols,
            "requires_approval": True,
            "validation": val,
        })

    bucket_rank = {"TV_CONFIRMED": 7, "STRONG_CANDIDATE": 6, "CANDIDATE": 5, "TV_WATCH": 4, "WATCHLIST": 3, "NEAR_MISS": 2, "TV_REJECTED": 1, "REJECTED": 0}
    rows.sort(key=lambda x: (bucket_rank.get(x.get("bucket"), 0), v8_float(x.get("rank_score"), 0), v8_int(x.get("trade_count"), 0)), reverse=True)
    summary = {
        "strict_thresholds": {"min_score": AUTO_PAPER_CANDIDATE_MIN_SCORE, "min_pf": AUTO_PAPER_CANDIDATE_MIN_PF, "min_trades": AUTO_PAPER_CANDIDATE_MIN_TRADES},
        "discovery_thresholds": {"min_score": DISCOVERY_MIN_SCORE, "min_pf": DISCOVERY_MIN_PF, "min_trades": DISCOVERY_MIN_TRADES},
        "tv_validation_thresholds": {"tv_confirm_pf": DISCOVERY_TV_CONFIRM_PF, "tv_watch_pf": DISCOVERY_TV_WATCH_PF, "tv_min_trades": DISCOVERY_TV_MIN_TRADES},
        "bucket_counts": counts,
        "quality_counts": quality_counts,
        "tv_status_counts": tv_counts,
        "strong_or_candidate": counts.get("STRONG_CANDIDATE", 0) + counts.get("CANDIDATE", 0) + counts.get("TV_CONFIRMED", 0),
        "watchlist_or_near_miss": counts.get("WATCHLIST", 0) + counts.get("NEAR_MISS", 0) + counts.get("TV_WATCH", 0),
    }
    return {"ok": True, "version": APP_FEATURE_LEVEL, "created_at": now_iso(), "interval": interval, "max_symbols": max_symbols, "count": len(rows[:DISCOVERY_TOP_N]), "total_rows_evaluated": len(data.get("rows") or []), "validation_rows": len(validations), "summary": summary, "items": rows[:DISCOVERY_TOP_N]}


@app.post("/discovery_validation_import")
async def discovery_validation_import(request: Request):
    body = await request.json()
    verify_secret(request, body)
    mode = str(body.get("mode", "upsert")).lower()
    items = body.get("items") or body.get("rows") or []
    if not isinstance(items, list):
        raise HTTPException(400, "items/rows must be a list")
    normalized = [normalize_discovery_validation_row(x) for x in items if isinstance(x, dict)]
    if not normalized:
        raise HTTPException(400, "No valid validation rows found")
    existing = [] if mode == "replace" else load_discovery_validations().get("rows", [])
    if mode not in {"upsert", "replace"}:
        raise HTTPException(400, "mode must be 'upsert' or 'replace'")
    save_discovery_validations(existing + normalized)
    final_rows = load_discovery_validations().get("rows", [])
    return {"ok": True, "mode": mode, "imported": len(normalized), "total_registry_rows": len(final_rows), "rows": normalized, "registry": final_rows}


@app.post("/discovery_validation_seed_recent")
async def discovery_validation_seed_recent(request: Request):
    body = await request.json()
    verify_secret(request, body)
    rows = [
        {"symbol": "NEARUSDT", "family": "trend_continuation", "side": "LONG", "interval": "15", "tv_pf": 1.085, "tv_trades": 132, "tv_win_rate": 47.73, "decision": "TV_REJECTED", "reason": "Manual TradingView validation 2026-01-01 to 2026-05-25: PF below 1.20"},
        {"symbol": "GRASSUSDT", "family": "trend_pullback", "side": "LONG", "interval": "15", "tv_pf": 0.936, "tv_trades": 46, "tv_win_rate": 41.30, "decision": "TV_REJECTED", "reason": "Manual TradingView validation 2026-01-01 to 2026-05-25: PF below 1.20"},
        {"symbol": "NEARUSDT", "family": "trend_pullback", "side": "LONG", "interval": "15", "tv_pf": 0.398, "tv_trades": 38, "tv_win_rate": 26.32, "decision": "TV_REJECTED", "reason": "Manual TradingView validation 2026-01-01 to 2026-05-25: PF below 1.20"},
    ]
    normalized = [normalize_discovery_validation_row(x) for x in rows]
    existing = load_discovery_validations().get("rows", [])
    save_discovery_validations(existing + normalized)
    final_rows = load_discovery_validations().get("rows", [])
    return {"ok": True, "seeded": len(normalized), "total_registry_rows": len(final_rows), "rows": normalized}


@app.get("/discovery_validation_registry")
def discovery_validation_registry(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = load_discovery_validations()
    return {"ok": True, "version": APP_FEATURE_LEVEL, "count": len(data.get("rows", [])), "rows": data.get("rows", [])}


@app.post("/discovery_validation_clear")
async def discovery_validation_clear(request: Request):
    body = await request.json()
    verify_secret(request, body)
    write_json_file(DISCOVERY_VALIDATION_REGISTRY_FILE, {"rows": [], "updated_at": now_iso()})
    return {"ok": True, "cleared": True}


@app.get("/discovery_top_candidates")
def discovery_top_candidates(secret: str, max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, force_backtest: bool = False, include_rejected: bool = False, min_quality: str = "GOOD"):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = build_discovery_candidate_plan(max_symbols=max_symbols, interval=interval, force_backtest=force_backtest, include_rejected=include_rejected)
    allowed_by_min = {
        "TV_CONFIRMED": {"TV_CONFIRMED"},
        "STRONG": {"TV_CONFIRMED", "STRONG"},
        "GOOD": {"TV_CONFIRMED", "TV_WATCH", "STRONG", "GOOD"},
        "THIN_SAMPLE": {"TV_CONFIRMED", "TV_WATCH", "STRONG", "GOOD", "THIN_SAMPLE"},
        "ALL": None,
    }
    allowed = allowed_by_min.get(str(min_quality).upper(), allowed_by_min["GOOD"])
    rows = data.get("items", [])
    if allowed is not None:
        rows = [r for r in rows if r.get("quality") in allowed]
    return {"ok": True, "version": APP_FEATURE_LEVEL, "count": len(rows), "items": rows, "summary": data.get("summary")}


@app.get("/discovery_quality_dashboard", response_class=HTMLResponse)
def discovery_quality_dashboard(secret: str, max_symbols: int = MINI_BACKTEST_MAX_SYMBOLS, interval: str = SCANNER_DEFAULT_INTERVAL, force_backtest: bool = False, include_rejected: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = build_discovery_candidate_plan(max_symbols=max_symbols, interval=interval, force_backtest=force_backtest, include_rejected=include_rejected)
    rows = "".join([
        f"<tr><td>{h(x.get('bucket'))}</td><td>{h(x.get('quality'))}</td><td>{fmt_num(x.get('rank_score'))}</td><td>{h(x.get('symbol'))}</td><td>{h(x.get('family'))}</td><td>{fmt_num(x.get('profit_factor'))}</td><td>{x.get('trade_count')}</td><td>{fmt_num(x.get('average_r'))}</td><td>{fmt_num(x.get('current_score'))}</td><td>{h(x.get('tv_status'))}</td><td>{fmt_num(x.get('tv_pf'))}</td><td>{x.get('tv_trades') or ''}</td><td>{h('; '.join(x.get('quality_flags') or []))}</td><td>{h(x.get('action'))}</td></tr>"
        for x in data.get("items", [])
    ])
    s = data.get("summary", {})
    return HTMLResponse(f"""
    <html><head><title>Discovery Quality Dashboard</title><style>body{{font-family:Arial;margin:20px;background:#f6f8fb}} .card{{background:white;border-radius:12px;padding:14px;margin-bottom:14px;box-shadow:0 1px 6px #d1d5db}} table{{border-collapse:collapse;width:100%;background:white;font-size:13px}} th{{background:#111827;color:white}} td,th{{padding:7px;border-bottom:1px solid #ddd;text-align:left}} .TV_REJECTED{{color:#991b1b;font-weight:bold}} .TV_CONFIRMED{{color:#047857;font-weight:bold}} .STRONG{{color:#047857;font-weight:bold}} .UNRELIABLE{{color:#991b1b;font-weight:bold}}</style></head>
    <body><h1>Discovery Quality Dashboard v8.4.4</h1><div class='card'><b>Interval:</b> {h(interval)} | <b>Rows:</b> {data.get('total_rows_evaluated')} | <b>Shown:</b> {data.get('count')} | <b>Validation rows:</b> {data.get('validation_rows')}<br><b>Bucket counts:</b> {h(json.dumps(s.get('bucket_counts', {}), ensure_ascii=False))}<br><b>Quality counts:</b> {h(json.dumps(s.get('quality_counts', {}), ensure_ascii=False))}<br><b>TV status counts:</b> {h(json.dumps(s.get('tv_status_counts', {}), ensure_ascii=False))}</div>
    <table><tr><th>Bucket</th><th>Quality</th><th>Rank</th><th>Symbol</th><th>Family</th><th>Python PF</th><th>Py Trades</th><th>Avg R</th><th>Score</th><th>TV Status</th><th>TV PF</th><th>TV Trades</th><th>Quality flags</th><th>Action</th></tr>{rows}</table>
    <p><a href='/discovery_top_candidates?secret={h(secret)}&max_symbols={max_symbols}&interval={h(interval)}'>Top JSON</a> · <a href='/discovery_validation_registry?secret={h(secret)}'>Validation registry</a> · <a href='/discovery_candidate_dashboard?secret={h(secret)}&max_symbols={max_symbols}&interval={h(interval)}'>Classic discovery</a></p></body></html>
    """)


# ============================================================
# v9.0.0 - MULTI-MARKET RESEARCH FRAMEWORK
# ============================================================
# Purpose:
# - keep crypto execution/risk engine intact
# - add research-only higher timeframe crypto scans/backtests
# - add optional forex/ETF research branch through public Yahoo Finance chart data
# - add external TradingView/CSV result registry for manual validation
# - provide a combined dashboard to compare crypto 15m/1h/4h, forex and ETF candidates
# Safety:
# - no direct orders
# - no automatic promotion from v9 endpoints
# - all results are RESEARCH_ONLY / PAPER_REVIEW_ONLY

V9_RESEARCH_ENABLED = os.getenv("V9_RESEARCH_ENABLED", "true").lower() == "true"
V9_CRYPTO_HTF_INTERVALS = os.getenv("V9_CRYPTO_HTF_INTERVALS", "60,240")
V9_CRYPTO_HTF_MAX_SYMBOLS = int(os.getenv("V9_CRYPTO_HTF_MAX_SYMBOLS", "25"))
V9_CRYPTO_HTF_KLINE_LIMIT = int(os.getenv("V9_CRYPTO_HTF_KLINE_LIMIT", "1000"))
V9_EXTERNAL_ENABLED = os.getenv("V9_EXTERNAL_ENABLED", "true").lower() == "true"
V9_EXTERNAL_DEFAULT_RANGE = os.getenv("V9_EXTERNAL_DEFAULT_RANGE", "6mo")
V9_EXTERNAL_DEFAULT_INTERVAL = os.getenv("V9_EXTERNAL_DEFAULT_INTERVAL", "60")
V9_EXTERNAL_MAX_TICKERS = int(os.getenv("V9_EXTERNAL_MAX_TICKERS", "20"))
V9_YAHOO_QUERY_BASES = [x.strip().rstrip("/") for x in os.getenv("V9_YAHOO_QUERY_BASES", "https://query1.finance.yahoo.com,https://query2.finance.yahoo.com").split(",") if x.strip()]
V9_YAHOO_USER_AGENT = os.getenv("V9_YAHOO_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36")
V9_FOREX_TICKERS = [x.strip() for x in os.getenv("V9_FOREX_TICKERS", "EURUSD=X,GBPUSD=X,USDJPY=X,AUDUSD=X,USDCAD=X,USDCHF=X,EURJPY=X,GBPJPY=X").split(",") if x.strip()]
V9_ETF_TICKERS = [x.strip() for x in os.getenv("V9_ETF_TICKERS", "SPY,QQQ,IWM,DIA,TLT,GLD,SLV,USO").split(",") if x.strip()]
V9_RESEARCH_MIN_PF = float(os.getenv("V9_RESEARCH_MIN_PF", "1.15"))
V9_RESEARCH_MIN_TRADES = int(os.getenv("V9_RESEARCH_MIN_TRADES", "10"))
V9_RESEARCH_TOP_N = int(os.getenv("V9_RESEARCH_TOP_N", "30"))

V9_CRYPTO_HTF_RESEARCH_FILE = APP_DIR / "v9_crypto_htf_research.json"
V9_EXTERNAL_RESEARCH_FILE = APP_DIR / "v9_external_research.json"
V9_EXTERNAL_BACKTEST_REGISTRY_FILE = APP_DIR / "v9_external_backtest_registry.json"


def v9_parse_interval_list(value: str) -> list:
    out = []
    for part in str(value or "").replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            n = int(float(part))
            if n > 0:
                out.append(str(n))
        except Exception:
            pass
    return out or ["60", "240"]


def v9_market_catalog() -> Dict[str, Any]:
    return {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "mode": "RESEARCH_ONLY",
        "markets": [
            {
                "market": "crypto_bybit_linear",
                "status": "ACTIVE",
                "data_source": "Bybit public market endpoints",
                "execution_supported": True,
                "research_timeframes": v9_parse_interval_list(V9_CRYPTO_HTF_INTERVALS),
                "notes": "Execution remains controlled by strategy_state, risk engine and approval workflow.",
            },
            {
                "market": "forex_major_yahoo",
                "status": "RESEARCH_ONLY",
                "data_source": "Yahoo Finance chart API, tickers such as EURUSD=X",
                "execution_supported": False,
                "research_timeframes": ["60", "240"],
                "notes": "No broker execution integration. Use for proof-of-concept only.",
            },
            {
                "market": "etf_yahoo",
                "status": "RESEARCH_ONLY",
                "data_source": "Yahoo Finance chart API, tickers such as SPY/QQQ",
                "execution_supported": False,
                "research_timeframes": ["60", "240", "1D"],
                "notes": "No broker execution integration. Useful for swing / capital-growth research.",
            },
        ],
        "safety_rules": [
            "v9 endpoints do not send orders",
            "AI/research output may suggest watchlists only",
            "promotion to MICRO still requires existing promotion/risk rules",
            "forex/ETF are research-only until a regulated broker/API integration is explicitly added",
        ],
    }


def v9_research_label(row: Dict[str, Any]) -> str:
    pf = row.get("profit_factor")
    trades = v8_int(row.get("trade_count"), 0)
    avg_r = v8_float(row.get("average_r"), 0.0)
    if pf is None:
        return "NO_PF"
    try:
        pfv = float(pf)
    except Exception:
        return "NO_PF"
    if trades < max(5, V9_RESEARCH_MIN_TRADES):
        return "THIN_SAMPLE"
    if pfv >= 1.4 and trades >= 20 and avg_r > 0:
        return "STRONG_RESEARCH_CANDIDATE"
    if pfv >= V9_RESEARCH_MIN_PF and trades >= V9_RESEARCH_MIN_TRADES and avg_r > 0:
        return "RESEARCH_CANDIDATE"
    if pfv >= 1.0 and avg_r >= 0:
        return "WATCHLIST"
    return "REJECT"


def v9_quality_rank(row: Dict[str, Any]) -> float:
    pf = row.get("profit_factor")
    pf_score = min(60.0, max(0.0, (v8_float(pf, 0.0) - 1.0) * 80.0)) if pf is not None else 0.0
    trades = v8_int(row.get("trade_count"), 0)
    trade_score = min(25.0, trades / 2.0)
    avg_score = max(-20.0, min(20.0, v8_float(row.get("average_r"), 0.0) * 20.0))
    current_score = min(15.0, max(0.0, v8_float(row.get("current_score"), 0.0) / 8.0))
    return round(pf_score + trade_score + avg_score + current_score, 4)


def v9_crypto_higher_tf_research(max_symbols: int = V9_CRYPTO_HTF_MAX_SYMBOLS, intervals: Optional[str] = None, force: bool = False) -> Dict[str, Any]:
    if not V9_RESEARCH_ENABLED:
        return {"ok": False, "reason": "V9_RESEARCH_DISABLED"}
    interval_list = v9_parse_interval_list(intervals or V9_CRYPTO_HTF_INTERVALS)
    cached = read_json_file(V9_CRYPTO_HTF_RESEARCH_FILE, {})
    if cached and not force and cached.get("max_symbols") == max_symbols and cached.get("intervals") == interval_list:
        age = v8_now_ts() - v8_int(cached.get("created_ts"), 0)
        if age < max(900, UNIVERSE_CACHE_TTL_SEC):
            return cached
    rows = []
    for interval in interval_list:
        bt = run_python_mini_backtests(max_symbols=max_symbols, interval=interval, kline_limit=V9_CRYPTO_HTF_KLINE_LIMIT)
        for r in bt.get("rows") or []:
            row = dict(r)
            row["market"] = "crypto_bybit_linear"
            row["timeframe"] = interval
            row["research_label"] = v9_research_label(row)
            row["rank_score"] = v9_quality_rank(row)
            row["execution_supported"] = True
            row["research_only"] = True
            rows.append(row)
    rows.sort(key=lambda x: (x.get("research_label") in {"STRONG_RESEARCH_CANDIDATE", "RESEARCH_CANDIDATE"}, x.get("rank_score") or 0), reverse=True)
    summary = {
        "total": len(rows),
        "by_label": {},
        "by_timeframe": {},
    }
    for r in rows:
        summary["by_label"][r.get("research_label")] = summary["by_label"].get(r.get("research_label"), 0) + 1
        summary["by_timeframe"][str(r.get("timeframe"))] = summary["by_timeframe"].get(str(r.get("timeframe")), 0) + 1
    result = {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "created_at": now_iso(),
        "created_ts": v8_now_ts(),
        "market": "crypto_bybit_linear",
        "max_symbols": max_symbols,
        "intervals": interval_list,
        "summary": summary,
        "count": len(rows),
        "top": rows[:V9_RESEARCH_TOP_N],
        "rows": rows,
    }
    write_json_file(V9_CRYPTO_HTF_RESEARCH_FILE, result)
    return result


def v9_yahoo_interval(interval: str) -> Tuple[str, int]:
    s = str(interval or "60").strip().upper()
    if s in {"1D", "D", "1440"}:
        return "1d", 1440
    try:
        minutes = int(float(s))
    except Exception:
        minutes = 60
    if minutes <= 5:
        return "5m", 5
    if minutes <= 15:
        return "15m", 15
    if minutes <= 30:
        return "30m", 30
    # Yahoo supports 60m; 240m is created by aggregation below.
    return "60m", 60


def v9_extract_yahoo_candles(data: Dict[str, Any]) -> Tuple[list, Optional[Dict[str, Any]]]:
    chart = data.get("chart") or {}
    chart_error = chart.get("error")
    results = chart.get("result") or []
    if not results:
        return [], chart_error
    result = results[0] or {}
    ts = result.get("timestamp") or []
    quote_rows = (result.get("indicators") or {}).get("quote") or []
    quote_row = quote_rows[0] if quote_rows else {}
    opens = quote_row.get("open") or []
    highs = quote_row.get("high") or []
    lows = quote_row.get("low") or []
    closes = quote_row.get("close") or []
    volumes = quote_row.get("volume") or []
    candles = []
    for i, t in enumerate(ts):
        try:
            if i >= len(opens) or i >= len(highs) or i >= len(lows) or i >= len(closes):
                continue
            o, h_, l, c = opens[i], highs[i], lows[i], closes[i]
            if o is None or h_ is None or l is None or c is None:
                continue
            candles.append({
                "start_ms": int(t) * 1000,
                "open": float(o),
                "high": float(h_),
                "low": float(l),
                "close": float(c),
                "volume": float(volumes[i] or 0.0) if i < len(volumes) else 0.0,
                "turnover": 0.0,
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["start_ms"])
    return candles, chart_error


def v9_fetch_yahoo_candles_diagnostics(ticker: str, interval: str = V9_EXTERNAL_DEFAULT_INTERVAL, range_: str = V9_EXTERNAL_DEFAULT_RANGE) -> Dict[str, Any]:
    yahoo_interval, base_minutes = v9_yahoo_interval(interval)
    requested_minutes = 1440 if str(interval).upper() in {"1D", "D", "1440"} else v8_int(interval, 60)
    ticker_encoded = quote(str(ticker), safe="")
    params = {
        "range": str(range_),
        "interval": yahoo_interval,
        "includePrePost": "false",
        "events": "history",
    }
    headers = {
        "User-Agent": V9_YAHOO_USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }
    attempts = []
    for base in V9_YAHOO_QUERY_BASES:
        url = f"{base}/v8/finance/chart/{ticker_encoded}"
        attempt = {"base": base, "ticker": ticker, "interval": yahoo_interval, "range": str(range_)}
        try:
            resp = client.get(url, params=params, headers=headers, follow_redirects=True)
            attempt["status_code"] = resp.status_code
            attempt["content_type"] = resp.headers.get("content-type")
            if resp.status_code >= 400:
                attempt["error"] = f"HTTP_{resp.status_code}"
                attempt["body_preview"] = resp.text[:300]
                attempts.append(attempt)
                continue
            try:
                payload = resp.json()
            except Exception as exc:
                attempt["error"] = f"JSON_DECODE_ERROR_{type(exc).__name__}"
                attempt["body_preview"] = resp.text[:300]
                attempts.append(attempt)
                continue
            candles, chart_error = v9_extract_yahoo_candles(payload)
            attempt["raw_candle_count"] = len(candles)
            if chart_error:
                attempt["chart_error"] = chart_error
            if requested_minutes > base_minutes and base_minutes > 0 and requested_minutes % base_minutes == 0:
                factor = requested_minutes // base_minutes
                if factor > 1:
                    candles = v9_aggregate_candles(candles, factor)
                    attempt["aggregation_factor"] = factor
            attempt["final_candle_count"] = len(candles)
            attempts.append(attempt)
            if candles:
                return {
                    "ok": True,
                    "ticker": ticker,
                    "requested_interval": str(interval),
                    "yahoo_interval": yahoo_interval,
                    "range": str(range_),
                    "candle_count": len(candles),
                    "candles": candles,
                    "attempts": attempts,
                }
        except Exception as exc:
            attempt["error"] = f"{type(exc).__name__}: {str(exc)}"
            attempts.append(attempt)
    return {
        "ok": False,
        "ticker": ticker,
        "requested_interval": str(interval),
        "yahoo_interval": yahoo_interval,
        "range": str(range_),
        "candle_count": 0,
        "candles": [],
        "attempts": attempts,
        "reason": "YAHOO_DATA_UNAVAILABLE",
    }


def v9_fetch_yahoo_candles(ticker: str, interval: str = V9_EXTERNAL_DEFAULT_INTERVAL, range_: str = V9_EXTERNAL_DEFAULT_RANGE) -> list:
    return v9_fetch_yahoo_candles_diagnostics(ticker=ticker, interval=interval, range_=range_).get("candles") or []


def v9_aggregate_candles(candles: list, factor: int) -> list:
    out = []
    if factor <= 1:
        return candles
    for i in range(0, len(candles), factor):
        chunk = candles[i:i + factor]
        if len(chunk) < factor:
            continue
        out.append({
            "start_ms": chunk[0]["start_ms"],
            "open": chunk[0]["open"],
            "high": max(x["high"] for x in chunk),
            "low": min(x["low"] for x in chunk),
            "close": chunk[-1]["close"],
            "volume": sum(x.get("volume") or 0.0 for x in chunk),
            "turnover": 0.0,
        })
    return out


def v9_external_ticker_list(market: str, tickers: Optional[str]) -> list:
    if tickers:
        return [x.strip() for x in str(tickers).split(",") if x.strip()][:V9_EXTERNAL_MAX_TICKERS]
    market_l = str(market or "forex").lower()
    if market_l == "etf":
        return V9_ETF_TICKERS[:V9_EXTERNAL_MAX_TICKERS]
    if market_l == "all":
        return (V9_FOREX_TICKERS + V9_ETF_TICKERS)[:V9_EXTERNAL_MAX_TICKERS]
    return V9_FOREX_TICKERS[:V9_EXTERNAL_MAX_TICKERS]


def v9_external_market_research(market: str = "forex", tickers: Optional[str] = None, interval: str = V9_EXTERNAL_DEFAULT_INTERVAL, range_: str = V9_EXTERNAL_DEFAULT_RANGE, families: Optional[list] = None) -> Dict[str, Any]:
    if not V9_EXTERNAL_ENABLED:
        return {"ok": False, "reason": "V9_EXTERNAL_DISABLED"}
    ticker_list = v9_external_ticker_list(market, tickers)
    families = families or V8_STRATEGY_FAMILIES
    rows = []
    for ticker in ticker_list:
        candles = v9_fetch_yahoo_candles(ticker, interval=interval, range_=range_)
        if len(candles) < 230:
            rows.append({"market": market, "symbol": ticker, "timeframe": interval, "ok": False, "reason": f"INSUFFICIENT_CANDLES_{len(candles)}"})
            continue
        for fam in families:
            bt = run_strategy_mini_backtest(candles, fam)
            score_current = score_current_opportunity(candles, fam)
            row = {
                "ok": True,
                "market": str(market).lower(),
                "symbol": ticker,
                "timeframe": interval,
                "family": fam,
                "profit_factor": bt.get("profit_factor") if bt.get("ok") else None,
                "trade_count": bt.get("trade_count") if bt.get("ok") else 0,
                "win_rate": bt.get("win_rate") if bt.get("ok") else None,
                "total_r": bt.get("total_r") if bt.get("ok") else None,
                "average_r": bt.get("average_r") if bt.get("ok") else None,
                "current_score": score_current.get("score") if score_current.get("ok") else None,
                "current_recommendation": score_current.get("recommendation") if score_current.get("ok") else None,
                "signal_now": score_current.get("signal_now") if score_current.get("ok") else False,
                "execution_supported": False,
                "research_only": True,
                "data_source": "yahoo_chart_api",
            }
            row["research_label"] = v9_research_label(row)
            row["rank_score"] = v9_quality_rank(row)
            rows.append(row)
    rows.sort(key=lambda x: (x.get("research_label") in {"STRONG_RESEARCH_CANDIDATE", "RESEARCH_CANDIDATE"}, x.get("rank_score") or 0), reverse=True)
    summary = {"total": len(rows), "by_label": {}, "failed": len([r for r in rows if not r.get("ok", True)])}
    for r in rows:
        summary["by_label"][r.get("research_label") or r.get("reason") or "UNKNOWN"] = summary["by_label"].get(r.get("research_label") or r.get("reason") or "UNKNOWN", 0) + 1
    result = {"ok": True, "version": APP_FEATURE_LEVEL, "created_at": now_iso(), "market": market, "interval": interval, "range": range_, "summary": summary, "count": len(rows), "top": rows[:V9_RESEARCH_TOP_N], "rows": rows}
    write_json_file(V9_EXTERNAL_RESEARCH_FILE, result)
    return result


def v9_normalize_external_backtest_row(x: Dict[str, Any]) -> Dict[str, Any]:
    market = str(x.get("market") or x.get("asset_class") or "external").lower()
    symbol = str(x.get("symbol") or x.get("ticker") or "").upper()
    family = str(x.get("family") or x.get("strategy_family") or x.get("strategy") or "manual").lower()
    side = str(x.get("side") or "LONG").upper()
    interval = str(x.get("interval") or x.get("timeframe") or "60")
    return {
        "market": market,
        "symbol": symbol,
        "family": family,
        "side": side,
        "interval": interval,
        "profit_factor": v8_float(x.get("profit_factor") if x.get("profit_factor") is not None else x.get("pf"), 0.0),
        "trades": v8_int(x.get("trades") if x.get("trades") is not None else x.get("trade_count"), 0),
        "win_rate": v8_float(x.get("win_rate"), 0.0),
        "max_drawdown": v8_float(x.get("max_drawdown"), 0.0),
        "net_profit": v8_float(x.get("net_profit"), 0.0),
        "date_from": str(x.get("date_from") or ""),
        "date_to": str(x.get("date_to") or ""),
        "decision": str(x.get("decision") or "UNVALIDATED").upper(),
        "source": str(x.get("source") or "manual"),
        "notes": str(x.get("notes") or ""),
        "updated_at": now_iso(),
    }


def v9_load_external_registry() -> Dict[str, Any]:
    return read_json_file(V9_EXTERNAL_BACKTEST_REGISTRY_FILE, {"rows": [], "updated_at": now_iso()})


def v9_save_external_registry(rows: list) -> None:
    dedup = {}
    for r in rows:
        key = (str(r.get("market")), str(r.get("symbol")), str(r.get("family")), str(r.get("side")), str(r.get("interval")))
        dedup[key] = r
    write_json_file(V9_EXTERNAL_BACKTEST_REGISTRY_FILE, {"rows": list(dedup.values()), "updated_at": now_iso()})


def _finite_or_default(value: Any, default: float = 0.0) -> float:
    try:
        num = float(value)
        return num if math.isfinite(num) else default
    except Exception:
        return default


def _json_safe_value(value: Any) -> Any:
    """Recursively sanitize NaN/Infinity and unknown objects before JSON serialization."""
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe_value(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if value is None or isinstance(value, (str, int, bool)):
        return value
    try:
        return str(value)
    except Exception:
        return None


def _safe_market_context(symbol: str, interval: str = "60", limit: int = 300) -> Dict[str, Any]:
    """Best-effort market context. The regime gate must never fail because public market data is unavailable."""
    try:
        candles = fetch_bybit_klines(symbol, interval=interval, limit=limit)
        if not candles:
            return {
                "ok": False,
                "symbol": symbol,
                "interval": interval,
                "reason": "BYBIT_KLINES_EMPTY_OR_UNAVAILABLE",
                "score": 0.0,
            }
        result = score_current_opportunity(candles, "trend_continuation")
        if not isinstance(result, dict):
            return {
                "ok": False,
                "symbol": symbol,
                "interval": interval,
                "reason": "INVALID_OPPORTUNITY_RESULT",
                "score": 0.0,
            }
        result = dict(result)
        result["symbol"] = symbol
        result["interval"] = interval
        result["score"] = _finite_or_default(result.get("score"), 0.0)
        return _json_safe_value(result)
    except Exception as exc:
        log(f"[WARN] v9 market context fallback for {symbol}: {exc}")
        return {
            "ok": False,
            "symbol": symbol,
            "interval": interval,
            "reason": "MARKET_CONTEXT_EXCEPTION",
            "error": str(exc),
            "score": 0.0,
        }


def v9_market_regime_gate(days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS) -> Dict[str, Any]:
    safe_days = max(1, min(v8_int(days, PAPER_OUTCOME_DEFAULT_DAYS), 90))
    safe_limit = max(1, min(v8_int(limit, PAPER_OUTCOME_MAX_EVENTS), 10000))
    degraded_reasons: list[str] = []

    try:
        paper = build_paper_outcome_decision_report(days=safe_days, limit=safe_limit)
        if not isinstance(paper, dict):
            paper = {"summary": {}}
            degraded_reasons.append("PAPER_OUTCOME_INVALID_RESPONSE")
    except Exception as exc:
        log(f"[WARN] v9 market regime paper fallback: {exc}")
        paper = {"summary": {}}
        degraded_reasons.append(f"PAPER_OUTCOME_UNAVAILABLE:{exc}")

    try:
        risk_report = build_ai_risk_supervisor_report(days=safe_days, limit=safe_limit, include_plan=False)
        risk = (risk_report or {}).get("risk") or {}
        if not isinstance(risk, dict):
            risk = {}
            degraded_reasons.append("AI_RISK_INVALID_RESPONSE")
    except Exception as exc:
        log(f"[WARN] v9 market regime AI risk fallback: {exc}")
        risk = {}
        degraded_reasons.append(f"AI_RISK_UNAVAILABLE:{exc}")

    btc60 = _safe_market_context("BTCUSDT", interval="60", limit=300)
    eth60 = _safe_market_context("ETHUSDT", interval="60", limit=300)

    paper_summary = paper.get("summary") or {}
    avg_r = _finite_or_default(paper_summary.get("average_r_closed"), 0.0)
    closed = v8_int(paper_summary.get("closed_count"), 0)
    btc_score = _finite_or_default(btc60.get("score"), 0.0)
    eth_score = _finite_or_default(eth60.get("score"), 0.0)
    crypto_trend_score = round((btc_score + eth_score) / 2.0, 2)

    level = "NORMAL"
    reasons: list[str] = []

    if risk.get("level") == "HIGH":
        level = "HIGH"
        reasons.append("AI_RISK_SUPERVISOR_HIGH")

    if closed >= 5 and avg_r <= -0.5:
        level = "HIGH"
        reasons.append(f"PAPER_AVG_R_LOW_{avg_r:.2f}")
    elif closed >= 5 and avg_r <= -0.2 and level != "HIGH":
        level = "ELEVATED"
        reasons.append(f"PAPER_AVG_R_NEGATIVE_{avg_r:.2f}")

    market_data_complete = bool(btc60.get("ok")) and bool(eth60.get("ok"))
    if not market_data_complete:
        if level == "NORMAL":
            level = "CAUTIOUS"
        reasons.append("BTC_ETH_MARKET_CONTEXT_PARTIAL_OR_UNAVAILABLE")

    if crypto_trend_score < 45 and level == "NORMAL":
        level = "CAUTIOUS"
        reasons.append(f"BTC_ETH_TREND_SCORE_LOW_{crypto_trend_score:.1f}")

    if degraded_reasons:
        if level == "NORMAL":
            level = "CAUTIOUS"
        reasons.append("DEGRADED_DATA_MODE")

    allow_new_micro = level in {"LOW", "NORMAL"}

    payload = {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "created_at": now_iso(),
        "gate_level": level,
        "allow_new_micro": allow_new_micro,
        "allow_new_paper": True,
        "recommendation": "Do not promote to MICRO" if not allow_new_micro else "MICRO review allowed if strategy-level rules pass",
        "reasons": reasons or ["NO_BLOCKING_MARKET_REGIME_REASON"],
        "degraded_mode": bool(degraded_reasons) or not market_data_complete,
        "degraded_reasons": degraded_reasons,
        "paper": {
            "closed_count": closed,
            "average_r_closed": avg_r,
            "total_r": _finite_or_default(paper_summary.get("total_r"), 0.0),
        },
        "ai_risk": risk,
        "crypto_context": {
            "btc_1h": btc60,
            "eth_1h": eth60,
            "btc_eth_avg_score": crypto_trend_score,
            "market_data_complete": market_data_complete,
        },
    }
    return _json_safe_value(payload)


@app.get("/v9_market_catalog")
def v9_market_catalog_endpoint(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return v9_market_catalog()


@app.get("/v9_crypto_higher_tf_research")
def v9_crypto_higher_tf_research_endpoint(secret: str, max_symbols: int = V9_CRYPTO_HTF_MAX_SYMBOLS, intervals: str = V9_CRYPTO_HTF_INTERVALS, force: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return v9_crypto_higher_tf_research(max_symbols=max_symbols, intervals=intervals, force=force)


@app.get("/v9_crypto_higher_tf_dashboard", response_class=HTMLResponse)
def v9_crypto_higher_tf_dashboard(secret: str, max_symbols: int = V9_CRYPTO_HTF_MAX_SYMBOLS, intervals: str = V9_CRYPTO_HTF_INTERVALS, force: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = v9_crypto_higher_tf_research(max_symbols=max_symbols, intervals=intervals, force=force)
    rows = "".join([f"<tr><td>{h(x.get('research_label'))}</td><td>{h(x.get('symbol'))}</td><td>{h(x.get('timeframe'))}</td><td>{h(x.get('family'))}</td><td>{fmt_num(x.get('profit_factor'))}</td><td>{x.get('trade_count')}</td><td>{fmt_num(x.get('win_rate'))}</td><td>{fmt_num(x.get('average_r'))}</td><td>{fmt_num(x.get('current_score'))}</td><td>{fmt_num(x.get('rank_score'))}</td></tr>" for x in data.get("top", [])])
    return HTMLResponse(f"""
    <html><head><title>v9 Crypto Higher TF Research</title><style>body{{font-family:Arial;margin:20px;background:#f6f8fb}} .card{{background:white;border-radius:12px;padding:14px;margin-bottom:14px;box-shadow:0 1px 6px #d1d5db}} table{{border-collapse:collapse;width:100%;background:white}} th{{background:#111827;color:white}} td,th{{padding:7px;border-bottom:1px solid #ddd;text-align:left}}</style></head>
    <body><h1>v9 Crypto Higher Timeframe Research</h1><div class='card'><b>Intervals:</b> {h(','.join(data.get('intervals') or []))} | <b>Rows:</b> {data.get('count')}<br><b>Summary:</b> {h(json.dumps(data.get('summary', {}), ensure_ascii=False))}</div><table><tr><th>Label</th><th>Symbol</th><th>TF</th><th>Family</th><th>PF</th><th>Trades</th><th>Win %</th><th>Avg R</th><th>Score</th><th>Rank</th></tr>{rows}</table>
    <p><a href='/v9_multi_market_research_dashboard?secret={h(secret)}'>Combined v9 dashboard</a> · <a href='/v9_market_regime_gate?secret={h(secret)}'>Market regime gate</a></p></body></html>
    """)


@app.get("/v9_external_data_diagnostics")
def v9_external_data_diagnostics_endpoint(secret: str, ticker: str = "SPY", interval: str = V9_EXTERNAL_DEFAULT_INTERVAL, range: str = V9_EXTERNAL_DEFAULT_RANGE):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return v9_fetch_yahoo_candles_diagnostics(ticker=ticker, interval=interval, range_=range)


@app.get("/v9_external_market_research")
def v9_external_market_research_endpoint(secret: str, market: str = "forex", tickers: Optional[str] = None, interval: str = V9_EXTERNAL_DEFAULT_INTERVAL, range: str = V9_EXTERNAL_DEFAULT_RANGE):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return v9_external_market_research(market=market, tickers=tickers, interval=interval, range_=range)


@app.get("/v9_external_market_dashboard", response_class=HTMLResponse)
def v9_external_market_dashboard(secret: str, market: str = "forex", tickers: Optional[str] = None, interval: str = V9_EXTERNAL_DEFAULT_INTERVAL, range: str = V9_EXTERNAL_DEFAULT_RANGE):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = v9_external_market_research(market=market, tickers=tickers, interval=interval, range_=range)
    rows = "".join([f"<tr><td>{h(x.get('research_label') or x.get('reason'))}</td><td>{h(x.get('symbol'))}</td><td>{h(x.get('timeframe'))}</td><td>{h(x.get('family'))}</td><td>{fmt_num(x.get('profit_factor'))}</td><td>{x.get('trade_count') or ''}</td><td>{fmt_num(x.get('win_rate'))}</td><td>{fmt_num(x.get('average_r'))}</td><td>{fmt_num(x.get('rank_score'))}</td></tr>" for x in data.get("rows", [])[:100]])
    return HTMLResponse(f"""
    <html><head><title>v9 External Market Research</title><style>body{{font-family:Arial;margin:20px;background:#f6f8fb}} .card{{background:white;border-radius:12px;padding:14px;margin-bottom:14px;box-shadow:0 1px 6px #d1d5db}} table{{border-collapse:collapse;width:100%;background:white}} th{{background:#111827;color:white}} td,th{{padding:7px;border-bottom:1px solid #ddd;text-align:left}}</style></head>
    <body><h1>v9 External Market Research · {h(market.upper())}</h1><div class='card'><b>Interval:</b> {h(interval)} | <b>Range:</b> {h(range)} | <b>Rows:</b> {data.get('count')}<br><b>Summary:</b> {h(json.dumps(data.get('summary', {}), ensure_ascii=False))}<br><b>Note:</b> research-only; no broker execution integration.</div><table><tr><th>Label</th><th>Ticker</th><th>TF</th><th>Family</th><th>PF</th><th>Trades</th><th>Win %</th><th>Avg R</th><th>Rank</th></tr>{rows}</table>
    <p><a href='/v9_external_market_dashboard?secret={h(secret)}&market=etf&interval={h(interval)}'>ETF research</a> · <a href='/v9_multi_market_research_dashboard?secret={h(secret)}'>Combined v9 dashboard</a></p></body></html>
    """)


@app.post("/v9_external_backtest_import")
async def v9_external_backtest_import(request: Request):
    body = await request.json()
    verify_secret(request, body)
    rows_in = body.get("items") or body.get("rows") or []
    if not isinstance(rows_in, list):
        raise HTTPException(400, "items/rows must be a list")
    normalized = [v9_normalize_external_backtest_row(x) for x in rows_in if isinstance(x, dict)]
    existing = v9_load_external_registry().get("rows", [])
    v9_save_external_registry(existing + normalized)
    final = v9_load_external_registry().get("rows", [])
    return {"ok": True, "imported": len(normalized), "total_registry_rows": len(final), "rows": normalized}


@app.get("/v9_external_backtest_registry")
def v9_external_backtest_registry(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = v9_load_external_registry()
    return {"ok": True, "version": APP_FEATURE_LEVEL, "count": len(data.get("rows", [])), "rows": data.get("rows", []), "updated_at": data.get("updated_at")}


@app.get("/v9_market_regime_gate")
def v9_market_regime_gate_endpoint(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return v9_market_regime_gate(days=days, limit=limit)


@app.get("/v9_market_regime_diagnostics")
def v9_market_regime_diagnostics(secret: str, days: int = PAPER_OUTCOME_DEFAULT_DAYS, limit: int = PAPER_OUTCOME_MAX_EVENTS):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")

    diagnostics: Dict[str, Any] = {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "created_at": now_iso(),
        "checks": {},
    }

    try:
        diagnostics["checks"]["paper_outcome"] = {
            "ok": True,
            "data": _json_safe_value(build_paper_outcome_decision_report(days=days, limit=limit)),
        }
    except Exception as exc:
        diagnostics["checks"]["paper_outcome"] = {"ok": False, "error": str(exc)}

    try:
        diagnostics["checks"]["ai_risk"] = {
            "ok": True,
            "data": _json_safe_value(build_ai_risk_supervisor_report(days=days, limit=limit, include_plan=False)),
        }
    except Exception as exc:
        diagnostics["checks"]["ai_risk"] = {"ok": False, "error": str(exc)}

    diagnostics["checks"]["btc_1h"] = _safe_market_context("BTCUSDT", interval="60", limit=300)
    diagnostics["checks"]["eth_1h"] = _safe_market_context("ETHUSDT", interval="60", limit=300)

    try:
        diagnostics["gate"] = v9_market_regime_gate(days=days, limit=limit)
    except Exception as exc:
        diagnostics["gate"] = {"ok": False, "error": str(exc)}

    return _json_safe_value(diagnostics)


@app.get("/v9_multi_market_research")
def v9_multi_market_research(secret: str, max_symbols: int = 20, crypto_intervals: str = "60,240", external_interval: str = "60", external_range: str = V9_EXTERNAL_DEFAULT_RANGE, include_external: bool = True, force: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    crypto = v9_crypto_higher_tf_research(max_symbols=max_symbols, intervals=crypto_intervals, force=force)
    forex = v9_external_market_research(market="forex", interval=external_interval, range_=external_range) if include_external else {"rows": []}
    etf = v9_external_market_research(market="etf", interval=external_interval, range_=external_range) if include_external else {"rows": []}
    all_rows = (crypto.get("rows") or []) + (forex.get("rows") or []) + (etf.get("rows") or [])
    for r in all_rows:
        r["global_rank_score"] = v9_quality_rank(r)
    all_rows.sort(key=lambda x: (x.get("research_label") in {"STRONG_RESEARCH_CANDIDATE", "RESEARCH_CANDIDATE"}, x.get("global_rank_score") or 0), reverse=True)
    gate = v9_market_regime_gate()
    summary = {
        "total_rows": len(all_rows),
        "crypto_rows": len(crypto.get("rows") or []),
        "forex_rows": len(forex.get("rows") or []),
        "etf_rows": len(etf.get("rows") or []),
        "gate_level": gate.get("gate_level"),
        "allow_new_micro": gate.get("allow_new_micro"),
    }
    return {"ok": True, "version": APP_FEATURE_LEVEL, "created_at": now_iso(), "summary": summary, "market_regime_gate": gate, "top": all_rows[:V9_RESEARCH_TOP_N], "crypto": crypto.get("top", []), "forex": forex.get("top", []), "etf": etf.get("top", [])}


@app.get("/v9_multi_market_research_dashboard", response_class=HTMLResponse)
def v9_multi_market_research_dashboard(secret: str, max_symbols: int = 20, crypto_intervals: str = "60,240", external_interval: str = "60", external_range: str = V9_EXTERNAL_DEFAULT_RANGE, include_external: bool = True, force: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = v9_multi_market_research(secret=secret, max_symbols=max_symbols, crypto_intervals=crypto_intervals, external_interval=external_interval, external_range=external_range, include_external=include_external, force=force)
    rows = "".join([f"<tr><td>{h(x.get('market'))}</td><td>{h(x.get('research_label'))}</td><td>{h(x.get('symbol'))}</td><td>{h(x.get('timeframe') or x.get('interval'))}</td><td>{h(x.get('family'))}</td><td>{fmt_num(x.get('profit_factor'))}</td><td>{x.get('trade_count') or ''}</td><td>{fmt_num(x.get('win_rate'))}</td><td>{fmt_num(x.get('average_r'))}</td><td>{fmt_num(x.get('global_rank_score') or x.get('rank_score'))}</td><td>{'YES' if x.get('execution_supported') else 'NO'}</td></tr>" for x in data.get("top", [])])
    s = data.get("summary", {})
    gate = data.get("market_regime_gate", {})
    return HTMLResponse(f"""
    <html><head><title>v9 Multi-Market Research Framework</title><style>body{{font-family:Arial;margin:20px;background:#f6f8fb}} .card{{background:white;border-radius:12px;padding:14px;margin-bottom:14px;box-shadow:0 1px 6px #d1d5db}} table{{border-collapse:collapse;width:100%;background:white;font-size:13px}} th{{background:#111827;color:white}} td,th{{padding:7px;border-bottom:1px solid #ddd;text-align:left}} .HIGH{{color:#991b1b;font-weight:bold}} .ELEVATED{{color:#92400e;font-weight:bold}} .NORMAL{{color:#047857;font-weight:bold}}</style></head>
    <body><h1>v9 Multi-Market Research Framework</h1><div class='card'><b>Rows:</b> {s.get('total_rows')} | <b>Crypto:</b> {s.get('crypto_rows')} | <b>Forex:</b> {s.get('forex_rows')} | <b>ETF:</b> {s.get('etf_rows')}<br><b>Market regime gate:</b> <span class='{h(gate.get('gate_level'))}'>{h(gate.get('gate_level'))}</span> | <b>Allow new MICRO:</b> {h(gate.get('allow_new_micro'))}<br><b>Recommendation:</b> {h(gate.get('recommendation'))}<br><b>Safety:</b> v9 is research-only; external markets have no execution integration.</div>
    <table><tr><th>Market</th><th>Label</th><th>Symbol/Ticker</th><th>TF</th><th>Family</th><th>PF</th><th>Trades</th><th>Win %</th><th>Avg R</th><th>Rank</th><th>Execution supported</th></tr>{rows}</table>
    <p><a href='/v9_crypto_higher_tf_dashboard?secret={h(secret)}&max_symbols={max_symbols}&intervals={h(crypto_intervals)}'>Crypto HTF</a> · <a href='/v9_external_market_dashboard?secret={h(secret)}&market=forex&interval={h(external_interval)}&range={h(external_range)}'>Forex POC</a> · <a href='/v9_external_market_dashboard?secret={h(secret)}&market=etf&interval={h(external_interval)}&range={h(external_range)}'>ETF POC</a> · <a href='/v9_market_catalog?secret={h(secret)}'>Market catalog</a></p>
    </body></html>
    """)


# ============================================================
# v9.1.0 PERSISTENT STRATEGY REGISTRY + UNIVERSAL INSTANCE LAYER
# ============================================================
# The generic Supabase registry is optional. If the SQL table has not been
# created yet, all functionality falls back to local JSON files and the API
# reports the fallback explicitly. This keeps deploys safe while allowing
# Render's ephemeral filesystem to be removed as a source of truth later.

PERSISTENT_REGISTRY_ENABLED = os.getenv("PERSISTENT_REGISTRY_ENABLED", "true").lower() == "true"
SUPABASE_REGISTRY_TABLE = os.getenv("SUPABASE_REGISTRY_TABLE", "strategy_registry")
REGISTRY_HTTP_TIMEOUT = float(os.getenv("REGISTRY_HTTP_TIMEOUT", "12"))
REGISTRY_CLOUD_RETRY_SEC = int(os.getenv("REGISTRY_CLOUD_RETRY_SEC", "60"))
STRATEGY_INSTANCE_REGISTRY_FILE = APP_DIR / "strategy_instance_registry.json"
PROMOTION_HISTORY_FILE = APP_DIR / "promotion_history.json"

EARLY_WARNING_ENABLED = os.getenv("EARLY_WARNING_ENABLED", "true").lower() == "true"
EARLY_WARNING_WATCH_MIN_CLOSED = int(os.getenv("EARLY_WARNING_WATCH_MIN_CLOSED", "3"))
EARLY_WARNING_WATCH_AVG_R = float(os.getenv("EARLY_WARNING_WATCH_AVG_R", "-0.75"))
EARLY_WARNING_REVIEW_MIN_CLOSED = int(os.getenv("EARLY_WARNING_REVIEW_MIN_CLOSED", "5"))
EARLY_WARNING_REVIEW_AVG_R = float(os.getenv("EARLY_WARNING_REVIEW_AVG_R", "-0.50"))
EARLY_WARNING_REJECT_MIN_CLOSED = int(os.getenv("EARLY_WARNING_REJECT_MIN_CLOSED", "10"))
EARLY_WARNING_REJECT_AVG_R = float(os.getenv("EARLY_WARNING_REJECT_AVG_R", "-0.30"))

_registry_cloud_state: Dict[str, Any] = {
    "last_error": None,
    "last_error_at": 0.0,
    "last_success_at": None,
    "last_success_operation": None,
}


def registry_cloud_configured() -> bool:
    return bool(PERSISTENT_REGISTRY_ENABLED and supabase_enabled() and SUPABASE_REGISTRY_TABLE)


def registry_cloud_backoff_active() -> bool:
    last = float(_registry_cloud_state.get("last_error_at") or 0.0)
    return bool(last and (time.time() - last) < max(1, REGISTRY_CLOUD_RETRY_SEC))


def _registry_cloud_fail(operation: str, exc: Any) -> None:
    msg = f"{operation}: {exc}"
    _registry_cloud_state["last_error"] = msg
    _registry_cloud_state["last_error_at"] = time.time()
    log(f"[WARN] persistent registry cloud fallback: {msg}")


def _registry_cloud_ok(operation: str) -> None:
    _registry_cloud_state["last_success_at"] = now_iso()
    _registry_cloud_state["last_success_operation"] = operation
    _registry_cloud_state["last_error"] = None
    _registry_cloud_state["last_error_at"] = 0.0


def registry_table_url() -> str:
    return f"{SUPABASE_URL}/rest/v1/{SUPABASE_REGISTRY_TABLE}"


def registry_cloud_fetch(registry_type: str) -> Optional[list[Dict[str, Any]]]:
    if not registry_cloud_configured() or registry_cloud_backoff_active():
        return None
    try:
        response = client.get(
            registry_table_url(),
            headers=supabase_headers(prefer=""),
            params={
                "select": "registry_key,payload,updated_at",
                "registry_type": f"eq.{registry_type}",
                "order": "updated_at.asc",
            },
            timeout=REGISTRY_HTTP_TIMEOUT,
        )
        if response.status_code >= 300:
            _registry_cloud_fail(f"fetch:{registry_type}", f"HTTP_{response.status_code} {response.text[:240]}")
            return None
        raw_rows = response.json()
        rows = []
        for raw in raw_rows if isinstance(raw_rows, list) else []:
            payload = raw.get("payload") if isinstance(raw, dict) else None
            if isinstance(payload, dict):
                row = dict(payload)
                row.setdefault("registry_key", raw.get("registry_key"))
                row.setdefault("updated_at", raw.get("updated_at"))
                rows.append(row)
        _registry_cloud_ok(f"fetch:{registry_type}")
        return rows
    except Exception as exc:
        _registry_cloud_fail(f"fetch:{registry_type}", exc)
        return None


def registry_cloud_replace(registry_type: str, rows: list[Dict[str, Any]], key_fn) -> bool:
    if not registry_cloud_configured() or registry_cloud_backoff_active():
        return False
    try:
        # Replace the logical registry atomically enough for administrative use:
        # delete its old rows, then upsert its new normalized rows.
        delete_response = client.delete(
            registry_table_url(),
            headers=supabase_headers(prefer="return=minimal"),
            params={"registry_type": f"eq.{registry_type}"},
            timeout=REGISTRY_HTTP_TIMEOUT,
        )
        if delete_response.status_code >= 300:
            _registry_cloud_fail(f"delete:{registry_type}", f"HTTP_{delete_response.status_code} {delete_response.text[:240]}")
            return False
        records = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = str(key_fn(row) or "").strip()
            if not key:
                continue
            records.append({
                "registry_type": registry_type,
                "registry_key": key,
                "payload": row,
                "updated_at": now_iso(),
            })
        if records:
            headers = supabase_headers(prefer="resolution=merge-duplicates,return=minimal")
            upsert_response = client.post(
                registry_table_url(),
                headers=headers,
                params={"on_conflict": "registry_type,registry_key"},
                json=records,
                timeout=REGISTRY_HTTP_TIMEOUT,
            )
            if upsert_response.status_code >= 300:
                _registry_cloud_fail(f"upsert:{registry_type}", f"HTTP_{upsert_response.status_code} {upsert_response.text[:240]}")
                return False
        _registry_cloud_ok(f"replace:{registry_type}")
        return True
    except Exception as exc:
        _registry_cloud_fail(f"replace:{registry_type}", exc)
        return False


def registry_cloud_append(registry_type: str, row: Dict[str, Any], key: str) -> bool:
    if not registry_cloud_configured() or registry_cloud_backoff_active():
        return False
    try:
        record = {"registry_type": registry_type, "registry_key": key, "payload": row, "updated_at": now_iso()}
        response = client.post(
            registry_table_url(),
            headers=supabase_headers(prefer="resolution=merge-duplicates,return=minimal"),
            params={"on_conflict": "registry_type,registry_key"},
            json=record,
            timeout=REGISTRY_HTTP_TIMEOUT,
        )
        if response.status_code >= 300:
            _registry_cloud_fail(f"append:{registry_type}", f"HTTP_{response.status_code} {response.text[:240]}")
            return False
        _registry_cloud_ok(f"append:{registry_type}")
        return True
    except Exception as exc:
        _registry_cloud_fail(f"append:{registry_type}", exc)
        return False


# Preserve the local-file implementations as a safe fallback.
_load_backtest_results_file_v901 = load_backtest_results
_save_backtest_results_file_v901 = save_backtest_results
_load_discovery_validations_file_v901 = load_discovery_validations
_save_discovery_validations_file_v901 = save_discovery_validations
_classify_paper_candidate_v901 = classify_paper_candidate
_apply_strategy_action_v901 = _apply_strategy_action
_build_default_candidate_backtest_rows_v901 = build_default_candidate_backtest_rows


def load_backtest_results() -> list[Dict[str, Any]]:
    cloud_rows = registry_cloud_fetch("backtest")
    if cloud_rows is not None:
        return cloud_rows
    return _load_backtest_results_file_v901()


def save_backtest_results(rows: list[Dict[str, Any]]) -> None:
    # Local copy remains as a disaster-recovery cache.
    _save_backtest_results_file_v901(rows)
    registry_cloud_replace(
        "backtest",
        rows,
        lambda r: backtest_key(str(r.get("strategy", "UNKNOWN")), str(r.get("symbol", "")), str(r.get("side", "BOTH"))),
    )


def load_discovery_validations() -> Dict[str, Any]:
    cloud_rows = registry_cloud_fetch("discovery_validation")
    if cloud_rows is None:
        return _load_discovery_validations_file_v901()
    by_key: Dict[str, Dict[str, Any]] = {}
    for r in cloud_rows:
        key = r.get("key") or discovery_key(r.get("symbol", ""), r.get("family", ""), r.get("side", "LONG"), r.get("interval", "15"))
        if str(key).strip("|"):
            r["key"] = key
            by_key[key] = r
    return {"rows": list(by_key.values()), "by_key": by_key}


def save_discovery_validations(rows: list) -> None:
    _save_discovery_validations_file_v901(rows)
    unique: Dict[str, Dict[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        key = row.get("key") or discovery_key(row.get("symbol", ""), row.get("family", ""), row.get("side", "LONG"), row.get("interval", "15"))
        if str(key).strip("|"):
            row["key"] = key
            unique[str(key)] = row
    registry_cloud_replace("discovery_validation", list(unique.values()), lambda r: r.get("key"))


def build_default_candidate_backtest_rows() -> list[Dict[str, Any]]:
    rows = _build_default_candidate_backtest_rows_v901()
    rows += [
        {"strategy": "trend_continuation_nil_v11", "symbol": "NILUSDT", "side": "LONG", "profit_factor": 1.745, "trades": 47, "win_rate": 63.8, "source": "manual_tradingview"},
        {"strategy": "trend_continuation_wld_v11", "symbol": "WLDUSDT", "side": "LONG", "profit_factor": 1.582, "trades": 76, "win_rate": 51.3, "source": "manual_tradingview"},
    ]
    return merge_backtest_rows([], rows)


def classify_paper_candidate(group: Dict[str, Any], backtest: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    result = _classify_paper_candidate_v901(group, backtest)
    if not EARLY_WARNING_ENABLED:
        return result
    metrics = result.get("metrics") or {}
    closed = int(metrics.get("closed_count") or 0)
    avg_r = to_float_or_none(metrics.get("average_r_closed"))
    if avg_r is None:
        return result
    # Existing hard reject remains the final rule. Earlier layers surface risk
    # before a full rejection threshold is reached.
    if closed >= EARLY_WARNING_REJECT_MIN_CLOSED and avg_r <= EARLY_WARNING_REJECT_AVG_R:
        result["status"] = "REJECT"
        result["action"] = "Set to OFF or re-optimize before further use"
        result.setdefault("reasons", []).append(
            f"Early-warning hard reject: {closed} closed and Avg R {avg_r:.3f} <= {EARLY_WARNING_REJECT_AVG_R:.3f}"
        )
    elif closed >= EARLY_WARNING_REVIEW_MIN_CLOSED and avg_r <= EARLY_WARNING_REVIEW_AVG_R:
        result["status"] = "REJECT_REVIEW"
        result["action"] = "Manual OFF/re-optimization review required before more exposure"
        result.setdefault("reasons", []).append(
            f"Early-warning review: {closed} closed and Avg R {avg_r:.3f} <= {EARLY_WARNING_REVIEW_AVG_R:.3f}"
        )
    elif closed >= EARLY_WARNING_WATCH_MIN_CLOSED and avg_r <= EARLY_WARNING_WATCH_AVG_R:
        result["status"] = "WATCH_NEGATIVE"
        result["action"] = "Keep PAPER only; review again after the next outcomes"
        result.setdefault("reasons", []).append(
            f"Early-warning watch: {closed} closed and Avg R {avg_r:.3f} <= {EARLY_WARNING_WATCH_AVG_R:.3f}"
        )
    return result


def infer_strategy_family(strategy: str) -> str:
    value = str(strategy or "").lower()
    if "structure_swing" in value:
        return "structure_swing"
    if "momentum_breakout" in value:
        return "momentum_breakout"
    if "intraday_trend_pullback" in value:
        return "intraday_trend_pullback"
    if "trend_pullback" in value:
        return "trend_pullback"
    if "trend_continuation" in value:
        return "trend_continuation"
    return "custom"


def family_master_script(family: str) -> str:
    return {
        "trend_continuation": "universal_trend_continuation_v1",
        "trend_pullback": "universal_trend_pullback_v1",
        "intraday_trend_pullback": "universal_intraday_trend_pullback_v1",
        "momentum_breakout": "universal_momentum_breakout_v1",
        "structure_swing": "universal_structure_swing_v1",
    }.get(str(family), "custom_strategy")


def strategy_instance_key(row: Dict[str, Any]) -> str:
    return backtest_key(str(row.get("strategy", "UNKNOWN")), str(row.get("symbol", "")), str(row.get("side", "LONG")))


def load_strategy_instances() -> list[Dict[str, Any]]:
    cloud = registry_cloud_fetch("strategy_instance")
    if cloud is not None:
        return cloud
    data = read_json_file(STRATEGY_INSTANCE_REGISTRY_FILE, {"rows": []})
    return data.get("rows", []) if isinstance(data, dict) else []


def save_strategy_instances(rows: list[Dict[str, Any]]) -> None:
    unique: Dict[str, Dict[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        key = strategy_instance_key(row)
        row["key"] = key
        unique[key] = row
    values = list(unique.values())
    write_json_file(STRATEGY_INSTANCE_REGISTRY_FILE, {"updated_at": now_iso(), "rows": values})
    registry_cloud_replace("strategy_instance", values, strategy_instance_key)


def derive_strategy_instances_from_state(include_off: bool = False) -> list[Dict[str, Any]]:
    state = load_state()
    bt_index = backtest_index_by_candidate()
    existing = {strategy_instance_key(r): r for r in load_strategy_instances() if isinstance(r, dict)}
    rows = []
    for strategy, strategy_cfg in (state.get("strategies") or {}).items():
        family = infer_strategy_family(strategy)
        for symbol, symbol_cfg in (strategy_cfg.get("symbols") or {}).items():
            for side, side_cfg in (symbol_cfg or {}).items():
                mode = str((side_cfg or {}).get("mode", "OFF")).upper()
                if not include_off and mode == "OFF":
                    continue
                key = backtest_key(strategy, symbol, side)
                prev = existing.get(key, {})
                bt = bt_index.get(key, {})
                rows.append({
                    **prev,
                    "key": key,
                    "strategy": strategy,
                    "family": family,
                    "base_script": prev.get("base_script") or family_master_script(family),
                    "symbol": normalize_symbol(symbol),
                    "side": str(side).upper(),
                    "mode": mode,
                    "risk_pct": to_float_or_none((side_cfg or {}).get("risk_pct")),
                    "enabled": bool(strategy_cfg.get("enabled", True)),
                    "tv_pf": prev.get("tv_pf", bt.get("profit_factor")),
                    "tv_trades": prev.get("tv_trades", bt.get("trades")),
                    "tv_win_rate": prev.get("tv_win_rate", bt.get("win_rate")),
                    "source": prev.get("source") or "strategy_state_sync",
                    "updated_at": now_iso(),
                })
    return rows


def promotion_history_key(row: Dict[str, Any]) -> str:
    return str(row.get("history_id") or row.get("created_at") or now_ms())


def load_promotion_history(limit: int = 500) -> list[Dict[str, Any]]:
    cloud = registry_cloud_fetch("promotion_history")
    if cloud is not None:
        return sorted(cloud, key=lambda x: str(x.get("created_at", "")), reverse=True)[:max(1, limit)]
    data = read_json_file(PROMOTION_HISTORY_FILE, {"rows": []})
    rows = data.get("rows", []) if isinstance(data, dict) else []
    return sorted(rows, key=lambda x: str(x.get("created_at", "")), reverse=True)[:max(1, limit)]


def append_promotion_history(row: Dict[str, Any]) -> Dict[str, Any]:
    item = dict(row)
    item.setdefault("created_at", now_iso())
    item.setdefault("history_id", f"{int(time.time() * 1000)}-{hashlib.sha256(json.dumps(item, sort_keys=True, default=str).encode()).hexdigest()[:8]}")
    current = load_promotion_history(limit=2000)
    current.append(item)
    # Keep local cache bounded.
    current = sorted(current, key=lambda x: str(x.get("created_at", "")), reverse=True)[:2000]
    write_json_file(PROMOTION_HISTORY_FILE, {"updated_at": now_iso(), "rows": current})
    registry_cloud_append("promotion_history", item, promotion_history_key(item))
    return item


def _apply_strategy_action(action: Dict[str, Any], reason_prefix: str = "promotion_manager") -> Dict[str, Any]:
    before = {"strategy": action.get("strategy"), "symbol": action.get("symbol"), "side": action.get("side"), "current_mode": action.get("current_mode")}
    result = _apply_strategy_action_v901(action, reason_prefix=reason_prefix)
    append_promotion_history({
        "event_type": "STRATEGY_ACTION",
        "reason_prefix": reason_prefix,
        "before": before,
        "action": action,
        "result": result,
    })
    # Keep the instance registry synchronized after administrative changes.
    try:
        save_strategy_instances(derive_strategy_instances_from_state(include_off=False))
    except Exception as exc:
        log(f"[WARN] instance registry post-action sync failed: {exc}")
    return result


def registry_backend_status() -> Dict[str, Any]:
    return {
        "persistent_registry_enabled": PERSISTENT_REGISTRY_ENABLED,
        "supabase_enabled": supabase_enabled(),
        "registry_cloud_configured": registry_cloud_configured(),
        "supabase_registry_table": SUPABASE_REGISTRY_TABLE,
        "cloud_backoff_active": registry_cloud_backoff_active(),
        "cloud_state": dict(_registry_cloud_state),
        "fallback": "local_json_cache",
    }


@app.get("/v9_1_registry_health")
def v9_1_registry_health(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    probes = {}
    for registry_type in ["backtest", "discovery_validation", "strategy_instance", "promotion_history"]:
        cloud = registry_cloud_fetch(registry_type)
        probes[registry_type] = {
            "cloud_available": cloud is not None,
            "row_count": len(cloud or []),
        }
    return {"ok": True, "version": APP_FEATURE_LEVEL, "backend": registry_backend_status(), "probes": probes}


@app.post("/v9_1_registry_bootstrap")
async def v9_1_registry_bootstrap(request: Request):
    body = await request.json()
    verify_secret(request, body)
    # Merge the known TradingView benchmarks, then persist them.
    bt_rows = merge_backtest_rows(load_backtest_results(), build_default_candidate_backtest_rows())
    save_backtest_results(bt_rows)
    # Persist any previously local TV validation rows.
    local_validations = _load_discovery_validations_file_v901().get("rows", [])
    existing_validations = load_discovery_validations().get("rows", [])
    save_discovery_validations(existing_validations + local_validations)
    # Build the universal family/instance view from strategy_state.json.
    instances = derive_strategy_instances_from_state(include_off=bool(body.get("include_off", False)))
    save_strategy_instances(instances)
    history = append_promotion_history({
        "event_type": "REGISTRY_BOOTSTRAP",
        "backtest_rows": len(bt_rows),
        "validation_rows": len(load_discovery_validations().get("rows", [])),
        "instance_rows": len(instances),
    })
    return {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "backend": registry_backend_status(),
        "backtest_rows": len(bt_rows),
        "validation_rows": len(load_discovery_validations().get("rows", [])),
        "instance_rows": len(instances),
        "history": history,
    }


@app.get("/strategy_instance_registry")
def strategy_instance_registry(secret: str, include_off: bool = False, sync: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    if sync:
        rows = derive_strategy_instances_from_state(include_off=include_off)
        save_strategy_instances(rows)
    else:
        rows = load_strategy_instances()
        if not include_off:
            rows = [r for r in rows if str(r.get("mode", "OFF")).upper() != "OFF"]
    return {"ok": True, "version": APP_FEATURE_LEVEL, "backend": registry_backend_status(), "count": len(rows), "rows": rows}


@app.post("/strategy_instance_sync")
async def strategy_instance_sync(request: Request):
    body = await request.json()
    verify_secret(request, body)
    rows = derive_strategy_instances_from_state(include_off=bool(body.get("include_off", False)))
    save_strategy_instances(rows)
    return {"ok": True, "version": APP_FEATURE_LEVEL, "count": len(rows), "rows": rows}


@app.post("/strategy_instance_import")
async def strategy_instance_import(request: Request):
    body = await request.json()
    verify_secret(request, body)
    incoming = body.get("rows") or body.get("items") or []
    if not isinstance(incoming, list):
        raise HTTPException(400, "Expected rows/items list")
    mode = str(body.get("mode", "upsert")).lower()
    if mode not in {"upsert", "replace"}:
        raise HTTPException(400, "mode must be upsert or replace")
    existing = [] if mode == "replace" else load_strategy_instances()
    index = {strategy_instance_key(r): r for r in existing if isinstance(r, dict)}
    for raw in incoming:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        row["strategy"] = str(row.get("strategy") or "UNKNOWN")
        row["symbol"] = normalize_symbol(str(row.get("symbol") or ""))
        row["side"] = str(row.get("side") or "LONG").upper()
        row.setdefault("family", infer_strategy_family(row["strategy"]))
        row.setdefault("base_script", family_master_script(row["family"]))
        row.setdefault("updated_at", now_iso())
        index[strategy_instance_key(row)] = {**index.get(strategy_instance_key(row), {}), **row}
    rows = list(index.values())
    save_strategy_instances(rows)
    return {"ok": True, "version": APP_FEATURE_LEVEL, "mode": mode, "count": len(rows), "rows": rows}


@app.get("/strategy_instance_dashboard", response_class=HTMLResponse)
def strategy_instance_dashboard(secret: str, sync: bool = False):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    rows = derive_strategy_instances_from_state(include_off=False) if sync else load_strategy_instances()
    if sync:
        save_strategy_instances(rows)
    body_rows = "".join([
        f"<tr><td>{h(r.get('family'))}</td><td>{h(r.get('strategy'))}</td><td>{h(r.get('base_script'))}</td><td>{h(r.get('symbol'))}</td><td>{h(r.get('side'))}</td><td>{h(r.get('mode'))}</td><td>{fmt_num(r.get('risk_pct'))}</td><td>{fmt_num(r.get('tv_pf'))}</td><td>{h(r.get('tv_trades'))}</td></tr>"
        for r in rows if str(r.get("mode", "OFF")).upper() != "OFF"
    ]) or "<tr><td colspan='9'>No synchronized strategy instances yet. Use /strategy_instance_sync.</td></tr>"
    return HTMLResponse(f"""
    <html><head><title>Universal Strategy Instances</title><style>body{{font-family:Arial;margin:20px;background:#f6f8fb}}.card{{background:white;border-radius:12px;padding:14px;margin-bottom:14px;box-shadow:0 1px 6px #d1d5db}}table{{border-collapse:collapse;width:100%;background:white}}th{{background:#111827;color:white}}td,th{{padding:8px;border-bottom:1px solid #ddd;text-align:left}}</style></head>
    <body><h1>Universal Strategy Instance Registry · Platform v9.1.0</h1><div class='card'>Instances: {len(rows)} | Cloud configured: {registry_cloud_configured()} | Source: Supabase registry with local JSON fallback</div>
    <table><tr><th>Family</th><th>Instance</th><th>Master Pine Script</th><th>Symbol</th><th>Side</th><th>Mode</th><th>Risk %</th><th>TV PF</th><th>TV Trades</th></tr>{body_rows}</table>
    <p><a href='/strategy_instance_registry?secret={h(secret)}'>JSON</a> · <a href='/candidate_monitor_dashboard?secret={h(secret)}&days=14&limit=500'>Candidate monitor</a> · <a href='/v9_1_registry_health?secret={h(secret)}'>Registry health</a></p></body></html>
    """)


@app.get("/promotion_history")
def promotion_history(secret: str, limit: int = 200):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    rows = load_promotion_history(limit=max(1, min(limit, 2000)))
    return {"ok": True, "version": APP_FEATURE_LEVEL, "count": len(rows), "rows": rows}


@app.post("/promotion_history_snapshot")
async def promotion_history_snapshot(request: Request):
    body = await request.json()
    verify_secret(request, body)
    days = int(body.get("days", 14))
    limit = int(body.get("limit", 500))
    plan = build_strategy_promotion_plan(days=days, limit=limit)
    item = append_promotion_history({"event_type": "PROMOTION_PLAN_SNAPSHOT", "days": days, "plan": plan})
    return {"ok": True, "version": APP_FEATURE_LEVEL, "history": item}


@app.get("/early_warning_report")
def early_warning_report(secret: str, days: int = 14, limit: int = 500):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    report = build_paper_outcome_decision_report(days=days, limit=limit, include_outcomes=False)
    flagged = []
    for item in report.get("decisions", []):
        status = str((item.get("decision") or {}).get("status") or "")
        if status in {"WATCH_NEGATIVE", "REJECT_REVIEW", "REJECT"}:
            flagged.append(item)
    return {
        "ok": True,
        "version": APP_FEATURE_LEVEL,
        "thresholds": {
            "watch": {"min_closed": EARLY_WARNING_WATCH_MIN_CLOSED, "avg_r_lte": EARLY_WARNING_WATCH_AVG_R},
            "reject_review": {"min_closed": EARLY_WARNING_REVIEW_MIN_CLOSED, "avg_r_lte": EARLY_WARNING_REVIEW_AVG_R},
            "reject": {"min_closed": EARLY_WARNING_REJECT_MIN_CLOSED, "avg_r_lte": EARLY_WARNING_REJECT_AVG_R},
        },
        "flagged_count": len(flagged),
        "flagged": flagged,
        "full_report": report,
    }


@app.get("/early_warning_dashboard", response_class=HTMLResponse)
def early_warning_dashboard(secret: str, days: int = 14, limit: int = 500):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    data = early_warning_report(secret=secret, days=days, limit=limit)
    rows = "".join([
        f"<tr><td>{h(x.get('strategy'))}</td><td>{h(x.get('symbol'))}</td><td>{h(x.get('side'))}</td><td>{h((x.get('decision') or {}).get('status'))}</td><td>{fmt_num(((x.get('decision') or {}).get('metrics') or {}).get('closed_count'))}</td><td>{fmt_num(((x.get('decision') or {}).get('metrics') or {}).get('average_r_closed'))}</td><td>{h((x.get('decision') or {}).get('action'))}</td></tr>"
        for x in data.get("flagged", [])
    ]) or "<tr><td colspan='7'>No early-warning strategy flags in the selected window.</td></tr>"
    return HTMLResponse(f"""
    <html><head><title>Early Warning</title><style>body{{font-family:Arial;margin:20px;background:#f6f8fb}}.card{{background:white;border-radius:12px;padding:14px;margin-bottom:14px;box-shadow:0 1px 6px #d1d5db}}table{{border-collapse:collapse;width:100%;background:white}}th{{background:#111827;color:white}}td,th{{padding:8px;border-bottom:1px solid #ddd;text-align:left}}</style></head>
    <body><h1>Strategy Early Warning Dashboard · Platform v9.1.0</h1><div class='card'>Flagged: {data.get('flagged_count')} | WATCH: ≥{EARLY_WARNING_WATCH_MIN_CLOSED} trades and Avg R ≤ {EARLY_WARNING_WATCH_AVG_R} | REVIEW: ≥{EARLY_WARNING_REVIEW_MIN_CLOSED} trades and Avg R ≤ {EARLY_WARNING_REVIEW_AVG_R} | REJECT: ≥{EARLY_WARNING_REJECT_MIN_CLOSED} trades and Avg R ≤ {EARLY_WARNING_REJECT_AVG_R}</div>
    <table><tr><th>Strategy</th><th>Symbol</th><th>Side</th><th>Status</th><th>Closed</th><th>Avg R</th><th>Action</th></tr>{rows}</table>
    <p><a href='/early_warning_report?secret={h(secret)}&days={days}&limit={limit}'>JSON</a> · <a href='/candidate_monitor_dashboard?secret={h(secret)}&days={days}&limit={limit}'>Candidate monitor</a></p></body></html>
    """)


@app.get("/persistent_registry_schema_sql", response_class=HTMLResponse)
def persistent_registry_schema_sql(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    sql = """-- v9.1.0 generic persistent registry. Run once in Supabase SQL Editor.\ncreate table if not exists public.strategy_registry (\n  registry_type text not null,\n  registry_key text not null,\n  payload jsonb not null default '{}'::jsonb,\n  updated_at timestamptz not null default now(),\n  primary key (registry_type, registry_key)\n);\ncreate index if not exists strategy_registry_type_updated_idx\n  on public.strategy_registry (registry_type, updated_at desc);\nalter table public.strategy_registry enable row level security;\n-- The Render app uses SUPABASE_SERVICE_ROLE_KEY, which bypasses RLS.\n"""
    return HTMLResponse(f"<html><body><h1>Supabase persistent registry SQL</h1><pre>{h(sql)}</pre></body></html>")
