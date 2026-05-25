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
HTTP_TIMEOUT = 15.0

APP_DIR = Path(__file__).resolve().parent
PAPER_MONITOR_STATE_FILE = APP_DIR / "paper_monitor_state.json"
STATE_FILE = APP_DIR / "strategy_state.json"
TRADE_LOG_FILE = APP_DIR / "trade_log.csv"
RUNTIME_STATE_FILE = APP_DIR / "runtime_state.json"
BACKTEST_FILE = APP_DIR / "backtest_results.json"
DAILY_REPORT_STATE_FILE = APP_DIR / "daily_report_state.json"

app = FastAPI(title="TradingView Bybit Risk Engine", version="6.8.2")
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
    <h1>Candidate Strategy Monitor v6.8.2</h1>
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
    state = load_strategy_state()
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

APP_FEATURE_LEVEL = "6.8.2"

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


@app.get("/version")
def version(secret: Optional[str] = None):
    if secret is not None and secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "version": APP_FEATURE_LEVEL, "base": "5.3.0", "features": ["order_hardening", "safe_auto_close", "telegram_command_security", "strategy_state_rollback", "audit_log", "simulation_replay", "portfolio_correlation_guard", "market_regime_filter", "production_monitoring", "config_validation", "control_panel", "paper_trade_outcome_tracker", "paper_outcome_decision_layer", "candidate_monitor", "paper_backtest_alignment", "backtest_manual_import", "backtest_registry", "cron_paper_outcome_report"]}
