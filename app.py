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

HTTP_TIMEOUT = 15.0

APP_DIR = Path(__file__).resolve().parent
STATE_FILE = APP_DIR / "strategy_state.json"
TRADE_LOG_FILE = APP_DIR / "trade_log.csv"
RUNTIME_STATE_FILE = APP_DIR / "runtime_state.json"

app = FastAPI(title="TradingView Bybit Risk Engine", version="2.4.0")
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
        <div class="muted">Version 2.4.0 · Generated at {h(now_iso())} · Window: {safe_days} day(s)</div>
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
            {html_table(["Strategy", "Symbol", "Side", "Mode", "Events", "Paper", "Orders", "Rejects", "Failures", "Paused", "Quality Rejects", "Price Rejects", "Duplicate Rejects", "Alert Duplicates", "Closed PnL", "PF", "Health", "Score", "Reasons"], health_rows)}
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
    <p>version: 2.4.0</p>
    <p>real_orders_enabled: {ENABLE_REAL_ORDERS}</p>
    <p>trading_paused: {runtime_state.get("trading_paused")}</p>
    <p>supabase_enabled: {supabase_enabled()}</p>
    <p>cooldown_minutes: {ORDER_SIGNAL_COOLDOWN_MINUTES}</p>
    <p>alert_idempotency_lookback_hours: {ORDER_ALERT_IDEMPOTENCY_LOOKBACK_HOURS}</p>
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
        "order_guards": {
            "min_stop_distance_pct": ORDER_MIN_STOP_DISTANCE_PCT,
            "max_stop_distance_pct": ORDER_MAX_STOP_DISTANCE_PCT,
            "min_tp1_rr": ORDER_MIN_TP1_RR,
            "min_tp2_rr": ORDER_MIN_TP2_RR,
            "max_signal_price_deviation_pct": ORDER_MAX_SIGNAL_PRICE_DEVIATION_PCT,
            "duplicate_signal_cooldown_minutes": ORDER_SIGNAL_COOLDOWN_MINUTES,
            "alert_idempotency_lookback_hours": ORDER_ALERT_IDEMPOTENCY_LOOKBACK_HOURS,
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

    if is_trading_paused():
        write_trade_log(
            body=body,
            mode=mode,
            risk_pct_used=risk_pct_used,
            decision="RUNTIME_PAUSED",
            decision_reason="Trading paused by runtime kill switch",
            status="blocked_by_runtime_pause",
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

        return ok(
            {
                "order_sent": True,
                "decision": decision,
                "quality": quality,
                "price_deviation": price_deviation,
                "duplicate_signal": duplicate_signal,
                "alert_idempotency": alert_idempotency,
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
