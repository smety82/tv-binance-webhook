# app.py
import csv
import hmac
import hashlib
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

# Safety master switch. Keep false until paper/micro logging is verified.
ENABLE_REAL_ORDERS = os.getenv("ENABLE_REAL_ORDERS", "false").lower() == "true"

HTTP_TIMEOUT = 15.0

APP_DIR = Path(__file__).resolve().parent
STATE_FILE = APP_DIR / "strategy_state.json"
TRADE_LOG_FILE = APP_DIR / "trade_log.csv"

app = FastAPI(title="TradingView Bybit Risk Engine", version="1.4.0")
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
# ROUTES
# ============================================================

@app.get("/", response_class=HTMLResponse)
def root():
    return f"""
    <h3>TV Webhook ↔ Bybit Risk Engine: OK</h3>
    <p>version: 1.4.0</p>
    <p>real_orders_enabled: {ENABLE_REAL_ORDERS}</p>
    <p>time: {now_iso()}</p>
    """


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
        media_type="text/html"
    )


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

    return {
        "ok": True,
        "real_orders_enabled": ENABLE_REAL_ORDERS,
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
