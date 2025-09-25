# app.py
import os, time, hmac, hashlib, json, math, threading
from typing import Dict, Any, Optional, Tuple

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

# ======== ENV / CONFIG ========
def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing environment variable: {name}")
    return v

API_KEY       = require_env("BYBIT_KEY")
API_SECRET    = require_env("BYBIT_SECRET")
BYBIT_BASE    = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")
SHARED_SECRET = os.getenv("SHARED_SECRET", "Claas208!!")  # Pine 'secretKey' inputnek ezzel kell egyeznie

RECV_WINDOW   = "5000"
HTTP_TIMEOUT  = 15.0

app = FastAPI()
client = httpx.Client(timeout=HTTP_TIMEOUT)

# ======== SIMPLE GUARD (in-memory) ========
_guard = {
    "enabled": False,
    "limit_pct": None,   # napi max veszteség %-ban az equity baseline-hoz képest
    "limit_usd": None,   # alternatív fix USD limit
    "baseline": None,    # induló equity
    "equity_now": None,  # utolsó mért equity
    "drawdown_usd": 0.0,
    "drawdown_pct": 0.0,
    "block": False,
    "start_date": None,
}

# ======== HELPERS ========
def now_ms() -> str:
    return str(int(time.time() * 1000))

def hmac_sha256(key: str, s: str) -> str:
    return hmac.new(key.encode(), s.encode(), hashlib.sha256).hexdigest()

def sign_v5(ts: str, api_key: str, recv_window: str, payload: str) -> str:
    return hmac_sha256(API_SECRET, ts + api_key + recv_window + payload)

def round_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step

def fmt_qty(q: float) -> str:
    # Bybit string mennyiséget vár, tizedes kezeléssel
    return f"{q:.8f}".rstrip("0").rstrip(".") if "." in f"{q:.8f}" else str(int(q))

def fmt_price(p: float, tick: float) -> str:
    p2 = round_step(p, tick)
    s  = f"{p2:.8f}"
    return s.rstrip("0").rstrip(".") if "." in s else s

def ok(data: Dict[str, Any]) -> JSONResponse:
    return JSONResponse({"ok": True, **data})

def log(msg: str) -> None:
    print(msg, flush=True)

# ======== BYBIT CORE CALL ========
def bybit(method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = BYBIT_BASE + path
    ts  = now_ms()
    if method.upper() == "GET":
        query = ""
        if params:
            # stable order
            items = sorted((k, str(v)) for k, v in params.items() if v is not None)
            query = "&".join([f"{k}={v}" for k, v in items])
            url = url + "?" + query
        sign = sign_v5(ts, API_KEY, RECV_WINDOW, query)
        headers = {
            "X-BAPI-API-KEY": API_KEY,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": RECV_WINDOW,
            "X-BAPI-SIGN": sign,
        }
        r = client.get(url, headers=headers)
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
        r = client.post(url, headers=headers, content=body)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    data = r.json()
    return data

# ======== BYBIT HELPERS ========
def get_instrument(symbol: str) -> Tuple[float, float, float]:
    """Returns (tick_size, lot_step, min_qty) for linear USDT perp."""
    resp = bybit("GET", "/v5/market/instruments-info", {
        "category": "linear",
        "symbol": symbol
    })
    lst = (resp.get("result") or {}).get("list") or []
    if not lst:
        raise HTTPException(400, f"Symbol not found: {symbol}")
    it = lst[0]
    pf = it.get("priceFilter", {})
    lf = it.get("lotSizeFilter", {})
    tick = float(pf.get("tickSize", "0.01"))
    step = float(lf.get("qtyStep", "0.001"))
    minq = float(lf.get("minOrderQty", "0.001"))
    return (tick, step, minq)

def get_ticker_last(symbol: str) -> float:
    resp = bybit("GET", "/v5/market/tickers", {"category": "linear", "symbol": symbol})
    lst = (resp.get("result") or {}).get("list") or []
    if not lst:
        raise HTTPException(400, f"No ticker for {symbol}")
    return float(lst[0]["lastPrice"])

def get_equity_usdt() -> float:
    resp = bybit("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED", "coin": "USDT"})
    lst = (resp.get("result") or {}).get("list") or []
    if not lst:
        return 0.0
    coins = lst[0].get("coin", [])
    for c in coins:
        if c.get("coin") == "USDT":
            return float(c.get("equity", "0") or 0)
    return 0.0

def get_position_linear(symbol: str) -> Dict[str, Any]:
    resp = bybit("GET", "/v5/position/list", {"category": "linear", "symbol": symbol})
    listpos = (resp.get("result") or {}).get("list") or []
    # unified one-way: typically one entry with positionIdx 0
    if not listpos:
        return {"side": "", "size": "0"}
    # pick net / primary
    # choose the record with largest abs(size)
    best = None
    maxabs = 0.0
    for p in listpos:
        sz = abs(float(p.get("size", "0") or 0))
        if sz > maxabs:
            best = p
            maxabs = sz
    return best or listpos[0]

def set_leverage(symbol: str, lev: int) -> Dict[str, Any]:
    req = {
        "category": "linear",
        "symbol": symbol,
        "buyLeverage": str(lev),
        "sellLeverage": str(lev),
    }
    log(f"[REQ] set-leverage: {req}")
    resp = bybit("POST", "/v5/position/set-leverage", req)
    log(f"[RESP] set-leverage: {resp}")
    return resp

def cancel_all_reduce_only(symbol: str):
    """Optional cleanup helper to remove stale reduce-only orders for a symbol."""
    # You can implement calling /v5/order/cancel-all with filters;
    # left as placeholder in case needed later.
    pass

# ======== SECURITY ========
def verify_secret(request: Request, body: Dict[str, Any]) -> None:
    hdr = request.headers.get("x-alert-secret") or request.headers.get("X-Alert-Secret")
    body_secret = body.get("secret")
    if SHARED_SECRET and (hdr == SHARED_SECRET or body_secret == SHARED_SECRET):
        return
    raise HTTPException(401, "Unauthorized")

# ======== DRAWNDOWN GUARD ========
def guard_check_block() -> bool:
    if not _guard["enabled"]:
        return False
    if _guard["baseline"] is None:
        _guard["baseline"] = get_equity_usdt()
        _guard["start_date"] = int(time.time())
    eq = get_equity_usdt()
    _guard["equity_now"] = eq
    dd_usd = (_guard["baseline"] - eq)
    dd_pct = (dd_usd / _guard["baseline"] * 100.0) if _guard["baseline"] else 0.0
    _guard["drawdown_usd"] = max(0.0, dd_usd)
    _guard["drawdown_pct"] = max(0.0, dd_pct)
    limit_hit = False
    if _guard["limit_pct"] is not None and dd_pct >= _guard["limit_pct"]:
        limit_hit = True
    if _guard["limit_usd"] is not None and dd_usd >= _guard["limit_usd"]:
        limit_hit = True
    _guard["block"] = limit_hit
    return limit_hit

# ======== ROUTES ========
@app.get("/", response_class=HTMLResponse)
def root():
    return "<h3>TV Webhook ↔ Bybit middleware: OK</h3>"

@app.get("/guard_status")
def guard_status(secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    return {"ok": True, "status": _guard}

@app.post("/guard")
async def guard_set(request: Request):
    body = await request.json()
    verify_secret(request, body)
    _guard["enabled"]   = bool(body.get("enable", False))
    _guard["limit_pct"] = body.get("limit_pct")
    _guard["limit_usd"] = body.get("limit_usd")
    return ok({"msg": "guard updated"})

@app.get("/position")
def position(symbol: str, secret: str):
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    pos = get_position_linear(symbol)
    # also attach leverage info quickly
    return {"ok": True, "position": pos}

@app.post("/set_leverage")
async def set_lev(request: Request):
    body = await request.json()
    verify_secret(request, body)
    symbol = body["symbol"]
    lev    = int(body["leverage"])
    resp   = set_leverage(symbol, lev)
    return ok(resp)

# ======== CORE WEBHOOK ========
@app.post("/tv")
async def tv_webhook(request: Request):
    raw = await request.body()
    try:
        body = json.loads(raw)
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    # minimal ping logging
    if isinstance(body, dict) and body.get("type") == "ping":
        log(f'INCOMING /tv RAW: {json.dumps(body)}')
        return ok({"msg": "pong"})

    # full log for orders
    log(f"INCOMING /tv RAW: {raw.decode('utf-8')}")

    verify_secret(request, body)

    # guard check
    if guard_check_block():
        raise HTTPException(400, "Guard: daily loss limit reached, blocking new orders")

    # ---- parse payload ----
    exch   = body.get("exchange", "bybit").lower()
    if exch != "bybit":
        raise HTTPException(400, "Only `bybit` exchange supported")
    symbol = body["symbol"]
    side_s = body["side"].upper()  # LONG / SHORT
    order_type = body.get("orderType", "Market")
    reduce_only = bool(body.get("reduceOnly", False))
    tp1  = float(body.get("tp1")) if body.get("tp1") is not None else None
    tp2  = float(body.get("tp2")) if body.get("tp2") is not None else None
    sl   = float(body.get("sl"))  if body.get("sl")  is not None else None

    risk_pct = body.get("riskPct")  # % a teljes equityből
    qty_in   = body.get("qty")      # direkt mennyiség (opcionális)

    if order_type != "Market":
        raise HTTPException(400, "Currently only Market entries are supported")

    # ---- instrument info ----
    tick, lot_step, min_qty = get_instrument(symbol)
    log(f"[INFO] {symbol} tick={tick} lot={lot_step} min_qty={min_qty}")

    # ---- sizing ----
    if qty_in is not None:
        qty_in = float(qty_in)
        qty_calc = qty_in
        # bump to min lot if needed
        qty_rounded = max(round_step(qty_calc, lot_step), min_qty)
        log(f"[INFO] sizing=explicit: qty_in={qty_in} -> qty_rounded={qty_rounded}")
    else:
        if risk_pct is None or sl is None:
            raise HTTPException(400, "riskPct and sl are required when qty is not provided")
        risk_pct = float(risk_pct)
        equity   = get_equity_usdt()
        last_px  = get_ticker_last(symbol)
        stop_dist = abs(last_px - float(sl))
        if stop_dist <= 0:
            raise HTTPException(400, "Invalid stop distance")
        risk_usd = equity * (risk_pct / 100.0)
        qty_calc = risk_usd / stop_dist
        qty_rounded = max(round_step(qty_calc, lot_step), min_qty)
        log(f"[INFO] sizing=riskPct: equity={equity:.4f} riskPct={risk_pct:.4f}% riskUsd={risk_usd:.4f} "
            f"lastPx={last_px:.4f} sl={sl:.4f} stopDist={stop_dist:.4f} "
            f"qty_calc={qty_calc:.6f} -> qty_rounded={qty_rounded}")

    desired_side = "Buy" if side_s == "LONG" else "Sell"

    # ---- check current position (one-way flip logic) ----
    pos   = get_position_linear(symbol)
    pside = (pos.get("side") or "")
    psize = float(pos.get("size", "0") or 0.0)

    actual_qty = qty_rounded
    if psize > 0 and pside and pside != desired_side:
        # flip: close old and open new in one market order
        flip_qty   = psize + qty_rounded
        actual_qty = max(round_step(flip_qty, lot_step), min_qty)
        log(f"[FLIP] Existing {pside} {psize} -> desired {desired_side} {qty_rounded} => sending {desired_side} {actual_qty}")

    # ---- ENTRY ----
    link_id = f"TV-{symbol}-{now_ms()}"
    entry_req = {
        "category": "linear",
        "symbol": symbol,
        "side": "Buy" if desired_side == "Buy" else "Sell",
        "orderType": "Market",
        "qty": fmt_qty(actual_qty),
        "timeInForce": "IOC",
        "reduceOnly": False,
        "orderLinkId": link_id
    }
    log(f"[REQ] order/create ENTRY: {entry_req}")
    entry_resp = bybit("POST", "/v5/order/create", entry_req)
    log(f"[RESP] order/create ENTRY: {entry_resp}")

    # ---- POLL POSITION to ensure side & size ----
    size = 0.0
    side_now = ""
    for i in range(12):
        time.sleep(0.25)
        p = get_position_linear(symbol)
        side_now = p.get("side") or ""
        size = float(p.get("size", "0") or 0.0)
        log(f"[INFO] poll pos {i+1}/12: side={side_now} size={size}")
        if size > 0.0 and side_now == desired_side:
            break

    if size <= 0.0 or side_now != desired_side:
        # No net position in desired direction -> likely flattened (one-way close)
        log("[WARN] No net position in desired direction after ENTRY; skipping TP placement; trying protective SL fallback.")
        # try to place stop-market reduceOnly as a last resort (won't trigger if no position)
        if sl is not None:
            fallback_req = {
                "category": "linear",
                "symbol": symbol,
                "side": ("Sell" if desired_side == "Buy" else "Buy"),
                "orderType": "Market",
                "timeInForce": "IOC",
                "reduceOnly": True,
                "qty": fmt_qty(max(round_step(qty_rounded, lot_step), min_qty)),
                "triggerPrice": fmt_price(sl, tick),
                "triggerBy": "MarkPrice",
                "triggerDirection": (2 if desired_side == "Buy" else 1),
                "orderLinkId": f"{link_id}-SLB"
            }
            log(f"[REQ] order/create FALLBACK stop-market: {fallback_req}")
            try:
                fb = bybit("POST", "/v5/order/create", fallback_req)
                log(f"[RESP] order/create FALLBACK stop-market: {fb}")
            except HTTPException as e:
                log(f"[ERR] fallback stop-market failed: {e.detail}")
        return ok({"msg": "entry ok, but no net position in desired direction; tp/sl skipped"})

    # ---- TP sizing ----
    tp1_pct = None
    if body.get("tp1") is not None:
        tp1_pct = body.get("tp1_pct")  # nem biztos, hogy jön – TP1 ár általában fix átadva
    # Nálad TP1/TP2 ár érkezik fixen; TP megosztást a qty alapján végezzük:
    tp1_share_pct = 30.0  # ha mást szeretnél, kiteheted env-be vagy payloadba
    tp1_qty = round_step(size * (tp1_share_pct / 100.0), lot_step)
    if tp1_qty < min_qty:
        tp1_qty = 0.0
    tp2_qty = round_step(size - tp1_qty, lot_step)
    if tp2_qty < min_qty:
        # ha kerekítés lecsapta, akkor inkább mind menjen TP2-re
        tp1_qty = 0.0
        tp2_qty = round_step(size, lot_step)

    log(f"[INFO] tp1_qty={tp1_qty} tp2_qty={tp2_qty}")

    # ---- PLACE TPs ----
    if tp1_qty > 0 and tp1 is not None:
        tp1_req = {
            "category": "linear",
            "symbol": symbol,
            "side": ("Sell" if desired_side == "Buy" else "Buy"),
            "orderType": "Limit",
            "price": fmt_price(tp1, tick),
            "qty": fmt_qty(tp1_qty),
            "timeInForce": "GTC",
            "reduceOnly": True,
            "orderLinkId": f"{link_id}-TP1"
        }
        log(f"[REQ] order/create TP1: {tp1_req}")
        try:
            tp1_resp = bybit("POST", "/v5/order/create", tp1_req)
            log(f"[RESP] order/create TP1: {tp1_resp}")
        except HTTPException as e:
            log(f"[ERR] order/create TP1 failed: {e.detail}")

    if tp2_qty > 0 and tp2 is not None:
        tp2_req = {
            "category": "linear",
            "symbol": symbol,
            "side": ("Sell" if desired_side == "Buy" else "Buy"),
            "orderType": "Limit",
            "price": fmt_price(tp2, tick),
            "qty": fmt_qty(tp2_qty),
            "timeInForce": "GTC",
            "reduceOnly": True,
            "orderLinkId": f"{link_id}-TP2"
        }
        log(f"[REQ] order/create TP2: {tp2_req}")
        try:
            tp2_resp = bybit("POST", "/v5/order/create", tp2_req)
            log(f"[RESP] order/create TP2: {tp2_resp}")
        except HTTPException as e:
            log(f"[ERR] order/create TP2 failed: {e.detail}")

    # ---- SL via position/trading-stop (preferred) ----
    if sl is not None:
        sl_req = {
            "category": "linear",
            "symbol": symbol,
            "stopLoss": fmt_price(sl, tick),
            "slTriggerBy": "MarkPrice",
            "tpslMode": "Full"
        }
        log(f"[REQ] position/trading-stop SL (MarkPrice): {sl_req}")
        try:
            sl_resp = bybit("POST", "/v5/position/trading-stop", sl_req)
            log(f"[RESP] position/trading-stop SL: {sl_resp}")
        except HTTPException as e:
            log(f"[WARN] trading-stop (MarkPrice) failed: {e.detail}")
            # fallback try LastPrice
            sl_req_lp = dict(sl_req)
            sl_req_lp["slTriggerBy"] = "LastPrice"
            log(f"[REQ] position/trading-stop SL (LastPrice): {sl_req_lp}")
            try:
                sl_resp2 = bybit("POST", "/v5/position/trading-stop", sl_req_lp)
                log(f"[RESP] position/trading-stop SL (LastPrice): {sl_resp2}")
            except HTTPException as e2:
                log(f"[ERR] trading-stop failed both triggers: {e2.detail}")
                # as last resort place conditional stop-market reduceOnly
                fallback_req = {
                    "category": "linear",
                    "symbol": symbol,
                    "side": ("Sell" if desired_side == "Buy" else "Buy"),
                    "orderType": "Market",
                    "timeInForce": "IOC",
                    "reduceOnly": True,
                    "qty": fmt_qty(max(round_step(size, lot_step), min_qty)),
                    "triggerPrice": fmt_price(sl, tick),
                    "triggerBy": "MarkPrice",
                    "triggerDirection": (2 if desired_side == "Buy" else 1),
                    "orderLinkId": f"{link_id}-SLB"
                }
                log(f"[REQ] order/create FALLBACK stop-market: {fallback_req}")
                try:
                    fb = bybit("POST", "/v5/order/create", fallback_req)
                    log(f"[RESP] order/create FALLBACK stop-market: {fb}")
                except HTTPException as e3:
                    log(f"[ERR] fallback stop-market failed: {e3.detail}")

    return ok({"msg": "entry+tp/sl processed"})

# ======== ADJUST (BE/TRAIL/SET_SL) ========
@app.post("/adjust")
async def adjust(request: Request):
    body = await request.json()
    verify_secret(request, body)
    symbol = body["symbol"]
    action = body["action"]

    pos = get_position_linear(symbol)
    size = float(pos.get("size", "0") or 0.0)
    side = pos.get("side") or ""
    if size <= 0.0 or not side:
        raise HTTPException(400, "No open position")

    if action == "be":
        be_offset_bp = int(body.get("be_offset_bp", 0))
        # Compute BE based on sessionAvgPrice/avgPrice if available
        entry = float(pos.get("avgPrice", "0") or 0.0)
        if entry <= 0:
            raise HTTPException(400, "avgPrice missing")
        # bp: 1bp = 0.01%
        be_px = entry * (1.0 + (be_offset_bp / 10000.0)) if side == "Buy" else entry * (1.0 - (be_offset_bp / 10000.0))
        req = {
            "category": "linear",
            "symbol": symbol,
            "tpslMode": "Full",
            "stopLoss": fmt_price(be_px, get_instrument(symbol)[0]),  # tick from get_instrument
            "slTriggerBy": "MarkPrice",
            "positionIdx": 0
        }
        log(f"[REQ] trading-stop set_sl: {req}")
        resp = bybit("POST", "/v5/position/trading-stop", req)
        log(f"[RESP] trading-stop set_sl: {resp}")
        return ok({"msg": "be set"})

    elif action == "trail":
        trail_dist = float(body["trail_dist"])  # absolute price distance
        # Bybit TS param is string in USD? You can place via trading-stop with takeProfit/stopLoss/trailingStop
        req = {
            "category": "linear",
            "symbol": symbol,
            "tpslMode": "Full",
            "trailingStop": fmt_qty(trail_dist),
            "positionIdx": 0
        }
        log(f"[REQ] trading-stop set_trail: {req}")
        resp = bybit("POST", "/v5/position/trading-stop", req)
        log(f"[RESP] trading-stop set_trail: {resp}")
        return ok({"msg": "trail set"})

    elif action == "cancel_trail":
        req = {
            "category": "linear",
            "symbol": symbol,
            "tpslMode": "Full",
            "trailingStop": "0",
            "positionIdx": 0
        }
        log(f"[REQ] trading-stop cancel_trail: {req}")
        resp = bybit("POST", "/v5/position/trading-stop", req)
        log(f"[RESP] trading-stop cancel_trail: {resp}")
        return ok({"msg": "trail canceled"})

    elif action == "set_sl":
        sl = float(body["sl"])
        req = {
            "category": "linear",
            "symbol": symbol,
            "tpslMode": "Full",
            "stopLoss": fmt_price(sl, get_instrument(symbol)[0]),
            "slTriggerBy": "MarkPrice",
            "positionIdx": 0
        }
        log(f"[REQ] trading-stop set_sl: {req}")
        resp = bybit("POST", "/v5/position/trading-stop", req)
        log(f"[RESP] trading-stop set_sl: {resp}")
        return ok({"msg": "sl set"})

    else:
        raise HTTPException(400, f"Unknown action: {action}")
