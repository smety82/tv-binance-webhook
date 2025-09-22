import os, time, hmac, hashlib, json, math, urllib.parse, datetime
import requests
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import JSONResponse

# -------- Env helpers --------
def require_env(name: str, default: str | None = None):
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing environment variable: {name}")
    return v

API_KEY      = require_env("BYBIT_KEY")
API_SECRET   = require_env("BYBIT_SECRET").encode()
BASE_URL     = require_env("BYBIT_BASE", "https://api-testnet.bybit.com")
RECV_WINDOW  = require_env("RECV_WINDOW", "5000")
SHARED       = require_env("SHARED_SECRET")

app = FastAPI()

@app.get("/")
def health():
    mode = "mainnet" if "api.bybit.com" in BASE_URL else "testnet"
    return {"ok": True, "mode": mode}

@app.head("/")
def health_head():
    return Response(status_code=200)

# -------- Sign helpers (v5) --------
def _sign_headers(ts: str, payload: str):
    sign = hmac.new(API_SECRET, payload.encode(), hashlib.sha256).hexdigest()
    return {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": sign,
        "X-BAPI-SIGN-TYPE": "2",
        "Content-Type": "application/json",
    }

def _post(path: str, body: dict):
    body_str = json.dumps(body, separators=(",", ":"))
    ts = str(int(time.time() * 1000))
    payload = f"{ts}{API_KEY}{RECV_WINDOW}{body_str}"
    headers = _sign_headers(ts, payload)
    r = requests.post(BASE_URL + path, headers=headers, data=body_str, timeout=20)
    try:
        data = r.json()
    except Exception:
        raise HTTPException(r.status_code, r.text)
    if data.get("retCode", -1) != 0:
        raise HTTPException(400, f"POST {path} failed: {data}")
    return data

def _get_public(path: str, params: dict):
    r = requests.get(BASE_URL + path, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def _get_private(path: str, params: dict):
    ts = str(int(time.time() * 1000))
    qs = urllib.parse.urlencode(sorted(params.items()), doseq=True)
    payload = f"{ts}{API_KEY}{RECV_WINDOW}{qs}"
    headers = _sign_headers(ts, payload)
    url = BASE_URL + path + ("?" + qs if qs else "")
    r = requests.get(url, headers=headers, timeout=20)
    try:
        data = r.json()
    except Exception:
        raise HTTPException(r.status_code, r.text)
    if data.get("retCode", -1) != 0:
        raise HTTPException(400, f"GET {path} failed: {data}")
    return data

# -------- Market meta + rounding --------
def get_symbol_filters(symbol: str):
    j = _get_public("/v5/market/instruments-info", {"category": "linear", "symbol": symbol})
    sym = j["result"]["list"][0]
    tick = float(sym["priceFilter"]["tickSize"])
    lot = float(sym["lotSizeFilter"]["qtyStep"])
    min_qty = float(sym["lotSizeFilter"]["minOrderQty"])
    return tick, lot, min_qty

def get_last_price(symbol: str) -> float:
    j = _get_public("/v5/market/tickers", {"category": "linear", "symbol": symbol})
    last = float(j["result"]["list"][0]["lastPrice"])
    return last

def round_to_step(x: float, step: float) -> float:
    return math.floor(float(x) / step) * step

def fmt_num(x: float) -> str:
    s = f"{float(x):.8f}"
    s = s.rstrip("0").rstrip(".")
    return s if s != "" else "0"

def round_price(p: float, tick: float) -> str:
    return fmt_num(round_to_step(p, tick))

def round_qty(q: float, lot: float, min_qty: float) -> str:
    q_ = round_to_step(q, lot)
    if q_ < min_qty:
        q_ = min_qty
    return fmt_num(q_)

# -------- Secret helper --------
def _check_secret(req: Request, body: dict) -> bool:
    return (body.get("secret")
            or req.query_params.get("secret")
            or req.headers.get("x-alert-secret")) == SHARED

# -------- Position & orders helpers --------
def get_position_list(symbol: str):
    pos = _get_private("/v5/position/list", {"category": "linear", "symbol": symbol})
    return pos.get("result", {}).get("list", []) or []

def get_position(symbol: str):
    lst = get_position_list(symbol)
    return lst[0] if lst else None

def autodetect_position_idx(symbol: str, explicit_idx: int | None = None) -> int:
    if explicit_idx in (0, 1, 2):
        return explicit_idx
    try:
        p = get_position(symbol)
        if not p:
            return 0
        idx = int(p.get("positionIdx") or 0)
        return idx if idx in (0, 1, 2) else 0
    except Exception:
        return 0

def list_open_orders(symbol: str, category: str = "linear"):
    j = _get_private("/v5/order/realtime", {"category": category, "symbol": symbol})
    return j.get("result", {}).get("list", []) or []

# -------- Equity helper (for risk sizing) --------
def get_total_equity() -> float:
    # try UNIFIED first
    try:
        j = _get_private("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        lst = j.get("result", {}).get("list", []) or []
        if lst and lst[0].get("totalEquity") is not None:
            return float(lst[0]["totalEquity"])
    except Exception:
        pass
    # fallback CONTRACT
    try:
        j = _get_private("/v5/account/wallet-balance", {"accountType": "CONTRACT"})
        lst = j.get("result", {}).get("list", []) or []
        if lst:
            te = lst[0].get("equity") or lst[0].get("walletBalance")
            if te is not None:
                return float(te)
    except Exception:
        pass
    return 0.0

# ======================================================
# ===============  DAILY LOSS GUARD ====================
# ======================================================
import datetime
GUARD = {
    "enabled": False,
    "limit_pct": None,
    "limit_usd": None,
    "baseline": None,
    "block": False,
    "start_date": None,
}

def _today_utc() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")

def _guard_status_now():
    eq = get_total_equity()
    base = GUARD["baseline"] or eq
    dd_usd = max(0.0, base - eq)
    dd_pct = (dd_usd / base * 100.0) if base > 0 else 0.0
    return {
        "enabled": GUARD["enabled"],
        "limit_pct": GUARD["limit_pct"],
        "limit_usd": GUARD["limit_usd"],
        "baseline": base,
        "equity_now": eq,
        "drawdown_usd": dd_usd,
        "drawdown_pct": dd_pct,
        "block": GUARD["block"],
        "start_date": GUARD["start_date"],
    }

def guard_reset_baseline():
    eq = get_total_equity()
    GUARD["baseline"] = eq
    GUARD["block"] = False
    GUARD["start_date"] = _today_utc()
    return _guard_status_now()

def guard_check_and_update_block() -> dict:
    if not GUARD["enabled"]:
        return {"ok": True, "blocked": False, "reason": None, "status": _guard_status_now()}
    if GUARD["start_date"] != _today_utc():
        guard_reset_baseline()
    st = _guard_status_now()
    lim_pct = GUARD["limit_pct"]; lim_usd = GUARD["limit_usd"]
    should_block = False; reasons = []
    if lim_pct is not None and st["drawdown_pct"] >= float(lim_pct):
        should_block = True; reasons.append(f"drawdown_pct {st['drawdown_pct']:.2f}% >= limit_pct {float(lim_pct):.2f}%")
    if lim_usd is not None and st["drawdown_usd"] >= float(lim_usd):
        should_block = True; reasons.append(f"drawdown_usd {st['drawdown_usd']:.2f} >= limit_usd {float(lim_usd):.2f}")
    GUARD["block"] = should_block
    return {"ok": not should_block, "blocked": should_block, "reason": "; ".join(reasons) if reasons else None, "status": _guard_status_now()}

@app.post("/guard")
async def guard_set(req: Request):
    body = await req.json()
    if not _check_secret(req, body): return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)
    if body.get("reset"): st = guard_reset_baseline(); return {"ok": True, "action": "reset", "status": st}
    if "enable" in body:
        GUARD["enabled"] = bool(body["enable"])
        if GUARD["enabled"]: guard_reset_baseline()
        else: GUARD["block"] = False
    if "limit_pct" in body and body["limit_pct"] is not None: GUARD["limit_pct"] = float(body["limit_pct"])
    if "limit_usd" in body and body["limit_usd"] is not None: GUARD["limit_usd"] = float(body["limit_usd"])
    return {"ok": True, "status": _guard_status_now()}

@app.get("/guard_status")
async def guard_status(secret: str):
    if secret != SHARED: return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)
    return {"ok": True, "status": _guard_status_now()}

@app.post("/guard_reset")
async def guard_reset(req: Request):
    body = await req.json()
    if not _check_secret(req, body): return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)
    st = guard_reset_baseline(); return {"ok": True, "status": st}

# ======================================================
# ===============  TradingView webhook  ================
# ======================================================
@app.post("/tv")
async def tv_webhook(req: Request):
    raw_text = (await req.body()).decode("utf-8", "ignore")
    print("INCOMING /tv RAW:", raw_text)
    try:
        body = json.loads(raw_text) if raw_text else {}
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"invalid json: {e}"}, status_code=400)
    if not _check_secret(req, body):
        return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)
    if body.get("type") == "ping":
        return JSONResponse({"ok": True, "kind": "ping"}, status_code=200)

    required = ["symbol", "side", "sl", "tp1", "tp2"]  # qty or riskPct later
    missing = [k for k in required if k not in body]
    if missing:
        return JSONResponse({"ok": False, "error": f"missing fields: {missing}"}, status_code=400)

    # Guard
    g = guard_check_and_update_block()
    if g["blocked"]:
        print("[GUARD] BLOCKED:", g["reason"])
        return JSONResponse({"ok": False, "blocked": True, "reason": g["reason"], "guard": g["status"]}, status_code=200)

    symbol = str(body["symbol"])
    side = str(body["side"]).upper()       # LONG/SHORT
    sl   = float(body["sl"])
    tp1  = float(body["tp1"])
    tp2  = float(body["tp2"])

    # Hedge index autodetect/override
    positionIdx = 0
    if body.get("positionIdx") in (0, 1, 2): positionIdx = int(body["positionIdx"])
    elif body.get("hedge", False): positionIdx = 1 if side == "LONG" else 2

    # Filters
    try:
        tick, lot, min_qty = get_symbol_filters(symbol)
        print(f"[INFO] {symbol} tick={tick} lot={lot} min_qty={min_qty}")
    except Exception as e:
        print("[ERR] instruments-info:", repr(e))
        return JSONResponse({"ok": False, "error": f"instruments-info failed: {e}"}, status_code=400)

    # ---------- SIZING: qty OR riskPct ----------
    qty_in = float(body.get("qty") or 0.0)
    qty_rounded_f = 0.0
    sizing_mode = "qty"

    if qty_in > 0:
        qty_rounded_f = max(round_to_step(qty_in, lot), min_qty)
        sizing_mode = "qty"
        print(f"[INFO] sizing=payload qty: qty_in={qty_in} -> qty_rounded={qty_rounded_f}")

    else:
        riskPct = body.get("riskPct")
        if riskPct is None: riskPct = body.get("risk_pct")
        if riskPct is None:
            return JSONResponse({"ok": False, "error": "need qty>0 or riskPct"}, status_code=400)
        riskPct = float(riskPct)
        equity = get_total_equity()
        last_px = get_last_price(symbol)
        stopDist = abs(last_px - sl)
        if stopDist <= 0:
            return JSONResponse({"ok": False, "error": "invalid stop distance"}, status_code=400)
        riskUsd = equity * (riskPct / 100.0)
        qty_calc = riskUsd / stopDist
        qty_rounded_f = max(round_to_step(qty_calc, lot), min_qty)
        sizing_mode = "riskPct"
        print(f"[INFO] sizing=riskPct: equity={equity:.4f} riskPct={riskPct:.4f}% riskUsd={riskUsd:.4f} "
              f"lastPx={last_px:.4f} sl={sl:.4f} stopDist={stopDist:.4f} "
              f"qty_calc={qty_calc:.6f} -> qty_rounded={qty_rounded_f}")

    qty_str = fmt_num(qty_rounded_f)

    # ENTRY
    by_side = "Buy" if side == "LONG" else "Sell"
    oid_base = f"TV-{symbol}-{int(time.time()*1000)}"
    entry = {
        "category": "linear", "symbol": symbol,
        "side": by_side, "orderType": "Market",
        "qty": qty_str, "timeInForce": "IOC",
        "reduceOnly": False, "orderLinkId": oid_base
    }
    if positionIdx != 0: entry["positionIdx"] = positionIdx
    print("[REQ] order/create ENTRY:", entry)
    entry_resp = _post("/v5/order/create", entry)
    print("[RESP] order/create ENTRY:", entry_resp)

    # Wait position (so reduceOnly TPs are allowed)
    def wait_position(symbol_: str, tries=12, sleep_s=0.35) -> float:
        for i in range(tries):
            pos = _get_private("/v5/position/list", {"category": "linear", "symbol": symbol_})
            lst = pos.get("result", {}).get("list", []) or []
            size = float(lst[0].get("size") or 0) if lst else 0.0
            pos_side = lst[0].get("side") if lst else ""
            print(f"[INFO] poll pos {i+1}/{tries}: side={pos_side} size={size}")
            if size > 0: return size
            time.sleep(sleep_s)
        return 0.0
    _ = wait_position(symbol)

    # Auto-detect hedge idx after fill
    try:
        p = get_position(symbol)
        if p:
            idx = int(p.get("positionIdx") or 0)
            if idx in (1, 2):
                positionIdx = idx
                print(f"[INFO] auto positionIdx={positionIdx} (hedge mode detected)")
    except Exception as e:
        print("[WARN] could not auto-detect positionIdx:", repr(e))

    # TP split 30/70
    tp1_ratio = 0.30
    tp1_qty_f = round_to_step(qty_rounded_f * tp1_ratio, lot)
    tp2_qty_f = round_to_step(qty_rounded_f - tp1_qty_f, lot)
    if tp1_qty_f < min_qty:
        if qty_rounded_f >= 2 * min_qty:
            tp1_qty_f = round_to_step(min_qty, lot)
            tp2_qty_f = round_to_step(qty_rounded_f - tp1_qty_f, lot)
        else:
            tp1_qty_f = 0.0
            tp2_qty_f = qty_rounded_f
    print(f"[INFO] tp1_qty={tp1_qty_f} tp2_qty={tp2_qty_f}")

    def place_tp(px: float, qf: float, suffix: str):
        if qf <= 0:
            print(f"[SKIP] {suffix} qty=0"); return None
        body_tp = {
            "category": "linear", "symbol": symbol,
            "side": "Sell" if side == "LONG" else "Buy",
            "orderType": "Limit",
            "price": round_price(px, tick),
            "qty": round_qty(qf, lot, min_qty),
            "timeInForce": "GTC",
            "reduceOnly": True,
            "orderLinkId": f"{oid_base}-{suffix}"
        }
        if positionIdx != 0: body_tp["positionIdx"] = positionIdx
        print(f"[REQ] order/create {suffix}:", body_tp)
        try:
            r = _post("/v5/order/create", body_tp)
            print(f"[RESP] order/create {suffix}:", r)
            return r
        except HTTPException as e:
            print(f"[ERR] order/create {suffix} failed:", e.detail)
            return None

    tp1_resp = place_tp(tp1, tp1_qty_f, "TP1")
    tp2_resp = place_tp(tp2, tp2_qty_f, "TP2")

    # SL via trading-stop + fallback
    sl_px = round_price(sl, tick)
    ts_body = {"category":"linear","symbol":symbol,"stopLoss":sl_px,"slTriggerBy":"MarkPrice","tpslMode":"Full"}
    if positionIdx != 0: ts_body["positionIdx"] = positionIdx
    print("[REQ] position/trading-stop SL (MarkPrice):", ts_body)
    sl_set, sl_mode = False, "position"
    try:
        ts_resp = _post("/v5/position/trading-stop", ts_body)
        print("[RESP] position/trading-stop SL:", ts_resp)
        sl_set = True
    except HTTPException as e1:
        print("[WARN] trading-stop (MarkPrice) failed:", e1.detail)
        ts_body["slTriggerBy"] = "LastPrice"
        print("[REQ] position/trading-stop SL (LastPrice):", ts_body)
        try:
            ts_resp = _post("/v5/position/trading-stop", ts_body)
            print("[RESP] position/trading-stop SL:", ts_resp)
            sl_set = True
        except HTTPException as e2:
            print("[ERR] trading-stop failed both triggers:", e2.detail)
            trig_dir = 2 if side == "LONG" else 1
            fallback = {
                "category":"linear","symbol":symbol,
                "side":"Sell" if side=="LONG" else "Buy",
                "orderType":"Market","timeInForce":"IOC","reduceOnly":True,
                "qty": fmt_num(qty_rounded_f),
                "triggerPrice": sl_px,"triggerBy":"MarkPrice","triggerDirection":trig_dir,
                "orderLinkId": f"{oid_base}-SLB"
            }
            if positionIdx != 0: fallback["positionIdx"] = positionIdx
            print("[REQ] order/create FALLBACK stop-market:", fallback)
            try:
                fb_resp = _post("/v5/order/create", fallback)
                print("[RESP] order/create FALLBACK stop-market:", fb_resp)
                sl_set = True; sl_mode = "stopMarket"
            except HTTPException as e3:
                print("[ERR] fallback stop-market failed:", e3.detail)
                sl_set = False; sl_mode = "none"

    return {
        "ok": True,
        "sizing": sizing_mode,
        "entryId": entry_resp["result"]["orderId"],
        "tp1Id": tp1_resp["result"]["orderId"] if tp1_resp else None,
        "tp2Id": tp2_resp["result"]["orderId"] if tp2_resp else None,
        "sl_set": sl_set,
        "sl_mode": sl_mode
    }

# ======================================================
# ==========  Admin endpoints (secured by secret) ======
# ======================================================

@app.post("/set_leverage")
async def set_leverage(req: Request):
    body = await req.json()
    if not _check_secret(req, body): return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)
    cat = body.get("category", "linear")
    symbol = body["symbol"]
    lev = body.get("leverage")
    buyL = body.get("buyLeverage", lev)
    sellL = body.get("sellLeverage", lev if buyL is None else buyL)
    if buyL is None or sellL is None:
        return JSONResponse({"ok": False, "error": "missing leverage/buyLeverage"}, status_code=400)
    payload = {"category": cat, "symbol": symbol, "buyLeverage": str(buyL), "sellLeverage": str(sellL)}
    print("[REQ] set-leverage:", payload)
    resp = _post("/v5/position/set-leverage", payload)
    print("[RESP] set-leverage:", resp)
    return {"ok": True, "result": resp}

@app.post("/set_margin_mode")
async def set_margin_mode(req: Request):
    body = await req.json()
    if not _check_secret(req, body): return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)
    cat = body.get("category", "linear")
    symbol = body["symbol"]
    mode = str(body["mode"]).lower()
    tmode = 1 if mode in ("isolated", "iso") else 0
    lev = body.get("leverage")
    if lev is None:
        return JSONResponse({"ok": False, "error": "missing leverage"}, status_code=400)
    payload = {"category": cat,"symbol": symbol,"tradeMode": tmode,"buyLeverage": str(lev),"sellLeverage": str(lev)}
    print("[REQ] switch-isolated:", payload)
    resp = _post("/v5/position/switch-isolated", payload)
    print("[RESP] switch-isolated:", resp)
    return {"ok": True, "result": resp}

@app.post("/set_position_mode")
async def set_position_mode(req: Request):
    body = await req.json()
    if not _check_secret(req, body): return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)
    cat = body.get("category", "linear")
    symbol = body.get("symbol")
    coin = body.get("coin")
    if not symbol and not coin:
        return JSONResponse({"ok": False, "error": "need symbol or coin"}, status_code=400)
    mode = str(body["mode"]).lower()
    m = 3 if mode in ("hedge", "both", "both_sides") else 0
    payload = {"category": cat, "mode": m}
    if symbol: payload["symbol"] = symbol
    if coin:   payload["coin"]   = coin
    print("[REQ] switch-mode:", payload)
    resp = _post("/v5/position/switch-mode", payload)
    print("[RESP] switch-mode:", resp)
    return {"ok": True, "result": resp}

@app.get("/position")
async def read_position(symbol: str, secret: str):
    if secret != SHARED: return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)
    p = get_position(symbol)
    return {"ok": True, "position": p}

# ======================================================
# ===============  Adjust endpoint (BE / TS) ===========
# ======================================================
@app.post("/adjust")
async def adjust(req: Request):
    body = await req.json()
    if not _check_secret(req, body): return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)
    symbol = body["symbol"]; act = str(body["action"]).lower()
    triggerBy = body.get("triggerBy", "MarkPrice")
    positionIdx = autodetect_position_idx(symbol, body.get("positionIdx"))
    tick, lot, _ = get_symbol_filters(symbol)
    pos = get_position(symbol)

    # early exit if no position
    size = 0.0
    try: size = float((pos or {}).get("size") or 0)
    except Exception: size = 0.0
    if size <= 0:
        return JSONResponse({"ok": False, "error": "no position"}, status_code=400)

    if act == "be":
        entry = float(pos.get("avgPrice") or 0)
        side  = (body.get("side") or pos.get("side") or "").upper()
        bp = float(body.get("be_offset_bp", 10.0))
        factor = 1 + (bp / 10000.0) if side in ("BUY","LONG") else 1 - (bp / 10000.0)
        sl_px = round_price(entry * factor, tick)
        payload = {"category":"linear","symbol":symbol,"tpslMode":"Full","stopLoss":sl_px,"slTriggerBy":triggerBy,"positionIdx":positionIdx}
        print("[REQ] trading-stop BE:", payload)
        resp = _post("/v5/position/trading-stop", payload)
        print("[RESP] trading-stop BE:", resp)
        return {"ok": True, "mode": "BE", "sl": sl_px}

    elif act == "set_sl":
        sl = float(body["sl"]); sl_px = round_price(sl, tick)
        payload = {"category":"linear","symbol":symbol,"tpslMode":"Full","stopLoss":sl_px,"slTriggerBy":triggerBy,"positionIdx":positionIdx}
        print("[REQ] trading-stop set_sl:", payload)
        resp = _post("/v5/position/trading-stop", payload)
        print("[RESP] trading-stop set_sl:", resp)
        return {"ok": True, "mode": "SL", "sl": sl_px}

    elif act == "cancel_sl":
        payload = {"category":"linear","symbol":symbol,"tpslMode":"Full","stopLoss":"0","positionIdx":positionIdx}
        print("[REQ] trading-stop cancel_sl:", payload)
        resp = _post("/v5/position/trading-stop", payload)
        print("[RESP] trading-stop cancel_sl:", resp)
        return {"ok": True, "mode": "SL_CANCELLED"}

    elif act == "trail":
        dist = float(body["trail_dist"]); ts = fmt_num(dist)
        payload = {"category":"linear","symbol":symbol,"tpslMode":"Full","trailingStop":ts,"positionIdx":positionIdx}
        if body.get("activePrice") is not None:
            payload["activePrice"] = fmt_num(float(body["activePrice"]))
        print("[REQ] trading-stop trail:", payload)
        resp = _post("/v5/position/trading-stop", payload)
        print("[RESP] trading-stop trail:", resp)
        return {"ok": True, "mode": "TRAIL", "distance": ts}

    elif act == "cancel_trail":
        payload = {"category":"linear","symbol":symbol,"tpslMode":"Full","trailingStop":"0","positionIdx":positionIdx}
        print("[REQ] trading-stop cancel_trail:", payload)
        resp = _post("/v5/position/trading-stop", payload)
        print("[RESP] trading-stop cancel_trail:", resp)
        return {"ok": True, "mode": "TRAIL_CANCELLED"}

    else:
        return JSONResponse({"ok": False, "error": f"unknown action: {act}"}, status_code=400)

# ======================================================
# ===============  Order management utils  =============
# ======================================================
@app.post("/cancel_all")
async def cancel_all(req: Request):
    body = await req.json()
    if not _check_secret(req, body): return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)
    symbol = body["symbol"]; cat = body.get("category", "linear")
    payload = {"category": cat, "symbol": symbol}
    print("[REQ] order/cancel-all:", payload)
    resp = _post("/v5/order/cancel-all", payload)
    print("[RESP] order/cancel-all:", resp)
    return {"ok": True, "result": resp}

@app.post("/cancel_orders")
async def cancel_orders(req: Request):
    body = await req.json()
    if not _check_secret(req, body): return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)
    symbol = body["symbol"]; cat = body.get("category", "linear")
    reduce_only_only = bool(body.get("reduceOnlyOnly", False))
    side_in = body.get("side")
    if side_in:
        s = str(side_in).upper()
        side = "Sell" if s in ("LONG","SELL") and s!="BUY" else ("Buy" if s.startswith("B") else "Sell")
    else:
        side = None
    if not reduce_only_only and side is None:
        payload = {"category": cat, "symbol": symbol}
        print("[REQ] order/cancel-all:", payload)
        resp = _post("/v5/order/cancel-all", payload)
        print("[RESP] order/cancel-all:", resp)
        return {"ok": True, "result": resp, "mode": "all"}

    lst = list_open_orders(symbol, cat)
    to_cancel = []
    for o in lst:
        if side and o.get("side") != side: continue
        if reduce_only_only and not bool(o.get("reduceOnly", False)): continue
        to_cancel.append(o)

    results = []
    for o in to_cancel:
        payload = {"category": cat, "symbol": symbol, "orderId": o["orderId"]}
        print("[REQ] order/cancel:", payload)
        try:
            r = _post("/v5/order/cancel", payload)
            print("[RESP] order/cancel:", r)
            results.append({"orderId": o["orderId"], "ok": True})
        except HTTPException as e:
            print("[ERR] order/cancel failed:", e.detail)
            results.append({"orderId": o["orderId"], "ok": False, "err": str(e.detail)})
    return {"ok": True, "cancelled": results, "total": len(results), "listed": len(lst)}

@app.post("/close_position")
async def close_position(req: Request):
    body = await req.json()
    if not _check_secret(req, body): return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)
    symbol = body["symbol"]; cat = body.get("category", "linear"); which = (body.get("which") or "both").lower()
    tick, lot, min_qty = get_symbol_filters(symbol)
    plist = get_position_list(symbol)
    if not plist: return JSONResponse({"ok": False, "error": "no position"}, status_code=400)
    actions = []
    for p in plist:
        size = float(p.get("size") or 0)
        if size <= 0: continue
        pos_side = (p.get("side") or "").upper()
        idx = int(p.get("positionIdx") or 0)
        if which == "long" and pos_side != "BUY": continue
        if which == "short" and pos_side != "SELL": continue
        close_side = "Sell" if pos_side == "BUY" else "Buy"
        qty_str = round_qty(size, lot, min_qty)
        payload = {"category":cat,"symbol":symbol,"side":close_side,"orderType":"Market","timeInForce":"IOC","reduceOnly":True,"qty":qty_str,"orderLinkId":f"CLOSE-{symbol}-{int(time.time()*1000)}"}
        if idx in (1,2): payload["positionIdx"]=idx
        print("[REQ] order/create CLOSE:", payload)
        try:
            resp = _post("/v5/order/create", payload)
            print("[RESP] order/create CLOSE:", resp)
            actions.append({"positionIdx": idx, "side": pos_side, "qty": qty_str, "ok": True})
        except HTTPException as e:
            print("[ERR] close failed:", e.detail)
            actions.append({"positionIdx": idx, "side": pos_side, "qty": qty_str, "ok": False, "err": str(e.detail)})
    return {"ok": True, "actions": actions}

# -------- Local run --------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
