import os, time, hmac, hashlib, json, math, urllib.parse
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
    r = requests.post(BASE_URL + path, headers=headers, data=body_str, timeout=15)
    try:
        data = r.json()
    except Exception:
        raise HTTPException(r.status_code, r.text)
    if data.get("retCode", -1) != 0:
        raise HTTPException(400, f"POST {path} failed: {data}")
    return data

def _get_public(path: str, params: dict):
    r = requests.get(BASE_URL + path, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def _get_private(path: str, params: dict):
    # v5 GET sign: timestamp + apiKey + recvWindow + queryString (ABC szerint rendezve)
    ts = str(int(time.time() * 1000))
    qs = urllib.parse.urlencode(sorted(params.items()), doseq=True)
    payload = f"{ts}{API_KEY}{RECV_WINDOW}{qs}"
    headers = _sign_headers(ts, payload)
    url = BASE_URL + path + ("?" + qs if qs else "")
    r = requests.get(url, headers=headers, timeout=15)
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

# -------- TV webhook --------
@app.post("/tv")
async def tv_webhook(req: Request):
    # Nyers log
    raw_bytes = await req.body()
    raw_text = raw_bytes.decode("utf-8", "ignore")
    print("INCOMING /tv RAW:", raw_text)

    # JSON parse
    try:
        body = json.loads(raw_text) if raw_text else {}
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"invalid json: {e}"}, status_code=400)

    # Secret (body/query/header)
    secret = body.get("secret") or req.query_params.get("secret") or req.headers.get("x-alert-secret")
    if secret != SHARED:
        return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)

    # PING kezelés
    if body.get("type") == "ping":
        return JSONResponse({"ok": True, "kind": "ping"}, status_code=200)

    # Kötelező mezők
    required = ["symbol", "side", "qty", "sl", "tp1", "tp2"]
    missing = [k for k in required if k not in body]
    if missing:
        return JSONResponse({"ok": False, "error": f"missing fields: {missing}"}, status_code=400)

    symbol = str(body["symbol"])
    side = str(body["side"]).upper()  # LONG/SHORT
    qty_in = float(body["qty"])
    sl = float(body["sl"])
    tp1 = float(body["tp1"])
    tp2 = float(body["tp2"])

    # Hedge mód pozíció index (opcionális)
    positionIdx = 0
    if body.get("positionIdx") in (0, 1, 2):
        positionIdx = int(body["positionIdx"])
    elif body.get("hedge", False):
        positionIdx = 1 if side == "LONG" else 2

    # Szimbólum szűrők
    try:
        tick, lot, min_qty = get_symbol_filters(symbol)
        print(f"[INFO] {symbol} tick={tick} lot={lot} min_qty={min_qty}")
    except Exception as e:
        print("[ERR] instruments-info:", repr(e))
        return JSONResponse({"ok": False, "error": f"instruments-info failed: {e}"}, status_code=400)

    # Qty kerekítés
    qty_rounded_f = max(round_to_step(qty_in, lot), min_qty)
    qty_str = fmt_num(qty_rounded_f)
    print(f"[INFO] qty_in={qty_in}, qty_rounded={qty_rounded_f}")

    # --- Market ENTRY
    by_side = "Buy" if side == "LONG" else "Sell"
    oid_base = f"TV-{symbol}-{int(time.time()*1000)}"
    entry = {
        "category": "linear", "symbol": symbol,
        "side": by_side, "orderType": "Market",
        "qty": qty_str, "timeInForce": "IOC",
        "reduceOnly": False, "orderLinkId": oid_base
    }
    if positionIdx != 0:
        entry["positionIdx"] = positionIdx
    print("[REQ] order/create ENTRY:", entry)
    entry_resp = _post("/v5/order/create", entry)
    print("[RESP] order/create ENTRY:", entry_resp)

    # --- Várunk, míg pozíció látszik (reduceOnly miatt)
    def wait_position(symbol_: str, tries=12, sleep_s=0.35) -> float:
        for i in range(tries):
            pos = _get_private("/v5/position/list", {"category": "linear", "symbol": symbol_})
            lst = pos.get("result", {}).get("list", []) or []
            size = float(lst[0].get("size") or 0) if lst else 0.0
            pos_side = lst[0].get("side") if lst else ""
            print(f"[INFO] poll pos {i+1}/{tries}: side={pos_side} size={size}")
            if size > 0:
                return size
            time.sleep(sleep_s)
        return 0.0

    _ = wait_position(symbol)

    # --- Auto-detect positionIdx (Hedge módhoz)
    try:
        cur = _get_private("/v5/position/list", {"category": "linear", "symbol": symbol})
        lst = cur.get("result", {}).get("list", []) or []
        if lst:
            idx = int(lst[0].get("positionIdx") or 0)
            if idx in (1, 2):
                positionIdx = idx
                print(f"[INFO] auto positionIdx={positionIdx} (hedge mode detected)")
    except Exception as e:
        print("[WARN] could not auto-detect positionIdx:", repr(e))

    # --- TP-k qty: sose legyen 0
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

    # --- TP rendelők (nem-halálos)
    def place_tp(px: float, qf: float, suffix: str):
        if qf <= 0:
            print(f"[SKIP] {suffix} qty=0"); return None
        body_tp = {
            "category": "linear", "symbol": symbol,
            "side": "Sell" if side == "LONG" else "Buy",
            "orderType": "Limit",
            "price": round_price(px, tick),
            "qty": round_qty(qf, lot, min_qty),
            "timeInForce": "GTC",               # GTC a Bybit v5-nél
            "reduceOnly": True,
            "orderLinkId": f"{oid_base}-{suffix}"
        }
        if positionIdx != 0:
            body_tp["positionIdx"] = positionIdx
        print(f"[REQ] order/create {suffix}:", body_tp)
        try:
            r = _post("/v5/order/create", body_tp)
            print(f"[RESP] order/create {suffix}:", r)
            return r
        except HTTPException as e:
            # ne álljon meg a folyamat – log és tovább
            print(f"[ERR] order/create {suffix} failed:", e.detail)
            return None

    tp1_resp = place_tp(tp1, tp1_qty_f, "TP1")
    tp2_resp = place_tp(tp2, tp2_qty_f, "TP2")

    # --- Position-level SL (robosztus, fallback-kel)
    sl_px = round_price(sl, tick)

    ts_body = {
        "category": "linear",
        "symbol": symbol,
        "stopLoss": sl_px,
        "slTriggerBy": "MarkPrice",   # explicit trigger forrás
        "tpslMode": "Full"            # pozíció-szintű (nem részleges) TP/SL
    }
    if positionIdx != 0:
        ts_body["positionIdx"] = positionIdx

    print("[REQ] position/trading-stop SL (MarkPrice):", ts_body)

    sl_set = False
    sl_mode = "position"

    try:
        ts_resp = _post("/v5/position/trading-stop", ts_body)
        print("[RESP] position/trading-stop SL:", ts_resp)
        sl_set = True
    except HTTPException as e1:
        print("[WARN] trading-stop (MarkPrice) failed:", e1.detail)
        # próbáljuk LastPrice triggerrel
        ts_body["slTriggerBy"] = "LastPrice"
        print("[REQ] position/trading-stop SL (LastPrice):", ts_body)
        try:
            ts_resp = _post("/v5/position/trading-stop", ts_body)
            print("[RESP] position/trading-stop SL:", ts_resp)
            sl_set = True
        except HTTPException as e2:
            print("[ERR] trading-stop failed both triggers:", e2.detail)
            # --- Fallback: reduceOnly Stop-Market (feltételes) order ---
            # triggerDirection: LONG stopnál lefelé törés -> 2, SHORT stopnál felfelé -> 1
            trig_dir = 2 if side == "LONG" else 1
            fallback = {
                "category": "linear",
                "symbol": symbol,
                "side": "Sell" if side == "LONG" else "Buy",
                "orderType": "Market",
                "timeInForce": "IOC",
                "reduceOnly": True,
                "qty": qty_str,
                "triggerPrice": sl_px,
                "triggerBy": "MarkPrice",
                "triggerDirection": trig_dir,
                "orderLinkId": f"{oid_base}-SLB"
            }
            if positionIdx != 0:
                fallback["positionIdx"] = positionIdx
            print("[REQ] order/create FALLBACK stop-market:", fallback)
            try:
                fb_resp = _post("/v5/order/create", fallback)
                print("[RESP] order/create FALLBACK stop-market:", fb_resp)
                sl_set = True
                sl_mode = "stopMarket"
            except HTTPException as e3:
                print("[ERR] fallback stop-market failed:", e3.detail)
                sl_set = False
                sl_mode = "none"

    return {
        "ok": True,
        "entryId": entry_resp["result"]["orderId"],
        "tp1Id": tp1_resp["result"]["orderId"] if tp1_resp else None,
        "tp2Id": tp2_resp["result"]["orderId"] if tp2_resp else None,
        "sl_set": sl_set,
        "sl_mode": sl_mode
    }

# -------- Local run --------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
