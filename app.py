# bybit_v5_router.py
import os, time, hmac, hashlib, json, math
import requests
from fastapi import FastAPI, Request, HTTPException

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


def _sign(body_str: str) -> dict:
    ts = str(int(time.time() * 1000))
    payload = f"{ts}{API_KEY}{RECV_WINDOW}{body_str}"
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
    headers  = _sign(body_str)
    r = requests.post(BASE_URL + path, headers=headers, data=body_str, timeout=10)
    if r.status_code != 200:
        raise HTTPException(r.status_code, r.text)
    data = r.json()
    if data.get("retCode", 0) != 0:
        raise HTTPException(400, f"{data.get('retCode')} {data.get('retMsg')}: {data}")
    return data

def _get(path: str, params: dict):
    # GET-ekhez Bybitnél a sign alapja: timestamp+apiKey+recvWindow+queryString (v5 guide)
    # Egyszerűség kedvéért itt csak publikus GET-et hívunk (instruments-info)
    r = requests.get(BASE_URL + path, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def get_sym_info(symbol: str):
    j = _get("/v5/market/instruments-info", {"category":"linear","symbol":symbol})
    info = j["result"]["list"][0]
    tick = float(info["priceFilter"]["tickSize"])
    lot  = float(info["lotSizeFilter"]["qtyStep"])
    min_qty = float(info["lotSizeFilter"]["minOrderQty"])
    return tick, lot, min_qty

def round_to(x, step):
    return math.floor(x / step) * step

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import json, time, math

@app.post("/tv")
async def tv_webhook(req: Request):
    # --- nyers log
    raw_bytes = await req.body()
    raw_text  = raw_bytes.decode("utf-8", "ignore")
    print("INCOMING /tv RAW:", raw_text)

    # --- JSON parse
    try:
        body = json.loads(raw_text) if raw_text else {}
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"invalid json: {e}"}, status_code=400)

    # --- secret több helyről
    body_secret = body.get("secret")
    qp_secret   = req.query_params.get("secret")
    hdr_secret  = req.headers.get("x-alert-secret")
    secret      = body_secret or qp_secret or hdr_secret
    if secret != SHARED:
        return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)

    # --- PING
    if body.get("type") == "ping":
        return JSONResponse({"ok": True, "kind": "ping"}, status_code=200)

    # --- kötelező mezők
    required = ["symbol", "side", "qty", "sl", "tp1", "tp2"]
    miss = [k for k in required if k not in body]
    if miss:
        return JSONResponse({"ok": False, "error": f"missing fields: {miss}"}, status_code=400)

    symbol = str(body["symbol"])
    side   = str(body["side"]).upper()          # LONG / SHORT
    qty    = float(body["qty"])
    tp1    = float(body["tp1"])
    tp2    = float(body["tp2"])
    sl     = float(body["sl"])

    # --- szimbólum meta
    try:
        info = _get("/v5/market/instruments-info", {"category":"linear","symbol":symbol})
        sym = info["result"]["list"][0]
        tick = float(sym["priceFilter"]["tickSize"])
        lot  = float(sym["lotSizeFilter"]["qtyStep"])
        min_qty = float(sym["lotSizeFilter"]["minOrderQty"])
        print(f"[INFO] {symbol} tick={tick} lot={lot} min_qty={min_qty}")
    except Exception as e:
        print("[ERR] instruments-info:", repr(e))
        return JSONResponse({"ok": False, "error": f"instruments-info failed: {e}"}, status_code=400)

    def round_to(x, step):
        return math.floor(x / step) * step

    def round_price(p):
        # tickre kerekítés, fix decimális formátum
        return f"{round_to(p, tick):.8f}".rstrip("0").rstrip(".")

    def round_qty(q):
        # qtyStep-re lefelé kerekítés, majd min_qty biztosítása
        q_ = round_to(q, lot)
        if q_ < min_qty:
            q_ = min_qty
        # formázás
        return f"{q_:.8f}".rstrip("0").rstrip(".")

    # --- qty előkészítés
    qty_rounded = float(round_qty(qty))
    print(f"[INFO] qty_in={qty}, qty_rounded={qty_rounded}")

    # --- hedge mód kezelése (ha kell)
    # One-Way-ben 0 vagy elhagyható; Hedge-ben 1=Long, 2=Short
    positionIdx = 0
    if body.get("positionIdx") in (0,1,2):
        positionIdx = int(body["positionIdx"])
    elif body.get("hedge", False):
        positionIdx = 1 if side=="LONG" else 2

   # --- Market belépő (meglévő kódod maradhat)
print("[REQ] order/create ENTRY:", entry)
entry_resp = _post("/v5/order/create", entry)
print("[RESP] order/create ENTRY:", entry_resp)

# === ÚJ: várunk, míg a pozíció tényleg látszik, hogy a reduceOnly TP-ket ne dobja vissza
def wait_position_filled(symbol: str, want_side: str, tries=10, sleep_s=0.25) -> float:
    # want_side: "LONG"/"SHORT"
    import time as _t
    for i in range(tries):
        pos = _get("/v5/position/list", {"category":"linear", "symbol": symbol})
        lst = pos.get("result", {}).get("list", []) or []
        # one-way módban 1 sor van; méret string
        size = 0.0
        side_str = ""
        if lst:
            size = float(lst[0].get("size") or 0)
            side_str = lst[0].get("side") or ""
        print(f"[INFO] poll pos {i+1}/{tries}: side={side_str} size={size}")
        if size > 0:
            return size
        _t.sleep(sleep_s)
    return 0.0

filled_size = wait_position_filled(symbol, side)

# === ÚJ: TP darabok – sose legyen 0. lot/min_qty figyelembevétele
tp1_ratio = 0.30
tp1_qty_val = round_to(qty_rounded * tp1_ratio, lot)
tp2_qty_val = round_to(qty_rounded - tp1_qty_val, lot)

# ha TP1 0-ra kerekedne, osszuk  a min lot szerint:
if tp1_qty_val < min_qty:
    if qty_rounded >= 2 * min_qty:
        tp1_qty_val = round_to(min_qty, lot)
        tp2_qty_val = round_to(qty_rounded - tp1_qty_val, lot)
    else:
        # túl kicsi az össz-mennyiség – mindent egy TP-re rakunk
        tp1_qty_val = 0.0
        tp2_qty_val = qty_rounded

print(f"[INFO] tp1_qty={tp1_qty_val} tp2_qty={tp2_qty_val} (lot={lot}, min_qty={min_qty})")

# === ÚJ: reduceOnly TP-k lerakása – csak ha tényleg van nyitott pozíció
def place_tp(px, q, suffix):
    if q <= 0:
        print(f"[SKIP] {suffix} qty=0"); return None
    body_tp = {
        "category":"linear","symbol":symbol,
        "side":"Sell" if side=="LONG" else "Buy",
        "orderType":"Limit",
        "price": round_price(px),
        "qty":   round_qty(q),
        "timeInForce":"GoodTillCancel",
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
    except Exception as e:
        # több log, ha Bybit 400-al tér vissza (pl. "Reduce-only rule not satisfied")
        print(f"[ERR] TP {suffix} create failed:", repr(e))
        raise

# TP-k
tp1_resp = place_tp(tp1, tp1_qty_val, "TP1")
tp2_resp = place_tp(tp2, tp2_qty_val, "TP2")

# SL (pozíció szintű) – ezt korábban is hívtuk, hagyd meg:
ts_body = {
    "category":"linear","symbol":symbol,
    "stopLoss": round_price(sl)
}
if positionIdx != 0:
    ts_body["positionIdx"] = positionIdx
print("[REQ] position/trading-stop SL:", ts_body)
ts_resp = _post("/v5/position/trading-stop", ts_body)
print("[RESP] position/trading-stop SL:", ts_resp)

return {
    "ok": True,
    "orderId": entry_resp["result"]["orderId"],
    "tp1": tp1_resp["result"]["orderId"] if tp1_resp else None,
    "tp2": tp2_resp["result"]["orderId"] if tp2_resp else None,
    "sl_set": True
}

