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
import json, time

@app.post("/tv")
async def tv_webhook(req: Request):
    # --- nyers log a könnyebb hibakereséshez
    raw_bytes = await req.body()
    raw_text  = raw_bytes.decode("utf-8", "ignore")
    print("INCOMING /tv RAW:", raw_text)

    # --- próbáljuk JSON-ná alakítani
    try:
        body = json.loads(raw_text) if raw_text else {}
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"invalid json: {e}"}, status_code=400)

    # --- secret több helyről (body / query / header)
    body_secret = body.get("secret")
    qp_secret   = req.query_params.get("secret")
    hdr_secret  = req.headers.get("x-alert-secret")
    secret      = body_secret or qp_secret or hdr_secret
    if secret != SHARED:
        return JSONResponse({"ok": False, "error": "bad secret"}, status_code=401)

    # --- PING kezelése (debugPing a Pine-ban)
    if body.get("type") == "ping":
        return JSONResponse({"ok": True, "kind": "ping"}, status_code=200)

    # --- kötelező mezők ellenőrzése (entry jelhez)
    required = ["symbol", "side", "qty", "sl", "tp1", "tp2"]
    missing  = [k for k in required if k not in body]
    if missing:
        return JSONResponse({"ok": False, "error": f"missing fields: {missing}"}, status_code=400)

    symbol = body["symbol"]
    side   = str(body["side"]).upper()           # LONG / SHORT
    qty    = float(body["qty"])
    tp1    = float(body["tp1"])
    tp2    = float(body["tp2"])
    sl     = float(body["sl"])

    # --- szimbólum meta (tick/lot)
    tick, lot, min_qty = get_sym_info(symbol)
    def round_to(x, step):  # helyi shadow, ha külön is van definiálva, törölhető
        import math
        return max(math.floor(x / step) * step, step)

    qty_rounded = max(round_to(qty, lot), min_qty)

    # --- Market belépő
    by_side = "Buy" if side == "LONG" else "Sell"
    oid_base = f"TV-{symbol}-{int(time.time()*1000)}"
    entry = {
        "category":"linear","symbol":symbol,
        "side":by_side,"orderType":"Market",
        "qty": str(qty_rounded),
        "timeInForce":"IOC","reduceOnly":False,
        "orderLinkId": oid_base
    }
    entry_resp = _post("/v5/order/create", entry)

    # --- TP-k reduceOnly Limittel (30% / 70%)
    tp1_qty = max(round_to(qty_rounded * 0.30, lot), 0.0)
    tp2_qty = max(round_to(qty_rounded - tp1_qty, lot), 0.0)
    if tp1_qty == 0.0 and tp2_qty == 0.0:
        tp2_qty = qty_rounded

    def place_tp(px, q, suffix):
        if q <= 0: return
        _post("/v5/order/create", {
            "category":"linear","symbol":symbol,
            "side":"Sell" if side=="LONG" else "Buy",
            "orderType":"Limit","price": str(round_to(px, tick)),
            "qty": str(q),
            "timeInForce":"GoodTillCancel","reduceOnly": True,
            "orderLinkId": f"{oid_base}-{suffix}"
        })

    place_tp(tp1, tp1_qty, "TP1")
    place_tp(tp2, tp2_qty, "TP2")

    # --- Pozíció szintű SL
    _post("/v5/position/trading-stop", {
        "category":"linear","symbol":symbol,
        "stopLoss": str(round_to(sl, tick))
    })

    return {"ok": True, "orderId": entry_resp["result"]["orderId"]}

