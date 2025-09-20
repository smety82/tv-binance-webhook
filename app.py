# bybit_v5_router.py
import os, time, hmac, hashlib, json, math
import requests
from fastapi import FastAPI, Request, HTTPException

API_KEY     = os.getenv("BYBIT_KEY")
API_SECRET  = os.getenv("BYBIT_SECRET").encode()
BASE_URL    = os.getenv("BYBIT_BASE", "https://api-testnet.bybit.com")
RECV_WINDOW = os.getenv("RECV_WINDOW", "5000")
SHARED      = os.getenv("SHARED_SECRET", "change-me")

app = FastAPI()

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

@app.post("/tv")
async def tv_webhook(req: Request):
    body = await req.json()
    if body.get("secret") != SHARED:
        raise HTTPException(401, "bad secret")

    symbol = body["symbol"]
    side   = body["side"].upper()            # LONG/SHORT
    qty    = float(body["qty"])
    tp1    = float(body["tp1"])
    tp2    = float(body["tp2"])
    sl     = float(body["sl"])

    tick, lot, min_qty = get_sym_info(symbol)
    qty_rounded = max(round_to(qty, lot), min_qty)

    # 1) (opcionális) Leverage beállítás egyszer (szimbólumonként)
    # _post("/v5/position/set-leverage", {"category":"linear","symbol":symbol,"buyLeverage":"3","sellLeverage":"3"})

    # 2) Market belépő (One-Way módban nem kell positionIdx)
    by_side = "Buy" if side == "LONG" else "Sell"
    entry = {
        "category":"linear","symbol":symbol,
        "side":by_side,"orderType":"Market",
        "qty": str(qty_rounded), "timeInForce":"IOC", "reduceOnly": False
    }
    entry_resp = _post("/v5/order/create", entry)

    # 3) Két reduceOnly TP limit
    tp1_qty = str(round_to(qty_rounded * 0.30, lot))
    tp2_qty = str(round_to(qty_rounded - float(tp1_qty), lot))
    # Ha a kerekítés miatt 0 lenne valamelyik, toljuk a másikra:
    if float(tp1_qty) <= 0: tp1_qty = "0"
    if float(tp2_qty) <= 0: tp2_qty = str(qty_rounded)

    def place_tp(px, q):
        if float(q) <= 0: return
        return _post("/v5/order/create", {
            "category":"linear","symbol":symbol,
            "side":"Sell" if side=="LONG" else "Buy",
            "orderType":"Limit","price": str(round_to(px, tick)),
            "qty": q, "timeInForce":"GoodTillCancel","reduceOnly": True
        })

    place_tp(tp1, tp1_qty)
    place_tp(tp2, tp2_qty)

    # 4) Pozíció SL (position trading-stop) – ár iránytól függően
    _post("/v5/position/trading-stop", {
        "category":"linear","symbol":symbol,
        "stopLoss": str(round_to(sl, tick))
    })

    return {"ok": True, "entry": entry_resp["result"]["orderId"]}
