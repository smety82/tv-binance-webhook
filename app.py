import os, time, math, logging, functools
from datetime import datetime, timezone
from flask import Flask, request, jsonify, redirect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from pybit.unified_trading import HTTP

# ── App+log ───────────────────────────────────────────────────────────────────
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tv-bybit-webhook")

limiter = Limiter(key_func=get_remote_address, app=app, default_limits=["10 per minute"])

# ── Env ──────────────────────────────────────────────────────────────────────
BYBIT_API_KEY     = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET  = os.getenv("BYBIT_API_SECRET")
BYBIT_TESTNET     = os.getenv("BYBIT_TESTNET", "true").lower() == "true"
CATEGORY          = os.getenv("BYBIT_CATEGORY", "spot")   # "spot" | "linear"
WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET")
ALLOWED_SYMBOLS   = set((os.getenv("ALLOWED_SYMBOLS") or "BTCUSDT,ETHUSDT").replace(" ", "").split(","))

# Risk knobs (állítható env-ből)
MAX_QUOTE         = float(os.getenv("MAX_QUOTE", "20000"))     # spot quote cap
DEFAULT_QTY       = float(os.getenv("DEFAULT_QTY", "0.001"))   # linear alap qty
MAX_QTY           = float(os.getenv("MAX_QTY", "1.0"))         # linear cap
MAX_DAILY_LOSS    = float(os.getenv("MAX_DAILY_LOSS_USDT", "100"))  # net PnL lock
TRADE_COOLDOWN_S  = int(os.getenv("TRADE_COOLDOWN_SEC", "30"))
DEDUP_WINDOW_S    = int(os.getenv("DEDUP_WINDOW_SEC", "15"))

# Per-stratégia allokáció (összesen 1.0 körül)
ALLOC = {
    "ema_cross":   float(os.getenv("ALLOC_EMA", "0.2")),
    "supertrend":  float(os.getenv("ALLOC_ST", "0.2")),
    "donchian_brk":float(os.getenv("ALLOC_DON", "0.2")),
    "bb_revert":   float(os.getenv("ALLOC_BB", "0.15")),
    "rsi_macd":    float(os.getenv("ALLOC_RM", "0.15")),
    "ichimoku":    float(os.getenv("ALLOC_ICH", "0.1")),
}

def _mask(s, keep=4):
    if not s: return "MISSING"
    s = str(s)
    return s[:keep] + "..." + s[-keep:] if len(s) > keep*2 else "***"

log.info("BYBIT_TESTNET=%s", os.getenv("BYBIT_TESTNET"))
log.info("BYBIT_CATEGORY=%s", os.getenv("BYBIT_CATEGORY"))
log.info("BYBIT_API_KEY(masked)=%s", _mask(BYBIT_API_KEY))
log.info("BYBIT_API_SECRET(masked)=%s", _mask(BYBIT_API_SECRET))
log.info("ALLOWED_SYMBOLS=%s", ",".join(sorted(ALLOWED_SYMBOLS)))

# ── Bybit HTTP kliens ────────────────────────────────────────────────────────
session = HTTP(
    testnet=BYBIT_TESTNET,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
)

# ── Cache / state (in-memory; ha újraindul a dyno, nullázódik) ───────────────
_instruments_cache = {}
_last_signal = {}       # key: (strategy_id, symbol, signal) -> ts
_cooldown_until = {}    # symbol -> ts
_daily = {"date": None, "loss": 0.0}

def _utc_date_today():
    return datetime.now(timezone.utc).date()

def _reset_daily_if_newdate():
    today = _utc_date_today()
    if _daily["date"] != today:
        _daily["date"] = today
        _daily["loss"] = 0.0

# ── Helpers ──────────────────────────────────────────────────────────────────
def get_instrument_info(category, symbol):
    key = (category, symbol)
    now = time.time()
    cached = _instruments_cache.get(key)
    if cached and now - cached["ts"] < 300:
        return cached["data"]
    data = session.get_instruments_info(category=category, symbol=symbol)
    _instruments_cache[key] = {"ts": now, "data": data}
    return data

def get_filters(category, symbol):
    info = get_instrument_info(category, symbol)
    lst = (((info or {}).get("result") or {}).get("list") or [])
    if not lst:
        return {}
    item = lst[0]
    lot = item.get("lotSizeFilter", {}) or {}
    prc = item.get("priceFilter", {}) or {}
    return {
        "minOrderAmt": float(lot.get("minOrderAmt") or 0),      # spot
        "minOrderQty": float(lot.get("minOrderQty") or 0),
        "qtyStep":     float(lot.get("qtyStep") or 0.00000001),
        "tickSize":    float(prc.get("tickSize") or 0.01),
    }

def round_step(value, step):
    if step <= 0:
        return value
    return math.floor(float(value)/step) * step

def get_available_usdt(account_type="UNIFIED"):
    r = session.get_wallet_balance(accountType=account_type)
    result = (r or {}).get("result") or {}
    lst = (result.get("list") or [])
    if not lst: return 0.0
    coins = lst[0].get("coin") or []
    for c in coins:
        if c.get("coin") == "USDT":
            avail = c.get("availableToUse") or c.get("availableToWithdraw") or c.get("walletBalance") or "0"
            try: return float(avail)
            except: return 0.0
    return 0.0

def get_last_price(category, symbol):
    r = session.get_tickers(category=category, symbol=symbol)
    lst = (((r or {}).get("result") or {}).get("list") or [])
    if not lst: return None
    return float(lst[0].get("lastPrice"))

# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/")
def index():
    return {"service": "tv-bybit-webhook-v2", "endpoints": ["/health", "/authcheck", "/webhook"]}, 200

@app.post("/")
def root_post():
    return redirect("/webhook", code=307)

@app.get("/health")
def health():
    return {"ok": True}, 200

@app.get("/authcheck")
def authcheck():
    try:
        r = session.get_wallet_balance(accountType="UNIFIED")
        return {"ok": True, "retCode": r.get("retCode"), "retMsg": r.get("retMsg",""), "resultKeys": list((r.get("result") or {}).keys())}, 200
    except Exception as e:
        log.exception("authcheck failed")
        return {"ok": False, "error": str(e)}, 500

# ── Core webhook ─────────────────────────────────────────────────────────────
@app.route("/webhook", methods=["POST"])
@limiter.limit("10 per minute")
def webhook():
    _reset_daily_if_newdate()

    data = request.get_json(silent=True) or {}
    log.info("incoming: %s", data)

    # Secret check
    if WEBHOOK_SECRET and data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error":"Unauthorized"}), 403

    # Required fields
    strategy_id = (data.get("strategy_id") or "unknown").lower()
    signal      = (data.get("signal") or "").lower()  # "buy" | "sell"
    symbol      = (data.get("symbol") or "BTCUSDT").upper().replace("BYBIT:", "")
    category    = (data.get("category") or CATEGORY).lower()

    if signal not in {"buy","sell"}:
        return jsonify({"error":"Invalid signal"}), 400
    if symbol not in ALLOWED_SYMBOLS:
        return jsonify({"error":f"Symbol {symbol} not allowed"}), 400
    if category not in {"spot","linear"}:
        return jsonify({"error":"Invalid category"}), 400

    # Risk: dedup window
    key = (strategy_id, symbol, signal)
    now = time.time()
    last_ts = _last_signal.get(key, 0)
    if now - last_ts < DEDUP_WINDOW_S:
        return jsonify({"skipped":"duplicate_signal_window"}), 200
    _last_signal[key] = now

    # Risk: cooldown per symbol
    until = _cooldown_until.get(symbol, 0)
    if now < until:
        return jsonify({"skipped":"cooldown_active","until": until}), 200

    # Risk: max daily loss lock
    if _daily["loss"] <= -abs(MAX_DAILY_LOSS):
        return jsonify({"skipped":"daily_loss_lock","loss": _daily["loss"], "limit": -abs(MAX_DAILY_LOSS)}), 200

    # Sizing/allocation
    alloc = ALLOC.get(strategy_id, 0.1)
    tp_pct  = float(data.get("tp_pct")  or 0)
    sl_pct  = float(data.get("sl_pct")  or 0)
    trail_p = float(data.get("trail_pct") or 0)

    params = {
        "category": category,
        "symbol": symbol,
        "side": "Buy" if signal=="buy" else "Sell",
        "orderType": "Market",
    }

    try:
        filters = get_filters(category, symbol)
        qtyStep  = filters.get("qtyStep", 0.00000001)
        minAmt   = filters.get("minOrderAmt", 0.0)  # spot
        minQty   = filters.get("minOrderQty", 0.0)

        # Balances
        avail_usdt = get_available_usdt("UNIFIED")

        if category == "spot":
            # preferált: quote-ban jövünk (script is így küldi)
            quote_qty = data.get("quote_qty")
            if quote_qty is None:
                # ha nincs a payloadban, méretezzünk allokációval
                # max költés: elérhető USDT * alloc * 0.98
                max_spend = max(0.0, avail_usdt * alloc * 0.98)
                if max_spend <= 0:
                    return jsonify({"error":"insufficient_balance","available_usdt":avail_usdt}), 400
                quote_qty = max_spend
            q = float(quote_qty)

            # min notional
            if minAmt and q < minAmt:
                return jsonify({"error":"min_notional","minOrderAmt":minAmt,"try_at_least":minAmt}), 400
            if q <= 0 or q > MAX_QUOTE:
                return jsonify({"error":"invalid_quote","limits":[0,MAX_QUOTE]}), 400

            params["qty"] = str(q)
            params["marketUnit"] = "quoteCoin"

        else:  # linear
            base_qty = data.get("quantity")
            if base_qty is None:
                # lastPrice alapján méretezzünk allokációval
                last = get_last_price("linear", symbol) or 0
                # cél notional = elérhető_usdt * alloc * 0.98
                target_notional = max(0.0, avail_usdt * alloc * 0.98)
                est_qty = target_notional/last if last>0 else DEFAULT_QTY
                base_qty = max(est_qty, DEFAULT_QTY)
            q = round_step(float(base_qty), qtyStep)
            if minQty and q < minQty:
                q = max(minQty, q)
                q = round_step(q, qtyStep)
            if q <= 0 or q > MAX_QTY:
                return jsonify({"error":"invalid_quantity","limits":[0,MAX_QTY]}), 400
            params["qty"] = str(q)

        # Cooldown beállítása (sikeres order után frissítjük végleg)
        _cooldown_until[symbol] = now + TRADE_COOLDOWN_S

        # OrderLinkId a deduphoz
        order_link_id = f"{strategy_id}-{signal}-{int(now*1000)}"
        params["orderLinkId"] = order_link_id

        # Place order
        res = session.place_order(**params)
        log.info("order resp: %s", res)
        if res.get("retCode") != 0:
            return jsonify({"error":"bybit_error","retCode":res.get("retCode"),"retMsg":res.get("retMsg"),"res":res}), 502

        # ── TP/SL/trailing ─ only linear (pozícióhoz kötve)
        ts_resp = None
        if category == "linear" and (tp_pct>0 or sl_pct>0 or trail_p>0):
            last = get_last_price("linear", symbol) or 0
            tp_price = last * (1 + tp_pct/100) if tp_pct>0 else None
            sl_price = last * (1 - sl_pct/100) if sl_pct>0 else None
            ts_resp = session.set_trading_stop(
                category="linear",
                symbol=symbol,
                takeProfit=str(tp_price) if tp_price else None,
                stopLoss=str(sl_price) if sl_price else None,
                tpTriggerBy="LastPrice",
                slTriggerBy="LastPrice",
                trailingStop=str(trail_p) if trail_p>0 else None
            )

        return jsonify({
            "message":"order executed",
            "params": params,
            "tp_sl_set": bool(ts_resp) if category=="linear" else False,
            "res": res
        }), 200

    except Exception as e:
        log.exception("order failed")
        return jsonify({"error":"server_error","detail":str(e)}), 500

# ── main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
