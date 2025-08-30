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

from decimal import Decimal, ROUND_DOWN, getcontext
getcontext().prec = 28  # bőven elég kriptohoz

from decimal import Decimal, ROUND_DOWN, getcontext
getcontext().prec = 28  # bőséges pontosság

def decimals_for_step(step: float) -> int:
    s = f"{step:.16f}".rstrip("0").rstrip(".")
    return len(s.split(".")[1]) if "." in s else 0

def quantize_down(value: float, step: float) -> Decimal:
    v = Decimal(str(value))
    st = Decimal(str(step))
    return (v / st).to_integral_value(rounding=ROUND_DOWN) * st

def format_with_precision(value: float, max_decimals: int) -> str:
    """Formázás max tizedesre, e-notation nélkül, vágás nélkül."""
    q = Decimal(str(value))
    if max_decimals <= 0:
        return f"{int(q):d}"
    fmt = "0." + "0"*max_decimals
    return format(q.quantize(Decimal(fmt), rounding=ROUND_DOWN), 'f')

def format_qty(value: float, qty_step: float, base_precision: int) -> str:
    """Step-re illeszt + a tizedeseket legfeljebb basePrecision-re korlátozza."""
    q = quantize_down(value, qty_step)
    step_dec = decimals_for_step(qty_step)
    d = min(step_dec, base_precision)
    return format_with_precision(float(q), d)

def format_price(value: float, tick_size: float, price_precision: int) -> str:
    """Tick-re illeszt + legfeljebb pricePrecision tizedes."""
    p = quantize_down(value, tick_size)
    tick_dec = decimals_for_step(tick_size)
    d = min(tick_dec, price_precision)
    return format_with_precision(float(p), d)


def get_filters(category, symbol):
    info = get_instrument_info(category, symbol)
    lst = (((info or {}).get("result") or {}).get("list") or [])
    if not lst:
        return {}
    item = lst[0]
    lot = item.get("lotSizeFilter", {}) or {}
    prc = item.get("priceFilter", {}) or {}
    # ÚJ: precision mezők is
    base_prec  = int(lot.get("basePrecision")  or 8)
    quote_prec = int(lot.get("quotePrecision") or 8)
    price_prec = int(prc.get("pricePrecision") or 2)
    return {
        "minOrderAmt": float(lot.get("minOrderAmt") or 0.0),   # spot
        "minOrderQty": float(lot.get("minOrderQty") or 0.0),
        "qtyStep":     float(lot.get("qtyStep")     or 0.00000001),
        "tickSize":    float(prc.get("tickSize")    or 0.01),
        "basePrecision":  base_prec,
        "quotePrecision": quote_prec,
        "pricePrecision": price_prec,
    }


def decimals_for_step(step: float) -> int:
    """
    Meghatározza, hány tizedesjegy engedélyezett egy step alapján.
    Pl. 0.00001 -> 5; 0.1 -> 1; 1.0 -> 0
    """
    s = f"{step:.16f}".rstrip("0").rstrip(".")
    if "." in s:
        return len(s.split(".")[1])
    return 0

def quantize_down(value: float, step: float) -> Decimal:
    """
    Lefelé kerekít a megadott step rácsra, Decimal-lel (precíz) – nem ad vissza e-notationt.
    """
    v = Decimal(str(value))
    st = Decimal(str(step))
    # rácsra illesztés: floor(value/step) * step
    return (v / st).to_integral_value(rounding=ROUND_DOWN) * st

def format_step(value: float, step: float) -> str:
    """
    Lefelé kerekít step-re és úgy formázza, hogy ne legyen 'e-05',
    valamint pontosan annyi tizedes legyen, amennyit a step enged.
    """
    q = quantize_down(value, step)
    d = decimals_for_step(step)
    return f"{q:.{d}f}"


def _mask(s, keep=4):
    if not s: return "MISSING"
    s = str(s)
    return s[:keep] + "..." + s[-keep:] if len(s) > keep*2 else "***"

log.info("BYBIT_TESTNET=%s", os.getenv("BYBIT_TESTNET"))
log.info("BYBIT_CATEGORY=%s", os.getenv("BYBIT_CATEGORY"))
log.info("BYBIT_API_KEY(masked)=%s", _mask(BYBIT_API_KEY))
log.info("BYBIT_API_SECRET(masked)=%s", _mask(BYBIT_API_SECRET))
log.info("ALLOWED_SYMBOLS=%s", ",".join(sorted(ALLOWED_SYMBOLS)))

def round_to_step(value: float, step: float) -> float:
    if step <= 0:
        return float(value)
    return math.floor(float(value) / step) * step

def round_price(price: float, tick: float) -> float:
    return round_to_step(price, tick)

def fetch_filled_base_qty(category: str, symbol: str, order_id: str, order_link_id: str | None = None) -> float:
    """Lekéri a frissen vett mennyiséget (base). Ha nem sikerül, 0-t ad vissza."""
    try:
        r = session.get_order_history(
            category=category,
            orderId=order_id
        )
        lst = (((r or {}).get("result") or {}).get("list") or [])
        if lst:
            cum = lst[0].get("cumExecQty") or "0"
            return float(cum)
    except Exception:
        log.exception("fetch_filled_base_qty failed")
    return 0.0

def place_spot_brackets(symbol: str, base_filled: float, tp1: float, tp2: float, sl: float, portions=(0.5, 0.5)):
    f = get_filters("spot", symbol)
    log.info("Bybit filters %s -> qtyStep=%s basePrec=%s tickSize=%s pricePrec=%s minQty=%s", symbol, f.get("qtyStep"), f.get("basePrecision"), f.get("tickSize"), f.get("pricePrecision"), f.get("minOrderQty"))
    qty_step   = filters.get("qtyStep", 0.00000001)
    min_qty    = filters.get("minOrderQty", 0.0)
    tick_size  = filters.get("tickSize", 0.01)
    base_prec  = filters.get("basePrecision", 8)
    price_prec = filters.get("pricePrecision", 2)

    # Ár kerekítés + formázás
    tp1_s = format_price(float(tp1), tick_size, price_prec)
    tp2_s = format_price(float(tp2), tick_size, price_prec)
    sl_s  = format_price(float(sl),  tick_size, price_prec)

    # Darabolás + step + precision
    q1_raw = max(0.0, base_filled * float(portions[0]))
    q2_raw = max(0.0, base_filled - q1_raw)

    q1_s = format_qty(q1_raw, qty_step, base_prec)
    q2_s = format_qty(q2_raw, qty_step, base_prec)

    # Ellenőrzés minQty-re (számmá alakítva hasonlítunk)
    q1_f = float(q1_s)
    q2_f = float(q2_s)

    if (min_qty and q1_f < min_qty) or q1_f == 0.0:
        # mindent egy TP-be
        q1_s = format_qty(base_filled, qty_step, base_prec)
        q2_s = "0"

    created = {"tp1": None, "tp2": None, "sl": None}

    # TP1
    if float(q1_s) > 0.0 and (not min_qty or float(q1_s) >= min_qty):
        res1 = session.place_order(
            category="spot",
            symbol=symbol,
            side="Sell",
            orderType="Limit",
            qty=q1_s,                  # <<< string, precízen formázva
            price=tp1_s,               # <<< string
            timeInForce="GTC",
            triggerPrice=tp1_s,
            triggerDirection=1,
            orderFilter="tpslOrder",
            orderLinkId=f"tp1-{int(time.time()*1000)}"
        )
        if res1.get("retCode") == 0:
            created["tp1"] = res1["result"]["orderId"]
        else:
            log.warning("TP1 rejected: %s", res1)

    # TP2
    if float(q2_s) > 0.0 and (not min_qty or float(q2_s) >= min_qty):
        res2 = session.place_order(
            category="spot",
            symbol=symbol,
            side="Sell",
            orderType="Limit",
            qty=q2_s,
            price=tp2_s,
            timeInForce="GTC",
            triggerPrice=tp2_s,
            triggerDirection=1,
            orderFilter="tpslOrder",
            orderLinkId=f"tp2-{int(time.time()*1000)}"
        )
        if res2.get("retCode") == 0:
            created["tp2"] = res2["result"]["orderId"]
        else:
            log.warning("TP2 rejected: %s", res2)

    # SL – stop-market a teljes mennyiségre
    sl_qty_s = format_qty(base_filled, qty_step, base_prec)
    if float(sl_qty_s) > 0.0 and (not min_qty or float(sl_qty_s) >= min_qty):
        res3 = session.place_order(
            category="spot",
            symbol=symbol,
            side="Sell",
            orderType="Market",
            qty=sl_qty_s,
            triggerPrice=sl_s,
            triggerDirection=2,
            orderFilter="tpslOrder",
            orderLinkId=f"sl-{int(time.time()*1000)}"
        )
        if res3.get("retCode") == 0:
            created["sl"] = res3["result"]["orderId"]
        else:
            log.warning("SL rejected: %s", res3)

    return created


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

    # Secret
    if WEBHOOK_SECRET and data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 403

    strategy_id = (data.get("strategy_id") or "unknown").lower()
    signal      = (data.get("signal") or "").lower()  # buy / sell
    symbol      = (data.get("symbol") or "BTCUSDT").upper().replace("BYBIT:", "")
    category    = (data.get("category") or CATEGORY).lower()

    if signal not in {"buy", "sell"}:
        return jsonify({"error": "Invalid signal"}), 400
    if symbol not in ALLOWED_SYMBOLS:
        return jsonify({"error": f"Symbol {symbol} not allowed"}), 400
    if category not in {"spot", "linear"}:
        return jsonify({"error": "Invalid category"}), 400

    # dedup + cooldown + daily lock
    key = (strategy_id, symbol, signal)
    now = time.time()
    if now - _last_signal.get(key, 0) < DEDUP_WINDOW_S:
        return jsonify({"skipped": "duplicate_signal_window"}), 200
    _last_signal[key] = now
    if now < _cooldown_until.get(symbol, 0):
        return jsonify({"skipped": "cooldown_active"}), 200
    if _daily["loss"] <= -abs(MAX_DAILY_LOSS):
        return jsonify({"skipped": "daily_loss_lock"}), 200

    # Sizing / fallback
    alloc = ALLOC.get(strategy_id, 0.15)
    tp_mode = data.get("tp_mode") or ""
    tp1 = data.get("tp1")
    tp2 = data.get("tp2")
    sl  = data.get("sl")

    params = {
        "category": category,
        "symbol": symbol,
        "side": "Buy" if signal == "buy" else "Sell",
        "orderType": "Market",
    }

    try:
        filters  = get_filters(category, symbol)
        qtyStep  = filters.get("qtyStep", 0.00000001)
        minAmt   = filters.get("minOrderAmt", 0.0)   # spot min notional
        minQty   = filters.get("minOrderQty", 0.0)

        avail_usdt = get_available_usdt("UNIFIED")
        res = None
        base_filled = 0.0

        if category == "spot":
            # marketUnit: quoteCoin (értékben vásárlunk)
            quote_qty = data.get("quote_qty")
            if quote_qty is None:
                max_spend = max(0.0, avail_usdt * alloc * 0.98)
                if max_spend <= 0:
                    return jsonify({"error":"insufficient_balance","available_usdt":avail_usdt}), 400
                quote_qty = max_spend
            q = float(quote_qty)
            if minAmt and q < minAmt:
                return jsonify({"error":"min_notional","minOrderAmt":minAmt,"try_at_least":minAmt}), 400
            if q <= 0 or q > MAX_QUOTE:
                return jsonify({"error":"invalid_quote","limits":[0,MAX_QUOTE]}), 400

            params["qty"] = str(q)
            params["marketUnit"] = "quoteCoin"

            # BUY / SELL (spot)
            res = session.place_order(**params)
            if res.get("retCode") != 0:
                return jsonify({"error":"bybit_error","retCode":res.get("retCode"),"retMsg":res.get("retMsg"),"res":res}), 502

            order_id = (res.get("result") or {}).get("orderId")
            base_filled = fetch_filled_base_qty("spot", symbol, order_id)

            # Ha nem tudtuk kiolvasni, becslünk:
            if base_filled <= 0:
                last = get_last_price("spot", symbol) or 0
                if last > 0:
                    base_filled = round_to_step(q / last * 0.995, qtyStep)

            # Spot TP/SL csak BUY jelre értelmes (long irány)
            brackets = None
            if signal == "buy" and tp_mode == "fib" and all(x is not None for x in (tp1, tp2, sl)):
                brackets = place_spot_brackets(symbol, base_filled, float(tp1), float(tp2), float(sl), portions=(0.5, 0.5))

            # cooldown
            _cooldown_until[symbol] = now + TRADE_COOLDOWN_S

            return jsonify({
                "message": "spot order executed",
                "params": params,
                "filled_base_est": base_filled,
                "brackets": brackets,
                "res": res
            }), 200

        else:
            # linear (futures) – marad a korábbi logika
            base_qty = data.get("quantity")
            if base_qty is None:
                last = get_last_price("linear", symbol) or 0
                target_notional = max(0.0, avail_usdt * alloc * 0.98)
                est_qty = target_notional/last if last>0 else DEFAULT_QTY
                base_qty = max(est_qty, DEFAULT_QTY)
            q = round_to_step(float(base_qty), qtyStep)
            if minQty and q < minQty:
                q = max(minQty, q)
                q = round_to_step(q, qtyStep)
            if q <= 0 or q > MAX_QTY:
                return jsonify({"error":"invalid_quantity","limits":[0,MAX_QTY]}), 400
            params["qty"] = str(q)

            res = session.place_order(**params)
            if res.get("retCode") != 0:
                return jsonify({"error":"bybit_error","retCode":res.get("retCode"),"retMsg":res.get("retMsg"),"res":res}), 502

            # (TP/SL/trailing marad a futures beállítás szerint – ha kell, hozzáigazítjuk)
            _cooldown_until[symbol] = now + TRADE_COOLDOWN_S
            return jsonify({"message":"linear order executed","params":params,"res":res}), 200

    except Exception as e:
        log.exception("order failed")
        return jsonify({"error":"server_error","detail":str(e)}), 500

# ── main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
