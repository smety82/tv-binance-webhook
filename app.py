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
getcontext().prec = 28

def decimals_for_step(step: float) -> int:
    s = f"{step:.16f}".rstrip("0").rstrip(".")
    return len(s.split(".")[1]) if "." in s else 0

def quantize_down(value: float, step: float) -> Decimal:
    v = Decimal(str(value))
    st = Decimal(str(step))
    return (v / st).to_integral_value(rounding=ROUND_DOWN) * st

def format_with_precision(value: float, max_decimals: int) -> str:
    q = Decimal(str(value))
    if max_decimals <= 0:
        return f"{int(q):d}"
    fmt = "0." + "0"*max_decimals
    return format(q.quantize(Decimal(fmt), rounding=ROUND_DOWN), 'f')

def format_qty(value: float, qty_step: float, base_precision: int) -> str:
    """Step-re illeszt + legfeljebb basePrecision tizedes (e-notation NINCS)."""
    q = quantize_down(value, qty_step)
    step_dec = decimals_for_step(qty_step)
    d = min(step_dec, base_precision if base_precision is not None else step_dec)
    return format_with_precision(float(q), d)

def format_price(value: float, tick_size: float, price_precision: int) -> str:
    p = quantize_down(value, tick_size)
    tick_dec = decimals_for_step(tick_size)
    d = min(tick_dec, price_precision if price_precision is not None else tick_dec)
    return format_with_precision(float(p), d)


def _safe_int(x, default):
    try:
        return int(x)
    except Exception:
        return default

def _safe_float(x, default):
    try:
        return float(x)
    except Exception:
        return default

def _decimals_from_step(step: float) -> int:
    s = f"{step:.16f}".rstrip("0").rstrip(".")
    return len(s.split(".")[1]) if "." in s else 0

def get_filters(category, symbol):
    info = get_instrument_info(category, symbol)
    lst = (((info or {}).get("result") or {}).get("list") or [])
    if not lst:
        return {}

    item = lst[0] or {}
    lot = item.get("lotSizeFilter")  or {}
    prc = item.get("priceFilter")    or {}

    qty_step   = _safe_float(lot.get("qtyStep"),     0.00000001)
    tick_size  = _safe_float(prc.get("tickSize"),    0.01)
    min_qty    = _safe_float(lot.get("minOrderQty"), 0.0)
    min_amt    = _safe_float(lot.get("minOrderAmt"), 0.0)

    # bybit gyakran ad basePrecision/pricePrecision-t, de néha None
    base_prec  = lot.get("basePrecision")
    price_prec = prc.get("pricePrecision")
    base_prec  = _safe_int(base_prec,  _decimals_from_step(qty_step))
    price_prec = _safe_int(price_prec, _decimals_from_step(tick_size))

    # BTCUSDT spoton konzervatív plafon 6 tizedes (ha nem adtak vissza értelmeset)
    if category == "spot" and symbol.upper() == "BTCUSDT":
        base_prec = min(base_prec if base_prec is not None else 6, 6)

    return {
        "minOrderAmt":    min_amt,
        "minOrderQty":    min_qty,
        "qtyStep":        qty_step,
        "tickSize":       tick_size,
        "basePrecision":  base_prec,
        "pricePrecision": price_prec,
    }


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

def _place_with_retry(limit_or_market_params, is_qty: bool, qty_or_price_key: str, qty_step: float, base_prec: int, tick_size: float, price_prec: int):
    """170137 esetén 1 tizedessel kevesebbre vág és újrapróbálja egyszer."""
    # első próbálkozás
    res = session.place_order(**limit_or_market_params)
    if res.get("retCode") == 0:
        return res

    if str(res.get("retCode")) == "170137":
        # Túl sok tizedes → vágjunk még egyet
        if is_qty:
            cur = Decimal(limit_or_market_params[qty_or_price_key])
            new = format_with_precision(float(cur), max(0, (base_prec or 6) - 1))
        else:
            cur = Decimal(limit_or_market_params[qty_or_price_key])
            new = format_with_precision(float(cur), max(0, (price_prec or 2) - 1))
        limit_or_market_params[qty_or_price_key] = new
        # ha ár is érintett (TP limitnél), a triggerPrice-ot is állítsuk
        if not is_qty and "triggerPrice" in limit_or_market_params:
            limit_or_market_params["triggerPrice"] = new
        # retry egyszer
        res2 = session.place_order(**limit_or_market_params)
        return res2

    return res


def place_spot_brackets(symbol: str, base_filled: float, tp1: float, tp2: float, sl: float, portions=(0.5, 0.5)):
    # pontos szűrők a precíz formázáshoz
    filters = get_filters("spot", symbol)
    qty_step   = filters.get("qtyStep", 0.00000001)
    min_qty    = filters.get("minOrderQty", 0.0)
    tick_size  = filters.get("tickSize", 0.01)
    base_prec  = filters.get("basePrecision", 8)
    price_prec = filters.get("pricePrecision", 2)

    # nagyon kicsi darabnál ne felezzünk – minden menjen TP1-re
    MIN_SPLIT_BASE = 5e-5  # ~0.00005 BTC
    if base_filled < MIN_SPLIT_BASE:
        portions = (1.0, 0.0)

    # árak: tick-re illesztés + max price_prec tizedes, e-notation nélkül
    tp1_s = format_price(float(tp1), tick_size, price_prec)
    tp2_s = format_price(float(tp2), tick_size, price_prec)
    sl_s  = format_price(float(sl),  tick_size, price_prec)

    # darabolás
    q1_raw = max(0.0, base_filled * float(portions[0]))
    q2_raw = max(0.0, base_filled - q1_raw)

    # mennyiségek: step-re illesztés + max base_prec tizedes
    q1_s = format_qty(q1_raw, qty_step, base_prec)
    q2_s = format_qty(q2_raw, qty_step, base_prec)

    # --- Elérhető BTC lekérdezés + safety margin (ne allokáljunk 100%-ot se)
    # Tipp: a buy után 1-2x próbáld újraolvasni, ha 0-t kapsz (latency miatt).
    def _get_btc_available():
        try:
            r = session.get_wallet_balance(accountType="UNIFIED")
            coins = ((r or {}).get("result") or {}).get("list", [{}])[0].get("coin", [])
            for c in coins:
                if c.get("coin") == "BTC":
                    v = c.get("availableToUse") or c.get("availableToWithdraw") or c.get("walletBalance") or "0"
                    return float(v)
        except Exception:
            log.exception("wallet read failed")
        return 0.0

    avail_btc = _get_btc_available()
    if avail_btc <= 0:
        # próbáljuk még egyszer kicsit várva (buy fill/settlement késleltetés)
        time.sleep(0.8)
        avail_btc = _get_btc_available()

    # Biztonsági tartalék a lekerekítések/fee miatt
    SAFE = 0.998
    sell_cap = max(0.0, avail_btc * SAFE)

    # TP darabok számmá
    q1f = float(q1_s)
    q2f = float(q2_s)

    # --- Min notional szerinti korrekció (ha felezve nem érné el, összevonjuk)
    min_amt = filters.get("minOrderAmt", 0.0)
    p1f, p2f = float(tp1_s), float(tp2_s)
    if min_amt and min_amt > 0:
        if q1f > 0 and (q1f * p1f) < min_amt:
            q2f += q1f; q1f = 0.0
        if q2f > 0 and (q2f * p2f) < min_amt:
            q1f += q2f; q2f = 0.0

    # --- Ne lépjük túl a rendelkezésre álló mennyiséget
    # Prioritás: hagyjunk legalább egy SL-t, és ha lehet 1-2 TP-t.
    # 1) először igazítsuk a TP-k összegét a sell_cap-hez
    tp_total = q1f + q2f
    if tp_total > sell_cap:
        # először vágjuk vissza TP2-t
        over = tp_total - sell_cap
        cut = min(over, q2f)
        q2f -= cut
        tp_total = q1f + q2f
    if tp_total > sell_cap:
        # ha még mindig sok, vágjuk TP1-et is
        over = tp_total - sell_cap
        q1f = max(0.0, q1f - over)
        tp_total = q1f + q2f

    # 2) SL mennyisége = ami még marad a cap-ből
    sl_qty_f = max(0.0, sell_cap - tp_total)

    # 3) formázzuk vissza step/precision szerint
    q1_s = format_qty(q1f, qty_step, base_prec)
    q2_s = format_qty(q2f, qty_step, base_prec)
    sl_qty_s = format_qty(sl_qty_f, qty_step, base_prec)

    # 4) minQty és minNotional utóellenőrzés
    def _valid_leg(q_str, price):
        if float(q_str) <= 0: return False
        if min_qty and float(q_str) < min_qty: return False
        if min_amt and (float(q_str) * price) < min_amt: return False
        return True

    # ha egy láb nem éri el a minimumot, ejtsük
    if not _valid_leg(q2_s, p2f):
        q2_s = "0"
    if not _valid_leg(q1_s, p1f):
        # ha TP1 sem jó, ejtsük
        q1_s = "0"
    if not _valid_leg(sl_qty_s, float(sl_s)):
        sl_qty_s = "0"

    log.info("alloc after cap: avail_btc=%.8f sell_cap=%.8f | TP1=%s @ %s, TP2=%s @ %s, SL=%s @ %s",
             avail_btc, sell_cap, q1_s, tp1_s, q2_s, tp2_s, sl_qty_s, sl_s)


    # --- Min notional (minOrderAmt) védelem: ha a felezett láb túl kicsi, összevonjuk egy TP-be
    min_amt = filters.get("minOrderAmt", 0.0)
    p1f = float(tp1_s)
    p2f = float(tp2_s)
    q1f = float(q1_s)
    q2f = float(q2_s)

    if min_amt and min_amt > 0:
        # ha az első TP értéke < min_amt → átrakjuk a mennyiséget a másodikba
        if q1f > 0 and (q1f * p1f) < min_amt:
            q2f += q1f
            q1f = 0.0
        # ha a második TP értéke < min_amt → átrakjuk az elsőbe
        if q2f > 0 and (q2f * p2f) < min_amt:
            q1f += q2f
            q2f = 0.0

        # újraformázás a step/precision szerint
        q1_s = format_qty(q1f, qty_step, base_prec)
        q2_s = format_qty(q2f, qty_step, base_prec)

        # ha még így is mindegyik láb túl kicsi (pl. az egész pozíció min_amt alatt van) → ne tegyünk ki TP-t
        if (float(q1_s) == 0.0 and float(q2_s) == 0.0) or (
            (float(q1_s) > 0.0 and float(q1_s)*p1f < min_amt) and
            (float(q2_s) > 0.0 and float(q2_s)*p2f < min_amt)
        ):
            log.warning("TP skipped: position notional below minOrderAmt (minAmt=%.6f, base_filled=%.8f, tp1=%.2f, tp2=%.2f)",
                        min_amt, base_filled, p1f, p2f)
            q1_s, q2_s = "0", "0"


    # ha az első rész túl kicsi → mindent egy TP-be
    if float(q1_s) < max(min_qty, 0.0) or float(q1_s) == 0.0:
        q1_s = format_qty(base_filled, qty_step, base_prec)
        q2_s = "0"

    created = {"tp1": None, "tp2": None, "sl": None}

    # ── TP1 (Limit + trigger felfelé)
    if float(q1_s) >= max(min_qty, 0.0) and float(q1_s) > 0.0:
        params1 = dict(
            category="spot",
            symbol=symbol,
            side="Sell",
            orderType="Limit",
            qty=q1_s,
            price=tp1_s,
            timeInForce="GTC",
            triggerPrice=tp1_s,
            triggerDirection=1,
            orderFilter="tpslOrder",
            orderLinkId=f"tp1-{int(time.time()*1000)}"
        )
        res1 = _place_with_retry(
            params1,
            is_qty=True,
            qty_or_price_key="qty",
            qty_step=qty_step,
            base_prec=base_prec,
            tick_size=tick_size,
            price_prec=price_prec
        )
        if res1.get("retCode") == 0:
            created["tp1"] = (res1.get("result") or {}).get("orderId")
        else:
            log.warning("TP1 rejected: %s", res1)

    # ── TP2 (Limit + trigger felfelé) – figyelem: q2_s / tp2_s!
    if float(q2_s) >= max(min_qty, 0.0) and float(q2_s) > 0.0:
        params2 = dict(
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
        res2 = _place_with_retry(
            params2,
            is_qty=True,
            qty_or_price_key="qty",
            qty_step=qty_step,
            base_prec=base_prec,
            tick_size=tick_size,
            price_prec=price_prec
        )
        if res2.get("retCode") == 0:
            created["tp2"] = (res2.get("result") or {}).get("orderId")
        else:
            log.warning("TP2 rejected: %s", res2)

    # ── SL (Stop-Market + trigger lefelé) – teljes mennyiségre
    sl_qty_s = format_qty(base_filled, qty_step, base_prec)
    if float(sl_qty_s) >= max(min_qty, 0.0) and float(sl_qty_s) > 0.0:
        params3 = dict(
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
        res3 = session.place_order(**params3)
        if res3.get("retCode") == 0:
            created["sl"] = (res3.get("result") or {}).get("orderId")
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
