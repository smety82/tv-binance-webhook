import os, logging
from flask import Flask, request, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from pybit.unified_trading import HTTP  # Bybit V5

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tv-bybit-webhook")
limiter = Limiter(get_remote_address, app=app, default_limits=["10 per minute"])

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
BYBIT_TESTNET = os.getenv("BYBIT_TESTNET", "true").lower() == "true"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
CATEGORY = os.getenv("BYBIT_CATEGORY", "spot")  # "spot" vagy "linear"
ALLOWED_SYMBOLS = {"BTCUSDT", "ETHUSDT"}  # bővítsd
DEFAULT_QTY = 0.001  # base mennyiség
MAX_QTY = 1.0
MAX_QUOTE = 20000  # USDT

# Bybit V5 HTTP kliens
session = HTTP(
    testnet=BYBIT_TESTNET,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
)

@app.get("/health")
def health():
    return {"ok": True}, 200

@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    log.info("incoming: %s", data)

    # Secret védelem
    if WEBHOOK_SECRET and data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 403

    action = (data.get("action") or "").lower()
    if action not in {"buy", "sell"}:
        return jsonify({"error": "Invalid action"}), 400

    symbol = (data.get("symbol") or "BTCUSDT").upper()
    if ALLOWED_SYMBOLS and symbol not in ALLOWED_SYMBOLS:
        return jsonify({"error": f"Symbol {symbol} not allowed"}), 400

    # Mennyiség: vagy base qty (pl. 0.001 BTC), vagy quote (pl. 50 USDT) spot markethez
    qty = data.get("quantity")
    quote_qty = data.get("quote_qty")

    # Bybit order paraméterek (V5 /v5/order/create)
    side = "Buy" if action == "buy" else "Sell"
    order_type = "Market"

    try:
        params = {
            "category": CATEGORY,       # "spot" vagy "linear"
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            # Market -> IOC implicit, price nem kell
        }

        if CATEGORY == "spot" and quote_qty is not None:
            # UTA spot: Market Buy alapból quote-ban megy; marketUnit-tal szabályozhatod
            q = float(quote_qty)
            if q <= 0 or q > MAX_QUOTE:
                return jsonify({"error": f"Invalid quote_qty (0 < x <= {MAX_QUOTE})"}), 400
            params.update({
                "qty": str(q),          # quote összeg
                "marketUnit": "quoteCoin"
            })
        else:
            # base qty (spot/linear)
            if qty is None:
                qty = DEFAULT_QTY
            q = float(qty)
            if q <= 0 or q > MAX_QTY:
                return jsonify({"error": f"Invalid quantity (0 < x <= {MAX_QTY})"}), 400
            params["qty"] = str(q)

        # --- Opcionális: pontosítás tick/step szerint (qtyStep, minOrderQty stb.) ---
        # instr = session.get_instruments_info(category=CATEGORY, symbol=symbol)
        # ... itt kerekíthetsz qtyStep-hez

        res = session.place_order(**params)
        log.info("order resp: %s", res)
        return jsonify({"message": "order executed", "params": params, "res": res}), 200

    except Exception as e:
        log.exception("order failed")
        return jsonify({"error": "server_error", "detail": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

# ... importok és app létrehozása után ...
def _mask(s, keep=4):
    if not s: return "MISSING"
    return s[:keep] + "..." + s[-keep:]

log.info("BYBIT_TESTNET=%s", os.getenv("BYBIT_TESTNET"))
log.info("BYBIT_CATEGORY=%s", os.getenv("BYBIT_CATEGORY"))
log.info("BYBIT_API_KEY(masked)=%s", _mask(os.getenv("BYBIT_API_KEY")))
log.info("BYBIT_API_SECRET(masked)=%s", _mask(os.getenv("BYBIT_API_SECRET")))

session = HTTP(
    testnet=BYBIT_TESTNET,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
)

@app.get("/authcheck")
def authcheck():
    try:
        # UNIFIED a leggyakoribb UTA számlán; ha SPOT/CONTRACT a tiéd, azt add meg.
        r = session.get_wallet_balance(accountType="UNIFIED")
        return {"ok": True, "retCode": r.get("retCode"), "retMsg": r.get("retMsg", ""), "resultKeys": list((r.get("result") or {}).keys())}, 200
    except Exception as e:
        log.exception("authcheck failed")
        return {"ok": False, "error": str(e)}, 500

