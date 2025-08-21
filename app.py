import os
import logging
from flask import Flask, request, jsonify, redirect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from pybit.unified_trading import HTTP  # Bybit V5

# --- App és log ---
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tv-bybit-webhook")

# --- Rate limit (10 kérés/perc/IP) ---
limiter = Limiter(key_func=get_remote_address, app=app, default_limits=["10 per minute"])

# --- Környezeti változók ---
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
BYBIT_TESTNET = os.getenv("BYBIT_TESTNET", "true").lower() == "true"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
CATEGORY = os.getenv("BYBIT_CATEGORY", "spot")  # "spot" vagy "linear"
ALLOWED_SYMBOLS = {"BTCUSDT", "ETHUSDT"}        # bővíthető
DEFAULT_QTY = 0.001
MAX_QTY = 1.0
MAX_QUOTE = 20000  # USDT

def _mask(s: str | None, keep: int = 4) -> str:
    if not s:
        return "MISSING"
    s = str(s)
    if len(s) <= keep * 2:
        return "***"
    return s[:keep] + "..." + s[-keep:]

log.info("BYBIT_TESTNET=%s", os.getenv("BYBIT_TESTNET"))
log.info("BYBIT_CATEGORY=%s", os.getenv("BYBIT_CATEGORY"))
log.info("BYBIT_API_KEY(masked)=%s", _mask(BYBIT_API_KEY))
log.info("BYBIT_API_SECRET(masked)=%s", _mask(BYBIT_API_SECRET))

# --- Bybit V5 HTTP kliens ---
session = HTTP(
    testnet=BYBIT_TESTNET,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
)

@app.get("/")
def index():
    return jsonify({"service": "tv-bybit-webhook", "endpoints": ["/health", "/authcheck", "/webhook"]}), 200

# Ha véletlenül a gyökérre POST-olsz, 307-tel átirányítjuk a /webhook-ra, megőrizve a metódust/body-t
@app.post("/")
def root_post():
    return redirect("/webhook", code=307)

@app.get("/health")
def health():
    return jsonify({"ok": True}), 200

@app.get("/authcheck")
def authcheck():
    try:
        # UNIFIED a leggyakoribb; ha nem UTA-s a számlád, állítsd át pl. "CONTRACT"/"SPOT"-ra
        r = session.get_wallet_balance(accountType="UNIFIED")
        return jsonify({
            "ok": True,
            "retCode": r.get("retCode"),
            "retMsg": r.get("retMsg", ""),
            "resultKeys": list((r.get("result") or {}).keys())
        }), 200
    except Exception as e:
        log.exception("authcheck failed")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/webhook", methods=["POST"])
@limiter.limit("10 per minute")
def webhook():
    data = request.get_json(silent=True) or {}
    log.info("incoming: %s", data)

    # Secret ellenőrzés
    if WEBHOOK_SECRET and data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 403

    # Action
    action = (data.get("action") or "").lower().strip()
    if action not in {"buy", "sell"}:
        return jsonify({"error": "Invalid action (buy/sell expected)"}), 400
    side = "Buy" if action == "buy" else "Sell"

    # Symbol
    symbol = (data.get("symbol") or "BTCUSDT").upper().strip()
    if ALLOWED_SYMBOLS and symbol not in ALLOWED_SYMBOLS:
        return jsonify({"error": f"Symbol {symbol} not allowed"}), 400

    # Paraméterek
    qty = data.get("quantity")      # base mennyiség (pl. 0.001 BTC)
    quote_qty = data.get("quote_qty")  # USDT összeg (spot market buy-hoz)

    params = {
        "category": CATEGORY,  # "spot" vagy "linear"
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
    }

    # Spot: megadhatod quote-ban is (USDT), linear: base qty kell
    if CATEGORY == "spot" and quote_qty is not None:
        try:
            q = float(quote_qty)
        except Exception:
            return jsonify({"error": "quote_qty must be a number"}), 400
        if q <= 0 or q > MAX_QUOTE:
            return jsonify({"error": f"Invalid quote_qty (0 < x <= {MAX_QUOTE})"}), 400
        params.update({"qty": str(q), "marketUnit": "quoteCoin"})
    else:
        if qty is None:
            qty = DEFAULT_QTY
        try:
            q = float(qty)
        except Exception:
            return jsonify({"error": "quantity must be a number"}), 400
        if q <= 0 or q > MAX_QTY:
            return jsonify({"error": f"Invalid quantity (0 < x <= {MAX_QTY})"}), 400
        params["qty"] = str(q)

    try:
        res = session.place_order(**params)
        log.info("order resp: %s", res)
        # Bybit V5: retCode == 0 → siker
        if res.get("retCode") != 0:
            return jsonify({
                "error": "bybit_error",
                "retCode": res.get("retCode"),
                "retMsg": res.get("retMsg"),
                "res": res
            }), 502

        return jsonify({"message": "order executed", "params": params, "res": res}), 200

    except Exception as e:
        log.exception("order failed")
        return jsonify({"error": "server_error", "detail": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
