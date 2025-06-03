from flask import Flask, request, jsonify
from binance.client import Client
import os

app = Flask(__name__)

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')
client = Client(API_KEY, API_SECRET)

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    action = data.get('action')
    symbol = data.get('symbol', 'BTCUSDT')
    quantity = 0.001

    try:
        if action == "buy":
            order = client.create_order(
                symbol=symbol,
                side='BUY',
                type='MARKET',
                quantity=quantity
            )
        elif action == "sell":
            order = client.create_order(
                symbol=symbol,
                side='SELL',
                type='MARKET',
                quantity=quantity
            )
        else:
            return jsonify({'message': 'Invalid action'}), 400

        return jsonify({'message': 'order executed', 'order': order}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
