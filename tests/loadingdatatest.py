import os
import json
import websocket
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
COMPANIES = ["AAPL", "TSLA", "MSFT", "NVDA", "AMZN"]


def on_message(ws, message):
    """Print trades pushed by Finnhub."""
    msg = json.loads(message)
    if msg.get("type") == "trade" and "data" in msg:
        for trade in msg["data"]:
            print(trade)


def on_error(ws, error):
    print("ERROR:", error)


def on_close(ws, code, reason):
    print("CLOSED:", code, reason)


def on_open(ws):
    print("Connected Successfully !! :)")
    # Subscribe to each symbol right after connecting
    for symbol in COMPANIES:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))


if __name__ == "__main__":
    if not API_KEY:
        raise RuntimeError("API_KEY is missing; add it to your .env file")

    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={API_KEY}",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    # This call blocks and should produce prints once data starts flowing
    ws.run_forever()
