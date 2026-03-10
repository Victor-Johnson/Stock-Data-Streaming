import json
import os
from datetime import datetime, timezone
import websocket
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
COMPANIES = ["AAPL", "TSLA", "MSFT", "NVDA", "AMZN"]
TOPIC = "stocks"


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[OK] {msg.topic()} | symbol={msg.key().decode()} | offset={msg.offset()}")


def make_on_message(producer):
    def on_message(_ws, message):
        msg = json.loads(message)
        if msg.get("type") != "trade" or "data" not in msg:
            return
        for trade in msg["data"]:
            payload = json.dumps({
                "symbol": trade.get("s"),
                "price": trade.get("p"),
                "volume": trade.get("v"),
                "timestamp": trade.get("t"),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            })
            producer.produce(
                TOPIC,
                key=trade.get("s"),
                value=payload,
                callback=delivery_report,
            )
            producer.poll(0)
    return on_message


def on_error(ws, error):
    print("ERROR:", error)


def on_close(ws, code, reason):
    print("CLOSED:", code, reason)


def on_open(ws):
    print("Connected to Finnhub WebSocket")
    for symbol in COMPANIES:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))


def main():
    if not API_KEY:
        raise RuntimeError("API_KEY is missing; add it to your .env file")

    producer = Producer({"bootstrap.servers": "localhost:9092"})

    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={API_KEY}",
        on_open=on_open,
        on_message=make_on_message(producer),
        on_error=on_error,
        on_close=on_close,
    )

    try:
        ws.run_forever()
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
