import json
from collections import deque
from datetime import datetime, timezone

from confluent_kafka import Consumer
from db import create_tables, get_connection

TOPIC = "stocks"
WINDOW_SIZE = 60          # seconds to look back
PRICE_CHANGE_PCT = 2.0    # trigger alert if price moves > 2%
VOLUME_SPIKE_MULT = 5.0   # trigger alert if volume > 5x the rolling average


def insert_alert(symbol, alert_type, message, price):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alerts (symbol, alert_type, message, price)
                VALUES (%s, %s, %s, %s)
                """,
                (symbol, alert_type, message, price),
            )
        conn.commit()


def main():
    create_tables()

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "alerting",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([TOPIC])
    print(f"Alert consumer listening on '{TOPIC}'...")
    print(f"  Price change threshold: {PRICE_CHANGE_PCT}%")
    print(f"  Volume spike threshold: {VOLUME_SPIKE_MULT}x average")

    # Per-symbol sliding window: deque of (timestamp, price, volume)
    windows: dict[str, deque] = {}

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue

            trade = json.loads(msg.value().decode())
            symbol = trade.get("symbol")
            price = trade.get("price")
            volume = trade.get("volume")
            now = datetime.now(timezone.utc).timestamp()

            if not symbol or price is None:
                continue

            # Initialize window for new symbols
            if symbol not in windows:
                windows[symbol] = deque()

            window = windows[symbol]
            window.append((now, price, volume))

            # Evict entries older than the window
            while window and (now - window[0][0]) > WINDOW_SIZE:
                window.popleft()

            if len(window) < 2:
                continue

            # --- Price spike detection ---
            oldest_price = window[0][1]
            pct_change = ((price - oldest_price) / oldest_price) * 100

            if abs(pct_change) >= PRICE_CHANGE_PCT:
                direction = "UP" if pct_change > 0 else "DOWN"
                msg_text = (
                    f"{symbol} price {direction} {abs(pct_change):.2f}% "
                    f"in {WINDOW_SIZE}s (${oldest_price:.2f} -> ${price:.2f})"
                )
                print(f"[ALERT] {msg_text}")
                insert_alert(symbol, "price_spike", msg_text, price)

            # --- Volume spike detection ---
            volumes = [v for _, _, v in window if v]
            if len(volumes) >= 5:
                avg_vol = sum(volumes[:-1]) / len(volumes[:-1])
                if avg_vol > 0 and volume > avg_vol * VOLUME_SPIKE_MULT:
                    msg_text = (
                        f"{symbol} volume spike: {volume} "
                        f"({volume / avg_vol:.1f}x the {WINDOW_SIZE}s average of {avg_vol:.0f})"
                    )
                    print(f"[ALERT] {msg_text}")
                    insert_alert(symbol, "volume_spike", msg_text, price)

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
