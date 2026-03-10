import json
from confluent_kafka import Consumer
from db import create_tables, insert_trades_batch

TOPIC = "stocks"
BATCH_SIZE = 50


def main():
    create_tables()

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "db-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    print(f"DB consumer listening on '{TOPIC}'...")

    batch = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No new message — flush whatever we have
                if batch:
                    insert_trades_batch(batch)
                    consumer.commit()
                    print(f"Flushed {len(batch)} trades to DB (timeout)")
                    batch = []
                continue

            if msg.error():
                print(f"ERROR: {msg.error()}")
                continue

            trade = json.loads(msg.value().decode())
            batch.append((
                trade.get("symbol"),
                trade.get("price"),
                trade.get("volume"),
                trade.get("timestamp"),
                trade.get("ingested_at"),
            ))

            if len(batch) >= BATCH_SIZE:
                insert_trades_batch(batch)
                consumer.commit()
                print(f"Wrote {len(batch)} trades to DB")
                batch = []

    except KeyboardInterrupt:
        # Flush remaining on shutdown
        if batch:
            insert_trades_batch(batch)
            consumer.commit()
            print(f"Flushed {len(batch)} remaining trades to DB")
        print("Shutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
