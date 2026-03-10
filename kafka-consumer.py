import argparse
import json 

from confluent_kafka import Consumer

def main():
    parser = argparse.ArgumentParser(description="Test Kafka consumer")
    parser.add_argument("--group-id","-g",help="Consumer group ID")
    parser.add_argument("--topic-name", "-t",help="Topic name")
    parser.add_argument("--name","-n",help="Name of the consumer")

    args = parser.parse_args()

    group_id = args.group_id
    topic_name = args.topic_name
    consumer_name = args.name

    consumer_config ={
        "bootstrap.servers":"localhost:9092",
        "group.id": group_id,
        "auto.offset.reset":"earliest",
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([topic_name])

    print(f"[{consumer_name}] Listening on topic '{topic_name}' (group: {group_id})...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{consumer_name}] ERROR: {msg.error()}")
                continue

            trade = json.loads(msg.value().decode())
            print(
                f"[{consumer_name}] "
                f"{trade.get('symbol'):<5} "
                f"${trade.get('price'):>10.2f} "
                f"vol={trade.get('volume')} "
                f"partition={msg.partition()} "
                f"offset={msg.offset()}"
            )
    except KeyboardInterrupt:
        print(f"\n[{consumer_name}] Shutting down...")
    finally:
        consumer.close()
        

if __name__ == "__main__":
    main()
