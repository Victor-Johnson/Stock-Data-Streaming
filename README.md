# Stock Data Streaming Pipeline

A real-time stock data pipeline that streams live trades from the Finnhub API through Apache Kafka, persists them in PostgreSQL, detects price/volume anomalies, and visualizes everything in a live Streamlit dashboard.

## Architecture

```
Finnhub WebSocket API
        |
  kafka-producer.py
        |
  Kafka Topic: "stocks" (partitioned by symbol)
        |
   +---------+-----------------+
   |         |                 |
app.py   kafka-db-consumer   kafka-alert-consumer
   |         |                 |
WebSocket  PostgreSQL        PostgreSQL
(live)     trades table      alerts table
   |         |                 |
   |         +--------+--------+
   |                  |
   |            dashboard.py
   |            (Streamlit)
   |
REST API
(/api/prices, /api/volume)
```

## Components

| File | Description |
|------|-------------|
| `kafka-producer.py` | Streams live trades from Finnhub WebSocket into Kafka, keyed by stock symbol |
| `kafka-consumer.py` | CLI consumer for debugging — prints formatted trades with partition/offset info |
| `kafka-db-consumer.py` | Batches trades (50 at a time) into PostgreSQL with manual offset commits |
| `kafka-alert-consumer.py` | Detects price spikes (>2% in 60s) and volume anomalies (>5x rolling avg) |
| `app.py` | FastAPI server with REST endpoints (VWAP, price range, volume/min) and live WebSocket |
| `dashboard.py` | Streamlit dashboard with charts, alerts, and auto-refresh |
| `db.py` | PostgreSQL schema and connection management |

## Tracked Symbols

AAPL, TSLA, MSFT, NVDA, AMZN

## Prerequisites

- Python 3.12+
- Apache Kafka (running locally or via Docker)
- PostgreSQL

## Setup

### 1. Clone and install dependencies

```bash
git clone git@github.com:Victor-Johnson/Stock-Data-Streaming.git
cd Stock-Data-Streaming
python -m venv venv
source venv/bin/activate
pip install confluent-kafka websocket-client python-dotenv psycopg2-binary fastapi uvicorn streamlit plotly pandas finnhub-python
```

### 2. Configure environment variables

```bash
cp dummyenv.txt .env
```

Edit `.env` and add your values:

```
API_KEY=<your_finnhub_api_key>
DATABASE_URL=dbname=stocks_db
```

Get a free API key at [finnhub.io](https://finnhub.io/).

### 3. Start Kafka

```bash
docker-compose up -d
```

Or if running Kafka natively, ensure it's accessible at `localhost:9092`.

### 4. Create the Kafka topic

```bash
kafka-topics --bootstrap-server localhost:9092 --create \
  --topic stocks --partitions 5 --replication-factor 1
```

### 5. Initialize the database

```bash
createdb stocks_db
python db.py
```

## Running

Open four terminals:

```bash
# Terminal 1 — Producer (streams live trades into Kafka)
python kafka-producer.py

# Terminal 2 — DB Consumer (persists trades to PostgreSQL)
python kafka-db-consumer.py

# Terminal 3 — Alert Consumer (monitors for anomalies)
python kafka-alert-consumer.py

# Terminal 4 — Dashboard
streamlit run dashboard.py
```

### Optional: FastAPI server

```bash
uvicorn app:app --reload
```

**REST endpoints:**

- `GET /api/prices/latest` — latest price per symbol
- `GET /api/prices/vwap` — volume weighted average price
- `GET /api/prices/range` — high, low, spread per symbol
- `GET /api/volume/per-minute` — volume aggregated per minute (last 30 min)

**WebSocket:** `ws://localhost:8000/ws/trades` — live trade stream

**Interactive docs:** `http://localhost:8000/docs`

### Debugging with the CLI consumer

```bash
python kafka-consumer.py -t stocks -g debug-group -n my-consumer
```

## How It Works

**Kafka partitioning:** The producer keys each message by stock symbol. Kafka hashes the key to route messages to consistent partitions, so all AAPL trades go to the same partition, all TSLA trades to another, etc.

**Consumer groups:** Each consumer runs in its own group (`db-writer`, `alerting`, `fastapi-live`), so they all independently receive every message. You can scale any consumer by running multiple instances in the same group — Kafka will split partitions between them.

**Alerting:** The alert consumer maintains a 60-second sliding window per symbol. It triggers alerts when:
- Price moves more than 2% within the window
- A single trade's volume exceeds 5x the rolling average

**Batching:** The DB consumer collects up to 50 trades before writing to PostgreSQL in a single transaction, committing Kafka offsets only after a successful write. This ensures no data loss if the consumer crashes.

## Note

Finnhub's free WebSocket tier only sends trade data during **US market hours** (9:30 AM – 4:00 PM ET, weekdays). Outside those hours the producer will connect but won't receive trade messages.
