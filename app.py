import asyncio
import json
import threading
from contextlib import asynccontextmanager
from decimal import Decimal

from confluent_kafka import Consumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from db import get_connection

TOPIC = "stocks"


# ---------------------------------------------------------------------------
# WebSocket connection manager
# ---------------------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.connections.append(ws)

    def disconnect(self, ws: WebSocket):
        self.connections.remove(ws)

    async def broadcast(self, data: dict):
        for ws in list(self.connections):
            try:
                await ws.send_json(data)
            except Exception:
                self.disconnect(ws)


manager = ConnectionManager()


# ---------------------------------------------------------------------------
# Background Kafka consumer → pushes to WebSocket clients
# ---------------------------------------------------------------------------
def _kafka_consumer_loop(loop: asyncio.AbstractEventLoop):
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "fastapi-live",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([TOPIC])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            trade = json.loads(msg.value().decode())
            asyncio.run_coroutine_threadsafe(manager.broadcast(trade), loop)
    finally:
        consumer.close()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    loop = asyncio.get_running_loop()
    thread = threading.Thread(target=_kafka_consumer_loop, args=(loop,), daemon=True)
    thread.start()
    yield


app = FastAPI(title="Stock Stream", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Helper to convert Decimal to float for JSON serialization
# ---------------------------------------------------------------------------
def _row_to_dict(row, columns):
    return {col: (float(val) if isinstance(val, Decimal) else val)
            for col, val in zip(columns, row)}


# ---------------------------------------------------------------------------
# REST endpoints — aggregations from PostgreSQL
# ---------------------------------------------------------------------------
@app.get("/api/prices/latest")
def latest_prices():
    """Latest price per symbol."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (symbol) symbol, price, volume, ingested_at
                FROM trades
                ORDER BY symbol, ingested_at DESC
            """)
            cols = ["symbol", "price", "volume", "ingested_at"]
            return [_row_to_dict(r, cols) for r in cur.fetchall()]


@app.get("/api/prices/vwap")
def vwap():
    """Volume Weighted Average Price per symbol."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol,
                       SUM(price * volume) / SUM(volume) AS vwap,
                       SUM(volume) AS total_volume,
                       COUNT(*) AS trade_count
                FROM trades
                GROUP BY symbol
                ORDER BY symbol
            """)
            cols = ["symbol", "vwap", "total_volume", "trade_count"]
            return [_row_to_dict(r, cols) for r in cur.fetchall()]


@app.get("/api/prices/range")
def price_range():
    """High, low, spread per symbol."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol,
                       MIN(price) AS low,
                       MAX(price) AS high,
                       MAX(price) - MIN(price) AS spread
                FROM trades
                GROUP BY symbol
                ORDER BY symbol
            """)
            cols = ["symbol", "low", "high", "spread"]
            return [_row_to_dict(r, cols) for r in cur.fetchall()]


@app.get("/api/volume/per-minute")
def volume_per_minute():
    """Trade volume aggregated per minute, last 30 minutes."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol,
                       date_trunc('minute', ingested_at) AS minute,
                       SUM(volume) AS volume,
                       COUNT(*) AS trade_count
                FROM trades
                WHERE ingested_at > now() - interval '30 minutes'
                GROUP BY symbol, minute
                ORDER BY minute DESC
            """)
            cols = ["symbol", "minute", "volume", "trade_count"]
            return [_row_to_dict(r, cols) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# WebSocket — live trade stream
# ---------------------------------------------------------------------------
@app.websocket("/ws/trades")
async def ws_trades(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()  # keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(ws)
