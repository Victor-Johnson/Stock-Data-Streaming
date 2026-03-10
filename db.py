import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL", "dbname=stocks_db")


def get_connection():
    return psycopg2.connect(DB_URL)


def create_tables():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id          BIGSERIAL PRIMARY KEY,
                    symbol      VARCHAR(10)    NOT NULL,
                    price       NUMERIC(12,4)  NOT NULL,
                    volume      INTEGER        NOT NULL,
                    trade_ts    BIGINT,
                    ingested_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
                    stored_at   TIMESTAMPTZ    NOT NULL DEFAULT now()
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades (symbol);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_trades_ingested ON trades (ingested_at);
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id          BIGSERIAL PRIMARY KEY,
                    symbol      VARCHAR(10)    NOT NULL,
                    alert_type  VARCHAR(30)    NOT NULL,
                    message     TEXT           NOT NULL,
                    price       NUMERIC(12,4)  NOT NULL,
                    created_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
                );
            """)
        conn.commit()


def insert_trade(symbol, price, volume, trade_ts, ingested_at):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trades (symbol, price, volume, trade_ts, ingested_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (symbol, price, volume, trade_ts, ingested_at),
            )
        conn.commit()


def insert_trades_batch(trades):
    """Insert multiple trades in a single transaction."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO trades (symbol, price, volume, trade_ts, ingested_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                trades,
            )
        conn.commit()


if __name__ == "__main__":
    create_tables()
    print("Tables created successfully.")
