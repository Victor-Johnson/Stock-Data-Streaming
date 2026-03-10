import streamlit as st
import plotly.express as px
import pandas as pd
from db import get_connection

st.set_page_config(page_title="Stock Stream", layout="wide")
st.title("Stock Stream Dashboard")


def query_df(sql):
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


# ---------------------------------------------------------------------------
# Sidebar — auto-refresh
# ---------------------------------------------------------------------------
refresh = st.sidebar.slider("Refresh interval (seconds)", 1, 30, 5)

# ---------------------------------------------------------------------------
# Row 1 — Latest prices + VWAP
# ---------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Latest Prices")
    latest = query_df("""
        SELECT DISTINCT ON (symbol) symbol, price, volume, ingested_at
        FROM trades ORDER BY symbol, ingested_at  DESC
    """)
    if not latest.empty:
        st.dataframe(latest, use_container_width=True, hide_index=True)
    else:
        st.info("No trades yet.")

with col2:
    st.subheader("VWAP")
    vwap = query_df("""
        SELECT symbol,
               ROUND(SUM(price * volume) / SUM(volume), 4) AS vwap,
               SUM(volume) AS total_volume,
               COUNT(*) AS trades
        FROM trades GROUP BY symbol ORDER BY symbol
    """)
    if not vwap.empty:
        st.dataframe(vwap, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Row 2 — Price range bar chart
# ---------------------------------------------------------------------------
st.subheader("Price Range (High / Low)")
price_range = query_df("""
    SELECT symbol,
           MIN(price) AS low,
           MAX(price) AS high,
           MAX(price) - MIN(price) AS spread
    FROM trades GROUP BY symbol ORDER BY symbol
""")
if not price_range.empty:
    price_range["low"] = price_range["low"].astype(float)
    price_range["high"] = price_range["high"].astype(float)
    fig = px.bar(
        price_range.melt(id_vars="symbol", value_vars=["low", "high"]),
        x="symbol", y="value", color="variable", barmode="group",
        labels={"value": "Price ($)", "variable": ""},
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 3 — Volume per minute line chart
# ---------------------------------------------------------------------------
st.subheader("Volume per Minute (last 30 min)")
vol = query_df("""
    SELECT symbol,
           date_trunc('minute', ingested_at) AS minute,
           SUM(volume)::int AS volume
    FROM trades
    WHERE ingested_at > now() - interval '30 minutes'
    GROUP BY symbol, minute
    ORDER BY minute
""")
if not vol.empty:
    fig2 = px.line(vol, x="minute", y="volume", color="symbol",
                   labels={"minute": "", "volume": "Volume"})
    st.plotly_chart(fig2, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 4 — Alerts
# ---------------------------------------------------------------------------
st.subheader("Recent Alerts")
alerts = query_df("""
    SELECT symbol, alert_type, message, price, created_at
    FROM alerts ORDER BY created_at DESC LIMIT 20
""")
if not alerts.empty:
    st.dataframe(alerts, use_container_width=True, hide_index=True)
else:
    st.info("No alerts triggered yet.")

# ---------------------------------------------------------------------------
# Row 5 — Recent trades table
# ---------------------------------------------------------------------------
st.subheader("Recent Trades")
recent = query_df("""
    SELECT symbol, price, volume, ingested_at
    FROM trades ORDER BY ingested_at DESC LIMIT 50
""")
if not recent.empty:
    st.dataframe(recent, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------
st.sidebar.caption(f"Refreshing every {refresh}s")
import time
time.sleep(refresh)
st.rerun()
