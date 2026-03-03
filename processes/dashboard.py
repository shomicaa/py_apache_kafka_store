import json
import os
from collections import defaultdict
from dotenv import load_dotenv

import pandas as pd
import psycopg2
import streamlit as st

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

DB_CONN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", 6111)),
    dbname=os.getenv("DB_NAME", "postgres"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASS", "postgres")
)

st.set_page_config(page_title="Shirt Store Dashboard", layout="wide")
st.title("Shirt Store — Sales Dashboard")


@st.cache_data(ttl=5)
def load_data():
    con = psycopg2.connect(**DB_CONN)

    try:
        orders = pd.read_sql_query(
            "SELECT event_id, event_type, order_id, ts, payload FROM orders ORDER BY ts DESC",
            con
        )
    except Exception:
        orders = pd.DataFrame()

    try:
        order_status = pd.read_sql_query(
            "SELECT order_id, status, product_name, quantity, reason, updated_at FROM order_status ORDER BY updated_at DESC",
            con
        )
    except Exception:
        order_status = pd.DataFrame()

    try:
        dlq = pd.read_sql_query(
            "SELECT event_id, order_id, ts, reason, source_topic FROM dlq_events ORDER BY ts DESC",
            con
        )
    except Exception:
        dlq = pd.DataFrame()

    con.close()
    return orders, order_status, dlq


orders, order_status, dlq = load_data()

# --- KPI metrics ---
col1, col2, col3, col4 = st.columns(4)

total = len(orders)
reserved = len(orders[orders["event_type"] == "INVENTORY_RESERVED"]) if not orders.empty else 0
rejected = len(orders[orders["event_type"] == "INVENTORY_REJECTED"]) if not orders.empty else 0
dlq_count = len(dlq)

col1.metric("Total Events", total)
col2.metric("Reserved (OK)", reserved)
col3.metric("Rejected (Out of Stock)", rejected)
col4.metric("DLQ Events", dlq_count)

st.divider()

# --- Sales breakdown by color and size ---
if not orders.empty:
    reserved_rows = orders[orders["event_type"] == "INVENTORY_RESERVED"].copy()

    color_sales = defaultdict(int)
    size_sales = defaultdict(int)

    for _, row in reserved_rows.iterrows():
        try:
            p = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
            color_sales[p["color"]] += p["quantity"]
            size_sales[p["size"]] += p["quantity"]
        except (json.JSONDecodeError, KeyError):
            pass

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Units sold by color")
        if color_sales:
            df_color = pd.DataFrame(color_sales.items(), columns=["Color", "Units sold"])
            st.bar_chart(df_color.set_index("Color"))
        else:
            st.info("No sales yet.")

    with col_b:
        st.subheader("Units sold by size")
        if size_sales:
            df_size = pd.DataFrame(size_sales.items(), columns=["Size", "Units sold"])
            st.bar_chart(df_size.set_index("Size"))
        else:
            st.info("No sales yet.")

st.divider()

# --- Recent order outcomes ---
st.subheader("Recent order outcomes")
if not order_status.empty:
    st.dataframe(order_status.head(50), use_container_width=True)
else:
    st.info("No order data yet.")

# --- DLQ table ---
st.subheader("Recent DLQ events")
if not dlq.empty:
    st.dataframe(dlq.head(20), use_container_width=True)
else:
    st.success("No DLQ events.")

if st.button("Refresh"):
    st.cache_data.clear()
    st.rerun()
