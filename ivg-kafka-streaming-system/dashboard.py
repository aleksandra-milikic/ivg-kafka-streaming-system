import streamlit as st
import pandas as pd
import psycopg2
from contextlib import contextmanager

st.set_page_config(page_title="IVG Analytics Dashboard", layout="wide")

st.title("IVG Gallery Analytics Dashboard")



@contextmanager
def get_conn():
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="localhost",
        port=5432
    )
    try:
        yield conn
    finally:
        conn.close()


@st.cache_data(ttl=10)
def run_query(query):
    with get_conn() as conn:
        return pd.read_sql(query, conn)



total_sql = "SELECT COUNT(*) AS total_events FROM ivg_events"

event_sql = """
SELECT event_type, COUNT(*) AS count
FROM ivg_events
GROUP BY event_type
ORDER BY count DESC
"""

art_sql = """
SELECT
COALESCE(payload->>'artwork_title', 'unknown') AS artwork,
COUNT(*) AS views
FROM ivg_events
WHERE event_type = 'artwork_viewed'
GROUP BY artwork
ORDER BY views DESC
LIMIT 10
"""

user_sql = """
SELECT user_id, COUNT(*) AS activity_count
FROM ivg_events
GROUP BY user_id
ORDER BY activity_count DESC
"""

latest_sql = """
SELECT event_type, user_id, timestamp
FROM ivg_events
ORDER BY timestamp DESC
LIMIT 20
"""



try:
    total_df = run_query(total_sql)
    event_df = run_query(event_sql)
    art_df = run_query(art_sql)
    user_df = run_query(user_sql)
    latest_df = run_query(latest_sql)

    
    st.metric("Total Events", int(total_df.iloc[0]["total_events"]))

    st.subheader("Event Type Distribution")
    st.bar_chart(event_df.set_index("event_type"))

    st.subheader("Most Viewed Artworks")
    st.dataframe(art_df, use_container_width=True)
    st.bar_chart(art_df.set_index("artwork"))

    st.subheader("Most Active Users")
    st.dataframe(user_df, use_container_width=True)

    st.subheader("Latest Events")
    st.dataframe(latest_df, use_container_width=True)

except Exception as e:
    st.error(f"Database error: {e}")