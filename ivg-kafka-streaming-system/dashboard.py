import streamlit as st
import pandas as pd
import psycopg2

st.set_page_config(page_title="IVG Analytics Dashboard", layout="wide")

st.title("IVG Gallery Analytics Dashboard")


conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="postgres",
    host="localhost",
    port=5432
)

total_sql = """
SELECT COUNT(*) AS total_events
FROM ivg_events
"""

total_df = pd.read_sql(total_sql, conn)

st.metric("Total Events", int(total_df.iloc[0]["total_events"]))


event_sql = """
SELECT event_type, COUNT(*) AS count
FROM ivg_events
GROUP BY event_type
ORDER BY count DESC
"""

event_df = pd.read_sql(event_sql, conn)

st.subheader("Event Type Distribution")

st.bar_chart(
    event_df.set_index("event_type")
)


art_sql = """
SELECT
payload->>'artwork_title' AS artwork,
COUNT(*) AS views
FROM ivg_events
WHERE event_type = 'artwork_viewed'
GROUP BY artwork
ORDER BY views DESC
LIMIT 10
"""

art_df = pd.read_sql(art_sql, conn)

st.subheader("Most Viewed Artworks")

st.dataframe(art_df, use_container_width=True)

st.bar_chart(
    art_df.set_index("artwork")
)


user_sql = """
SELECT user_id, COUNT(*) AS activity_count
FROM ivg_events
GROUP BY user_id
ORDER BY activity_count DESC
"""

user_df = pd.read_sql(user_sql, conn)

st.subheader("Most Active Users")

st.dataframe(user_df, use_container_width=True)


latest_sql = """
SELECT
event_type,
user_id,
timestamp
FROM ivg_events
ORDER BY timestamp DESC
LIMIT 20
"""

latest_df = pd.read_sql(latest_sql, conn)

st.subheader("Latest Events")

st.dataframe(latest_df, use_container_width=True)

conn.close()