# src/app/streamlit_app.py
import os, json, sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
from streamlit_plotly_events import plotly_events
from itertools import cycle
import plotly.graph_objects as go
import pydeck as pdk
from src.app.etl_runner import run_etl

# ---------------------- APP CONFIG ----------------------
st.set_page_config(page_title="TRV Incidents Dashboard", layout="wide")
DB_PATH = os.getenv("TRAFIK_DB_PATH", "trafik.db")

# ---------------------- LOAD DATA ----------------------
@st.cache_data(ttl=300)
def load_data():
    """Load data from SQLite database and apply type conversions."""
    con = sqlite3.connect(DB_PATH)
    query = """
        SELECT incident_id, message, message_type, location_descriptor,
               road_number, county_name, county_no,
               start_time_utc, end_time_utc, modified_time_utc,
               latitude, longitude, status
        FROM incidents
        WHERE start_time_utc > datetime('now', '-30 day')
    """
    df = pd.read_sql_query(
        query, con,
        parse_dates=["start_time_utc", "end_time_utc", "modified_time_utc"]
    )
    con.close()

    # Convert county_no to numeric
    if "county_no" in df.columns:
        df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")

    # Ensure lat/lon are numeric
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Clean up string columns
    textish = ["incident_id","message","message_type",
               "location_descriptor","road_number","county_name","status"]
    for col in textish:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
    return df

df = load_data()
st.title("TRV Incidents – Ongoing & Upcoming")

# ---------------------- SIDEBAR FILTERS ----------------------
with st.sidebar:
    st.header("ETL Control")
    if st.button("🔄 Run ETL now"):
        with st.spinner("Fetching fresh data from TRV API..."):
            try:
                summary = run_etl(DB_PATH, days_back=1)
                st.success(
                    f"✅ ETL finished – {summary['rows']} rows "
                    f"(Ongoing={summary['pagar']}, Upcoming={summary['kommande']}) "
                    f"in {summary['seconds']}s"
                )
                # Reload data after ETL
                df = load_data()
                f = df.copy()
            except Exception as e:
                st.error(f"🚨 ETL failed: {e}")

    st.header("Filters")
    status_val = st.multiselect("Status", ["PÅGÅR", "KOMMANDE"], default=["PÅGÅR","KOMMANDE"])
    county_opts = sorted(df["county_name"].dropna().unique()) if not df.empty else []
    county_val = st.multiselect("County", county_opts, default=list(county_opts))
    q = st.text_input("Free text search (message/place/road)", "")
    road = st.text_input("Road number (e.g. E6, 40, 76)", "").strip()
    only_geo = st.checkbox("Only incidents with coordinates (map)", value=False)

    # Date range filter
    min_dt = df["start_time_utc"].min() if not df.empty else pd.Timestamp.utcnow() - pd.Timedelta(days=7)
    max_dt = df["start_time_utc"].max() if not df.empty else pd.Timestamp.utcnow()
    min_date, max_date = min_dt.date(), max_dt.date()
    date_range = st.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        date_from, date_to = date_range
    else:
        date_from, date_to = min_date, max_date

    # Sorting & table options
    sort_col = st.selectbox("Sort table by", ["modified_time_utc","start_time_utc","county_name","message_type","road_number"])
    sort_desc = st.checkbox("Descending (latest first)", value=True)
    max_rows = st.slider("Max rows in table", 20, 500, 100, step=20)

# ---------------------- APPLY FILTERS ----------------------
f = df.copy()
if not f.empty:
    if status_val:
        f = f[f["status"].isin(status_val)]
    if county_val:
        f = f[f["county_name"].isin(county_val)]

    # Date range
    start_ts = pd.to_datetime(date_from).tz_localize("UTC")
    end_ts = (pd.to_datetime(date_to) + pd.Timedelta(days=1)).tz_localize("UTC")
    if getattr(f["start_time_utc"].dtype, "tz", None) is None:
        f["start_time_utc"] = pd.to_datetime(f["start_time_utc"], errors="coerce").dt.tz_localize("UTC")
    f = f[(f["start_time_utc"] >= start_ts) & (f["start_time_utc"] < end_ts)]

    # Free text search
    if q:
        qlc = q.lower()
        mask = (
            f["message"].astype("string").str.lower().str.contains(qlc, na=False) |
            f["location_descriptor"].astype("string").str.lower().str.contains(qlc, na=False) |
            f["road_number"].astype("string").str.lower().str.contains(qlc, na=False)
        )
        f = f[mask]

    # Road number filter
    if road:
        f = f[f["road_number"].astype("string").str.contains(road, case=False, na=False)]

    # Only keep rows with coordinates
    if only_geo:
        f = f.dropna(subset=["latitude","longitude"])

# ---------------------- KPI METRICS ----------------------
c1, c2, c3 = st.columns(3)
c1.metric("Ongoing", int((f["status"]=="PÅGÅR").sum()) if not f.empty else 0)
c2.metric("Upcoming", int((f["status"]=="KOMMANDE").sum()) if not f.empty else 0)
c3.metric("Total (filtered)", 0 if f.empty else len(f))

# Make checkbox available for map
approx_missing = st.checkbox("Show missing coordinates in county centers", value=True)

# ---------------------- CLICKABLE BAR CHART ----------------------
st.subheader("Incidents per county – click to filter")

COLOR_MAP_PATH = "county_colors.json"

def load_color_map(path=COLOR_MAP_PATH):
    """Load color mapping from file."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fjson:
                return json.load(fjson)
        except Exception:
            return {}
    return {}

def save_color_map(color_map, path=COLOR_MAP_PATH):
    """Save color mapping to file."""
    try:
        with open(path, "w", encoding="utf-8") as fjson:
            json.dump(color_map, fjson, ensure_ascii=False, indent=2)
    except Exception:
        pass

def short_label(s, n=24):
    """Return shortened label with ellipsis."""
    s = str(s)
    return (s[:n] + "…") if len(s) > n else s
