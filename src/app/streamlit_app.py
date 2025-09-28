# src/app/streamlit_app.py
import os, json, sqlite3
from itertools import cycle

import pandas as pd
import streamlit as st
import plotly.express as px
from streamlit_plotly_events import plotly_events
import plotly.graph_objects as go
import pydeck as pdk

st.set_page_config(page_title="TRV Väghändelser", layout="wide")
DB_PATH = os.getenv("TRAFIK_DB_PATH", "trafik.db")

# ---- Länsnummer -> namn (för att fylla när county_name saknas) ----
COUNTY_NAMES = {
    1: "Stockholms län", 3: "Uppsala län", 4: "Södermanlands län", 5: "Östergötlands län",
    6: "Jönköpings län", 7: "Kronobergs län", 8: "Kalmar län", 9: "Gotlands län",
    10: "Blekinge län", 12: "Skåne län", 13: "Hallands län", 14: "Västra Götalands län",
    17: "Värmlands län", 18: "Örebro län", 19: "Västmanlands län", 20: "Dalarnas län",
    21: "Gävleborgs län", 22: "Västernorrlands län", 23: "Jämtlands län",
    24: "Västerbottens län", 25: "Norrbottens län"
}

# ---- fallback-koordinater för kartan ----
COUNTY_CENTER = {
    "Stockholms län": (59.334, 18.063),
    "Uppsala län": (59.858, 17.638),
    "Skåne län": (55.604, 13.003),
    "Västra Götalands län": (57.708, 11.974),
}

# ---------------------- DATA ----------------------
@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    cols = ["incident_id","message","message_type","location_descriptor","road_number",
            "county_name","county_no","start_time_utc","end_time_utc","modified_time_utc",
            "latitude","longitude","status"]
    try:
        con = sqlite3.connect(DB_PATH)
        query = """
            SELECT incident_id, message, message_type, location_descriptor,
                   road_number, county_name, county_no,
                   start_time_utc, end_time_utc, modified_time_utc,
                   latitude, longitude, status
            FROM incidents
            WHERE start_time_utc > datetime('now', '-30 day')
        """
        df = pd.read_sql_query(query, con)
        con.close()
    except Exception as e:
        st.warning(f"Databas kunde inte läsas ({e}). Visar tom vy.")
        return pd.DataFrame(columns=cols)

    # Dtypes
    for c in ("county_no",):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in ("latitude","longitude"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    textish = ["incident_id","message","message_type","location_descriptor",
               "road_number","county_name","status"]
    for c in textish:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    # Tider -> UTC (undvik FutureWarning)
    for c in ("start_time_utc","end_time_utc","modified_time_utc"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)

    # Fyll länsnamn om saknas -> från county_no
    if "county_name" in df.columns and "county_no" in df.columns:
        # tomma/NaN som None
        df["county_name"] = df["county_name"].where(df["county_name"].str.len() > 0, pd.NA)
        # lägg till county_display = namn eller mappat från county_no
        mapped = df["county_no"].map(lambda x: COUNTY_NAMES.get(int(x)) if pd.notna(x) else None)
        df["county_display"] = df["county_name"].fillna(mapped).fillna("Okänt län").astype("string")
    else:
        df["county_display"] = "Okänt län"

    return df

df = load_data()
st.title("TRV Väghändelser – pågår & kommande")

# ---------------------- SIDOFILTER ----------------------
with st.sidebar:
    st.header("Filter")
    status_val = st.multiselect("Status", ["PÅGÅR", "KOMMANDE"], default=["PÅGÅR","KOMMANDE"])
    county_opts = sorted(df["county_display"].dropna().unique()) if not df.empty else []
    county_val = st.multiselect("Län", county_opts, default=list(county_opts))
    q = st.text_input("Fritextsökning (meddelande/plats/väg)", "")
    road = st.text_input("Vägnummer (t.ex. E6, 40, 76)", "").strip()
    only_geo = st.checkbox("Endast med koordinater (kartan)", value=False)

    min_dt = df["start_time_utc"].min() if not df.empty else pd.Timestamp.utcnow() - pd.Timedelta(days=7)
    max_dt = df["start_time_utc"].max() if not df.empty else pd.Timestamp.utcnow()
    min_date, max_date = (min_dt.tz_localize(None) if getattr(min_dt, "tzinfo", None) else min_dt).date(), \
                         (max_dt.tz_localize(None) if getattr(max_dt, "tzinfo", None) else max_dt).date()
    date_range = st.date_input("Datumintervall", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        date_from, date_to = date_range
    else:
        date_from, date_to = min_date, max_date

    sort_col = st.selectbox("Sortera tabell efter",
                            ["modified_time_utc","start_time_utc","county_display","message_type","road_number"])
    sort_desc = st.checkbox("Fallande (senaste först)", value=True)
    max_rows = st.slider("Max rader i tabell", 20, 500, 100, step=20)

# ---------------------- FILTRERING ----------------------
f = df.copy()
if not f.empty:
    if status_val:
        f = f[f["status"].isin(status_val)]
    if county_val:
        f = f[f["county_display"].isin(county_val)]

    # datumfilter (UTC)
    start_ts = pd.to_datetime(date_from).tz_localize("UTC")
    end_ts = (pd.to_datetime(date_to) + pd.Timedelta(days=1)).tz_localize("UTC")
    f = f[(f["start_time_utc"] >= start_ts) & (f["start_time_utc"] < end_ts)]

    # fritext
    if q:
        qlc = q.lower()
        mask = (
            f["message"].astype("string").str.lower().str.contains(qlc, na=False) |
            f["location_descriptor"].astype("string").str.lower().str.contains(qlc, na=False) |
            f["road_number"].astype("string").str.lower().str.contains(qlc, na=False)
        )
        f = f[mask]

    # vägnummer
    if road:
        f = f[f["road_number"].astype("string").str.contains(road, case=False, na=False)]

    # endast koordinater
    if only_geo:
        f = f.dropna(subset=["latitude","longitude"])

# ---------------------- KPI ----------------------
c1, c2, c3 = st.columns(3)
c1.metric("PÅGÅR", int((f["status"]=="PÅGÅR").sum()) if not f.empty else 0)
c2.metric("KOMMANDE", int((f["status"]=="KOMMANDE").sum()) if not f.empty else 0)
c3.metric("Totalt i urval", 0 if f.empty else len(f))

# ---------------------- KLICKBAR STAPELGRAF ----------------------
st.subheader("Händelser per län – klicka för att filtrera")

COLOR_MAP_PATH = "county_colors.json"

def load_color_map(path=COLOR_MAP_PATH):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fjson:
                return json.load(fjson)
        except Exception:
            return {}
    return {}

def save_color_map(color_map, path=COLOR_MAP_PATH):
    try:
        with open(path, "w", encoding="utf-8") as fjson:
            json.dump(color_map, fjson, ensure_ascii=False, indent=2)
    except Exception:
        pass

def short_label(s, n=24):
    s = str(s)
    return (s[:n] + "…") if len(s) > n else s

f_base = f.copy()
if not f_base.empty:
    f_base["county_display"] = f_base["county_display"].astype("string").str.strip().fillna("Okänt län")
    g = (
        f_base.groupby("county_display", as_index=False)
              .size()
              .rename(columns={"size": "count", "county_display": "county"})
    )
    g["count"] = pd.to_numeric(g["count"], errors="coerce").fillna(0).astype("int64")
else:
    g = pd.DataFrame(columns=["county","count"])

if g.empty or g["count"].sum() == 0:
    st.info("Inga händelser i urvalet för att rita staplar.")
else:
    show_all = st.toggle("Visa alla län", value=False)
    g_sorted = g.sort_values("count", ascending=False).reset_index(drop=True)
    plot_df = (g_sorted if show_all else g_sorted.head(10)).copy()

    if "county_colors" not in st.session_state:
        st.session_state.county_colors = load_color_map()

    palette_full = (
        px.colors.qualitative.Alphabet
        + px.colors.qualitative.Plotly
        + px.colors.qualitative.Set3
        + px.colors.qualitative.Safe
    )
    color_cycle = cycle(palette_full)

    updated = False
    for lbl in g_sorted["county"]:
        if lbl not in st.session_state.county_colors:
            st.session_state.county_colors[lbl] = next(color_cycle)
            updated = True
    if updated:
        save_color_map(st.session_state.county_colors)

    plot_df = plot_df.sort_values("count", ascending=True)
    labels_full = plot_df["county"].tolist()
    labels_disp = [short_label(x) for x in labels_full]
    values = plot_df["count"].astype(int).tolist()
    bar_colors = [st.session_state.county_colors[lbl] for lbl in labels_full]

    fig = go.Figure(
        data=[go.Bar(
            y=labels_disp,
            x=values,
            orientation="h",
            text=values,
            textposition="outside",
            marker=dict(color=bar_colors),
            customdata=labels_full,
            hovertemplate="<b>%{customdata}</b><br>Antal händelser: %{x}<extra></extra>",
        )]
    )
    max_count = max(values) if values else 1
    fig.update_layout(
        title=("Alla län" if show_all else "Topp 10 län med flest händelser"),
        yaxis=dict(type="category", categoryorder="array", categoryarray=labels_disp, title=""),
        xaxis=dict(type="linear", rangemode="tozero", title="Antal händelser",
                   range=[0, max(1, int(max_count * 1.15))]),
        showlegend=False, bargap=0.2, margin=dict(l=110, r=40, t=50, b=40), height=540,
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font=dict(size=14),
    )

    if "clicked_counties" not in st.session_state:
        st.session_state.clicked_counties = set()

    clicked = plotly_events(fig, click_event=True, hover_event=False, select_event=False, override_height=560)
    if clicked:
        pt = clicked[0]
        clicked_short = pt.get("y")
        name = None
        if clicked_short in labels_disp:
            idx = labels_disp.index(clicked_short)
            name = labels_full[idx]
        else:
            name = pt.get("label") or pt.get("x") or pt.get("y")

        if name:
            last = st.session_state.get("_last_clicked")
            if last != name:  # debounce
                if name in st.session_state.clicked_counties:
                    st.session_state.clicked_counties.remove(name)
                else:
                    st.session_state.clicked_counties.add(name)
            st.session_state["_last_clicked"] = name

    c1_, c2_ = st.columns([3, 1])
    with c1_:
        if st.session_state.clicked_counties:
            st.info("Filter via klick: " + ", ".join(sorted(st.session_state.clicked_counties)))
    with c2_:
        if st.button("Rensa klick-filter"):
            st.session_state.clicked_counties = set()

    if st.session_state.clicked_counties:
        f = f[f["county_display"].isin(st.session_state.clicked_counties)]

# ---------------------- KARTA (pydeck + auto-zoom) ----------------------
st.subheader("Karta över aktiva händelser")

# UI-kontroller
colA, colB, colC = st.columns([1.3, 1, 1])
with colA:
    map_mode = st.radio("Kartläge", ["Prickar", "Heatmap", "Båda"], horizontal=True)
with colB:
    map_style = st.selectbox("Bakgrund", ["light", "dark", "road", "satellite"], index=0)
with colC:
    st.toggle("Länsfärger", key="use_county_colors",
              value=st.session_state.get("use_county_colors", False),
              help="Annars röd för alla punkter")
use_county_colors = st.session_state.get("use_county_colors", False)

# gör approx_missing tillgänglig för kartdelen
approx_missing = st.checkbox("Visa saknade koordinater i länscentrum", value=True)

m = f.copy()
if approx_missing and not m.empty:
    m["latitude"] = m.apply(
        lambda r: r["latitude"] if pd.notna(r["latitude"])
        else COUNTY_CENTER.get(r["county_display"], (None, None))[0], axis=1
    )
    m["longitude"] = m.apply(
        lambda r: r["longitude"] if pd.notna(r["longitude"])
        else COUNTY_CENTER.get(r["county_display"], (None, None))[1], axis=1
    )
map_df = m.dropna(subset=["latitude", "longitude"]).copy()

if map_df.empty:
    st.info("Inga koordinater tillgängliga i nuvarande urval.")
else:
    # Tooltip-fält (rena strängar)
    for c in ("county_display","road_number","location_descriptor","status"):
        map_df[c] = map_df[c].astype("string").fillna("")
    map_df["start_str"] = pd.to_datetime(map_df["start_time_utc"], utc=True, errors="coerce").astype("string").fillna("")
    map_df["mod_str"]   = pd.to_datetime(map_df["modified_time_utc"], utc=True, errors="coerce").astype("string").fillna("")

    def hex_to_rgba(h, a=210):
        h = str(h).lstrip("#")
        if len(h) != 6:
            return [230, 57, 70, a]  # fallback röd
        return [int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16), a]

    if use_county_colors and "county_colors" in st.session_state:
        map_df["__color_rgba__"] = map_df["county_display"].map(st.session_state.county_colors).apply(lambda c: hex_to_rgba(c, 210))
    else:
        map_df["__color_rgba__"] = [[230, 57, 70, 210]] * len(map_df)

    # Auto-zoom mot ev. klickade län
    selected = set(st.session_state.get("clicked_counties", []))
    focus_df = map_df[map_df["county_display"].isin(selected)] if selected else map_df
    if focus_df.empty:
        focus_df = map_df

    lat_min, lat_max = float(focus_df["latitude"].min()), float(focus_df["latitude"].max())
    lon_min, lon_max = float(focus_df["longitude"].min()), float(focus_df["longitude"].max())
    lat_center = (lat_min + lat_max) / 2.0
    lon_center = (lon_min + lon_max) / 2.0
    span = max(lat_max - lat_min, lon_max - lon_min)
    zoom = 11 if span <= 0.08 else 9 if span <= 0.25 else 7 if span <= 0.6 else 6 if span <= 1.2 else 5 if span <= 3.0 else 4

    # Reglage
    c1x, c2x = st.columns(2)
    with c1x:
        point_radius = st.slider("Punktstorlek (pixlar)", 2, 20, 8)
    with c2x:
        heat_intensity = st.slider("Heatmap-intensitet", 1, 20, 8)

    data_records = map_df.to_dict(orient="records")
    layers = []
    if map_mode in ("Prickar", "Båda"):
        layers.append(pdk.Layer(
            "ScatterplotLayer",
            data=data_records,
            get_position="[longitude, latitude]",
            get_fill_color="__color_rgba__",
            get_line_color="[0,0,0,80]",
            line_width_min_pixels=0.5,
            radius_min_pixels=point_radius,
            radius_max_pixels=point_radius + 8,
            pickable=True,
            auto_highlight=True,
        ))
    if map_mode in ("Heatmap", "Båda"):
        layers.append(pdk.Layer(
            "HeatmapLayer",
            data=data_records,
            get_position="[longitude, latitude]",
            aggregation='"SUM"',
            intensity=heat_intensity,
            opacity=0.58,
            threshold=0.01,
        ))

    tooltip = {
        "html": (
            "<b>{county_display}</b><br/>"
            "{road_number} – {location_descriptor}<br/>"
            "Status: {status}<br/>"
            "Start: {start_str}<br/>"
            "Ändrad: {mod_str}"
        ),
        "style": {"backgroundColor": "rgba(30,30,30,0.85)", "color": "white", "fontSize": "12px"},
    }
    style_map = {"light": "light", "dark": "dark", "road": "road", "satellite": "satellite"}

    st.pydeck_chart(
        pdk.Deck(
            layers=layers,
            initial_view_state=pdk.ViewState(latitude=lat_center, longitude=lon_center, zoom=zoom),
            map_style=style_map.get(map_style, "light"),
            tooltip=tooltip,
        ),
        width="stretch"=True,
    )

# ---------------------- TABELL ----------------------
st.subheader(f"Senaste händelser (max {max_rows} rader)")
f_sorted = f.sort_values(by=sort_col, ascending=not sort_desc).head(max_rows) if not f.empty else f

if not f_sorted.empty:
    show_cols = ["incident_id","message_type","status","county_display","road_number",
                 "location_descriptor","start_time_utc","end_time_utc","modified_time_utc",
                 "latitude","longitude"]
    table = f_sorted[show_cols].copy()
    for c in ["start_time_utc","end_time_utc","modified_time_utc"]:
        table[c] = pd.to_datetime(table[c], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    st.dataframe(table.rename(columns={"county_display": "county"}), width="stretch")
else:
    st.info("Ingen data att visa i tabellen.")

    st.dataframe(
        f_sorted[show_cols].rename(columns={"county_display": "county"}),
        width="stretch",
    )
else:
    st.info("Ingen data att visa i tabellen.")

# ---------------------- TREND ÖVER TID ----------------------
st.subheader("Antal händelser per dag")
if not f.empty:
    trend = (
        f.assign(date=pd.to_datetime(f["start_time_utc"], utc=True).dt.date)
         .groupby("date")
         .size()
         .reset_index(name="count")
    )
    fig_trend = px.line(
        trend, x="date", y="count", markers=True,
        labels={"date": "Datum", "count": "Antal händelser"},
        title="Utveckling av händelser över tid"
    )
    st.plotly_chart(fig_trend, width="stretch"=True)
else:
    st.info("Ingen data att visa i tidsserien.")

# ---------------------- TYPKLASSIFICERING ----------------------
st.subheader("Fördelning av händelsetyper")
if not f.empty and "message_type" in f.columns:
    type_counts = f["message_type"].value_counts().reset_index()
    type_counts.columns = ["Typ", "Antal"]
    fig_types = px.bar(
        type_counts, x="Antal", y="Typ", orientation="h",
        text="Antal", title="Fördelning av händelsetyper"
    )
    fig_types.update_traces(textposition="outside")
    st.plotly_chart(fig_types, width="stretch"=True)
else:
    st.info("Ingen data att visa för händelsetyper.")
