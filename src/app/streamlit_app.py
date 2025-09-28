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

# ---------------------- I18N ----------------------
LANG = {
    "sv": {
        "lang_label": "Language / Språk",
        "app_title": "TRV Väghändelser – pågår & kommande",
        "filters_hdr": "Filter",
        "status": "Status",
        "county": "Län",
        "search": "Fritextsökning (meddelande/plats/väg)",
        "road": "Vägnummer (t.ex. E6, 40, 76)",
        "only_geo": "Endast med koordinater (kartan)",
        "date_range": "Datumintervall",
        "sort_by": "Sortera tabell efter",
        "desc": "Fallande (senaste först)",
        "max_rows": "Max rader i tabell",
        "kpi_ongoing": "PÅGÅR",
        "kpi_upcoming": "KOMMANDE",
        "kpi_total": "Totalt i urval",
        "approx_missing": "Visa saknade koordinater i länscentrum",
        "bar_hdr": "Händelser per län – klicka för att filtrera",
        "bar_none": "Inga händelser i urvalet för att rita staplar.",
        "bar_all": "Visa alla län",
        "bar_title_all": "Alla län",
        "bar_title_top": "Topp 10 län med flest händelser",
        "bar_click_filter": "Filter via klick: ",
        "bar_clear": "Rensa klick-filter",
        "map_hdr": "Karta över aktiva händelser",
        "map_mode": "Kartläge",
        "map_modes": ["Prickar", "Heatmap", "Båda"],
        "map_style": "Bakgrund",
        "map_styles": ["light", "dark", "road", "satellite"],
        "map_color_toggle": "Länsfärger",
        "map_no_geo": "Inga koordinater tillgängliga i nuvarande urval.",
        "map_point_size": "Punktstorlek (pixlar)",
        "map_heat_intensity": "Heatmap-intensitet",
        "map_tooltip": (
            "<b>{county_display}</b><br/>"
            "{road_number} – {location_descriptor}<br/>"
            "Status: {status}<br/>"
            "Start: {start_str}<br/>"
            "Ändrad: {mod_str}"
        ),
        "table_hdr": "Senaste händelser (max {n} rader)",
        "trend_hdr": "Antal händelser per dag",
        "trend_title": "Utveckling av händelser över tid",
        "trend_date": "Datum",
        "trend_count": "Antal händelser",
        "types_hdr": "Fördelning av händelsetyper",
        "types_none": "Ingen data att visa för händelsetyper.",
        "types_type": "Typ",
        "types_count": "Antal",
        # Data-värden i databasen
        "status_options": ["PÅGÅR", "KOMMANDE"],
        "no_rows_for_bars": "Inga händelser i urvalet för att rita staplar.",
        "no_rows_for_table": "Ingen data att visa i tabellen.",
        "no_rows_for_trend": "Ingen data att visa i tidsserien.",
    },
    "en": {
        "lang_label": "Language / Språk",
        "app_title": "TRV Incidents – Ongoing & Upcoming",
        "filters_hdr": "Filters",
        "status": "Status",
        "county": "County",
        "search": "Free text search (message/place/road)",
        "road": "Road number (e.g. E6, 40, 76)",
        "only_geo": "Only incidents with coordinates (map)",
        "date_range": "Date range",
        "sort_by": "Sort table by",
        "desc": "Descending (latest first)",
        "max_rows": "Max rows in table",
        "kpi_ongoing": "Ongoing",
        "kpi_upcoming": "Upcoming",
        "kpi_total": "Total (filtered)",
        "approx_missing": "Show missing coordinates in county centers",
        "bar_hdr": "Incidents per county – click to filter",
        "bar_none": "No incidents to plot for current selection.",
        "bar_all": "Show all counties",
        "bar_title_all": "All counties",
        "bar_title_top": "Top 10 counties by incidents",
        "bar_click_filter": "Click filter: ",
        "bar_clear": "Clear click filter",
        "map_hdr": "Map of active incidents",
        "map_mode": "Map mode",
        "map_modes": ["Dots", "Heatmap", "Both"],
        "map_style": "Basemap",
        "map_styles": ["light", "dark", "road", "satellite"],
        "map_color_toggle": "County colors",
        "map_no_geo": "No coordinates available for current selection.",
        "map_point_size": "Point size (px)",
        "map_heat_intensity": "Heatmap intensity",
        "map_tooltip": (
            "<b>{county_display}</b><br/>"
            "{road_number} – {location_descriptor}<br/>"
            "Status: {status}<br/>"
            "Start: {start_str}<br/>"
            "Updated: {mod_str}"
        ),
        "table_hdr": "Latest incidents (max {n} rows)",
        "trend_hdr": "Incidents per day",
        "trend_title": "Incidents over time",
        "trend_date": "Date",
        "trend_count": "Incidents",
        "types_hdr": "Incident types distribution",
        "types_none": "No data for incident types.",
        "types_type": "Type",
        "types_count": "Count",
        "status_options": ["PÅGÅR", "KOMMANDE"],  # keep raw DB values
        "no_rows_for_bars": "No incidents to plot for current selection.",
        "no_rows_for_table": "No data to show in the table.",
        "no_rows_for_trend": "No data to show in the time series.",
    },
}

def t(lang: str, key: str, **kwargs) -> str:
    s = LANG[lang].get(key, key)
    return s.format(**kwargs) if kwargs else s

# ---------------------- Länsnamn och centers ----------------------
COUNTY_NAMES = {
    1: "Stockholms län", 3: "Uppsala län", 4: "Södermanlands län", 5: "Östergötlands län",
    6: "Jönköpings län", 7: "Kronobergs län", 8: "Kalmar län", 9: "Gotlands län",
    10: "Blekinge län", 12: "Skåne län", 13: "Hallands län", 14: "Västra Götalands län",
    17: "Värmlands län", 18: "Örebro län", 19: "Västmanlands län", 20: "Dalarnas län",
    21: "Gävleborgs län", 22: "Västernorrlands län", 23: "Jämtlands län",
    24: "Västerbottens län", 25: "Norrbottens län"
}
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
        df = pd.read_sql_query(
            """
            SELECT incident_id, message, message_type, location_descriptor,
                   road_number, county_name, county_no,
                   start_time_utc, end_time_utc, modified_time_utc,
                   latitude, longitude, status
            FROM incidents
            WHERE start_time_utc > datetime('now', '-30 day')
            """,
            con,
        )
        con.close()
    except Exception as e:
        st.warning(f"Databas kunde inte läsas ({e}). Visar tom vy.")
        return pd.DataFrame(columns=cols)

    # dtypes
    if "county_no" in df: df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")
    for c in ("latitude","longitude"):
        if c in df: df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("incident_id","message","message_type","location_descriptor","road_number","county_name","status"):
        if c in df: df[c] = df[c].astype("string").str.strip()
    for c in ("start_time_utc","end_time_utc","modified_time_utc"):
        if c in df: df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)

    # county_display fallback via county_no
    if "county_name" in df and "county_no" in df:
        df["county_name"] = df["county_name"].where(df["county_name"].str.len() > 0, pd.NA)
        mapped = df["county_no"].map(lambda x: COUNTY_NAMES.get(int(x)) if pd.notna(x) else None)
        df["county_display"] = df["county_name"].fillna(mapped).fillna("Okänt län").astype("string")
    else:
        df["county_display"] = "Okänt län"
    return df

# ---------------------- UI ----------------------
# språkval i sidopanel
with st.sidebar:
    lang = st.selectbox(LANG["sv"]["lang_label"], ["sv","en"], index=0, key="lang_sel")

st.title(t(lang, "app_title"))

# Filter
with st.sidebar:
    st.header(t(lang, "filters_hdr"))
    status_val = st.multiselect(t(lang, "status"),
                                LANG[lang]["status_options"],
                                default=LANG[lang]["status_options"])
    df = load_data()
    county_opts = sorted(df["county_display"].dropna().unique()) if not df.empty else []
    county_val = st.multiselect(t(lang, "county"), county_opts, default=list(county_opts))
    q = st.text_input(t(lang, "search"), "")
    road = st.text_input(t(lang, "road"), "").strip()
    only_geo = st.checkbox(t(lang, "only_geo"), value=False)

    min_dt = df["start_time_utc"].min() if not df.empty else pd.Timestamp.utcnow() - pd.Timedelta(days=7)
    max_dt = df["start_time_utc"].max() if not df.empty else pd.Timestamp.utcnow()
    # ta bort tz för date_input
    if getattr(min_dt, "tzinfo", None): min_dt = min_dt.tz_convert(None)
    if getattr(max_dt, "tzinfo", None): max_dt = max_dt.tz_convert(None)
    date_range = st.date_input(t(lang, "date_range"),
                               value=(min_dt.date(), max_dt.date()),
                               min_value=min_dt.date(), max_value=max_dt.date())
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        date_from, date_to = date_range
    else:
        date_from, date_to = min_dt.date(), max_dt.date()

    sort_col = st.selectbox(t(lang, "sort_by"),
                            ["modified_time_utc","start_time_utc","county_display","message_type","road_number"])
    sort_desc = st.checkbox(t(lang, "desc"), value=True)
    max_rows = st.slider(t(lang, "max_rows"), 20, 500, 100, step=20)

# Filtrering
f = df.copy()
if not f.empty:
    if status_val: f = f[f["status"].isin(status_val)]
    if county_val: f = f[f["county_display"].isin(county_val)]

    start_ts = pd.to_datetime(date_from).tz_localize("UTC")
    end_ts   = (pd.to_datetime(date_to) + pd.Timedelta(days=1)).tz_localize("UTC")
    f = f[(f["start_time_utc"] >= start_ts) & (f["start_time_utc"] < end_ts)]

    if q:
        qlc = q.lower()
        mask = (
            f["message"].astype("string").str.lower().str.contains(qlc, na=False) |
            f["location_descriptor"].astype("string").str.lower().str.contains(qlc, na=False) |
            f["road_number"].astype("string").str.lower().str.contains(qlc, na=False)
        )
        f = f[mask]

    if road:
        f = f[f["road_number"].astype("string").str.contains(road, case=False, na=False)]

    if only_geo:
        f = f.dropna(subset=["latitude","longitude"])

# KPI
c1, c2, c3 = st.columns(3)
c1.metric(t(lang, "kpi_ongoing"), int((f["status"]=="PÅGÅR").sum()) if not f.empty else 0)
c2.metric(t(lang, "kpi_upcoming"), int((f["status"]=="KOMMANDE").sum()) if not f.empty else 0)
c3.metric(t(lang, "kpi_total"), 0 if f.empty else len(f))

# ---------------------- Staplar ----------------------
st.subheader(t(lang, "bar_hdr"))

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
    g = (f_base.groupby("county_display", as_index=False).size()
         .rename(columns={"size":"count", "county_display":"county"}))
    g["count"] = pd.to_numeric(g["count"], errors="coerce").fillna(0).astype("int64")
else:
    g = pd.DataFrame(columns=["county","count"])

if g.empty or g["count"].sum() == 0:
    st.info(t(lang, "bar_none"))
else:
    show_all = st.toggle(t(lang, "bar_all"), value=False)
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
            y=labels_disp, x=values, orientation="h",
            text=values, textposition="outside",
            marker=dict(color=bar_colors),
            customdata=labels_full,
            hovertemplate="<b>%{customdata}</b><br>"
                          + (t(lang,"types_count") if lang=="en" else "Antal händelser")
                          + ": %{x}<extra></extra>",
        )]
    )
    max_count = max(values) if values else 1
    fig.update_layout(
        title=(t(lang,"bar_title_all") if show_all else t(lang,"bar_title_top")),
        yaxis=dict(type="category", categoryorder="array", categoryarray=labels_disp, title=""),
        xaxis=dict(type="linear", rangemode="tozero",
                   title=(t(lang,"types_count") if lang=="en" else "Antal händelser"),
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
            idx = labels_disp.index(clicked_short); name = labels_full[idx]
        else:
            name = pt.get("label") or pt.get("x") or pt.get("y")
        if name:
            last = st.session_state.get("_last_clicked")
            if last != name:
                if name in st.session_state.clicked_counties:
                    st.session_state.clicked_counties.remove(name)
                else:
                    st.session_state.clicked_counties.add(name)
            st.session_state["_last_clicked"] = name

    c1_, c2_ = st.columns([3, 1])
    with c1_:
        if st.session_state.clicked_counties:
            st.info(t(lang,"bar_click_filter") + ", ".join(sorted(st.session_state.clicked_counties)))
    with c2_:
        if st.button(t(lang,"bar_clear")):
            st.session_state.clicked_counties = set()

    if st.session_state.clicked_counties:
        f = f[f["county_display"].isin(st.session_state.clicked_counties)]

# ---------------------- Karta ----------------------
st.subheader(t(lang, "map_hdr"))

colA, colB, colC = st.columns([1.3, 1, 1])
with colA:
    map_mode = st.radio(t(lang,"map_mode"), LANG[lang]["map_modes"], horizontal=True)
with colB:
    map_style = st.selectbox(t(lang,"map_style"), LANG[lang]["map_styles"], index=0)
with colC:
    st.toggle(t(lang,"map_color_toggle"), key="use_county_colors",
              value=st.session_state.get("use_county_colors", False),
              help=t(lang,"map_color_toggle"))
use_county_colors = st.session_state.get("use_county_colors", False)

approx_missing = st.checkbox(t(lang,"approx_missing"), value=True)

m = f.copy()
if approx_missing and not m.empty:
    m["latitude"]  = m.apply(lambda r: r["latitude"]  if pd.notna(r["latitude"])
                             else COUNTY_CENTER.get(r["county_display"], (None, None))[0], axis=1)
    m["longitude"] = m.apply(lambda r: r["longitude"] if pd.notna(r["longitude"])
                             else COUNTY_CENTER.get(r["county_display"], (None, None))[1], axis=1)
map_df = m.dropna(subset=["latitude", "longitude"]).copy()

if map_df.empty:
    st.info(t(lang,"map_no_geo"))
else:
    for c in ("county_display","road_number","location_descriptor","status"):
        map_df[c] = map_df[c].astype("string").fillna("")
    map_df["start_str"] = pd.to_datetime(map_df["start_time_utc"], utc=True, errors="coerce").astype("string").fillna("")
    map_df["mod_str"]   = pd.to_datetime(map_df["modified_time_utc"], utc=True, errors="coerce").astype("string").fillna("")

    def hex_to_rgba(h, a=210):
        h = str(h).lstrip("#")
        if len(h) != 6: return [230, 57, 70, a]
        return [int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16), a]

    if use_county_colors and "county_colors" in st.session_state:
        map_df["__color_rgba__"] = map_df["county_display"].map(st.session_state.county_colors).apply(lambda c: hex_to_rgba(c, 210))
    else:
        map_df["__color_rgba__"] = [[230, 57, 70, 210]] * len(map_df)

    selected = set(st.session_state.get("clicked_counties", []))
    focus_df = map_df[map_df["county_display"].isin(selected)] if selected else map_df
    if focus_df.empty: focus_df = map_df

    lat_min, lat_max = float(focus_df["latitude"].min()), float(focus_df["latitude"].max())
    lon_min, lon_max = float(focus_df["longitude"].min()), float(focus_df["longitude"].max())
    lat_center = (lat_min + lat_max) / 2.0
    lon_center = (lon_min + lon_max) / 2.0
    span = max(lat_max - lat_min, lon_max - lon_min)
    zoom = 11 if span <= 0.08 else 9 if span <= 0.25 else 7 if span <= 0.6 else 6 if span <= 1.2 else 5 if span <= 3.0 else 4

    c1x, c2x = st.columns(2)
    with c1x:
        point_radius = st.slider(t(lang,"map_point_size"), 2, 20, 8)
    with c2x:
        heat_intensity = st.slider(t(lang,"map_heat_intensity"), 1, 20, 8)

    data_records = map_df.to_dict(orient="records")
    layers = []
    modes = LANG[lang]["map_modes"]
    if map_mode in (modes[0], modes[2]):
        layers.append(pdk.Layer(
            "ScatterplotLayer", data=data_records,
            get_position="[longitude, latitude]",
            get_fill_color="__color_rgba__",
            get_line_color="[0,0,0,80]",
            line_width_min_pixels=0.5,
            radius_min_pixels=point_radius,
            radius_max_pixels=point_radius + 8,
            pickable=True, auto_highlight=True,
        ))
    if map_mode in (modes[1], modes[2]):
        layers.append(pdk.Layer(
            "HeatmapLayer", data=data_records,
            get_position="[longitude, latitude]",
            aggregation='"SUM"', intensity=heat_intensity, opacity=0.58, threshold=0.01,
        ))

    deck = pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=lat_center, longitude=lon_center, zoom=zoom),
        map_style={"light":"light","dark":"dark","road":"road","satellite":"satellite"}.get(map_style, "light"),
        tooltip={"html": t(lang,"map_tooltip"),
                 "style": {"backgroundColor":"rgba(30,30,30,0.85)","color":"white","fontSize":"12px"}},
    )
    st.pydeck_chart(deck)  # OBS: pydeck har ingen width-parameter ännu

# ---------------------- Tabell ----------------------
st.subheader(t(lang, "table_hdr", n=max_rows))
f_sorted = f.sort_values(by=sort_col, ascending=not sort_desc).head(max_rows) if not f.empty else f
if not f_sorted.empty:
    show_cols = ["incident_id","message_type","status","county_display",
                 "road_number","location_descriptor",
                 "start_time_utc","end_time_utc","modified_time_utc","latitude","longitude"]
    table = f_sorted[show_cols].copy()
    for c in ("start_time_utc","end_time_utc","modified_time_utc"):
        table[c] = pd.to_datetime(table[c], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    st.dataframe(table.rename(columns={"county_display": "county"}), width="stretch")
else:
    st.info(t(lang, "no_rows_for_table"))

# ---------------------- Trend ----------------------
st.subheader(t(lang, "trend_hdr"))
if not f.empty:
    trend = (f.assign(date=pd.to_datetime(f["start_time_utc"], utc=True).dt.date)
               .groupby("date").size().reset_index(name="count"))
    fig_trend = px.line(
        trend, x="date", y="count", markers=True,
        labels={"date": t(lang,"trend_date"), "count": t(lang,"trend_count")},
        title=t(lang,"trend_title"),
    )
    st.plotly_chart(fig_trend, width="stretch")
else:
    st.info(t(lang, "no_rows_for_trend"))

# ---------------------- Typer ----------------------
st.subheader(t(lang, "types_hdr"))
if not f.empty and "message_type" in f.columns:
    type_counts = f["message_type"].value_counts().reset_index()
    type_counts.columns = [t(lang,"types_type"), t(lang,"types_count")]
    fig_types = px.bar(
        type_counts, x=t(lang,"types_count"), y=t(lang,"types_type"),
        orientation="h", text=t(lang,"types_count"), title=t(lang,"types_hdr")
    )
    fig_types.update_traces(textposition="outside")
    st.plotly_chart(fig_types, width="stretch")
else:
    st.info(t(lang, "types_none"))
