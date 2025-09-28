# src/app/streamlit_app.py
import os, json, sqlite3, sys, pathlib, math
from itertools import cycle
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from streamlit_plotly_events import plotly_events
import pydeck as pdk

# --- ensure repo root on sys.path so `src.*` imports work both locally and on Cloud
ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ===================== APP CONFIG =====================
st.set_page_config(page_title="TRV Incidents Dashboard", layout="wide")
DB_PATH = os.getenv("TRAFIK_DB_PATH", "trafik.db")

# ===================== i18n (sv/en) =====================
LANG = {
    "sv": {
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
        "map_color_toggle": "Länsfärger",
        "map_no_geo": "Inga koordinater tillgängliga i nuvarande urval.",
        "map_point_size": "Punktstorlek (pixlar)",
        "map_heat_intensity": "Heatmap-intensitet",
        "map_tooltip": (
            "<b>{county_name}</b><br/>"
            "{road_number} – {location_descriptor}<br/>"
            "Status: {status}<br/>"
            "Start: {start_str}<br/>"
            "Ändrad: {mod_str}"
        ),
        "table_hdr": "Senaste händelser (max {n} rader)",
        "trend_hdr": "Antal händelser per dag",
        "trend_title": "Utveckling av händelser över tid",
        "trend_none": "Ingen data att visa i tidsserien.",
        "types_hdr": "Fördelning av händelsetyper",
        "types_none": "Ingen data att visa för händelsetyper.",
        "types_count": "Antal",
        "types_type": "Typ",
        "status_options": ["PÅGÅR", "KOMMANDE"],
        "sort_options": ["modified_time_utc","start_time_utc","county_name","message_type","road_number"],
        "map_styles": ["light", "dark", "road", "satellite"],
        "lang_label": "Language / Språk",
        "db_missing": "Hittar inte databasen: {p}",
        "db_info": "Databasinfo",
        "db_size": "Storlek",
        "db_mtime": "Senast uppdaterad",
        "db_rows": "Rader (senaste 30 d)",
        "schema_warn": "Schemat i `incidents` matchar inte helt – saknade kolumner fylls på i minnet.",
    },
    "en": {
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
        "map_color_toggle": "County colors",
        "map_no_geo": "No coordinates available for current selection.",
        "map_point_size": "Point size (px)",
        "map_heat_intensity": "Heatmap intensity",
        "map_tooltip": (
            "<b>{county_name}</b><br/>"
            "{road_number} – {location_descriptor}<br/>"
            "Status: {status}<br/>"
            "Start: {start_str}<br/>"
            "Updated: {mod_str}"
        ),
        "table_hdr": "Latest incidents (max {n} rows)",
        "trend_hdr": "Incidents per day",
        "trend_title": "Incidents over time",
        "trend_none": "No data for time series.",
        "types_hdr": "Incident types distribution",
        "types_none": "No data for incident types.",
        "types_count": "Count",
        "types_type": "Type",
        "status_options": ["PÅGÅR", "KOMMANDE"],
        "sort_options": ["modified_time_utc","start_time_utc","county_name","message_type","road_number"],
        "map_styles": ["light", "dark", "road", "satellite"],
        "lang_label": "Language / Språk",
        "db_missing": "Database not found: {p}",
        "db_info": "Database info",
        "db_size": "Size",
        "db_mtime": "Last updated",
        "db_rows": "Rows (last 30 d)",
        "schema_warn": "Table `incidents` schema differs — missing columns are created in-memory.",
    },
}

def _lang_from_query():
    try:
        qp = st.query_params
        val = qp.get("lang", "sv")
        return val if val in ("sv", "en") else "sv"
    except Exception:
        return "sv"

LANG_OPTIONS = ["sv", "en"]
lang_qp = _lang_from_query()
default_idx = LANG_OPTIONS.index(lang_qp) if lang_qp in LANG_OPTIONS else 0
lang = st.sidebar.selectbox(LANG[lang_qp]["lang_label"], LANG_OPTIONS, index=default_idx, key="lang_select")
if lang != lang_qp:
    st.query_params["lang"] = lang

def t(key, **kwargs):
    s = LANG[lang].get(key, key)
    return s.format(**kwargs) if kwargs else s

# ===================== DB HEALTH =====================
def _fmt_size(n):
    if n is None: return "–"
    for unit in ["B","KB","MB","GB"]:
        if n < 1024: return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

def _db_stats(path: str):
    if not os.path.exists(path):
        return {"exists": False}
    st_ = os.stat(path)
    size = st_.st_size
    mtime = datetime.fromtimestamp(st_.st_mtime, tz=timezone.utc)
    return {"exists": True, "size": size, "mtime": mtime}

with st.sidebar.expander(t("db_info"), expanded=True):
    stats = _db_stats(DB_PATH)
    if not stats.get("exists"):
        st.error(t("db_missing", p=DB_PATH))
    else:
        st.write(t("db_size") + f": {_fmt_size(stats['size'])}")
        st.write(t("db_mtime") + f": {stats['mtime'].strftime('%Y-%m-%d %H:%M:%SZ')}")

# ===================== DATA LOADING =====================
_EXPECTED_COLS = [
    "incident_id","message","message_type","location_descriptor","road_number",
    "county_name","county_no","start_time_utc","end_time_utc","modified_time_utc",
    "latitude","longitude","status",
]

@st.cache_data(ttl=180)
def load_data(db_path: str) -> pd.DataFrame:
    if not os.path.exists(db_path):
        return pd.DataFrame(columns=_EXPECTED_COLS)

    con = sqlite3.connect(db_path)
    try:
        # försök med exakt SELECT (snabbt)
        query = """
            SELECT incident_id, message, message_type, location_descriptor,
                   road_number, county_name, county_no,
                   start_time_utc, end_time_utc, modified_time_utc,
                   latitude, longitude, status
            FROM incidents
        """
        df = pd.read_sql_query(query, con)
    except Exception:
        # om kolumner saknas: hämta allt och lappa
        try:
            df = pd.read_sql_query("SELECT * FROM incidents", con)
            missing = [c for c in _EXPECTED_COLS if c not in df.columns]
            for c in missing:
                df[c] = pd.NA
            st.warning(t("schema_warn"))
            df = df[_EXPECTED_COLS]
        except Exception:
            df = pd.DataFrame(columns=_EXPECTED_COLS)
    finally:
        con.close()

    # typer
    if "county_no" in df.columns:
        df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")
    for col in ["latitude","longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["incident_id","message","message_type","location_descriptor","road_number","county_name","status"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # datum → dt64[ns, UTC] om finns
    for col in ["start_time_utc","end_time_utc","modified_time_utc"]:
        if col in df.columns:
            s = pd.to_datetime(df[col], errors="coerce", utc=True)
            df[col] = s

    return df

df = load_data(DB_PATH)

# visa radantal i sidopanel (senaste 30 dagar om kolumn finns)
if not df.empty and "start_time_utc" in df.columns and pd.api.types.is_datetime64tz_dtype(df["start_time_utc"]):
    last30 = (df["start_time_utc"] >= (pd.Timestamp.utcnow().tz_localize("UTC") - pd.Timedelta(days=30))).sum()
    st.sidebar.write(t("db_rows") + f": {int(last30)}")
else:
    st.sidebar.write(t("db_rows") + ": 0")

# ===================== TITLE =====================
st.title(t("app_title"))

# ===================== SIDEBAR FILTERS =====================
with st.sidebar:
    st.header(t("filters_hdr"))

    status_val = st.multiselect(
        t("status"),
        LANG[lang]["status_options"],
        default=LANG[lang]["status_options"],
        key="flt_status"
    )

    county_opts = sorted(df["county_name"].dropna().unique()) if not df.empty else []
    county_val = st.multiselect(t("county"), county_opts, default=list(county_opts), key="flt_county")

    q = st.text_input(t("search"), value="", key="flt_search")
    road = st.text_input(t("road"), value="", key="flt_road").strip()
    only_geo = st.checkbox(t("only_geo"), value=False, key="flt_only_geo")

    # Date range
    if "start_time_utc" in df.columns and not df.empty and pd.api.types.is_datetime64tz_dtype(df["start_time_utc"]):
        min_dt = df["start_time_utc"].min()
        max_dt = df["start_time_utc"].max()
    else:
        min_dt = pd.Timestamp.utcnow().tz_localize("UTC") - pd.Timedelta(days=7)
        max_dt = pd.Timestamp.utcnow().tz_localize("UTC")

    min_date, max_date = min_dt.date(), max_dt.date()
    date_range = st.date_input(
        t("date_range"),
        value=(min_date, max_date),
        min_value=min_date, max_value=max_date,
        key="flt_daterange"
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        date_from, date_to = date_range
    else:
        date_from, date_to = min_date, max_date

    sort_col = st.selectbox(t("sort_by"), LANG[lang]["sort_options"], key="flt_sortby")
    sort_desc = st.checkbox(t("desc"), value=True, key="flt_desc")
    max_rows = st.slider(t("max_rows"), 20, 500, 100, step=20, key="flt_maxrows")

# ===================== FILTER APPLICATION =====================
f = df.copy()
if not f.empty:
    if status_val:
        f = f[f["status"].isin(status_val)]
    if county_val:
        f = f[f["county_name"].isin(county_val)]

    # datumfilter om kolumn finns
    if "start_time_utc" in f.columns and pd.api.types.is_datetime64tz_dtype(f["start_time_utc"]):
        start_ts = pd.to_datetime(date_from).tz_localize("UTC")
        end_ts = (pd.to_datetime(date_to) + pd.Timedelta(days=1)).tz_localize("UTC")
        f = f[(f["start_time_utc"] >= start_ts) & (f["start_time_utc"] < end_ts)]

    # fritext
    if q:
        qlc = q.lower()
        for c in ["message","location_descriptor","road_number"]:
            if c not in f.columns:
                f[c] = ""
        mask = (
            f["message"].astype("string").str.lower().str.contains(qlc, na=False) |
            f["location_descriptor"].astype("string").str.lower().str.contains(qlc, na=False) |
            f["road_number"].astype("string").str.lower().str.contains(qlc, na=False)
        )
        f = f[mask]

    if road:
        if "road_number" not in f.columns:
            f["road_number"] = ""
        f = f[f["road_number"].astype("string").str.contains(road, case=False, na=False)]

    if only_geo:
        if not {"latitude","longitude"}.issubset(f.columns):
            f["latitude"] = pd.NA; f["longitude"] = pd.NA
        f = f.dropna(subset=["latitude","longitude"])

# ===================== KPI METRICS =====================
c1, c2, c3 = st.columns(3)
c1.metric(t("kpi_ongoing"), int((f.get("status","")=="PÅGÅR").sum()) if not f.empty else 0)
c2.metric(t("kpi_upcoming"), int((f.get("status","")=="KOMMANDE").sum()) if not f.empty else 0)
c3.metric(t("kpi_total"), 0 if f.empty else len(f))

approx_missing = st.checkbox(t("approx_missing"), value=True)

# ===================== BAR =====================
st.subheader(t("bar_hdr"))
COLOR_MAP_PATH = "county_colors.json"

def _load_color_map(path=COLOR_MAP_PATH):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fjson:
                return json.load(fjson)
        except Exception:
            return {}
    return {}

def _save_color_map(color_map, path=COLOR_MAP_PATH):
    try:
        with open(path, "w", encoding="utf-8") as fjson:
            json.dump(color_map, fjson, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _short_label(s, n=24):
    s = str(s)
    return (s[:n] + "…") if len(s) > n else s

f_base = f.copy()
if not f_base.empty and "county_name" in f_base.columns:
    f_base["county_name"] = f_base["county_name"].astype("string").str.strip().fillna("Okänt län")
    g = (f_base.groupby("county_name", as_index=False).size().rename(columns={"size":"count"}))
    g["count"] = pd.to_numeric(g["count"], errors="coerce").fillna(0).astype("int64")
else:
    g = pd.DataFrame(columns=["county_name","count"])

if g.empty or g["count"].sum() == 0:
    st.info(t("bar_none"))
else:
    show_all = st.toggle(t("bar_all"), value=False)
    g_sorted = g.sort_values("count", ascending=False).reset_index(drop=True)
    plot_df = (g_sorted if show_all else g_sorted.head(10)).copy()

    if "county_colors" not in st.session_state:
        st.session_state.county_colors = _load_color_map()

    palette_full = (
        px.colors.qualitative.Alphabet
        + px.colors.qualitative.Plotly
        + px.colors.qualitative.Set3
        + px.colors.qualitative.Safe
    )
    color_cycle = cycle(palette_full)
    updated = False
    for lbl in g_sorted["county_name"]:
        if lbl not in st.session_state.county_colors:
            st.session_state.county_colors[lbl] = next(color_cycle); updated = True
    if updated:
        _save_color_map(st.session_state.county_colors)

    plot_df = plot_df.sort_values("count", ascending=True)
    labels_full = plot_df["county_name"].tolist()
    labels_disp = [_short_label(x) for x in labels_full]
    values = plot_df["count"].astype(int).tolist()
    bar_colors = [st.session_state.county_colors[lbl] for lbl in labels_full]

    fig = go.Figure(data=[go.Bar(
        y=labels_disp, x=values, orientation="h",
        text=values, textposition="outside",
        marker=dict(color=bar_colors),
        customdata=labels_full,
        hovertemplate="<b>%{customdata}</b><br>Count: %{x}<extra></extra>",
    )])
    max_count = max(values) if values else 1
    fig.update_layout(
        title=t("bar_title_all") if show_all else t("bar_title_top"),
        yaxis=dict(type="category", categoryorder="array", categoryarray=labels_disp, title=""),
        xaxis=dict(type="linear", rangemode="tozero", title="Count", range=[0, max(1, int(max_count * 1.15))]),
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
            if last != name:
                if name in st.session_state.clicked_counties:
                    st.session_state.clicked_counties.remove(name)
                else:
                    st.session_state.clicked_counties.add(name)
            st.session_state["_last_clicked"] = name

    c1_, c2_ = st.columns([3, 1])
    with c1_:
        if st.session_state.clicked_counties:
            st.info(t("bar_click_filter") + ", ".join(sorted(st.session_state.clicked_counties)))
    with c2_:
        if st.button(t("bar_clear")):
            st.session_state.clicked_counties = set()

    if st.session_state.clicked_counties:
        f = f[f["county_name"].astype("string").str.strip().fillna("Okänt län").isin(st.session_state.clicked_counties)]

# ===================== MAP =====================
st.subheader(t("map_hdr"))

colA, colB, colC = st.columns([1.3, 1, 1], gap="small")
with colA:
    map_mode = st.radio(t("map_mode"), LANG[lang]["map_modes"], horizontal=True, key="map_mode")
with colB:
    map_style = st.selectbox(t("map_style"), LANG[lang]["map_styles"], index=0, key="map_style")
with colC:
    use_county_colors = st.toggle(t("map_color_toggle"), key="use_county_colors",
                                  value=st.session_state.get("use_county_colors", False),
                                  help=t("map_color_toggle"))

approx_missing = st.checkbox(t("approx_missing"), value=True, key="approx_missing")

COUNTY_CENTER = {
    "Stockholms län": (59.334, 18.063),
    "Uppsala län": (59.858, 17.638),
    "Skåne län": (55.604, 13.003),
    "Västra Götalands län": (57.708, 11.974),
}

m = f.copy()
if approx_missing and not m.empty:
    if "latitude" not in m.columns: m["latitude"] = pd.NA
    if "longitude" not in m.columns: m["longitude"] = pd.NA
    m["latitude"] = m.apply(lambda r: r["latitude"] if pd.notna(r["latitude"])
                            else COUNTY_CENTER.get(r.get("county_name"), (None, None))[0], axis=1)
    m["longitude"] = m.apply(lambda r: r["longitude"] if pd.notna(r["longitude"])
                             else COUNTY_CENTER.get(r.get("county_name"), (None, None))[1], axis=1)

map_df = m.dropna(subset=["latitude","longitude"]).copy()

if map_df.empty:
    st.info(t("map_no_geo"))
else:
    for c in ["county_name","road_number","location_descriptor","status"]:
        if c not in map_df.columns: map_df[c] = ""
        map_df[c] = map_df[c].astype("string").fillna("")
    for c, out in [("start_time_utc","start_str"), ("modified_time_utc","mod_str")]:
        if c in map_df.columns:
            map_df[out] = pd.to_datetime(map_df[c], errors="coerce").astype("string").fillna("")
        else:
            map_df[out] = ""

    def _hex_to_rgba(h, a=210):
        h = str(h).lstrip("#")
        if len(h) != 6:
            return [230, 57, 70, a]
        return [int(h[0:2],16), int(h[2:4],16), int(h[4:6],16), a]

    if use_county_colors and "county_colors" in st.session_state:
        map_df["__color_rgba__"] = map_df["county_name"].map(st.session_state.county_colors).apply(lambda c: _hex_to_rgba(c,210))
    else:
        map_df["__color_rgba__"] = [[230,57,70,210]] * len(map_df)

    selected = set(st.session_state.get("clicked_counties", []))
    focus_df = map_df[map_df["county_name"].isin(selected)] if selected else map_df
    if focus_df.empty: focus_df = map_df

    lat_min, lat_max = float(focus_df["latitude"].min()), float(focus_df["latitude"].max())
    lon_min, lon_max = float(focus_df["longitude"].min()), float(focus_df["longitude"].max())
    lat_center, lon_center = (lat_min + lat_max)/2.0, (lon_min + lon_max)/2.0
    span = max(lat_max - lat_min, lon_max - lon_min)
    zoom = 11 if span <= 0.08 else 9 if span <= 0.25 else 7 if span <= 0.6 else 6 if span <= 1.2 else 5 if span <= 3.0 else 4

    c1x, c2x = st.columns(2)
    with c1x:
        point_radius = st.slider(t("map_point_size"), 2, 20, 8, key="map_point_size")
    with c2x:
        heat_intensity = st.slider(t("map_heat_intensity"), 1, 20, 8, key="map_heat_intensity")

    data_records = map_df.to_dict(orient="records")
    layers = []
    modes = LANG[lang]["map_modes"]
    if map_mode in (modes[0], modes[2]):
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
    if map_mode in (modes[1], modes[2]):
        layers.append(pdk.Layer(
            "HeatmapLayer",
            data=data_records,
            get_position="[longitude, latitude]",
            aggregation='"SUM"',
            intensity=heat_intensity,
            opacity=0.58,
            threshold=0.01,
        ))

    tooltip = {"html": LANG[lang]["map_tooltip"],
               "style": {"backgroundColor":"rgba(30,30,30,0.85)","color":"white","fontSize":"12px"}}
    style_map = {"light":"light","dark":"dark","road":"road","satellite":"satellite"}

    st.pydeck_chart(pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=lat_center, longitude=lon_center, zoom=zoom),
        map_style=style_map.get(map_style, "light"),
        tooltip=tooltip,
    ), use_container_width=True)

# ===================== TREND =====================
st.subheader(t("trend_hdr"))
if not f.empty and "start_time_utc" in f.columns:
    trend = (f.assign(date=pd.to_datetime(f["start_time_utc"]).dt.date)
               .groupby("date").size().reset_index(name="count"))
    fig_trend = px.line(trend, x="date", y="count", markers=True,
                        labels={"date":"Datum" if lang=="sv" else "Date",
                                "count":"Antal händelser" if lang=="sv" else "Incidents"},
                        title=t("trend_title"))
    st.plotly_chart(fig_trend, use_container_width=True,
                    config={"displayModeBar": True, "scrollZoom": True})
else:
    st.info(t("trend_none"))

# ===================== TYPES =====================
st.subheader(t("types_hdr"))
if not f.empty and "message_type" in f.columns:
    type_counts = f["message_type"].value_counts().reset_index()
    type_counts.columns = [t("types_type"), t("types_count")]
    fig_types = px.bar(type_counts, x=t("types_count"), y=t("types_type"),
                       orientation="h", text=t("types_count"), title=t("types_hdr"))
    fig_types.update_traces(textposition="outside")
    st.plotly_chart(fig_types, use_container_width=True,
                    config={"displayModeBar": True, "scrollZoom": True})
else:
    st.info(t("types_none"))
