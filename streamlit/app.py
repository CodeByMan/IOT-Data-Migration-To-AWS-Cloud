from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import pydeck as pdk
import snowflake.connector
import streamlit as st

DATABASE = "HACKATHON_IOT"
SCHEMA = "ANALYTICS"
TABLE = "GOLD_DEVICE_DAILY"
WAREHOUSE = "HACKATHON_DBT_WH"
ROLE = "HACKATHON_KAFKA_CONNECTOR_ROLE"
REFRESH_SECONDS = 30
DISPLAY_TIMEZONE = ZoneInfo("Asia/Karachi")

st.set_page_config(
    page_title="IoT Migration Command Center",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

PREMIUM_CSS = """
<style>
:root {
    --primary: #0f4cbd;
    --primary-2: #1d63d8;
    --surface: rgba(255,255,255,.90);
    --border: rgba(148,163,184,.24);
    --text: #0f172a;
    --muted: #64748b;
}
html, body, [class*="css"] { font-family: Inter, "Segoe UI", Arial, sans-serif; }
[data-testid="stAppViewContainer"] {
    background:
      radial-gradient(circle at 10% 8%, rgba(37,99,235,.12), transparent 25rem),
      radial-gradient(circle at 90% 5%, rgba(14,165,233,.10), transparent 24rem),
      linear-gradient(180deg,#f8fbff 0%,#f3f7fc 100%);
}
[data-testid="stHeader"] {
    height: 3.4rem;
    background: rgba(248,251,255,.90);
    backdrop-filter: blur(18px);
    border-bottom: 1px solid rgba(148,163,184,.18);
}
.block-container {
    max-width: 1500px;
    padding-top: 5.2rem;
    padding-bottom: 3rem;
    padding-left: clamp(1rem,2.4vw,2.2rem);
    padding-right: clamp(1rem,2.4vw,2.2rem);
}
[data-testid="stSidebar"] {
    background: linear-gradient(180deg,#0f4cbd 0%,#123b8f 55%,#0b2f72 100%);
    border-right: 1px solid rgba(255,255,255,.12);
}
[data-testid="stSidebar"] > div:first-child {
    padding-top: 1.4rem;
}

/* Sidebar headings, labels and supporting text remain white. */
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] h4,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] small,
[data-testid="stSidebar"] [data-testid="stCaptionContainer"],
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
    color: #f8fafc !important;
}

/* Every input/select/date control receives a light surface. */
[data-testid="stSidebar"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] [data-baseweb="input"] > div,
[data-testid="stSidebar"] [data-baseweb="base-input"],
[data-testid="stSidebar"] [data-testid="stDateInput"] > div,
[data-testid="stSidebar"] [data-testid="stMultiSelect"] > div,
[data-testid="stSidebar"] [role="combobox"] {
    background: #ffffff !important;
    border-color: #cbd5e1 !important;
    color: #0f172a !important;
    -webkit-text-fill-color: #0f172a !important;
}

/* Force all field values, placeholders and nested text to dark. */
[data-testid="stSidebar"] input,
[data-testid="stSidebar"] input *,
[data-testid="stSidebar"] [role="combobox"],
[data-testid="stSidebar"] [role="combobox"] *,
[data-testid="stSidebar"] [data-baseweb="select"] span,
[data-testid="stSidebar"] [data-baseweb="input"] span,
[data-testid="stSidebar"] [data-testid="stDateInput"] span,
[data-testid="stSidebar"] [data-testid="stMultiSelect"] span {
    color: #0f172a !important;
    -webkit-text-fill-color: #0f172a !important;
    caret-color: #0f172a !important;
    opacity: 1 !important;
}

/* Placeholder text must also remain readable. */
[data-testid="stSidebar"] input::placeholder {
    color: #475569 !important;
    -webkit-text-fill-color: #475569 !important;
    opacity: 1 !important;
}

/* Selected multiselect tags. */
[data-testid="stSidebar"] [data-baseweb="tag"] {
    background: #dbeafe !important;
    border: 1px solid #93c5fd !important;
}
[data-testid="stSidebar"] [data-baseweb="tag"],
[data-testid="stSidebar"] [data-baseweb="tag"] * {
    color: #0f172a !important;
    -webkit-text-fill-color: #0f172a !important;
}

/* Icons within light fields are dark, while the sidebar collapse icon stays white. */
[data-testid="stSidebar"] [data-baseweb="select"] svg,
[data-testid="stSidebar"] [data-baseweb="input"] svg,
[data-testid="stSidebar"] [data-testid="stDateInput"] svg,
[data-testid="stSidebar"] [data-testid="stMultiSelect"] svg {
    fill: #0f172a !important;
    color: #0f172a !important;
}
[data-testid="stSidebarCollapseButton"] svg {
    fill: #ffffff !important;
    color: #ffffff !important;
}

/* Slider text remains visible on blue. */
[data-testid="stSidebar"] [data-testid="stSlider"] span,
[data-testid="stSidebar"] [data-testid="stSlider"] p {
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
}

/* Data-source block. */
[data-testid="stSidebar"] [data-testid="stCodeBlock"] {
    background: rgba(3,18,55,.46) !important;
    border: 1px solid rgba(255,255,255,.18);
    border-radius: 12px;
}
[data-testid="stSidebar"] [data-testid="stCodeBlock"] pre,
[data-testid="stSidebar"] [data-testid="stCodeBlock"] code {
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    background: transparent !important;
    white-space: pre-wrap !important;
    overflow-wrap: anywhere !important;
    font-size: .72rem !important;
}

/* BaseWeb dropdown/calendar popovers are rendered outside the sidebar. */
div[data-baseweb="popover"] [role="option"],
div[data-baseweb="popover"] [role="option"] *,
div[data-baseweb="menu"] [role="option"],
div[data-baseweb="menu"] [role="option"] *,
div[data-baseweb="calendar"] *,
ul[role="listbox"] li,
ul[role="listbox"] li * {
    color: #0f172a !important;
    -webkit-text-fill-color: #0f172a !important;
}
div[data-baseweb="popover"],
div[data-baseweb="menu"],
div[data-baseweb="calendar"],
ul[role="listbox"] {
    background: #ffffff !important;
}
.hero {
    position: relative;
    overflow: hidden;
    border: 1px solid var(--border);
    background: linear-gradient(135deg,rgba(255,255,255,.96),rgba(234,243,255,.92));
    box-shadow: 0 22px 60px rgba(15,23,42,.09);
    border-radius: 26px;
    padding: clamp(1.2rem,2.5vw,2rem);
    margin-bottom: 1.4rem;
}
.hero::after {
    content: "";
    position: absolute;
    width: 15rem;
    height: 15rem;
    right: -5rem;
    top: -6rem;
    border-radius: 50%;
    background: radial-gradient(circle,rgba(37,99,235,.18),transparent 68%);
}
.hero-kicker { color: var(--primary); font-weight: 850; font-size:.78rem; letter-spacing:.13em; text-transform:uppercase; margin-bottom:.42rem; }
.hero-title { color:var(--text); font-size:clamp(1.75rem,3.2vw,3.15rem); font-weight:900; letter-spacing:-.045em; line-height:1.04; margin:0; max-width:980px; }
.hero-subtitle { color:#475569; font-size:clamp(.9rem,1.3vw,1.05rem); line-height:1.65; max-width:920px; margin-top:.65rem; margin-bottom:0; }
.status-row { display:flex; flex-wrap:wrap; gap:.55rem; margin-top:1rem; }
.status-pill { display:inline-flex; align-items:center; gap:.42rem; padding:.4rem .72rem; border-radius:999px; border:1px solid rgba(37,99,235,.17); background:rgba(255,255,255,.78); color:#334155; font-size:.78rem; font-weight:750; }
.status-dot { width:.5rem; height:.5rem; border-radius:50%; background:#22c55e; box-shadow:0 0 0 4px rgba(34,197,94,.13); }
div[data-testid="stHorizontalBlock"] { gap: 1rem; }
[data-testid="stMetric"] { min-height:118px; background:rgba(255,255,255,.90); border:1px solid var(--border); padding:1rem 1.05rem; border-radius:19px; box-shadow:0 12px 34px rgba(15,23,42,.055); }
[data-testid="stMetricLabel"] { color:var(--muted); font-weight:750; }
[data-testid="stMetricValue"] { color:var(--text); font-weight:900; letter-spacing:-.03em; font-size:clamp(1.45rem,2.2vw,2rem); }
.latest-event-card { display:grid; grid-template-columns:auto 1fr; align-items:center; gap:.9rem; margin:1rem 0 1.2rem; padding:1rem 1.15rem; border-radius:18px; background:linear-gradient(90deg,rgba(15,76,189,.10),rgba(255,255,255,.92)); border:1px solid rgba(15,76,189,.18); box-shadow:0 10px 28px rgba(15,23,42,.05); }
.latest-event-icon { width:2.5rem; height:2.5rem; display:grid; place-items:center; border-radius:13px; background:var(--primary); color:#fff; font-size:1.15rem; }
.latest-event-label { color:var(--muted); font-size:.76rem; font-weight:800; text-transform:uppercase; letter-spacing:.09em; }
.latest-event-value { color:var(--text); font-size:clamp(1rem,1.7vw,1.28rem); font-weight:850; margin-top:.15rem; overflow-wrap:anywhere; }

.current-time-card {
    margin: 0 0 1.2rem;
    padding: .9rem 1.1rem;
    border-radius: 17px;
    background: rgba(255,255,255,.90);
    border: 1px solid var(--border);
    box-shadow: 0 10px 28px rgba(15,23,42,.045);
}
.current-time-label {
    color: var(--muted);
    font-size: .74rem;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: .08em;
}
.current-time-value {
    color: var(--primary);
    font-size: clamp(1rem,1.7vw,1.22rem);
    font-weight: 850;
    margin-top: .14rem;
}

.section-card { border:1px solid var(--border); background:rgba(255,255,255,.86); border-radius:20px; padding:1rem 1.05rem .65rem; box-shadow:0 14px 38px rgba(15,23,42,.05); margin-top:1.2rem; margin-bottom:.75rem; }
.section-title { color:var(--text); font-size:1.08rem; font-weight:850; margin:0; }
.section-copy { color:var(--muted); font-size:.84rem; margin:.25rem 0 .1rem; line-height:1.55; }
.refresh-line { display:flex; flex-wrap:wrap; justify-content:space-between; gap:.7rem; color:var(--muted); font-size:.78rem; padding-top:.9rem; }
div[data-testid="stAlert"] { border-radius:16px; }
.stDataFrame { border-radius:16px; overflow:hidden; }
@media (max-width:1100px) { .block-container { padding-top:4.8rem; } [data-testid="stMetric"] { min-height:108px; } }
@media (max-width:768px) {
    .block-container { padding-top:4.5rem; padding-left:.8rem; padding-right:.8rem; }
    .hero { border-radius:20px; padding:1.15rem; }
    .status-row { gap:.4rem; }
    .status-pill { font-size:.72rem; padding:.34rem .58rem; }
    .latest-event-card { grid-template-columns:1fr; }
    .latest-event-icon { display:none; }
    [data-testid="stMetric"] { min-height:100px; }
    .section-card { margin-top:1rem; }
}
@media (max-width:480px) {
    .block-container { padding-top:4.2rem; padding-left:.6rem; padding-right:.6rem; }
    .hero-title { font-size:1.72rem; }
    .hero-subtitle { font-size:.88rem; }
    .latest-event-value { font-size:.98rem; }
}
footer { visibility:hidden; }
</style>
"""

st.markdown(PREMIUM_CSS, unsafe_allow_html=True)


def required_environment(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def create_connection() -> snowflake.connector.SnowflakeConnection:
    key_path = Path(required_environment("STREAMLIT_SNOWFLAKE_PRIVATE_KEY_PATH"))
    if not key_path.is_file():
        raise RuntimeError("Configured Snowflake private-key file does not exist.")
    return snowflake.connector.connect(
        account=required_environment("STREAMLIT_SNOWFLAKE_ACCOUNT"),
        user=required_environment("STREAMLIT_SNOWFLAKE_USER"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=str(key_path),
        private_key_file_pwd=required_environment("STREAMLIT_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        client_session_keep_alive=False,
        session_parameters={"QUERY_TAG":"hackathon_streamlit_dashboard","TIMEZONE":"Asia/Karachi"},
    )


def execute_dataframe(connection: snowflake.connector.SnowflakeConnection, sql: str) -> pd.DataFrame:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        return cursor.fetch_pandas_all()
    finally:
        cursor.close()


def load_dashboard_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    connection = create_connection()
    try:
        daily = execute_dataframe(connection, """
            SELECT
                DEVICE_ID AS "device_id",
                EVENT_DATE AS "event_date",
                TOTAL_CDC_EVENTS AS "total_events",
                CREATE_EVENTS AS "create_events",
                UPDATE_EVENTS AS "update_events",
                DELETE_EVENTS AS "delete_events",
                MINIMUM_AQI AS "minimum_aqi",
                MAXIMUM_AQI AS "maximum_aqi",
                AVERAGE_AQI AS "average_aqi",
                AVERAGE_LATITUDE AS "latitude",
                AVERAGE_LONGITUDE AS "longitude",
                FIRST_EVENT_TIMESTAMP AS "first_event_timestamp",
                LAST_EVENT_TIMESTAMP AS "last_event_timestamp"
            FROM HACKATHON_IOT.ANALYTICS.GOLD_DEVICE_DAILY
            ORDER BY EVENT_DATE, DEVICE_ID
        """)
        activity = execute_dataframe(connection, """
            SELECT
                DEVICE_ID AS "device_id",
                SUM(TOTAL_CDC_EVENTS) AS "total_events",
                SUM(CREATE_EVENTS) AS "create_events",
                SUM(UPDATE_EVENTS) AS "update_events",
                SUM(DELETE_EVENTS) AS "delete_events",
                AVG(AVERAGE_LATITUDE) AS "latitude",
                AVG(AVERAGE_LONGITUDE) AS "longitude",
                AVG(AVERAGE_AQI) AS "average_aqi",
                MAX(MAXIMUM_AQI) AS "maximum_aqi",
                MAX(LAST_EVENT_TIMESTAMP) AS "last_event_timestamp"
            FROM HACKATHON_IOT.ANALYTICS.GOLD_DEVICE_DAILY
            GROUP BY DEVICE_ID
            ORDER BY "total_events" DESC, "device_id"
        """)
        return daily, activity
    finally:
        connection.close()


def to_islamabad_time(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    return parsed.dt.tz_convert(DISPLAY_TIMEZONE)


def normalize_data(daily: pd.DataFrame, activity: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily = daily.copy(); activity = activity.copy()
    numeric_columns = ["total_events","create_events","update_events","delete_events","minimum_aqi","maximum_aqi","average_aqi","latitude","longitude"]
    for frame in (daily, activity):
        for column in numeric_columns:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "event_date" in daily.columns:
        daily["event_date"] = pd.to_datetime(daily["event_date"], errors="coerce")
    for frame in (daily, activity):
        for column in ["first_event_timestamp", "last_event_timestamp"]:
            if column in frame.columns:
                frame[column] = to_islamabad_time(frame[column])
    return daily, activity


def render_section_header(title: str, copy: str) -> None:
    st.markdown(f'<div class="section-card"><p class="section-title">{title}</p><p class="section-copy">{copy}</p></div>', unsafe_allow_html=True)


def render_device_map(activity: pd.DataFrame) -> None:
    render_section_header("Device Activity Map", "Gold-layer device locations sized by total CDC event activity.")
    map_data = activity.dropna(subset=["latitude", "longitude"]).copy()
    if map_data.empty:
        st.info("No valid device coordinates are currently available."); return
    map_data["radius"] = map_data["total_events"].fillna(1).clip(lower=1) * 15000
    map_data["average_aqi"] = map_data["average_aqi"].round(1)
    latitude = float(map_data["latitude"].mean()); longitude = float(map_data["longitude"].mean())
    layer = pdk.Layer("ScatterplotLayer", data=map_data, get_position="[longitude, latitude]", get_radius="radius", pickable=True, auto_highlight=True, radius_min_pixels=9, radius_max_pixels=38, get_fill_color=[15,76,189,220], get_line_color=[219,234,254,255], line_width_min_pixels=3)
    view_state = pdk.ViewState(latitude=latitude, longitude=longitude, zoom=4.2, pitch=35, bearing=0)
    tooltip = {"html":"<div style='font-family:Inter,Arial;padding:6px'><b>{device_id}</b><br/>CDC events: {total_events}<br/>Average AQI: {average_aqi}<br/>Maximum AQI: {maximum_aqi}</div>","style":{"backgroundColor":"#0f172a","color":"#f8fafc","borderRadius":"12px","border":"1px solid rgba(255,255,255,.14)"}}
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip, map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"), width="stretch", height=500)


def render_aqi_trend(daily: pd.DataFrame) -> None:
    render_section_header("AQI Time-Series Trend", "Smoothed daily AQI movement with clear average and peak readings from the Gold layer.")
    chart_data = daily.dropna(subset=["event_date"]).copy()
    if chart_data.empty:
        st.info("No dated AQI records are currently available."); return
    trend = chart_data.groupby("event_date", as_index=False).agg(average_aqi=("average_aqi","mean"), maximum_aqi=("maximum_aqi","max")).sort_values("event_date")
    melted = trend.melt(id_vars=["event_date"], value_vars=["average_aqi","maximum_aqi"], var_name="metric", value_name="aqi")
    melted["metric"] = melted["metric"].map({"average_aqi":"Average AQI","maximum_aqi":"Maximum AQI"})
    base = alt.Chart(melted).encode(
        x=alt.X(
            "event_date:T",
            title=None,
            axis=alt.Axis(
                format="%d %b",
                labelAngle=0,
                labelColor="#64748b",
                tickColor="#cbd5e1",
                domainColor="#cbd5e1",
                grid=False,
            ),
        ),
        y=alt.Y(
            "aqi:Q",
            title="AQI",
            scale=alt.Scale(zero=False),
            axis=alt.Axis(
                titleColor="#475569",
                labelColor="#64748b",
                tickColor="#cbd5e1",
                domain=False,
                gridColor="#e2e8f0",
                gridDash=[3, 4],
            ),
        ),
        color=alt.Color(
            "metric:N",
            title=None,
            scale=alt.Scale(
                domain=["Average AQI", "Maximum AQI"],
                range=["#0f4cbd", "#f97316"],
            ),
            legend=alt.Legend(
                orient="top",
                direction="horizontal",
                labelColor="#475569",
                symbolStrokeWidth=4,
                padding=8,
            ),
        ),
        tooltip=[
            alt.Tooltip("event_date:T", title="Date", format="%A, %d %B %Y"),
            alt.Tooltip("metric:N", title="Metric"),
            alt.Tooltip("aqi:Q", title="AQI", format=".1f"),
        ],
    )

    glow = base.mark_line(
        strokeWidth=9,
        opacity=0.10,
        interpolate="monotone",
    )

    line = base.mark_line(
        strokeWidth=3.5,
        interpolate="monotone",
    )

    points = base.mark_circle(
        size=95,
        stroke="#ffffff",
        strokeWidth=2,
        opacity=1,
    )

    chart = (
        (glow + line + points)
        .properties(height=365)
        .configure_view(
            stroke="#dbe5f3",
            strokeWidth=1,
            cornerRadius=14,
            fill="#fbfdff",
        )
        .configure_axis(
            labelFont="Segoe UI",
            titleFont="Segoe UI",
            labelFontSize=12,
            titleFontSize=12,
        )
        .configure_legend(
            labelFont="Segoe UI",
            labelFontSize=12,
        )
    )

    st.altair_chart(chart, width="stretch")


def render_top_devices(activity: pd.DataFrame, top_n: int) -> None:
    render_section_header(f"Top {top_n} Devices by Activity", "Activity ranking with event totals and AQI severity encoded in the bar color.")
    chart_data = activity.dropna(subset=["device_id","total_events"]).nlargest(top_n,"total_events").sort_values("total_events",ascending=True)
    if chart_data.empty:
        st.info("No device activity records are currently available."); return
    bars = alt.Chart(chart_data).mark_bar(
        cornerRadiusTopRight=9,
        cornerRadiusBottomRight=9,
        height=22,
    ).encode(
        x=alt.X(
            "total_events:Q",
            title="CDC events",
            axis=alt.Axis(
                titleColor="#475569",
                labelColor="#64748b",
                tickColor="#cbd5e1",
                domain=False,
                gridColor="#e2e8f0",
                gridDash=[3, 4],
            ),
        ),
        y=alt.Y(
            "device_id:N",
            title=None,
            sort=None,
            axis=alt.Axis(
                labelColor="#334155",
                labelFontWeight=600,
                labelLimit=180,
                ticks=False,
                domain=False,
            ),
        ),
        color=alt.Color(
            "average_aqi:Q",
            title="Average AQI",
            scale=alt.Scale(
                domain=[0, 50, 100, 150, 200, 300],
                range=[
                    "#22c55e",
                    "#84cc16",
                    "#eab308",
                    "#f97316",
                    "#ef4444",
                    "#7c3aed",
                ],
            ),
            legend=alt.Legend(
                orient="top",
                direction="horizontal",
                gradientLength=150,
                labelColor="#475569",
                titleColor="#475569",
                padding=8,
            ),
        ),
        tooltip=[
            alt.Tooltip("device_id:N", title="Device"),
            alt.Tooltip("total_events:Q", title="CDC events", format=".0f"),
            alt.Tooltip("create_events:Q", title="Creates", format=".0f"),
            alt.Tooltip("update_events:Q", title="Updates", format=".0f"),
            alt.Tooltip("delete_events:Q", title="Deletes", format=".0f"),
            alt.Tooltip("average_aqi:Q", title="Average AQI", format=".1f"),
            alt.Tooltip("maximum_aqi:Q", title="Maximum AQI", format=".1f"),
        ],
    )

    labels = alt.Chart(chart_data).mark_text(
        align="left",
        baseline="middle",
        dx=7,
        fontWeight=700,
        color="#334155",
    ).encode(
        x="total_events:Q",
        y=alt.Y("device_id:N", sort=None),
        text=alt.Text("total_events:Q", format=".0f"),
    )

    chart = (
        (bars + labels)
        .properties(height=365)
        .configure_view(
            stroke="#dbe5f3",
            strokeWidth=1,
            cornerRadius=14,
            fill="#fbfdff",
        )
        .configure_axis(
            labelFont="Segoe UI",
            titleFont="Segoe UI",
            labelFontSize=12,
            titleFontSize=12,
        )
        .configure_legend(
            labelFont="Segoe UI",
            titleFont="Segoe UI",
            labelFontSize=11,
            titleFontSize=11,
        )
    )

    st.altair_chart(chart, width="stretch")


def apply_filters(daily: pd.DataFrame, activity: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    st.sidebar.markdown("## 📡 Command Center")
    st.sidebar.caption("Snowflake Gold analytics")
    device_options = sorted(daily["device_id"].dropna().astype(str).unique().tolist())
    selected_devices = st.sidebar.multiselect("Devices", options=device_options, default=device_options, help="Filter all dashboard views by device.")
    valid_dates = daily["event_date"].dropna()
    if valid_dates.empty:
        selected_range = None
    else:
        min_date = valid_dates.min().date(); max_date = valid_dates.max().date()
        selected_range = st.sidebar.date_input("Event date range", value=(min_date,max_date), min_value=min_date, max_value=max_date, format="YYYY/MM/DD")
    max_top_n = max(1, min(10, len(device_options)))
    top_n = st.sidebar.slider("Top-N devices", min_value=1, max_value=max_top_n, value=max_top_n)
    st.sidebar.divider(); st.sidebar.markdown("**Data source**"); st.sidebar.code(f"{DATABASE}.{SCHEMA}.{TABLE}", language=None)
    st.sidebar.caption(f"Dashboard re-query: every {REFRESH_SECONDS} seconds"); st.sidebar.caption("New Gold data appears only after ingestion and dbt refresh."); st.sidebar.caption("Display timezone: Islamabad (Asia/Karachi)")
    filtered_daily = daily.copy()
    if selected_devices:
        filtered_daily = filtered_daily[filtered_daily["device_id"].astype(str).isin(selected_devices)]
    else:
        filtered_daily = filtered_daily.iloc[0:0]
    if selected_range and len(selected_range) == 2:
        start_date, end_date = selected_range
        filtered_daily = filtered_daily[filtered_daily["event_date"].dt.date.between(start_date,end_date)]
    device_ids = filtered_daily["device_id"].dropna().astype(str).unique()
    filtered_activity = activity[activity["device_id"].astype(str).isin(device_ids)].copy()
    return filtered_daily, filtered_activity, top_n


st.markdown("""
<div class="hero">
  <div class="hero-kicker">Real-time cloud analytics</div>
  <h1 class="hero-title">IoT Migration Command Center</h1>
  <p class="hero-subtitle">PostgreSQL CDC telemetry transformed through Snowflake and dbt, presented from the Gold analytics layer.</p>
  <div class="status-row">
    <span class="status-pill"><span class="status-dot"></span>Live connection</span>
    <span class="status-pill">Snowflake Gold</span>
    <span class="status-pill">30-second refresh</span>
    <span class="status-pill">Debezium CDC</span>
  </div>
</div>
""", unsafe_allow_html=True)


@st.fragment(run_every=REFRESH_SECONDS)
def live_dashboard() -> None:
    try:
        daily, activity = load_dashboard_data(); daily, activity = normalize_data(daily, activity)
    except Exception as exc:
        st.error("Snowflake dashboard query failed. " f"Error type: {type(exc).__name__}"); return
    filtered_daily, filtered_activity, top_n = apply_filters(daily, activity)
    total_devices = int(filtered_daily["device_id"].nunique())
    total_events = int(filtered_daily["total_events"].fillna(0).sum())
    total_event_weight = filtered_daily["total_events"].fillna(0).sum()
    average_aqi = float(((filtered_daily["average_aqi"].fillna(0) * filtered_daily["total_events"].fillna(0)).sum() / total_event_weight)) if total_event_weight > 0 else 0.0
    maximum_aqi = float(filtered_daily["maximum_aqi"].dropna().max()) if not filtered_daily["maximum_aqi"].dropna().empty else 0.0
    latest_event = filtered_daily["last_event_timestamp"].dropna().max()
    metric1, metric2, metric3, metric4 = st.columns(4)
    metric1.metric("Devices in view", f"{total_devices:,}")
    metric2.metric("Total CDC events", f"{total_events:,}")
    metric3.metric("Weighted average AQI", f"{average_aqi:.1f}")
    metric4.metric("Peak AQI", f"{maximum_aqi:.1f}")
    current_time = datetime.now(DISPLAY_TIMEZONE)

    st.markdown(
        f'<div class="current-time-card">'
        f'<div class="current-time-label">Dashboard clock · Islamabad</div>'
        f'<div class="current-time-value">{current_time.strftime("%A, %d %B %Y · %I:%M:%S %p PKT")}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    if pd.notna(latest_event):
        latest_value = latest_event.strftime("%A, %d %B %Y · %I:%M:%S %p PKT")
        age = current_time - latest_event.to_pydatetime()
        total_seconds = max(0, int(age.total_seconds()))
        days, remainder = divmod(total_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)

        if total_seconds < 120:
            freshness = "Live data"
        elif days > 0:
            freshness = f"Data age: {days}d {hours}h {minutes}m"
        elif hours > 0:
            freshness = f"Data age: {hours}h {minutes}m"
        else:
            freshness = f"Data age: {minutes}m {seconds}s"
    else:
        latest_value = "No event timestamp available"
        freshness = "Data age unavailable"

    st.markdown(
        f'<div class="latest-event-card">'
        f'<div class="latest-event-icon">🕒</div>'
        f'<div><div class="latest-event-label">Newest Gold-layer event · Islamabad time</div>'
        f'<div class="latest-event-value">{latest_value} · {freshness}</div></div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    if filtered_daily.empty:
        st.warning("No Gold-layer records match the current filters."); return
    render_device_map(filtered_activity)
    left, right = st.columns(2, gap="large")
    with left: render_aqi_trend(filtered_daily)
    with right: render_top_devices(filtered_activity, top_n)
    render_section_header("Gold-Layer Detail", "Filtered daily aggregates used by the dashboard.")
    detail_columns = ["device_id","event_date","total_events","create_events","update_events","delete_events","average_aqi","maximum_aqi","last_event_timestamp"]
    available_columns = [c for c in detail_columns if c in filtered_daily.columns]
    with st.expander("View aggregate records"):
        st.dataframe(filtered_daily[available_columns].sort_values(["event_date","device_id"],ascending=[False,True]), width="stretch", hide_index=True, column_config={
            "device_id":"Device",
            "event_date":st.column_config.DateColumn("Event date",format="YYYY-MM-DD"),
            "total_events":st.column_config.NumberColumn("CDC events",format="%d"),
            "average_aqi":st.column_config.NumberColumn("Average AQI",format="%.1f"),
            "maximum_aqi":st.column_config.NumberColumn("Maximum AQI",format="%.1f"),
            "last_event_timestamp":st.column_config.DatetimeColumn("Latest event (PKT)",format="YYYY-MM-DD HH:mm:ss"),
        })
    refreshed_at = datetime.now(DISPLAY_TIMEZONE); next_refresh = refreshed_at + timedelta(seconds=REFRESH_SECONDS)
    st.markdown(f'<div class="refresh-line"><span>Last Snowflake query: {refreshed_at.strftime("%Y-%m-%d %H:%M:%S PKT")}</span><span>Next Snowflake query: {next_refresh.strftime("%H:%M:%S PKT")}</span></div>', unsafe_allow_html=True)


live_dashboard()
