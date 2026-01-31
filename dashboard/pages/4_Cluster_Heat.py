import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from src.db import read_df
from src.queries import cluster_workload_window, cluster_concurrency_window

st.set_page_config(page_title="Cluster Heat", page_icon="🔥", layout="wide")
st.markdown(
    """
<style>
/* Make sidebar a flex column so we can reorder */
section[data-testid="stSidebar"] > div {
  display: flex;
  flex-direction: column;
}

/* Push Streamlit multipage nav BELOW our custom header */
div[data-testid="stSidebarNav"] {
  order: 2;
  margin-top: 10px;
}

/* Put anything we add in st.sidebar at the top */
.rw-sidebar-top {
  order: 1;
  margin-top: 0.2rem;
  margin-bottom: 0.8rem;
}

/* Sidebar theme */
section[data-testid="stSidebar"] {
  background:
    radial-gradient(600px 600px at 15% 10%, rgba(255,0,120,0.18), transparent 55%),
    linear-gradient(180deg, #0a0b10 0%, #07080c 100%) !important;
  border-right: 1px solid rgba(255,255,255,0.08);
}
section[data-testid="stSidebar"] * {
  color: #e9ecf1 !important;
}

/* Sidebar branding */
.rw-sidebar-header {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-top: -0.2rem;
  margin-bottom: 4px;
}
.rw-logo {
  font-size: 26px;
  background: linear-gradient(135deg, #ff4d9d, #7c5cff);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}
.rw-brand-title {
  font-size: 22px;
  font-weight: 900;
  letter-spacing: 0.4px;
}
.rw-sidebar-sub {
  font-size: 11.5px;
  opacity: 0.7;
  margin-bottom: 10px;
  margin-left: 2px;
}
.rw-sidebar-divider {
  border: none;
  height: 1px;
  background: rgba(255,255,255,0.12);
  margin-bottom: 12px;
}

/* Main background */
.stApp {
  background: radial-gradient(1200px 800px at 12% 12%, rgba(255,0,120,0.12), transparent 55%),
              radial-gradient(1200px 800px at 88% 28%, rgba(120,80,255,0.14), transparent 60%),
              linear-gradient(180deg, #0a0b10 0%, #07080c 100%);
  color: #e9ecf1;
}

/* Global padding */
.main .block-container{
  padding-top: 2.4rem;
  padding-bottom: 3rem;
  padding-left: 3rem;
  padding-right: 3rem;
}

/* Topbar */
.rw-topbar{
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:18px;
  margin: 6px 0 18px 0;
}
.rw-title{
  font-size: 34px;
  font-weight: 900;
  letter-spacing: 0.2px;
  line-height: 1.1;
}
.rw-sub{
  font-size: 12px;
  opacity: .75;
  margin-top: 4px;
}
.rw-status{
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 18px;
  padding: 12px 14px;
  text-align: right;
  min-width: 300px;
  box-shadow: 0 8px 28px rgba(0,0,0,0.35);
}
.rw-status .k{ font-size: 12px; opacity: .72; }
.rw-status .v{ font-size: 13px; font-weight: 900; margin-top: 4px; }

.rw-divider{
  border: none;
  height: 1px;
  background: rgba(255,255,255,0.10);
  margin: 14px 0 18px 0;
}

/* Make Streamlit bordered containers look like cards */
div[data-testid="stVerticalBlockBorderWrapper"]{
  background: linear-gradient(180deg, rgba(255,255,255,0.07), rgba(255,255,255,0.035)) !important;
  border: 1px solid rgba(255,255,255,0.12) !important;
  border-radius: 20px !important;
  box-shadow: 0 14px 40px rgba(0,0,0,0.55),
              inset 0 1px 0 rgba(255,255,255,0.06) !important;
  padding: 16px 16px 12px 16px !important;
}

/* Metric tiles */
div[data-testid="stMetric"]{
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 16px;
  padding: 10px 12px;
}
</style>
""",
    unsafe_allow_html=True,
)

st.markdown("""
<style>
/* ============================= */
/* GLOBAL APP BACKGROUND */
/* ============================= */
.stApp {
  background:
    radial-gradient(1200px 800px at 12% 12%, rgba(255,0,120,0.12), transparent 55%),
    radial-gradient(1200px 800px at 88% 28%, rgba(120,80,255,0.14), transparent 60%),
    linear-gradient(180deg, #0a0b10 0%, #07080c 100%);
  color: #e9ecf1;
}

.main .block-container{
  padding-top: 2.4rem;
  padding-bottom: 3rem;
  padding-left: 3rem;
  padding-right: 3rem;
}

/* ============================= */
/* SIDEBAR THEME */
/* ============================= */
section[data-testid="stSidebar"] {
  background:
    radial-gradient(600px 600px at 15% 10%, rgba(255,0,120,0.18), transparent 55%),
    linear-gradient(180deg, #0a0b10 0%, #07080c 100%) !important;
  border-right: 1px solid rgba(255,255,255,0.08);
}
section[data-testid="stSidebar"] > div { padding-top: 1.4rem; }
section[data-testid="stSidebar"] * {
  color: #e9ecf1 !important;
  font-weight: 600;
}

.sidebar-logo {
  font-size: 26px;
  font-weight: 900;
  letter-spacing: 0.6px;
  margin-bottom: 6px;
}
.sidebar-sub {
  font-size: 12px;
  opacity: 0.7;
  margin-bottom: 18px;
}

/* Sidebar inputs */
section[data-testid="stSidebar"] div[data-baseweb="select"] > div,
section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] textarea {
  background: rgba(255,255,255,0.05) !important;
  border: 1px solid rgba(255,255,255,0.12) !important;
  border-radius: 12px !important;
}
section[data-testid="stSidebar"] details {
  background: rgba(255,255,255,0.03);
  border-radius: 14px;
  border: 1px solid rgba(255,255,255,0.08);
}
section[data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.08); }

/* ============================= */
/* CARDS (st.container(border=True)) */
/* ============================= */
div[data-testid="stVerticalBlockBorderWrapper"]{
  background: linear-gradient(180deg, rgba(255,255,255,0.07), rgba(255,255,255,0.035)) !important;
  border: 1px solid rgba(255,255,255,0.12) !important;
  border-radius: 20px !important;
  box-shadow: 0 14px 40px rgba(0,0,0,0.55), inset 0 1px 0 rgba(255,255,255,0.06) !important;
  padding: 16px 16px 12px 16px !important;
}
div[data-testid="stVerticalBlockBorderWrapper"] + div[data-testid="stVerticalBlockBorderWrapper"]{
  margin-top: 16px !important;
}

/* ============================= */
/* HEAT TABLE POLISH */
/* ============================= */
div[data-testid="stDataFrame"] > div{
  border-radius: 18px !important;
  border: 1px solid rgba(255,255,255,0.10) !important;
  background: rgba(0,0,0,0.35) !important;
  overflow: hidden !important;
}

/* Make dataframe header / toolbar match dark */
div[data-testid="stDataFrame"] [role="columnheader"]{
  background: rgba(255,255,255,0.04) !important;
}

/* ============================= */
/* CHART CONTAINER */
/* ============================= */
div[data-testid="stLineChart"] > div{
  border-radius: 18px !important;
  border: 1px solid rgba(255,255,255,0.10) !important;
  background: rgba(0,0,0,0.30) !important;
  overflow: hidden !important;
}

/* ============================= */
/* TITLES / CAPTIONS */
/* ============================= */
h1, h2, h3 { font-weight: 900 !important; letter-spacing: 0.3px; }
small, .stCaption { opacity: 0.75 !important; }

/* ============================= */
/* OPTIONAL: HEATMAP-LIKE STYLES */
/* If you use pandas .style later, this makes it look nicer */
/* ============================= */
.heat-pill {
  display:inline-block;
  padding: 6px 10px;
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.04);
  font-size: 12px;
  opacity: .9;
}
</style>
""", unsafe_allow_html=True)

from datetime import datetime, timezone

PAGE_TITLE = "🔥 Cluster Heat"
PAGE_SUB   = "Live workload distribution across clusters"

st.markdown(
    f"""
    <div class="rw-topbar">
      <div>
        <div class="rw-title">{PAGE_TITLE}</div>
        <div class="rw-sub">{PAGE_SUB}</div>
      </div>
      <div class="rw-status">
        <div class="k">Last refreshed</div>
        <div class="v">{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</div>
      </div>
    </div>
    <hr class="rw-divider"/>
    """,
    unsafe_allow_html=True
)


win = int(st.session_state.get("window_minutes", 60))

def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None

# Prefer actual known columns first
time_candidates = ["minute_ts", "window_end", "ts", "timestamp"]
cluster_candidates = ["instance_id", "cluster_id", "cluster", "node", "warehouse"]

# ---- load workload
work_df = pd.DataFrame()
tcol = None
for tc in time_candidates:
    work_df = read_df(cluster_workload_window(tc, win), show_error=False)
    if not work_df.empty:
        tcol = tc
        break

if work_df.empty:
    _ = read_df(cluster_workload_window(time_candidates[0], win), show_error=True)
    st.stop()

cid = pick_col(work_df, cluster_candidates)

# Pick a metric that exists in your table
metric_options = [
    "heavy_units_sum",
    "queries_count",
    "scanned_mb_sum",
    "spilled_mb_sum",
    "exec_ms_sum",
    "queue_ms_sum",
]
metric_options = [m for m in metric_options if m in work_df.columns]

if not cid or not tcol or not metric_options:
    st.warning("Missing cluster id, time column, or metric columns. Showing raw rows.")
    st.write("Columns:", list(work_df.columns))
    st.dataframe(work_df.tail(200), use_container_width=True)
    st.stop()

# ---- sidebar controls
with st.sidebar:
    st.header("Heatmap Controls")
    metric = st.selectbox("Metric (color)", metric_options, index=0)
    top_n = st.slider("Top N clusters", 5, 50, 15)
    bucket = st.selectbox("Time bucket", ["1min", "5min", "15min"], index=1)
    normalize = st.checkbox("Normalize per cluster (0–1)", value=False)
    show_values = st.checkbox("Show cell values (slow)", value=False)

# ---- bucket timestamps
work_df[tcol] = pd.to_datetime(work_df[tcol], utc=True, errors="coerce")

freq = {"1min": "1min", "5min": "5min", "15min": "15min"}[bucket]
work_df["bucket_ts"] = work_df[tcol].dt.floor(freq)

# ---- aggregate
agg = (
    work_df.groupby([cid, "bucket_ts"], as_index=False)[metric]
    .sum()
)

# pick top N clusters by total metric
top_clusters = (
    agg.groupby(cid, as_index=False)[metric]
    .sum()
    .sort_values(metric, ascending=False)
    .head(top_n)[cid]
    .astype(str)
    .tolist()
)

agg = agg[agg[cid].astype(str).isin(top_clusters)].copy()
agg[cid] = agg[cid].astype(str)

# ---- pivot to heatmap matrix
heat = agg.pivot_table(
    index=cid,
    columns="bucket_ts",
    values=metric,
    aggfunc="sum",
    fill_value=0,
)

# sort clusters by most recent column value (nice UX)
if heat.shape[1] > 0:
    heat = heat.sort_values(by=heat.columns.max(), ascending=False)

# normalize row-wise if enabled
if normalize:
    row_min = heat.min(axis=1)
    row_max = heat.max(axis=1)
    denom = (row_max - row_min).replace(0, 1)
    heat = (heat.sub(row_min, axis=0)).div(denom, axis=0)


# -----------------------------
# Heatmap (interactive + better scaling)
# -----------------------------

# Make sure cluster id is categorical (prevents numeric/negative axis behavior)
heat.index = heat.index.astype(str)

# X axis timestamps (ensure sorted)
x_vals = list(heat.columns)
y_vals = list(heat.index)

# Raw matrix: clusters x time (same orientation as heat)
z_raw = heat.values

# Robust color scaling: clip outliers so the palette stays readable
flat = pd.Series(z_raw.reshape(-1)).dropna()
if len(flat) == 0:
    clip_lo, clip_hi = 0.0, 1.0
else:
    clip_lo = float(flat.quantile(0.05))
    clip_hi = float(flat.quantile(0.95))
    if clip_hi <= clip_lo:
        clip_lo = float(flat.min())
        clip_hi = float(flat.max())
        if clip_hi <= clip_lo:
            clip_hi = clip_lo + 1.0

# Visual compression (optional): keeps spikes visible but avoids “all maxed out”
# IMPORTANT: hover will still show raw values.
z_vis = pd.DataFrame(z_raw, index=y_vals, columns=x_vals).clip(clip_lo, clip_hi).applymap(lambda v: v ** 0.5).values
customdata = 0
fig = go.Figure(
    data=go.Heatmap(
        x=x_vals,                 # time on X
        y=y_vals,                 # cluster/instance_id on Y
        z=z_vis,                  # compressed for visuals
        customdata=z_raw,         # raw metric for hover + text
        colorscale="Turbo",       # nicer contrast; you can try "Viridis" or "Cividis"
        zmin=float(pd.Series(z_vis.reshape(-1)).min()),
        zmax=float(pd.Series(z_vis.reshape(-1)).max()),
        hovertemplate=(
            "Cluster (Instance ID): %{y}<br>"
            "Time: %{x|%H:%M:%S}<br>"
            f"{metric}: %{customdata:.3f}"
            "<extra></extra>"
        ),
        colorbar=dict(title=metric, len=0.75),
    )
)

# Optional: show cell values (RAW), but this can be heavy for large matrices
if show_values:
    text = pd.DataFrame(z_raw, index=y_vals, columns=x_vals).round(2).astype(str).values
    fig.update_traces(text=text, texttemplate="%{text}", textfont=dict(size=10))

fig.update_layout(
    height=420,
    margin=dict(l=0, r=0, t=10, b=0),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
)

# Make axes clean + categorical Y axis (prevents numeric scaling / negative ticks)
fig.update_xaxes(showgrid=False, tickfont=dict(size=10), tickformat="%H:%M:%S")
fig.update_yaxes(showgrid=False, tickfont=dict(size=10), type="category", title="Cluster / Instance ID")

st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


# ---- optional: details panel
st.divider()
col1, col2 = st.columns([0.55, 0.45])

with col1:
    st.subheader("Top clusters summary")
    summary = (
        agg.groupby(cid, as_index=False)[metric]
        .sum()
        .sort_values(metric, ascending=False)
        .head(top_n)
    )
    st.dataframe(summary, use_container_width=True, hide_index=True)

with col2:
    st.subheader("Inspect a cluster")
    choice = st.selectbox("Cluster", top_clusters, index=0)
    sub = agg[agg[cid] == str(choice)].sort_values("bucket_ts")
    st.line_chart(sub.set_index("bucket_ts")[[metric]])

with st.expander("Raw KPI tail"):
    st.dataframe(work_df.tail(200), use_container_width=True)
