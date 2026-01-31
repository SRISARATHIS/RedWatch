import os
import pandas as pd
import streamlit as st
from datetime import datetime, timezone
from streamlit_autorefresh import st_autorefresh

from src.db import read_df
from src import queries as Q


# MUST be first Streamlit call
st.set_page_config(page_title="Leaderboards", page_icon="🏆", layout="wide")

# -----------------------------
# Styles (your existing CSS kept as-is) + small spacing improvements
# -----------------------------
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

/* Slightly nicer spacing for topN buttons inside cards */
.lb-topn-row div[data-testid="column"] {
  padding-top: 2px;
}
</style>
""",
    unsafe_allow_html=True,
)

# auto-refresh
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "5"))
st_autorefresh(interval=REFRESH_SECONDS * 1000, key="lb_autorefresh")

# -----------------------------
# Sidebar header
# -----------------------------
with st.sidebar:
    st.markdown(
        """
        <div class="rw-sidebar-header rw-sidebar-top">
          <div class="rw-logo">⚡</div>
          <div class="rw-brand-title">RedWatch</div>
        </div>
        <div class="rw-sidebar-sub rw-sidebar-top">Real-time cost • workload • query intelligence</div>
        <hr class="rw-sidebar-divider rw-sidebar-top"/>
        """,
        unsafe_allow_html=True,
    )

# -----------------------------
# Page header
# -----------------------------
PAGE_TITLE = "🏆 Leaderboard"
PAGE_SUB = "Top consumers in the latest 15-minute window"

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
    unsafe_allow_html=True,
)

# -----------------------------
# Helpers
# -----------------------------
def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None

def shorten(s: str, n=28) -> str:
    s = str(s)
    return s if len(s) <= n else f"{s[:12]}…{s[-10:]}"

def fmt_heavy(x) -> str:
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return str(x)

def fmt_int(x) -> str:
    try:
        return f"{int(float(x)):,d}"
    except Exception:
        return str(x)

def topn_controls(key_prefix: str) -> int:
    """Reusable Top N buttons that persist across refresh."""
    if key_prefix not in st.session_state:
        st.session_state[key_prefix] = 10

    st.markdown('<div class="lb-topn-row">', unsafe_allow_html=True)
    b1, b2, b3 = st.columns(3, gap="small")
    with b1:
        if st.button("Top 5", key=f"{key_prefix}_5", use_container_width=True):
            st.session_state[key_prefix] = 5
    with b2:
        if st.button("Top 10", key=f"{key_prefix}_10", use_container_width=True):
            st.session_state[key_prefix] = 10
    with b3:
        if st.button("Top 20", key=f"{key_prefix}_20", use_container_width=True):
            st.session_state[key_prefix] = 20
    st.markdown("</div>", unsafe_allow_html=True)

    return st.session_state[key_prefix]


# -----------------------------
# Load table
# -----------------------------
df = read_df(Q.leaderboard_15m(), show_error=False)
if df.empty:
    st.info("No leaderboard data yet.")
    st.stop()

# dimension schema is what you're using
dim_type_col = pick_col(df, ["dimension_type"])
dim_val_col = pick_col(df, ["dimension_value"])
rank_col = pick_col(df, ["rank_position", "rank"])
heavy_col = pick_col(df, ["heavy_units_sum", "heavy_sum", "heavy_units", "heavy"])
count_col = pick_col(df, ["queries_count", "query_count", "count"])
w_end_col = pick_col(df, ["window_end", "minute_ts", "ts", "timestamp"])

missing = [c for c in [dim_type_col, dim_val_col, rank_col, w_end_col] if c is None]
if missing:
    st.warning("Leaderboard table columns are not as expected. Showing raw table for diagnosis.")
    st.dataframe(df, use_container_width=True)
    st.stop()

st.markdown(
    "<div style='display:inline-block;padding:8px 12px;border-radius:999px;border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.04);font-size:12px;opacity:.9;margin-bottom:14px;'>"
    "Metric used: <b>heavy_units_sum</b> (higher = heavier/more expensive workload)"
    "</div>",
    unsafe_allow_html=True,
)

# Latest window only
df[w_end_col] = pd.to_datetime(df[w_end_col], utc=True, errors="coerce")
latest_window_end = df[w_end_col].max()
df = df[df[w_end_col] == latest_window_end].copy()

if df.empty:
    st.warning("No leaderboard rows in the latest window.")
    st.stop()

available_dim_types = set(df[dim_type_col].dropna().astype(str).unique().tolist())

# -----------------------------
# Card renderer
# -----------------------------
def render_dim_card(label: str, dim_type_value: str, key_prefix: str):
    with st.container(border=True):
        st.markdown(f"### {label}")
        st.caption("Ranked by heavy_units_sum. Use search + Top-N to explore.")

        topn = topn_controls(f"topn_{key_prefix}")

        # quick search per card (interactive)
        q = st.text_input("Search", value="", key=f"search_{key_prefix}", placeholder=f"Filter {label}…")

        if dim_type_value not in available_dim_types:
            st.info("No data available for this category in the current window.")
            return

        sub = df[df[dim_type_col].astype(str) == str(dim_type_value)].copy()
        sub[rank_col] = pd.to_numeric(sub[rank_col], errors="coerce")
        if heavy_col and heavy_col in sub.columns:
            sub[heavy_col] = pd.to_numeric(sub[heavy_col], errors="coerce").fillna(0.0)

        # apply search
        if q.strip():
            sub = sub[sub[dim_val_col].astype(str).str.contains(q.strip(), case=False, na=False)].copy()

        sub = sub.sort_values(rank_col, ascending=True).dropna(subset=[rank_col]).head(topn)

        if sub.empty:
            st.info("No matching rows (try clearing search).")
            return

        out = pd.DataFrame()
        out["Rank"] = sub[rank_col].astype("Int64")
        out[label] = [shorten(v) for v in sub[dim_val_col].tolist()]
        if heavy_col and heavy_col in sub.columns:
            out["Heavy units"] = [fmt_heavy(v) for v in sub[heavy_col].tolist()]
        if count_col and count_col in sub.columns:
            out["Queries"] = [fmt_int(v) for v in sub[count_col].tolist()]

        st.dataframe(out, use_container_width=True, hide_index=True, height=280)

# -----------------------------
# ✅ Your 5 categories (including WORKLOAD)
# -----------------------------
CARDS = [
    ("Query", "query_type", "query"),
    ("Workload", "workload", "workload"),          # ✅ ADDED
    ("Access scope", "access_scope", "scope"),
    ("Fingerprint", "fingerprint", "fp"),
    ("User", "user", "user"),
]

# Layout: 3 cards on first row, 2 on second row (looks clean)
row1 = st.columns(3, gap="large")
row2 = st.columns(2, gap="large")

for col, (label, dim_type_value, key_prefix) in zip(row1, CARDS[:3]):
    with col:
        render_dim_card(label, dim_type_value, key_prefix)

for col, (label, dim_type_value, key_prefix) in zip(row2, CARDS[3:]):
    with col:
        render_dim_card(label, dim_type_value, key_prefix)

# ---- Bottom explain box
st.markdown(
    """
    <div style="
      background: rgba(255,255,255,0.04);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 18px;
      padding: 12px 14px;
      margin-top: 18px;
      box-shadow: 0 8px 28px rgba(0,0,0,0.35);
    ">
      <b>How to interpret this</b><br>
      This leaderboard is computed for the most recent <b>15-minute window</b>.
      Items are ranked by <b>heavy_units_sum</b>.
    </div>
    """,
    unsafe_allow_html=True,
)
