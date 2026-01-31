import pandas as pd
import streamlit as st
from datetime import datetime, timezone
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go

from src.db import read_df
from src import queries as Q


# -----------------------------
# Helpers
# -----------------------------
def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    if df is None or df.empty:
        return None
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def last_and_delta(df: pd.DataFrame, col: str):
    """Returns (last_value, delta_from_prev)"""
    if df is None or df.empty or col is None or col not in df.columns:
        return 0.0, None
    s = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    last = float(s.iloc[-1]) if len(s) else 0.0
    if len(s) > 1:
        return last, float(s.iloc[-1] - s.iloc[-2])
    return last, None


def to_ms(value: float, colname: str | None) -> float:
    """If column name suggests ms -> keep; else treat as seconds -> ms."""
    if colname and "ms" in colname.lower():
        return float(value)
    return float(value) * 1000.0


def last_and_delta_ms(df: pd.DataFrame, col: str):
    v, d = last_and_delta(df, col)
    return to_ms(v, col), (None if d is None else to_ms(d, col))


def _try_read(sql: str, show_error: bool = False) -> pd.DataFrame:
    try:
        return read_df(sql, show_error=show_error)
    except TypeError:
        return read_df(sql)


def load_window(sql_builder, time_candidates: list[str], win: int, label: str):
    """
    Tries candidate time columns until we get rows.
    sql_builder: function(tcol, win)->SQL
    """
    last_err = None
    for tcol in time_candidates:
        try:
            sql = sql_builder(tcol, win)
            df = _try_read(sql, show_error=False)
            if df is not None and not df.empty:
                used_tcol = pick_col(df, time_candidates)
                return df, used_tcol
        except Exception as e:
            last_err = e

    # final attempt with visible error
    try:
        sql = sql_builder(time_candidates[0], win)
        df = _try_read(sql, show_error=True)
        used_tcol = pick_col(df, time_candidates) if df is not None and not df.empty else None
        return df, used_tcol
    except Exception as e:
        st.error(f"[{label}] Query failed: {e}")
        if last_err:
            st.caption(f"Previous attempt error: {last_err}")
        return pd.DataFrame(), None


def normalize_time(df: pd.DataFrame, tcol: str | None):
    if df is None or df.empty or not tcol or tcol not in df.columns:
        return df
    out = df.copy()
    out[tcol] = pd.to_datetime(out[tcol], errors="coerce", utc=True)
    out = out.dropna(subset=[tcol]).sort_values(tcol)
    return out


def last_window_range(df: pd.DataFrame, tcol: str, win_min: int):
    if df is None or df.empty or tcol not in df.columns:
        return None, None
    x_max = pd.to_datetime(df[tcol], utc=True, errors="coerce").max()
    if pd.isna(x_max):
        return None, None
    x_max = x_max.tz_convert("UTC").tz_localize(None)
    x_min = x_max - pd.Timedelta(minutes=win_min)
    return x_min, x_max
  
def prep_ts(df: pd.DataFrame, tcol: str, cols: list[str]) -> pd.DataFrame:
    """
    Make plotting-safe time series:
    - coerce datetime
    - keep only needed cols
    - drop NaT
    - aggregate to 1 row per minute (prevents vertical spikes)
    """
    if df is None or df.empty or tcol not in df.columns:
        return pd.DataFrame()

    out = df.copy()
    out[tcol] = pd.to_datetime(out[tcol], utc=True, errors="coerce")
    out = out.dropna(subset=[tcol])

    # floor to minute to ensure unique x
    out["__minute__"] = out[tcol].dt.floor("min")

    keep = ["__minute__"] + [c for c in cols if c and c in out.columns]
    out = out[keep]

    # numeric coercion
    for c in cols:
        if c and c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    # aggregate duplicates safely
    out = out.groupby("__minute__", as_index=False).mean(numeric_only=True)
    out = out.sort_values("__minute__")
    return out



# -----------------------------
# Page config
# -----------------------------
st.set_page_config(page_title="Query Efficiency", page_icon="⚙️", layout="wide")


# -----------------------------
# CSS (include SIDEBAR styling so it doesn't turn grey)
# -----------------------------
st.markdown(
    """
<style>
/* ---- Sidebar layout ordering like other pages ---- */
section[data-testid="stSidebar"] > div { display:flex; flex-direction:column; }
div[data-testid="stSidebarNav"] { order: 2; margin-top: 10px; }
.rw-sidebar-top { order: 1; margin-top: 0.2rem; margin-bottom: 0.8rem; }

/* ---- Sidebar theme ---- */
section[data-testid="stSidebar"] {
  background:
    radial-gradient(600px 600px at 15% 10%, rgba(255,0,120,0.18), transparent 55%),
    linear-gradient(180deg, #0a0b10 0%, #07080c 100%) !important;
  border-right: 1px solid rgba(255,255,255,0.08);
}
section[data-testid="stSidebar"] * { color: #e9ecf1 !important; }

/* Sidebar branding */
.rw-sidebar-header { display:flex; align-items:center; gap:10px; margin-top:-0.2rem; margin-bottom:4px; }
.rw-logo {
  font-size: 26px;
  background: linear-gradient(135deg, #ff4d9d, #7c5cff);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}
.rw-brand-title { font-size: 22px; font-weight: 900; letter-spacing: 0.4px; }
.rw-sidebar-sub { font-size: 11.5px; opacity: 0.7; margin-bottom: 10px; margin-left: 2px; }
.rw-sidebar-divider { border:none; height:1px; background: rgba(255,255,255,0.12); margin-bottom:12px; }

/* ---- App background + padding ---- */
.main .block-container{ padding-top: 2.2rem !important; padding-bottom: 3rem !important; padding-left: 2.8rem !important; padding-right: 2.8rem !important; }
.stApp {
  background: radial-gradient(1200px 800px at 10% 15%, rgba(255,0,120,0.12), transparent 55%),
              radial-gradient(1200px 800px at 85% 30%, rgba(120,80,255,0.14), transparent 60%),
              linear-gradient(180deg, #0a0b10 0%, #07080c 100%);
  color: #e9ecf1;
}

/* ---- Topbar ---- */
.rw-topbar{ display:flex; align-items:flex-start; justify-content:space-between; gap:18px; margin: 6px 0 18px 0; }
.rw-title{ font-size: 34px; font-weight: 900; letter-spacing: 0.2px; line-height: 1.1; }
.rw-sub{ font-size: 12px; opacity: .75; margin-top: 4px; }
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
.rw-divider{ border:none; height:1px; background: rgba(255,255,255,0.10); margin: 14px 0 18px 0; }

/* ---- Cards ---- */
div[data-testid="stVerticalBlockBorderWrapper"]{
  background: linear-gradient(180deg, rgba(255,255,255,0.07), rgba(255,255,255,0.035)) !important;
  border: 1px solid rgba(255,255,255,0.12) !important;
  border-radius: 20px !important;
  box-shadow: 0 14px 40px rgba(0,0,0,0.55), inset 0 1px 0 rgba(255,255,255,0.06) !important;
  padding: 16px 16px 12px 16px !important;
}

/* ---- “Colored” KPI pills for cached/aborted ---- */
.rw-pill {
  background: rgba(0,0,0,0.20);
  border: 1px solid rgba(255,255,255,0.10);
  border-radius: 16px;
  padding: 10px 12px;
}
.rw-pill .t { font-size: 12px; opacity: .72; }
.rw-pill .v { font-size: 22px; font-weight: 900; margin-top: 4px; }
.rw-pill.cached { border-color: rgba(255, 215, 0, 0.45); box-shadow: inset 0 0 0 1px rgba(255,215,0,0.15); }
.rw-pill.aborted { border-color: rgba(255, 60, 60, 0.45); box-shadow: inset 0 0 0 1px rgba(255,60,60,0.15); }
</style>
""",
    unsafe_allow_html=True,
)


# -----------------------------
# Auto-refresh + static window
# -----------------------------
REFRESH_SECONDS = 15
WIN_MIN = 5
refresh_count = st_autorefresh(interval=REFRESH_SECONDS * 1000, key="qe_autorefresh")


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

debug = st.sidebar.checkbox("Debug", value=False)


# -----------------------------
# Header
# -----------------------------
st.markdown(
    f"""
    <div class="rw-topbar">
      <div>
        <div class="rw-title">⚙️ Query Efficiency</div>
        <div class="rw-sub">Execution, queueing, scan/spill, cached/aborted — last {WIN_MIN} minutes</div>
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
# Load KPI minute table (existing)
# -----------------------------
time_candidates = ["minute_ts", "window_end", "ts", "timestamp", "arrival_minute", "minute"]
eff_df, eff_t = load_window(Q.efficiency_window, time_candidates, WIN_MIN, "efficiency_window")
eff_df = normalize_time(eff_df, eff_t)


# -----------------------------
# Load clean_table aggregation (ROBUST)
# IMPORTANT: clean_table has minute_ts; we should anchor to latest minute_ts instead of NOW().
# This avoids “empty window” KPIs while charts still show something elsewhere.
# -----------------------------
clean_df = _try_read(Q.clean_efficiency_window("minute_ts", WIN_MIN), show_error=False)
if clean_df is None:
    clean_df = pd.DataFrame()

c_min_col = pick_col(clean_df, ["minute_ts"])
clean_df = normalize_time(clean_df, c_min_col)


# -----------------------------
# Pick columns
# -----------------------------
exec_col = pick_col(eff_df, ["avg_exec_ms", "avg_execution_ms", "avg_exec_s", "avg_exec_sec", "exec_ms_avg"])
queue_col = pick_col(eff_df, ["avg_queue_ms", "queue_wait_ms", "avg_queue_s", "avg_queue_sec", "queue_ms_avg"])
spill_ratio_eff_col = pick_col(eff_df, ["spill_to_scan_ratio", "waste_pct", "waste_percent", "waste_percentage"])
scan_eff_col = pick_col(eff_df, ["scanned_mb_sum", "scan_mb", "scanned_mb", "mbytes_scanned", "scanned_gb", "scan_gb"])

# clean_efficiency_window output columns
c_spilled_col = pick_col(clean_df, ["spilled_mb_sum"])
c_scanned_col = pick_col(clean_df, ["scanned_mb_sum"])
c_cached_col = pick_col(clean_df, ["cached_count"])
c_aborted_col = pick_col(clean_df, ["aborted_count"])
c_ratio_col = pick_col(clean_df, ["spill_to_scan_ratio"])


if debug:
    st.sidebar.write(
        {
            "eff_rows": int(len(eff_df)) if eff_df is not None else 0,
            "eff_time_col": eff_t,
            "eff_cols": list(eff_df.columns) if eff_df is not None else [],
            "clean_rows": int(len(clean_df)) if clean_df is not None else 0,
            "clean_time_col": c_min_col,
            "clean_cols": list(clean_df.columns) if clean_df is not None else [],
        }
    )


# -----------------------------
# Compute KPIs
# -----------------------------
avg_exec_ms, exec_delta_ms = (0.0, None)
queue_ms, queue_delta_ms = (0.0, None)
if exec_col:
    avg_exec_ms, exec_delta_ms = last_and_delta_ms(eff_df, exec_col)
if queue_col:
    queue_ms, queue_delta_ms = last_and_delta_ms(eff_df, queue_col)

# Prefer clean_table scanned/spilled (same source as cached/aborted)
scanned_mb, scanned_delta = (0.0, None)
if c_scanned_col:
    scanned_mb, scanned_delta = last_and_delta(clean_df, c_scanned_col)
elif scan_eff_col:
    scanned_mb, scanned_delta = last_and_delta(eff_df, scan_eff_col)

spilled_mb, spilled_delta = (0.0, None)
cached_cnt, cached_delta = (0.0, None)
aborted_cnt, aborted_delta = (0.0, None)

if c_spilled_col:
    spilled_mb, spilled_delta = last_and_delta(clean_df, c_spilled_col)
if c_cached_col:
    cached_cnt, cached_delta = last_and_delta(clean_df, c_cached_col)
if c_aborted_col:
    aborted_cnt, aborted_delta = last_and_delta(clean_df, c_aborted_col)

spill_ratio, spill_ratio_delta = (0.0, None)
if c_ratio_col:
    spill_ratio, spill_ratio_delta = last_and_delta(clean_df, c_ratio_col)
elif spill_ratio_eff_col:
    spill_ratio, spill_ratio_delta = last_and_delta(eff_df, spill_ratio_eff_col)


# -----------------------------
# KPIs (border=True like your leaderboard page)
# Cached yellow when >0, Aborted red when >0
# -----------------------------
with st.container(border=True):
    st.markdown("### KPIs (latest point)")
    st.caption(f"Window is fixed: {WIN_MIN} minutes • Auto-refresh: {REFRESH_SECONDS}s")

    r1c1, r1c2, r1c3, r1c4 = st.columns(4, gap="large")
    r1c1.metric("Exec avg (ms)", f"{avg_exec_ms:,.0f}", None if exec_delta_ms is None else f"{exec_delta_ms:+,.0f}")
    r1c2.metric("Queue avg (ms)", f"{queue_ms:,.0f}", None if queue_delta_ms is None else f"{queue_delta_ms:+,.0f}")
    r1c3.metric("Scanned (MB)", f"{scanned_mb:,.2f}", None if scanned_delta is None else f"{scanned_delta:+,.2f}")
    r1c4.metric("Spilled (MB)", f"{spilled_mb:,.2f}", None if spilled_delta is None else f"{spilled_delta:+,.2f}")

    r2c1, r2c2, r2c3, r2c4 = st.columns(4, gap="large")

    cached_cls = "rw-pill cached" if cached_cnt > 0 else "rw-pill"
    aborted_cls = "rw-pill aborted" if aborted_cnt > 0 else "rw-pill"

    with r2c1:
        st.markdown(
            f"""<div class="{cached_cls}"><div class="t">Cached (count)</div><div class="v">{int(cached_cnt):,d}</div></div>""",
            unsafe_allow_html=True,
        )
    with r2c2:
        st.markdown(
            f"""<div class="{aborted_cls}"><div class="t">Aborted (count)</div><div class="v">{int(aborted_cnt):,d}</div></div>""",
            unsafe_allow_html=True,
        )
    r2c3.metric("Spill/Scan ratio", f"{spill_ratio:.3f}", None if spill_ratio_delta is None else f"{spill_ratio_delta:+.3f}")
    r2c4.metric("Window", f"{WIN_MIN} min", f"+ refresh {REFRESH_SECONDS}s")


st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

left, right = st.columns([0.58, 0.42], gap="large")


# -----------------------------
# Chart 1: Latency trend (ms) + ratio
# -----------------------------
with left:
    with st.container(border=True):
        st.markdown("### Latency + efficiency trend")
        st.caption("Exec + Queue on the left axis (ms), Spill/Scan ratio on the right axis.")

        fig = go.Figure()

        # ---- Build a clean per-minute series from efficiency table
        eff_plot = pd.DataFrame()
        if eff_df is not None and not eff_df.empty and eff_t and eff_t in eff_df.columns:
            need_cols = [exec_col, queue_col]
            eff_plot = prep_ts(eff_df, eff_t, need_cols)

            if not eff_plot.empty:
                x = eff_plot["__minute__"].dt.tz_convert("UTC").dt.tz_localize(None)

                if exec_col and exec_col in eff_plot.columns:
                    y = eff_plot[exec_col].fillna(0.0)
                    # convert to ms if needed
                    if "ms" not in (exec_col or "").lower():
                        y = y * 1000.0
                    fig.add_trace(
                        go.Scatter(
                            x=x, y=y,
                            mode="lines",
                            name="Exec avg (ms)",
                            line=dict(width=3),
                            connectgaps=True,
                            line_shape="spline",
                        )
                    )

                if queue_col and queue_col in eff_plot.columns:
                    y = eff_plot[queue_col].fillna(0.0)
                    if "ms" not in (queue_col or "").lower():
                        y = y * 1000.0
                    fig.add_trace(
                        go.Scatter(
                            x=x, y=y,
                            mode="lines",
                            name="Queue avg (ms)",
                            line=dict(width=3, dash="dot"),
                            connectgaps=True,
                            line_shape="spline",
                        )
                    )

        # ---- Ratio: prefer clean_df ratio (same source as scanned/spilled)
        ratio_plot = pd.DataFrame()
        if clean_df is not None and not clean_df.empty and c_min_col and c_ratio_col:
            ratio_plot = prep_ts(clean_df, c_min_col, [c_ratio_col])

            if not ratio_plot.empty:
                x2 = ratio_plot["__minute__"].dt.tz_convert("UTC").dt.tz_localize(None)
                r = ratio_plot[c_ratio_col].fillna(0.0)
                fig.add_trace(
                    go.Scatter(
                        x=x2, y=r,
                        mode="lines",
                        name="Spill/Scan ratio",
                        yaxis="y2",
                        line=dict(width=3, dash="dash"),
                        connectgaps=True,
                        line_shape="spline",
                    )
                )

        # ---- Smart axis scaling: cap latency y-axis so spikes don’t ruin readability
        # (still shows spikes, but keeps typical range readable)
        y_cap = None
        lat_vals = []
        for tr in fig.data:
            if tr.yaxis in (None, "y"):  # left axis traces
                try:
                    lat_vals.extend([v for v in tr.y if v is not None])
                except Exception:
                    pass
        if lat_vals:
            s = pd.Series(lat_vals).dropna()
            if not s.empty:
                y_cap = float(s.quantile(0.98))  # adjust to 0.95 if you want stronger cap
                if y_cap <= 0:
                    y_cap = None

        # ---- Rolling X-range (last WIN_MIN minutes)
        x_min, x_max = (None, None)
        if not eff_plot.empty:
            x_max = eff_plot["__minute__"].max()
            x_min = x_max - pd.Timedelta(minutes=WIN_MIN)

        fig.update_layout(
            height=380,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(
                showgrid=False,
                tickformat="%H:%M",
                range=None if x_min is None else [x_min, x_max],
            ),
            yaxis=dict(
                title="Milliseconds",
                gridcolor="rgba(255,255,255,0.08)",
                zeroline=False,
                range=None if y_cap is None else [0, y_cap],
            ),
            yaxis2=dict(
                title="Ratio",
                overlaying="y",
                side="right",
                gridcolor="rgba(255,255,255,0.05)",
                zeroline=False,
            ),
        )

        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=f"qe_lat_{refresh_count}")




# -----------------------------
# Chart 2: I/O outcomes (THIS is the “I/O outcome graph”)
# Uses ONLY clean_df so it won't be empty/ mismatched sources
# Cached yellow + Aborted red
# -----------------------------
with right:
    with st.container(border=True):
        st.markdown("### I/O outcomes")

        fig2 = go.Figure()

        if clean_df is not None and not clean_df.empty and c_min_col and c_min_col in clean_df.columns:
            x2 = clean_df[c_min_col].dt.tz_convert("UTC").dt.tz_localize(None)

            # Lines: scanned + spilled
            if c_scanned_col and c_scanned_col in clean_df.columns:
                scan2 = pd.to_numeric(clean_df[c_scanned_col], errors="coerce").fillna(0.0)
                fig2.add_trace(go.Scatter(x=x2, y=scan2, mode="lines", name="Scanned (MB)", line=dict(width=2)))

            if c_spilled_col and c_spilled_col in clean_df.columns:
                spill2 = pd.to_numeric(clean_df[c_spilled_col], errors="coerce").fillna(0.0)
                fig2.add_trace(go.Scatter(x=x2, y=spill2, mode="lines", name="Spilled (MB)", line=dict(width=2, dash="dash")))

            # Bars: cached + aborted on secondary axis
            cached_max = 0.0
            aborted_max = 0.0

            if c_cached_col and c_cached_col in clean_df.columns:
                cached = pd.to_numeric(clean_df[c_cached_col], errors="coerce").fillna(0.0)
                cached_max = float(cached.max()) if len(cached) else 0.0
                fig2.add_trace(
                    go.Bar(
                        x=x2,
                        y=cached,
                        name="Cached",
                        yaxis="y2",
                        marker_color="rgba(255, 215, 0, 0.75)",  # yellow
                        opacity=0.85,
                    )
                )

            if c_aborted_col and c_aborted_col in clean_df.columns:
                aborted = pd.to_numeric(clean_df[c_aborted_col], errors="coerce").fillna(0.0)
                aborted_max = float(aborted.max()) if len(aborted) else 0.0
                fig2.add_trace(
                    go.Bar(
                        x=x2,
                        y=aborted,
                        name="Aborted",
                        yaxis="y2",
                        marker_color="rgba(255, 60, 60, 0.75)",  # red
                        opacity=0.85,
                    )
                )

            # Axis range: keep last WIN_MIN minutes and make y2 readable even if small
            x_max = x2.max()
            x_min = x_max - pd.Timedelta(minutes=WIN_MIN)
            x_range = [x_min, x_max]

            y2max = max(cached_max, aborted_max)
            y2max = 1.0 if y2max <= 0 else (y2max * 1.25)

        else:
            x_range = None
            y2max = 1.0

        fig2.update_layout(
            barmode="overlay",
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(showgrid=False, tickformat="%H:%M:%S", range=x_range),
            yaxis=dict(title="MB", autorange=True, gridcolor="rgba(255,255,255,0.08)", zeroline=False),
            yaxis2=dict(
                title="Count",
                range=[0, y2max],
                overlaying="y",
                side="right",
                gridcolor="rgba(255,255,255,0.05)",
                zeroline=False,
            ),
        )

        st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False}, key=f"qe_io_{refresh_count}")


# -----------------------------
# Note
# -----------------------------
st.caption(
    "If Cached/Aborted/Spilled are still 0 while the chart is non-zero, check whether your clean-table query is aggregating "
    "the right fields (mbytes_scanned/mbytes_spilled/was_cached/was_aborted) and that they’re populated in the last 5 minutes."
)
