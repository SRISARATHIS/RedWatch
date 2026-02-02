import pandas as pd
import streamlit as st
from datetime import datetime, timezone
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go

from src.db import read_df
from src import queries as Q

def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    Pick the first matching column name from a list (case-insensitive).
    Returns: The actual column name from the DataFrame if found, else None.
    """
    if df is None or df.empty:
        return None
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def _try_read(sql: str, show_error: bool = False) -> pd.DataFrame:
     """
    Execute a SQL query via `read_df`, supporting older signatures.
    Returns: DataFrame result.
    """
    try:
        return read_df(sql, show_error=show_error)
    except TypeError:
        return read_df(sql)


def load_window(sql_builder, time_candidates: list[str], win: int, label: str):
    """
    Load windowed data by trying multiple time column candidates. The query builder is expected to accept.
    Returns a tuple:
          - df is the returned DataFrame (possibly empty)
          - used_tcol is the timestamp column that worked (or None)
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
    """
    Ensure the timestamp column is parsed to UTC and sorted.
    Returns: DataFrame with UTC timestamps (invalid timestamps removed) sorted by time.
    """
    if df is None or df.empty or not tcol or tcol not in df.columns:
        return df
    out = df.copy()
    out[tcol] = pd.to_datetime(out[tcol], errors="coerce", utc=True)
    out = out.dropna(subset=[tcol]).sort_values(tcol)
    return out


def latest_nonnull_and_delta(series: pd.Series):
    """
    Get the most recent numeric value and change vs the previous point.
    Returns a tuple:
          - latest_value is float or None
          - delta is float difference vs previous value (or None if unavailable)
    """
    if series is None or len(series) == 0:
        return None, None
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return None, None
    last = float(s.iloc[-1])
    if len(s) > 1:
        prev = float(s.iloc[-2])
        return last, (last - prev)
    return last, None


def kpi_latest(df: pd.DataFrame, col: str | None):
    """
    Convenience wrapper to read a KPI column from a DataFrame.
    Returns: `latest_nonnull_and_delta`, or (None).
    """
    if df is None or df.empty or not col or col not in df.columns:
        return None, None
    v, d = latest_nonnull_and_delta(df[col])
    return v, d


def prep_ts(df: pd.DataFrame, tcol: str, cols: list[str]) -> pd.DataFrame:
    """
    Build a clean per-minute time series for plotting.
    Returns:
        DataFrame with:
          - __minute__ column
          - numeric metric columns averaged per minute
    """
    if df is None or df.empty or tcol not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out[tcol] = pd.to_datetime(out[tcol], utc=True, errors="coerce")
    out = out.dropna(subset=[tcol])
    out["__minute__"] = out[tcol].dt.floor("min")  # tz-aware UTC

    keep = ["__minute__"] + [c for c in cols if c and c in out.columns]
    out = out[keep].copy()

    for c in cols:
        if c and c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.groupby("__minute__", as_index=False).mean(numeric_only=True).sort_values("__minute__")
    return out

st.set_page_config(page_title="Query Efficiency", page_icon="⚙️", layout="wide")

st.markdown(
    """
<style>
section[data-testid="stSidebar"] > div { display:flex; flex-direction:column; }
div[data-testid="stSidebarNav"] { order: 2; margin-top: 10px; }
.rw-sidebar-top { order: 1; margin-top: 0.2rem; margin-bottom: 0.8rem; }

section[data-testid="stSidebar"] {
  background:
    radial-gradient(600px 600px at 15% 10%, rgba(255,0,120,0.18), transparent 55%),
    linear-gradient(180deg, #0a0b10 0%, #07080c 100%) !important;
  border-right: 1px solid rgba(255,255,255,0.08);
}
section[data-testid="stSidebar"] * { color: #e9ecf1 !important; }

.rw-sidebar-header { display:flex; align-items:center; gap:10px; margin-top:-0.2rem; margin-bottom:4px; }
.rw-logo {
  font-size: 26px;
  background: linear-gradient(135deg, #ff2d55, #7c3aed);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}
.rw-brand-title {
  font-size: 22px;
  font-weight: 900;
  letter-spacing: 0.4px;
  background: linear-gradient(135deg, #ff2d55, #7c3aed);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}
.rw-sidebar-sub { font-size: 11.5px; opacity: 0.7; margin-bottom: 10px; margin-left: 2px; }
.rw-sidebar-divider { border:none; height:1px; background: rgba(255,255,255,0.12); margin-bottom:12px; }

.main .block-container{
  padding-top: 2.2rem !important;
  padding-bottom: 3rem !important;
  padding-left: 2.8rem !important;
  padding-right: 2.8rem !important;
}
.stApp {
  background: radial-gradient(1200px 800px at 10% 15%, rgba(255,0,120,0.12), transparent 55%),
              radial-gradient(1200px 800px at 85% 30%, rgba(124,58,237,0.14), transparent 60%),
              linear-gradient(180deg, #0a0b10 0%, #07080c 100%);
  color: #e9ecf1;
}

.rw-topbar{ display:flex; align-items:flex-start; justify-content:space-between; gap:18px; margin: 6px 0 18px 0; }
.rw-title{ font-size: 40px; font-weight: 900; letter-spacing: 0.2px; line-height: 1.1; background: linear-gradient(135deg, #f43f5e 0%, #a78bfa 100%);
  -webkit-background-clip: text;
  background-clip: text;
  -webkit-text-fill-color: transparent;}
.rw-sub{ font-size: 12px; opacity: .75; margin-top: 4px; }

.rw-status{
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 18px;
  padding: 12px 14px;
  text-align: right;
  min-width: 360px;
  box-shadow: 0 8px 28px rgba(0,0,0,0.35);
}
.rw-status .k{ font-size: 12px; opacity: .72; }
.rw-status .v{ font-size: 13px; font-weight: 900; margin-top: 4px; }
.rw-status .v2{ font-size: 12px; opacity: .78; margin-top: 2px; }

.rw-divider{ border:none; height:1px; background: rgba(255,255,255,0.10); margin: 14px 0 18px 0; }

div[data-testid="stVerticalBlockBorderWrapper"]{
  background: linear-gradient(180deg, rgba(255,255,255,0.07), rgba(255,255,255,0.035)) !important;
  border: 1px solid rgba(255,255,255,0.12) !important;
  border-radius: 20px !important;
  box-shadow: 0 14px 40px rgba(0,0,0,0.55), inset 0 1px 0 rgba(255,255,255,0.06) !important;
  padding: 16px 16px 12px 16px !important;
}

.rw-pill {
  background: rgba(0,0,0,0.20);
  border: 1px solid rgba(255,255,255,0.10);
  border-radius: 16px;
  padding: 10px 12px;
  height: 88px;
  display:flex;
  flex-direction:column;
  justify-content:center;
}
.rw-pill .t { font-size: 12px; opacity: .72; }
.rw-pill .v { font-size: 24px; font-weight: 900; margin-top: 4px; }
.rw-pill.cached { border-color: rgba(255, 215, 0, 0.55); box-shadow: 0 0 0 1px rgba(255,215,0,0.12) inset; }
.rw-pill.aborted { border-color: rgba(255, 60, 60, 0.55); box-shadow: 0 0 0 1px rgba(255,60,60,0.12) inset; }
</style>
""",
    unsafe_allow_html=True,
)

REFRESH_SECONDS = 15
WIN_MIN = 5
refresh_count = st_autorefresh(interval=REFRESH_SECONDS * 1000, key="qe_autorefresh")

with st.sidebar:
    st.markdown(
        """
        <div class="rw-sidebar-header rw-sidebar-top">
          <div class="rw-logo">⚡</div>
          <div class="rw-brand-title">RedWatch</div>
        </div>
        <div class="rw-sidebar-sub rw-sidebar-top">Always Watching You 🔍 </div>
        <hr class="rw-sidebar-divider rw-sidebar-top"/>
        """,
        unsafe_allow_html=True,
    )


time_candidates = ["minute_ts", "timestamp", "ts", "minute"]
eff_df, eff_t = load_window(Q.efficiency_window, time_candidates, WIN_MIN, "efficiency_window")
eff_df = normalize_time(eff_df, eff_t)

tcol = pick_col(eff_df, ["minute_ts"])
queries_col = pick_col(eff_df, ["queries_count"])
exec_col = pick_col(eff_df, ["exec_ms_avg"])
queue_col = pick_col(eff_df, ["queue_ms_avg"])
compile_col = pick_col(eff_df, ["compile_ms_avg"])
scan_col = pick_col(eff_df, ["scanned_mb_sum"])
spill_col = pick_col(eff_df, ["spilled_mb_sum"])
ratio_col = pick_col(eff_df, ["spill_to_scan_ratio"])
cached_col = pick_col(eff_df, ["cached_queries"])
aborted_col = pick_col(eff_df, ["aborted_queries"])
heavy_sum_col = pick_col(eff_df, ["heavy_units_sum"])
heavy_avg_col = pick_col(eff_df, ["heavy_unit_avg"])


eff_plot = pd.DataFrame()
if eff_df is not None and not eff_df.empty and tcol:
    eff_plot = prep_ts(
        eff_df,
        tcol,
        [
            queries_col,
            exec_col, queue_col, compile_col,
            scan_col, spill_col, ratio_col,
            cached_col, aborted_col,
            heavy_sum_col, heavy_avg_col,
        ],
    )

latest_db_ts = eff_plot["__minute__"].max() if (eff_plot is not None and not eff_plot.empty) else None
x_range = None
if latest_db_ts is not None:
    x_range = [latest_db_ts - pd.Timedelta(minutes=WIN_MIN), latest_db_ts]

db_ts_str = "—"
if latest_db_ts is not None:
    db_ts_str = pd.to_datetime(latest_db_ts).strftime("%Y-%m-%d %H:%M UTC")

exec_ms, exec_d = kpi_latest(eff_df, exec_col)
queue_ms, queue_d = kpi_latest(eff_df, queue_col)
compile_ms, compile_d = kpi_latest(eff_df, compile_col)
qpm, qpm_d = kpi_latest(eff_df, queries_col)

scanned_mb, scan_d = kpi_latest(eff_df, scan_col)
spilled_mb, spill_d = kpi_latest(eff_df, spill_col)
ratio, ratio_d = kpi_latest(eff_df, ratio_col)

cached_cnt, cached_d = kpi_latest(eff_df, cached_col)
aborted_cnt, aborted_d = kpi_latest(eff_df, aborted_col)

def dv(x, default=0.0):
    return default if x is None else x

exec_ms = dv(exec_ms, 0.0)
queue_ms = dv(queue_ms, 0.0)
compile_ms = dv(compile_ms, 0.0)
qpm = dv(qpm, 0.0)
scanned_mb = dv(scanned_mb, 0.0)
spilled_mb = dv(spilled_mb, 0.0)
ratio = dv(ratio, 0.0)
cached_cnt = dv(cached_cnt, 0.0)
aborted_cnt = dv(aborted_cnt, 0.0)

st.markdown(
    f"""
    <div class="rw-topbar">
      <div>
        <div class="rw-title">Query Efficiency</div>
      </div>
      <div class="rw-status">
        <div class="k">Last refreshed</div>
        <div class="v">{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</div>
        <div class="v2">Latest data minute: <b>{db_ts_str}</b></div>
      </div>
    </div>
    <hr class="rw-divider"/>
    """,
    unsafe_allow_html=True,
)

with st.container(border=True):
    st.markdown("### Key Performance Metrics")
    r1 = st.columns(4, gap="large")
    r1[0].metric("Exec avg (ms)", f"{exec_ms:,.0f}", None if exec_d is None else f"{exec_d:+,.0f}")
    r1[1].metric("Queue avg (ms)", f"{queue_ms:,.0f}", None if queue_d is None else f"{queue_d:+,.0f}")
    r1[2].metric("Compile avg (ms)", f"{compile_ms:,.0f}", None if compile_d is None else f"{compile_d:+,.0f}")
    r1[3].metric("Queries/min", f"{qpm:,.0f}", None if qpm_d is None else f"{qpm_d:+,.0f}")

    r2 = st.columns(4, gap="large")
    r2[0].metric("Scanned (MB)", f"{scanned_mb:,.2f}", None if scan_d is None else f"{scan_d:+,.2f}")
    r2[1].metric("Spilled (MB)", f"{spilled_mb:,.2f}", None if spill_d is None else f"{spill_d:+,.2f}")
    r2[2].metric("Spill/Scan ratio", f"{ratio:.3f}", None if ratio_d is None else f"{ratio_d:+.3f}")
    r2[3].metric("Cached / Aborted", f"{int(cached_cnt):,d} / {int(aborted_cnt):,d}", None)

st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

row1 = st.columns(2, gap="large")
row2 = st.columns(2, gap="large")

with row1[0]:
    with st.container(border=True):
        st.markdown("### Latency breakdown")
        st.caption("Stacked per-minute latency composition (ms). Easy to see what dominates.")

        fig = go.Figure()
        if eff_plot is not None and not eff_plot.empty:
            x = eff_plot["__minute__"]

            def add_stack(col, name, color):
                if col and col in eff_plot.columns:
                    y = pd.to_numeric(eff_plot[col], errors="coerce").fillna(0.0)
                    fig.add_trace(go.Scatter(
                        x=x, y=y, mode="lines",
                        name=name, stackgroup="one",
                        line=dict(width=2, color=color)
                    ))

            add_stack(exec_col, "Exec (ms)", "#a855f7")     
            add_stack(queue_col, "Queue (ms)", "#fb7185")  
            add_stack(compile_col, "Compile (ms)", "#22d3ee") 

        fig.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            hovermode="x unified",
            legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"),
            xaxis=dict(showgrid=False, tickformat="%H:%M", range=x_range),
            yaxis=dict(title="Milliseconds", gridcolor="rgba(255,255,255,0.08)", zeroline=False),
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=f"qe_g1_{refresh_count}")


with row1[1]:
    """
    The graph plot between Volume and curresponding Outcome
    """
    with st.container(border=True):
        st.markdown("### Volume & outcomes")
        st.caption("Queries/min line + cached/aborted bars (count).")

        fig = go.Figure()
        if eff_plot is not None and not eff_plot.empty:
            x = eff_plot["__minute__"]

            if queries_col and queries_col in eff_plot.columns:
                q = pd.to_numeric(eff_plot[queries_col], errors="coerce").fillna(0.0)
                fig.add_trace(go.Scatter(
                    x=x, y=q, mode="lines",
                    name="Queries/min",
                    line=dict(width=3, color="#60a5fa")  
                ))

            if cached_col and cached_col in eff_plot.columns:
                c = pd.to_numeric(eff_plot[cached_col], errors="coerce").fillna(0.0)
                fig.add_trace(go.Bar(
                    x=x, y=c, name="Cached",
                    yaxis="y2",
                    marker_color="rgba(255, 215, 0, 0.80)", opacity=0.85
                ))

            if aborted_col and aborted_col in eff_plot.columns:
                a = pd.to_numeric(eff_plot[aborted_col], errors="coerce").fillna(0.0)
                fig.add_trace(go.Bar(
                    x=x, y=a, name="Aborted",
                    yaxis="y2",
                    marker_color="rgba(255, 60, 60, 0.75)", opacity=0.85
                ))

        fig.update_layout(
            barmode="overlay",
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            hovermode="x unified",
            legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"),
            xaxis=dict(showgrid=False, tickformat="%H:%M", range=x_range),
            yaxis=dict(title="Queries/min", gridcolor="rgba(255,255,255,0.08)", zeroline=False),
            yaxis2=dict(title="Count", overlaying="y", side="right", gridcolor="rgba(255,255,255,0.05)", zeroline=False),
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=f"qe_g2_{refresh_count}")


with row2[0]:
    """
    The graph plot between Scanned vs spilled (MB) and also this graph has spill/scan ratio (right axis).
    """
    with st.container(border=True):
        st.markdown("### Data movement")
        st.caption("Scanned vs spilled (MB) + spill/scan ratio (right axis).")

        fig = go.Figure()
        if eff_plot is not None and not eff_plot.empty:
            x = eff_plot["__minute__"]

            if scan_col and scan_col in eff_plot.columns:
                s = pd.to_numeric(eff_plot[scan_col], errors="coerce").fillna(0.0)
                fig.add_trace(go.Scatter(
                    x=x, y=s, mode="lines",
                    name="Scanned (MB)",
                    line=dict(width=3, color="#34d399") 
                ))

            if spill_col and spill_col in eff_plot.columns:
                sp = pd.to_numeric(eff_plot[spill_col], errors="coerce").fillna(0.0)
                fig.add_trace(go.Scatter(
                    x=x, y=sp, mode="lines",
                    name="Spilled (MB)",
                    line=dict(width=3, color="#f97316", dash="dash")  
                ))

            if ratio_col and ratio_col in eff_plot.columns:
                r = pd.to_numeric(eff_plot[ratio_col], errors="coerce").fillna(0.0)
                fig.add_trace(go.Scatter(
                    x=x, y=r, mode="lines",
                    name="Spill/Scan ratio",
                    yaxis="y2",
                    line=dict(width=3, color="#22d3ee", dash="dot")  
                ))

        fig.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            hovermode="x unified",
            legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"),
            xaxis=dict(showgrid=False, tickformat="%H:%M", range=x_range),
            yaxis=dict(title="MB", gridcolor="rgba(255,255,255,0.08)", zeroline=False),
            yaxis2=dict(title="Ratio", overlaying="y", side="right", gridcolor="rgba(255,255,255,0.05)", zeroline=False),
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=f"qe_g3_{refresh_count}")


with row2[1]:
    """
    The graph plotted between Heavy units sum (left) + heavy unit avg (right), which Helps spot expensive minutes.
    """
    with st.container(border=True):
        st.markdown("### Compute pressure")
        st.caption("Heavy units sum (left) + heavy unit avg (right). Helps spot expensive minutes.")

        fig = go.Figure()
        if eff_plot is not None and not eff_plot.empty:
            x = eff_plot["__minute__"]

            if heavy_sum_col and heavy_sum_col in eff_plot.columns:
                hs = pd.to_numeric(eff_plot[heavy_sum_col], errors="coerce").fillna(0.0)
                fig.add_trace(go.Scatter(
                    x=x, y=hs, mode="lines",
                    name="Heavy units (sum)",
                    line=dict(width=3, color="#e879f9")  
                ))

            if heavy_avg_col and heavy_avg_col in eff_plot.columns:
                ha = pd.to_numeric(eff_plot[heavy_avg_col], errors="coerce").fillna(0.0)
                fig.add_trace(go.Scatter(
                    x=x, y=ha, mode="lines",
                    name="Heavy unit (avg)",
                    yaxis="y2",
                    line=dict(width=3, color="#facc15", dash="dot")  
                ))

        fig.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            hovermode="x unified",
            legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"),
            xaxis=dict(showgrid=False, tickformat="%H:%M", range=x_range),
            yaxis=dict(title="Heavy units (sum)", gridcolor="rgba(255,255,255,0.08)", zeroline=False),
            yaxis2=dict(title="Heavy unit (avg)", overlaying="y", side="right", gridcolor="rgba(255,255,255,0.05)", zeroline=False),
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=f"qe_g4_{refresh_count}")



