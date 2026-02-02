import os
import pandas as pd
import streamlit as st
from datetime import datetime, timezone
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go

from src.db import read_df
from src import queries as Q


def pick_col_exact(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    Find an existing DataFrame column using case/whitespace-insensitive matching.
    Returns: The *actual* column name from `df.columns` that matches one of the
        candidates, otherwise None.
    """
    col_map = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in col_map:
            return col_map[key]
    return None


def _to_dt(s: pd.Series) -> pd.Series:
    """
    Convert a Series to timezone-aware UTC datetimes.
    Returns: A pandas datetime Series normalized to UTC. Invalid values become NaT.
    """
    return pd.to_datetime(s, utc=True, errors="coerce")


def _safe_float(x, default: float = 0.0) -> float:
    """
    Used for metric tiles and computations where occasional bad values
    should not crash the dashboard.
    Returns: Float representation of `x` or `default` on failure.
    """
    try:
        return float(x)
    except Exception:
        return default


def main() -> None:
    """
    Render the Cost Trends dashboard page with auto-refresh for every 15 seconds, and it compute for required KPI's. 
    """
  
    st.set_page_config(page_title="Cost Trends", page_icon="📈", layout="wide")
    st.markdown(
        """
    <style>
    section[data-testid="stSidebar"] > div {
      display: flex;
      flex-direction: column;
    }

    div[data-testid="stSidebarNav"] {
      order: 2;
      margin-top: 10px;
    }

    .rw-sidebar-top {
      order: 1;
      margin-top: 0.2rem;
      margin-bottom: 0.8rem;
    }

    section[data-testid="stSidebar"] {
      background:
        radial-gradient(600px 600px at 15% 10%, rgba(255,0,120,0.18), transparent 55%),
        linear-gradient(180deg, #0a0b10 0%, #07080c 100%) !important;
      border-right: 1px solid rgba(255,255,255,0.08);
    }
    section[data-testid="stSidebar"] * {
      color: #e9ecf1 !important;
    }

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
      background: linear-gradient(135deg, #ff4d9d, #7c5cff);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;

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

    .stApp {
      background: radial-gradient(1200px 800px at 12% 12%, rgba(255,0,120,0.12), transparent 55%),
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

    .rw-topbar{
      display:flex;
      align-items:flex-start;
      justify-content:space-between;
      gap:18px;
      margin: 6px 0 18px 0;
    }
    .rw-title{
      font-size: 50px;
      font-weight: 900;
      letter-spacing: 0.2px;
      line-height: 1.1;
      background: linear-gradient(135deg, #f43f5e 0%, #a78bfa 100%);
      -webkit-background-clip: text;
      background-clip: text;
      -webkit-text-fill-color: transparent;
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

    /* Make Streamlit bordered containers look like cards */
    div[data-testid="stVerticalBlockBorderWrapper"]{
      background: linear-gradient(180deg, rgba(255,255,255,0.07), rgba(255,255,255,0.035)) !important;
      border: 1px solid rgba(255,255,255,0.12) !important;
      border-radius: 20px !important;
      box-shadow: 0 14px 40px rgba(0,0,0,0.55),
                  inset 0 1px 0 rgba(255,255,255,0.06) !important;
      padding: 16px 16px 12px 16px !important;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )

    REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "5"))
    st_autorefresh(interval=REFRESH_SECONDS * 1000, key="auto_refresh")

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
        
    last_refresh = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    st.markdown(
        f"""
        <div class="rw-topbar">
          <div>
            <div class="rw-title"> Cost Trends</div>
          </div>

          <div class="rw-status">
            <div class="k">Last refreshed</div>
            <div class="v">{last_refresh}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    win = int(st.session_state.get("window_minutes", 60))

    probe = read_df(f"SELECT * FROM {Q.T('kpi_minute_shadow_cost')} LIMIT 1;", show_error=True)
    if probe.empty and len(probe.columns) == 0:
        st.error("Could not read kpi_minute_shadow_cost (table/view missing or permissions).")
        st.stop()

    tcol = pick_col_exact(probe, ["window_end", "window_start", "minute_ts", "ts", "timestamp"])
    if not tcol:
        st.error(f"Could not find a time column. Columns are: {list(probe.columns)}")
        st.stop()

    df = read_df(Q.shadow_cost_window(tcol, win), show_error=True)
    if df.empty:
        st.info(f"No rows returned for last {win} minutes using time column `{tcol}`.")
        st.caption("Tip: increase window or check whether the pipeline is writing fresh rows.")
        st.stop()

    df = read_df(Q.shadow_cost_window(tcol, win), show_error=True)
    queries_col = pick_col_exact(df, ["queries_count"])
    heavy_col   = pick_col_exact(df, ["heavy_units_sum"])
    total_col   = pick_col_exact(df, ["total_heavy_units"])
    share_col   = pick_col_exact(df, ["cost_share_pct"])

    USD_PER_HEAVY_UNIT = float(os.getenv("USD_PER_HEAVY_UNIT", "0.08"))
    if heavy_col:
        df["_usd_per_min"] = pd.to_numeric(df[heavy_col], errors="coerce").fillna(0.0) * USD_PER_HEAVY_UNIT
    else:
        df["_usd_per_min"] = 0.0

    df_plot = df.copy()
    df_plot[tcol] = _to_dt(df_plot[tcol])
    df_plot = df_plot.dropna(subset=[tcol]).sort_values(tcol)


    real_cost_col = pick_col_exact(
        df_plot,
        ["spend_usd_per_min", "usd_per_min", "cost_per_min", "shadow_cost_usd_per_min"]
    )
    cost_col = real_cost_col if real_cost_col else "_usd_per_min"
    df_plot[cost_col] = pd.to_numeric(df_plot[cost_col], errors="coerce").fillna(0.0)

    total_queries_all_time = 0
    total_queries_window = 0

    if queries_col:
        total_queries_window = int(pd.to_numeric(df[queries_col], errors="coerce").fillna(0).sum())
        base_table = Q.T("kpi_minute_shadow_cost")
        q_sql = f"SELECT COALESCE(SUM({queries_col}), 0) AS total_queries FROM {base_table};"
        q_tot = read_df(q_sql, show_error=False)
        if q_tot is not None and not q_tot.empty and "total_queries" in q_tot.columns:
            total_queries_all_time = int(pd.to_numeric(q_tot["total_queries"], errors="coerce").fillna(0).iloc[0])

    latest = df_plot.iloc[-1]
    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Queries (total)",
        f"{total_queries_all_time:,}",
        delta=(f"+{total_queries_window:,} in window" if queries_col else None)
    )
    c2.metric("$ / min", f"${_safe_float(latest[cost_col]):.4f}")
    c3.metric(
        "Heavy units",
        _safe_float(latest[heavy_col]) if heavy_col else (_safe_float(latest[total_col]) if total_col else 0.0)
    )

    st.divider()

    ts = df_plot[tcol].astype("datetime64[ns, UTC]")
    rates = df_plot[cost_col].astype(float)

    if len(df_plot) >= 2:
        s = pd.to_numeric(df_plot[cost_col], errors="coerce").fillna(0.0)
        dt_min = ts.diff().dt.total_seconds().div(60.0)
        dt_min.iloc[0] = dt_min.iloc[1] if pd.notna(dt_min.iloc[1]) else 1.0
        dt_min = dt_min.clip(lower=0.0, upper=float(win))

        total_cost_window = float(s.sum())
        window_minutes = float(dt_min.sum())
    else:
        total_cost_window = float(rates.iloc[-1]) if len(df_plot) else 0.0
        window_minutes = float(win)

    avg_rate = total_cost_window / max(window_minutes, 1e-9)
    projected_day = avg_rate * 1440.0

    k3, k4 = st.columns(2)
    k3.metric("Avg spend Rate / day", f"${avg_rate:,.2f}")
    k4.metric("Projected (per day)", f"${projected_day:,.2f}")

    st.divider()

    st.subheader("Cost pressure (run-rate)")

    d = df_plot[[tcol, cost_col]].copy()
    d[cost_col] = pd.to_numeric(d[cost_col], errors="coerce").fillna(0.0)
    d["ma_5"] = d[cost_col].rolling(window=5, min_periods=1).mean()

    if len(d) >= 15:
        roll = d[cost_col].rolling(window=15, min_periods=5)
        mu = roll.mean()
        sigma = roll.std().replace(0, pd.NA)
        z = ((d[cost_col] - mu) / sigma).fillna(0.0)
        d["severity"] = z.clip(lower=0)
        d["is_spike"] = d["severity"] >= 1.5
    else:
        d["severity"] = 0.0
        d["is_spike"] = False

    spikes = d[d["is_spike"]].copy()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=d[tcol], y=d[cost_col], mode="lines", name="Spend rate ($/min)",
        line=dict(width=2, color="#60A5FA"),
        hovertemplate="Time: %{x}<br>$/min: %{y:.6f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=d[tcol], y=d["ma_5"], mode="lines", name="5-min avg",
        line=dict(width=2, dash="dash"),
        hovertemplate="Time: %{x}<br>5m avg: %{y:.6f}<extra></extra>",
    ))
    if not spikes.empty:
        fig.add_trace(go.Scatter(
            x=spikes[tcol], y=spikes[cost_col], mode="markers", name="Spikes",
            marker=dict(size=9, color=spikes["severity"], colorscale="Turbo", showscale=False),
            hovertemplate="Time: %{x}<br>$/min: %{y:.6f}<br>severity: %{marker.color:.2f}<extra></extra>",
        ))

    fig.update_layout(
        height=330,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.10, xanchor="left", x=0.0),
        xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.08)", zeroline=False, title="Time"),
        yaxis=dict(title="$ / min", showgrid=True, gridcolor="rgba(255,255,255,0.08)", zeroline=False),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.divider()

    cA, cB = st.columns(2, gap="large")

    with cA:
        st.subheader("Queries over time")
        if queries_col:
            fig_q = go.Figure()
            fig_q.add_trace(go.Scatter(
                x=df_plot[tcol],
                y=pd.to_numeric(df_plot[queries_col], errors="coerce").fillna(0.0),
                mode="lines",
                name="Queries",
                line=dict(width=2),
            ))
            fig_q.update_layout(
                height=260,
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis_title="Time",
                yaxis_title="Queries",
            )
            st.plotly_chart(fig_q, use_container_width=True, config={"displayModeBar": False})
        else:
            st.caption("No queries column found.")

    with cB:
        st.subheader("Heavy units over time")
        if heavy_col:
            fig_h = go.Figure()
            fig_h.add_trace(go.Scatter(
                x=df_plot[tcol],
                y=pd.to_numeric(df_plot[heavy_col], errors="coerce").fillna(0.0),
                mode="lines",
                name="Heavy units",
                line=dict(width=2),
            ))
            fig_h.update_layout(
                height=260,
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis_title="Time",
                yaxis_title="Heavy units",
            )
            st.plotly_chart(fig_h, use_container_width=True, config={"displayModeBar": False})
        else:
            st.caption("No heavy units column found.")

    st.divider()
    
    cC, cD = st.columns(2, gap="large")

    with cC:
        st.subheader("Cost distribution ($/min)")
        vals = df_plot[cost_col].astype(float)

        fig_dist = go.Figure()
        fig_dist.add_trace(go.Box(
            y=vals,
            name="$ / min",
            boxpoints="outliers",
            jitter=0.35,
            pointpos=0,
        ))
        fig_dist.update_layout(
            height=260,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            yaxis_title="$ / min",
        )
        st.plotly_chart(fig_dist, use_container_width=True)

    with cD:
        st.subheader("Accumulated cost (window)")
        d2 = df_plot[[tcol, cost_col]].copy()

        ts2 = d2[tcol].astype("datetime64[ns, UTC]")
        dt2 = ts2.diff().dt.total_seconds().div(60.0)
        if len(d2) >= 2:
            dt2.iloc[0] = dt2.iloc[1] if pd.notna(dt2.iloc[1]) else 1.0
            dt2 = dt2.clip(lower=0.0, upper=float(win))
            d2["cum_cost"] = (d2[cost_col].astype(float) * dt2).cumsum()
        else:
            d2["cum_cost"] = d2[cost_col].astype(float)

        fig_cum = go.Figure()
        fig_cum.add_trace(go.Scatter(
            x=d2[tcol],
            y=d2["cum_cost"],
            mode="lines",
            fill="tozeroy",
            name="Cumulative cost",
            line=dict(width=3),
        ))
        fig_cum.update_layout(
            height=260,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Time",
            yaxis_title="$",
        )
        st.plotly_chart(fig_cum, use_container_width=True)

    st.divider()


if __name__ == "__main__":
    main()
