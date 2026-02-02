import os
import pandas as pd
import streamlit as st
from datetime import datetime, timezone
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
import plotly.graph_objects as go
from src.db import read_df
from src import queries as Q

def safe_float(x, default=0.0):
    """
    Convert a value to float safely.
    Returns: Parsed float or default.
    """
    try:
        return float(x)
    except Exception:
        return default


def last_and_delta(df: pd.DataFrame, col: str):
    """
    Return the latest value and its change from the previous row.
    """
    if df is None or df.empty or col is None or col not in df.columns:
        return 0.0, None
    s = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    last = float(s.iloc[-1]) if len(s) else 0.0
    if len(s) > 1:
        return last, float(s.iloc[-1] - s.iloc[-2])
    return last, None


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    Pick the first matching column name from a list.
    Returns: The actual column name if found, else None.
    """
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def pick_time_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    Convenience wrapper for picking a timestamp column.
    Returns: The actual column name if found, else None.
    """
    return pick_col(df, candidates)


def _try_read(sql: str, params: dict | None, show_error: bool) -> pd.DataFrame:
    """
    Execute a SQL query via `read_df`, supporting different function signatures.
    Returns: DataFrame result.
    """
    try:
        if params is None:
            return read_df(sql, show_error=show_error)
        return read_df(sql, params, show_error=show_error)
    except TypeError:
        return read_df(sql)


def load_window(sql_builder, time_candidates: list[str], win: int, label: str):
    """
    Load a windowed KPI table by trying multiple timestamp column names.
    Returns a tuple:
          - df is the returned DataFrame.
          - used_tcol is the time column that worked.
    """
    last_err = None
    for tcol in time_candidates:
        try:
            sql = sql_builder(tcol, win)
            df = _try_read(sql, None, show_error=False)
            if not df.empty:
                used_tcol = pick_time_col(df, time_candidates)
                return df, used_tcol
        except Exception as e:
            last_err = e


    try:
        sql = sql_builder(time_candidates[0], win)
        df = _try_read(sql, None, show_error=True)
        used_tcol = pick_time_col(df, time_candidates) if not df.empty else None
        return df, used_tcol
    except Exception as e:
        st.error(f"[{label}] Query failed: {e}")
        if last_err:
            st.caption(f"Previous attempt error: {last_err}")
        return pd.DataFrame(), None

st.set_page_config(page_title="RedWatch", page_icon="⚡", layout="wide")


st.markdown(
    """
<style>

.main .block-container{
  padding-top: 2.2rem !important;
  padding-bottom: 3rem !important;
  padding-left: 2.8rem !important;
  padding-right: 2.8rem !important;
}


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
  padding: 10px 12px;
  text-align: right;
  min-width: 260px;
  box-shadow: 0 8px 28px rgba(0,0,0,0.35);
}
.rw-status .k{ font-size: 11px; opacity: .72; }
.rw-status .v{ font-size: 13px; font-weight: 900; margin-top: 4px; }

.rw-divider{
  border: none;
  height: 1px;
  background: rgba(255,255,255,0.10);
  margin: 14px 0 18px 0;
}

section[data-testid="stSidebar"] {
  background:
    radial-gradient(600px 600px at 15% 10%, rgba(255,0,120,0.18), transparent 55%),
    linear-gradient(180deg, #0a0b10 0%, #07080c 100%) !important;
  border-right: 1px solid rgba(255,255,255,0.08);
}
section[data-testid="stSidebar"] > div { padding-top: 0.9rem; }
section[data-testid="stSidebar"] * { color: #e9ecf1 !important; font-weight: 600; }

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

/* Sidebar top logo */
.rw-sidebar-top { margin-top: -0.2rem; margin-bottom: 12px; }
.rw-logo { font-size: 20px; font-weight: 900; letter-spacing: .4px; }
.rw-logo span {
  background: linear-gradient(135deg, #ff4d9d, #7c5cff);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}
.rw-sidebar-sub { font-size: 20px; opacity: .70; margin-top: 2px; margin-bottom: 10px; }
.rw-sidebar-divider { border:none; height:1px; background: rgba(255,255,255,0.12); margin: 10px 0 0 0; }

.stApp {
  background: radial-gradient(1200px 800px at 10% 15%, rgba(255,0,120,0.12), transparent 55%),
              radial-gradient(1200px 800px at 85% 30%, rgba(120,80,255,0.14), transparent 60%),
              linear-gradient(180deg, #0a0b10 0%, #07080c 100%);
  color: #e9ecf1;
}
.rw-header { display:flex; justify-content:space-between; align-items:flex-end; margin: 6px 0 12px 0;}

.rw-brand {
  font-size: 50px;
  font-weight: 1000;
  letter-spacing: 0.4px;
  background: linear-gradient(135deg, #ff4d9d, #7c5cff);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}

.rw-sub { font-size: 12px; opacity: .75; margin-top: -2px; }

.rw-card {
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 18px;
  padding: 16px;
  box-shadow: 0 8px 28px rgba(0,0,0,0.35);
}
.rw-card h3 { margin: 0 0 10px 0; font-size: 14px; font-weight: 800; opacity: .9; }
.rw-muted { opacity:.7; font-size:12px; }
.rw-big { font-size: 28px; font-weight: 900; margin-top: 4px; }
.rw-row { display:flex; gap:18px; align-items:flex-end; flex-wrap:wrap; }
.rw-kpi { min-width: 140px; }
.rw-kpi .lbl { font-size: 12px; opacity: .7; }
.rw-kpi .val { font-size: 18px; font-weight: 900; margin-top: 2px; }

.rw-pill { 
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 14px;
  padding: 12px 14px;
  margin-top: 10px;
}

.rw-pill .t { 
  font-size: 12px; 
  opacity: .70; 
  letter-spacing: .2px;
}

.rw-pill .v { 
  font-size: 14px; 
  font-weight: 900; 
  margin-top: 4px; 
  line-height: 1.25;
  word-break: break-word;
}

</style>
""",
    unsafe_allow_html=True,
)

REFRESH_SECONDS = 15 
WIN_MIN = 5         

refresh_count = st_autorefresh(interval=REFRESH_SECONDS * 1000, key="auto_refresh")
l, r = st.columns([0.72, 0.28], vertical_alignment="top")

with l:
    st.markdown(
        """
        <div class="rw-header">
          <div>
            <div class="rw-brand">RedWatch</div>
            <div class="rw-sidebar-sub rw-sidebar-top">Always Watching You 🔍 </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with r:
    last_refresh = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    st.markdown(
        f"""
        <div style="display:flex; justify-content:flex-end;">
          <div class="rw-status">
            <div class="v">Last refresh :{last_refresh}</div>
            <div class="v" style="margin-top:6px;">Auto-refresh:{REFRESH_SECONDS}s</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


time_candidates = ["minute_ts", "window_end", "ts", "timestamp", "arrival_minute", "minute"]
win = WIN_MIN

shadow_df, shadow_t = load_window(Q.shadow_cost_window, time_candidates, win, "shadow_cost_window")
eff_df, eff_t = load_window(Q.efficiency_window, time_candidates, win, "efficiency_window")
work_df, work_t = load_window(Q.cluster_workload_window, time_candidates, win, "cluster_workload_window")
conc_df, conc_t = load_window(Q.cluster_concurrency_window, time_candidates, win, "cluster_concurrency_window")

leader_df = _try_read(Q.leaderboard_15m(), None, show_error=False)
pred_df = _try_read(Q.predator_15m(), None, show_error=False)
C_NODE_USD_PER_MIN = float(os.getenv("C_NODE_USD_PER_MIN", "0.08"))  

hu_col = pick_col(shadow_df, ["heavy_units_sum"])
if not shadow_df.empty and hu_col:
    shadow_df["usd_per_min"] = (
        pd.to_numeric(shadow_df[hu_col], errors="coerce").fillna(0.0) * C_NODE_USD_PER_MIN
    )
else:
    shadow_df["usd_per_min"] = 0.0

spend_col = "usd_per_min" if (not shadow_df.empty and "usd_per_min" in shadow_df.columns) else "usd_per_min"
current_spend_per_min = 0.0
run_rate_per_day = 0.0
projected_24h = 0.0
projected_7d = 0.0
total_cost_window = 0.0
delta_pct = 0.0

if shadow_df is not None and (not shadow_df.empty) and spend_col in shadow_df.columns:
    s = pd.to_numeric(shadow_df[spend_col], errors="coerce").fillna(0.0)

    if len(s) > 0:
        current_spend_per_min = float(s.iloc[-1])
        total_cost_window = float(s.sum())

        window_minutes = float(win)
        avg_spend_per_min = (total_cost_window / window_minutes) if window_minutes > 0 else 0.0

        run_rate_per_day = avg_spend_per_min * 60.0 * 24.0
        projected_24h = run_rate_per_day
        projected_7d = run_rate_per_day * 7.0

        if len(s) > 1:
            prev = float(s.iloc[-2])
            delta_pct = ((current_spend_per_min - prev) / max(prev, 1e-9)) * 100.0

cid_col = pick_col(work_df, ["instance_id"])
wl_col = pick_col(work_df, ["workload", "workload_score", "query_pressure", "pressure", "cpu_pressure", "heavy_units_sum"])

max_workload = (
    float(pd.to_numeric(work_df[wl_col], errors="coerce").max())
    if (not work_df.empty and wl_col)
    else 0.0
)

hot_clusters = 0
if not work_df.empty and wl_col:
    wls = pd.to_numeric(work_df[wl_col], errors="coerce").fillna(0.0)
    thr = wls.quantile(0.80) if len(wls) > 5 else wls.max()
    hot_clusters = int((wls >= thr).sum())

dim_type_col = pick_col(leader_df, ["dimension_type"])
dim_val_col = pick_col(leader_df, ["dimension_value"])
rank_col = pick_col(leader_df, ["rank_position", "rank"])
heavy_col = pick_col(leader_df, ["heavy_units_sum", "heavy_sum", "heavy_units", "heavy"])
w_end_col = pick_col(leader_df, ["window_end", "minute_ts", "ts", "timestamp"])


def top_value_for_dimtype(dim_type_value: str, fallbacks: list[str] | None = None) -> str:
    """
    Return the top (rank 1) `dimension_value` for a given `dimension_type`, using the latest leaderboard window.
    Returns: The top value as a string, or "—" if unavailable.
    """
    if leader_df is None or leader_df.empty:
        return "—"
    if not (dim_type_col and dim_val_col and rank_col and w_end_col):
        return "—"

    tmp = leader_df.copy()
    tmp[w_end_col] = pd.to_datetime(tmp[w_end_col], utc=True, errors="coerce")
    tmp = tmp.dropna(subset=[w_end_col])
    if tmp.empty:
        return "—"

    latest_window_end = tmp[w_end_col].max()
    tmp = tmp[tmp[w_end_col] == latest_window_end].copy()

    tmp[dim_type_col] = tmp[dim_type_col].astype(str)
    tmp[rank_col] = pd.to_numeric(tmp[rank_col], errors="coerce")

    def _pick(dt: str) -> str:
        sub = tmp[tmp[dim_type_col] == str(dt)].copy()
        sub = sub.dropna(subset=[rank_col]).sort_values(rank_col, ascending=True)
        if sub.empty:
            return "—"
        return str(sub.iloc[0][dim_val_col])

    val = _pick(dim_type_value)
    if val != "—":
        return val
    for fb in (fallbacks or []):
        val = _pick(fb)
        if val != "—":
            return val
    return "—"

most_exp_query       = top_value_for_dimtype("query_type",   fallbacks=["read"])
most_exp_user        = top_value_for_dimtype("user",         fallbacks=["user_id", "instance_id"])
most_exp_instance_id = top_value_for_dimtype("instance_id",  fallbacks=["warehouse", "cluster_id"])
most_exp_access      = top_value_for_dimtype("access_scope")
most_exp_fingerprint = top_value_for_dimtype("fingerprint")
most_exp_workload    = top_value_for_dimtype("workload",     fallbacks=["workload_type"])


waste_col = pick_col(
    eff_df,
    ["waste_pct", "waste_percent", "waste_percentage", "spill_to_scan_ratio", "spilled_mb_sum"],
)
exec_col = pick_col(
    eff_df,
    ["avg_exec_s", "avg_exec_sec", "avg_exec_seconds", "avg_exec_ms", "avg_execution_ms", "exec_ms_avg"],
)
queue_col = pick_col(
    eff_df,
    ["avg_queue_s", "avg_queue_sec", "queue_wait_s", "queue_wait_seconds", "avg_queue_ms", "queue_wait_ms", "queue_ms_avg"],
)
scan_col = pick_col(
    eff_df,
    ["scanned_gb", "scan_gb", "scanned_mb", "scan_mb", "mbytes_scanned", "scanned_mb_sum"],
)
spilled_sum_col = pick_col(eff_df, ["spilled_mb_sum", "spill_mb_sum", "spilled_mb"])

avg_exec_s = 0.0
exec_delta = None
queue_s = 0.0
queue_delta = None
scanned = 0.0
scan_delta = None

if eff_df is not None and not eff_df.empty:
    if spilled_sum_col:
        eff_df["waste_sum"] = pd.to_numeric(eff_df[spilled_sum_col], errors="coerce").fillna(0.0)
    else:
        eff_df["waste_sum"] = 0.0

    if exec_col:
        v, d = last_and_delta(eff_df, exec_col)
        avg_exec_s = v / 1000.0 if "ms" in exec_col.lower() else v
        exec_delta = d / 1000.0 if (d is not None and "ms" in exec_col.lower()) else d

    if queue_col:
        v, d = last_and_delta(eff_df, queue_col)
        queue_s = v / 1000.0 if "ms" in queue_col.lower() else v
        queue_delta = d / 1000.0 if (d is not None and "ms" in queue_col.lower()) else d

    if scan_col:
        scanned, scan_delta = last_and_delta(eff_df, scan_col)

pred_total = 0.0
top_fp = "—"
top_fp_cost = 0.0
top_pred_user = "—"
     
pred_cost_col = pick_col(
    pred_df,
    ["pred_cost_7d", "predicted_cost_7d", "cost_7d", "next_7d_cost", "forecast_7d_cost"],
)
fp_col = pick_col(pred_df, ["feature_fingerprint", "fingerprint", "fp"])
pred_user_col = pick_col(pred_df, ["user", "user_id", "instance_id", "principal", "actor"])

if pred_df is not None and (not pred_df.empty) and pred_cost_col:
    ps = pred_df.copy()
    ps[pred_cost_col] = pd.to_numeric(ps[pred_cost_col], errors="coerce").fillna(0.0)

    pred_total = float(ps[pred_cost_col].sum())

    ps = ps.sort_values(pred_cost_col, ascending=False)

    if fp_col and fp_col in ps.columns:
        top_fp = str(ps[fp_col].iloc[0])

    top_fp_cost = float(ps[pred_cost_col].iloc[0])

    if pred_user_col and pred_user_col in ps.columns:
        top_pred_user = str(ps[pred_user_col].iloc[0])

else:
    if leader_df is not None and (not leader_df.empty):
        dim_type_col = pick_col(leader_df, ["dimension_type"])
        dim_val_col = pick_col(leader_df, ["dimension_value"])
        heavy_col = pick_col(leader_df, ["heavy_units_sum", "heavy_sum", "heavy_units", "heavy"])
        w_end_col = pick_col(leader_df, ["window_end", "minute_ts", "ts", "timestamp"])

        if dim_type_col and dim_val_col and heavy_col and w_end_col:
            tmp = leader_df.copy()
            tmp[w_end_col] = pd.to_datetime(tmp[w_end_col], utc=True, errors="coerce")
            tmp = tmp.dropna(subset=[w_end_col])

            if not tmp.empty:
                latest_w = tmp[w_end_col].max()
                tmp = tmp[tmp[w_end_col] == latest_w].copy()

                tmp[heavy_col] = pd.to_numeric(tmp[heavy_col], errors="coerce").fillna(0.0)
                tmp[dim_type_col] = tmp[dim_type_col].astype(str)
                tmp[dim_val_col] = tmp[dim_val_col].astype(str)

                fp_rows = tmp[tmp[dim_type_col].isin(["fingerprint", "feature_fingerprint"])].copy()
                if not fp_rows.empty:
                    fp_rows = fp_rows.sort_values(heavy_col, ascending=False)
                    top_fp = fp_rows[dim_val_col].iloc[0]
                    top_fp_cost = float(fp_rows[heavy_col].iloc[0])  
                user_rows = tmp[tmp[dim_type_col].isin(["user", "user_id", "instance_id"])].copy()
                if not user_rows.empty:
                    user_rows = user_rows.sort_values(heavy_col, ascending=False)
                    top_pred_user = user_rows[dim_val_col].iloc[0]

    
                pred_total = float(tmp[heavy_col].sum())

colA, colB, colC = st.columns([0.36, 0.44, 0.20], gap="large")

with colA:
    """
    Plotting graph for `$/min` with time and dollar value.
    """
    st.markdown('<div class="rw-card">', unsafe_allow_html=True)
    st.markdown("<h3>Shadow Costing</h3>", unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class="rw-row">
          <div class="rw-kpi">
            <div class="lbl">Cost pressure (run-rate)</div>
            <div class="rw-big">${current_spend_per_min:.3f} / min</div>
            <div class="rw-muted">{delta_pct:.2f}% vs prev min</div>
          </div>

          <div class="rw-kpi">
            <div class="lbl">Projected spend</div>
            <div class="val">${projected_24h:.3f} / 24h</div>
            <div class="val">${projected_7d:.3f} / 7d</div>
          </div>

          <div class="rw-kpi">
            <div class="lbl">Total cost (last {win} min)</div>
            <div class="val">${total_cost_window:.3f}</div>
            <div class="rw-muted">sum of $/min over window</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if (
        shadow_df is not None and not shadow_df.empty
        and shadow_t and shadow_t in shadow_df.columns
        and spend_col in shadow_df.columns
    ):
        plot_df = shadow_df[[shadow_t, spend_col]].copy()
        plot_df[shadow_t] = pd.to_datetime(plot_df[shadow_t], errors="coerce", utc=True)
        plot_df = plot_df.dropna(subset=[shadow_t]).sort_values(shadow_t)

        plot_df[spend_col] = pd.to_numeric(plot_df[spend_col], errors="coerce").fillna(0.0)

        if len(plot_df) >= 2:
            data_max = plot_df[shadow_t].max()
            data_min = data_max - pd.Timedelta(minutes=win)

            s = (
                plot_df.set_index(shadow_t)[spend_col]
                .sort_index()
                .astype(float)
            )
            smooth = s.resample("5S").mean().interpolate(method="time")
            smooth_df = smooth.reset_index().rename(columns={shadow_t: "t", spend_col: "y"})
            smooth_df = smooth_df[(smooth_df["t"] >= data_min) & (smooth_df["t"] <= data_max)]

            y_min = float(smooth_df["y"].min())
            y_max = float(smooth_df["y"].max())
            pad = (y_max - y_min) * 0.20
            if pad <= 0:
                pad = max(abs(y_max) * 0.15, 0.001)

            SMOOTH = 1.25 

            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=smooth_df["t"],
                    y=smooth_df["y"],
                    mode="lines",
                    line=dict(
                        width=14,
                        color="rgba(34,211,238,0.14)",
                        shape="spline",
                        smoothing=SMOOTH,
                    ),
                    hoverinfo="skip",
                    showlegend=False,
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=smooth_df["t"],
                    y=smooth_df["y"],
                    mode="lines",
                    name="Spend ($/min)",
                    line=dict(
                        width=4.5,
                        color="rgba(34,211,238,0.98)",
                        shape="spline",
                        smoothing=SMOOTH,
                    ),
                    hovertemplate="Time: %{x|%H:%M:%S}<br>Spend: $%{y:.6f}/min<extra></extra>",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=smooth_df["t"],
                    y=smooth_df["y"],
                    mode="lines",
                    line=dict(width=0, color="rgba(0,0,0,0)"),
                    fill="tozeroy",
                    fillcolor="rgba(34,211,238,0.08)",
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

            last_val = float(smooth_df["y"].iloc[-1])
            fig.add_hline(
                y=last_val,
                line_width=1,
                line_dash="dot",
                line_color="rgba(255,255,255,0.28)",
                annotation_text=f"Last: ${last_val:.6f}/min",
                annotation_position="top left",
                annotation_font=dict(size=11, color="rgba(255,255,255,0.75)"),
            )

            fig.update_layout(
                height=310,
                margin=dict(l=10, r=10, t=6, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                hovermode="x unified",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="left",
                    x=0.0,
                    font=dict(size=11),
                ),
            )

            fig.update_xaxes(
                showgrid=False,
                range=[data_min, data_max],
                tickformat="%H:%M:%S",
                showspikes=True,
                spikemode="across",
                spikesnap="cursor",
                spikedash="dot",
                spikecolor="rgba(255,255,255,0.22)",
            )

            fig.update_yaxes(
                title="$/min",
                range=[y_min - pad, y_max + pad],
                showgrid=True,
                gridcolor="rgba(255,255,255,0.08)",
                zeroline=False,
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"shadow_chart_smooth_{refresh_count}",
            )

        else:
            st.caption("Not enough spend data points yet (need at least 2).")
    else:
        st.caption("No spend trend available yet.")


with colB:
    """
    Plotting graph for Cluster heater Visualizer.
    """
    st.markdown('<div class="rw-card">', unsafe_allow_html=True)
    st.markdown("<h3>Cluster Heat Visualizer</h3>", unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class="rw-row">
          <div class="rw-kpi">
            <div class="lbl">Max workload</div>
            <div class="val">{max_workload:.1f}</div>
          </div>
          <div class="rw-kpi">
            <div class="lbl">Hot clusters</div>
            <div class="val">{hot_clusters}</div>
          </div>
        </div>
        <div class="rw-muted" style="margin-top:10px;">Hover a cell to see Instance ID + workload</div>
        """,
        unsafe_allow_html=True,
    )

    TOP_N = 12

    if not work_df.empty and work_t and cid_col and wl_col and work_t in work_df.columns:
        tmp = work_df[[work_t, cid_col, wl_col]].copy()

        tmp[work_t] = pd.to_datetime(tmp[work_t], errors="coerce", utc=True)
        tmp[wl_col] = pd.to_numeric(tmp[wl_col], errors="coerce")
        tmp = tmp.dropna(subset=[work_t, cid_col, wl_col])
        tmp[cid_col] = tmp[cid_col].astype(str)

        top_clusters = (
            tmp.groupby(cid_col)[wl_col]
            .mean()
            .sort_values(ascending=False)
            .head(TOP_N)
            .index
            .tolist()
        )
        tmp = tmp[tmp[cid_col].isin(top_clusters)].copy()

        heat_raw = (
            tmp.pivot_table(index=work_t, columns=cid_col, values=wl_col, aggfunc="mean")
            .sort_index()
            .fillna(0.0)
        )

        if heat_raw.empty:
            st.caption("No cluster workload data available yet.")
        else:
            z = heat_raw.values
            z_flat = z.flatten()
            z_flat = z_flat[~pd.isna(z_flat)]

            if len(z_flat) == 0:
                st.caption("No cluster workload data available yet.")
            else:
                zmin = float(pd.Series(z_flat).quantile(0.05))
                zmax = float(pd.Series(z_flat).quantile(0.95))
                if zmax <= zmin:
                    zmin = float(z_flat.min())
                    zmax = float(z_flat.max())

                heat_vis = heat_raw.clip(lower=zmin, upper=zmax).applymap(lambda v: v ** 0.5)

                fig = go.Figure(
                    data=go.Heatmap(
                        x=heat_vis.index,               
                        y=heat_vis.columns.astype(str),   
                        z=heat_vis.T.values,           
                        customdata=heat_raw.T.values,  
                        colorscale="Turbo",          
                        hovertemplate=(
                            "Instance ID: %{y}<br>"
                            "Time: %{x|%H:%M:%S}<br>"
                            "Workload: %{customdata:.3f}"
                            "<extra></extra>"
                        ),
                        colorbar=dict(title="workload", len=0.75),
                    )
                )

                fig.update_layout(
                    height=260,
                    margin=dict(l=0, r=0, t=10, b=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                )

                fig.update_xaxes(showgrid=False, tickfont=dict(size=9), tickformat="%H:%M:%S")
                fig.update_yaxes(showgrid=False, tickfont=dict(size=9), title="Instance ID", type="category")

                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    else:
        st.caption("No cluster workload data available yet.")

    st.markdown("</div>", unsafe_allow_html=True)

with colC:
    """
    Leaderboard Card Designs and Values.
    """
    st.markdown('<div class="rw-card">', unsafe_allow_html=True)
    st.markdown("<h3>Leaderboards</h3>", unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class="rw-pill"><div class="t">Most expensive query</div><div class="v">{most_exp_query}</div></div>
        <div class="rw-pill"><div class="t">Most expensive user</div><div class="v">{most_exp_user}</div></div>
        <div class="rw-pill"><div class="t">Most expensive access scope</div><div class="v">{most_exp_access}</div></div>
        <div class="rw-pill"><div class="t">Most expensive fingerprint</div><div class="v">{most_exp_fingerprint}</div></div>
        <div class="rw-pill"><div class="t">Most expensive workload</div><div class="v">{most_exp_workload}</div></div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("</div>", unsafe_allow_html=True)


colD, colE = st.columns([0.70, 0.30], gap="large")

with colD:
    """
    Query Efficiency Graph and cards.
    """
    st.markdown('<div class="rw-card">', unsafe_allow_html=True)
    st.markdown("<h3>Query Efficiency</h3>", unsafe_allow_html=True)

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        k1.metric("Avg exec (s)", f"{avg_exec_s:.2f}", delta=(None if exec_delta is None else f"{exec_delta:+.2f}"))
    with k2:
        waste_last, waste_delta = last_and_delta(eff_df, "waste_sum")
        k2.metric("Waste (MB)", f"{waste_last:,.2f}", delta=(None if waste_delta is None else f"{waste_delta:+,.2f}"))
    with k3:
        k3.metric("Queue wait (s)", f"{queue_s:.2f}", delta=(None if queue_delta is None else f"{queue_delta:+.2f}"))
    with k4:
        k4.metric("Scanned", f"{scanned:.2f}", delta=(None if scan_delta is None else f"{scan_delta:+.2f}"))

    if eff_df is not None and not eff_df.empty and eff_t and eff_t in eff_df.columns:
        plot_eff = eff_df.copy()
        plot_eff[eff_t] = pd.to_datetime(plot_eff[eff_t], errors="coerce", utc=True)
        plot_eff = plot_eff.dropna(subset=[eff_t]).sort_values(eff_t)

        fig = go.Figure()

        if exec_col and exec_col in plot_eff.columns:
            exec_s_series = pd.to_numeric(plot_eff[exec_col], errors="coerce").fillna(0.0)
            if "ms" in exec_col.lower():
                exec_s_series = exec_s_series / 1000.0
            fig.add_trace(go.Scatter(x=plot_eff[eff_t], y=exec_s_series, mode="lines", name="Avg Exec (s)", line=dict(width=2)))

        if queue_col and queue_col in plot_eff.columns:
            queue_s_series = pd.to_numeric(plot_eff[queue_col], errors="coerce").fillna(0.0)
            if "ms" in queue_col.lower():
                queue_s_series = queue_s_series / 1000.0
            fig.add_trace(go.Scatter(x=plot_eff[eff_t], y=queue_s_series, mode="lines", name="Queue Wait (s)", line=dict(width=2)))

        if waste_col and waste_col in plot_eff.columns:
            waste_series = pd.to_numeric(plot_eff[waste_col], errors="coerce").fillna(0.0)
            fig.add_trace(go.Scatter(x=plot_eff[eff_t], y=waste_series, mode="lines", name="Waste (%)", line=dict(width=2, dash="dash"), yaxis="y2"))

        fig.update_layout(
            height=300,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(showgrid=False, autorange=True, tickformat="%H:%M:%S"),
            yaxis=dict(title="Seconds", autorange=True, gridcolor="rgba(255,255,255,0.08)", zeroline=False),
            yaxis2=dict(title="Waste %", autorange=True, overlaying="y", side="right", gridcolor="rgba(255,255,255,0.05)", zeroline=False),
        )

        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    else:
        st.caption("No efficiency trend available yet.")

    st.markdown("</div>", unsafe_allow_html=True)


with colE:
    """
    Resource Predator Cards designs.
    """
    st.markdown('<div class="rw-card">', unsafe_allow_html=True)
    st.markdown("<h3>Resource Predator</h3>", unsafe_allow_html=True)

    using_forecast = (pred_df is not None and (not pred_df.empty) and pred_cost_col is not None)
    subtitle = "Next 7d predicted cost" if using_forecast else "Next 7d predicted cost"

    st.markdown(
        f"""
        <div class="rw-muted">{subtitle}</div>
        <div class="rw-big">${pred_total:.2f}</div>

        <div class="rw-pill">
          <div class="t">Top fingerprint</div>
          <div class="v">{top_fp}</div>
        </div>

        <div class="rw-pill" style="margin-top:10px;">
          <div class="t">Top user</div>
          <div class="v">{top_pred_user}</div>
        </div>

        <div class="rw-pill" style="margin-top:10px;">
          <div class="t">{'Pred cost (top fp)' if using_forecast else 'Heavy units (top fp proxy)'}</div>
          <div class="v">{top_fp_cost:.2f}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("</div>", unsafe_allow_html=True)
