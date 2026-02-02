import streamlit as st

REDWATCH_CSS = """
<style>
.stApp {
  background: radial-gradient(1200px 800px at 10% 15%, rgba(255,0,120,0.12), transparent 55%),
              radial-gradient(1200px 800px at 85% 30%, rgba(120,80,255,0.14), transparent 60%),
              linear-gradient(180deg, #0a0b10 0%, #07080c 100%);
  color: #e9ecf1;
}
.block-container { padding-top: 1.2rem; }
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
section[data-testid="stSidebar"] {
  background: linear-gradient(180deg, rgba(12,12,18,1) 0%, rgba(6,6,10,1) 100%);
  border-right: 1px solid rgba(255,255,255,0.06);
}
section[data-testid="stSidebar"] > div { padding-top: 18px; }

.rw-side-logo { font-size: 22px; font-weight: 900; letter-spacing: 0.3px; margin: 4px 0 10px 0; }
.rw-side-sub { font-size: 12px; opacity: 0.72; margin-top: -6px; }
.rw-nav a { text-decoration: none !important; }
.rw-nav .rw-nav-btn {
  display: block;
  padding: 10px 12px;
  margin: 6px 0;
  border-radius: 14px;
  border: 1px solid rgba(255,255,255,0.07);
  background: rgba(255,255,255,0.03);
  color: rgba(233,236,241,0.92);
  font-weight: 700;
}
.rw-nav .rw-nav-btn:hover {
  background: rgba(255,0,120,0.10);
  border: 1px solid rgba(255,0,120,0.22);
}
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
"""

def apply_redwatch_theme():
    st.markdown(REDWATCH_CSS, unsafe_allow_html=True)

def render_sidebar(REFRESH_SECONDS: int, DEFAULT_WINDOW_MIN: int):
    with st.sidebar:
        st.markdown('<div class="rw-side-logo">RedWatch</div>', unsafe_allow_html=True)
        st.markdown('<div class="rw-side-sub">Real-time cost • workload • query intelligence</div>', unsafe_allow_html=True)
        st.markdown("<div style='height:14px;'></div>", unsafe_allow_html=True)
        st.markdown('<div class="rw-nav">', unsafe_allow_html=True)
        st.page_link("streamlit_app.py", label="🏠 Home")
        st.page_link("pages/1_Cost_Trends.py", label="📈 Cost Trends")
        st.page_link("pages/2_Leaderboard.py", label="🏆 Leaderboard")
        st.page_link("pages/3_Query_Efficiency.py", label="⚙️ Query Efficiency")
        st.page_link("pages/4_Cluster_Heat.py", label="🔥 Cluster Heat")
        st.markdown("</div>", unsafe_allow_html=True)
        st.divider()
        win = st.slider("Window (minutes)", 15, 240, st.session_state.get("window_minutes", DEFAULT_WINDOW_MIN))
        st.session_state["window_minutes"] = win
        st.caption(f"Auto-refresh: {REFRESH_SECONDS}s")
