import streamlit as st

def redwatch_sidebar(active: str = "Home"):
    """
    active: one of ["Home","Cost Trends","Leaderboards","Efficiency","Cluster Heat"]
    """
    # ---------- CSS: Sidebar theme ----------
    st.markdown(
        """
        <style>
        /* Main app background stays as-is; this styles the sidebar */
        section[data-testid="stSidebar"] {
            background: radial-gradient(900px 600px at 25% 20%, rgba(255,0,120,0.14), transparent 55%),
                        radial-gradient(900px 600px at 85% 30%, rgba(120,80,255,0.16), transparent 60%),
                        linear-gradient(180deg, #0a0b10 0%, #07080c 100%);
            border-right: 1px solid rgba(255,255,255,0.08);
        }

        /* Sidebar padding */
        section[data-testid="stSidebar"] > div {
            padding-top: 14px;
            padding-left: 14px;
            padding-right: 14px;
        }

        /* Remove default "Pages" header spacing */
        [data-testid="stSidebarNav"] > ul {
            padding-top: 10px;
        }

        /* Style nav links */
        [data-testid="stSidebarNav"] a {
            border-radius: 14px;
            padding: 10px 12px;
            margin: 6px 0px;
            color: rgba(233, 236, 241, 0.85) !important;
            border: 1px solid rgba(255,255,255,0.08);
            background: rgba(255,255,255,0.03);
            font-weight: 800;
            text-decoration: none !important;
        }

        [data-testid="stSidebarNav"] a:hover {
            background: rgba(255,255,255,0.06);
            border-color: rgba(255,255,255,0.14);
            transform: translateY(-1px);
        }

        /* Attempt to visually emphasize current page (Streamlit adds aria-current) */
        [data-testid="stSidebarNav"] a[aria-current="page"] {
            background: rgba(255, 45, 125, 0.12);
            border: 1px solid rgba(255, 45, 125, 0.35);
            box-shadow: 0 0 18px rgba(255,45,125,0.18);
            color: rgba(255,255,255,0.95) !important;
        }

        /* Hide tiny default collapse icon spacing issues */
        button[title="Collapse sidebar"] {
            border-radius: 14px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ---------- Logo / header ----------
    with st.sidebar:
        st.markdown(
            """
            <div style="
                display:flex;
                align-items:center;
                gap:10px;
                padding: 10px 10px 6px 10px;
                border-radius: 18px;
                border: 1px solid rgba(255,255,255,0.08);
                background: rgba(255,255,255,0.03);
                box-shadow: 0 10px 30px rgba(0,0,0,0.35);
                ">
                <div style="
                    width:10px;height:10px;border-radius:999px;
                    background:#ff2d7d;
                    box-shadow:0 0 18px rgba(255,45,125,0.55);
                "></div>
                <div>
                    <div style="font-size:18px;font-weight:900;line-height:1;color:#e9ecf1;">RedWatch</div>
                    <div style="font-size:11px;opacity:.70;margin-top:3px;color:#e9ecf1;">
                        Real-time cost • workload • query intelligence
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)
