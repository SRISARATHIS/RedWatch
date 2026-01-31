import streamlit as st

def render_sidebar_brand():
    with st.sidebar:
        st.markdown("""
        <div class="rw-sidebar-top">
          <div class="rw-logo"><span>⚡ RedWatch</span></div>
          <div class="rw-sub">Real-time cost • workload • query intelligence</div>
          <hr class="rw-divider"/>
        </div>
        """, unsafe_allow_html=True)
