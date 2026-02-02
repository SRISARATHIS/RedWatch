import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

_ENGINE = None

def get_engine():
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    dsn = os.getenv("PG_DSN")
    if not dsn:
        # fallback to separate vars
        host = os.getenv("POSTGRES_HOST", "postgres")
        port = os.getenv("POSTGRES_PORT", "5432")
        db   = os.getenv("POSTGRES_DB", "redset_db")
        user = os.getenv("POSTGRES_USER", "redset")
        pw   = os.getenv("POSTGRES_PASSWORD", "redset")
        dsn = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"

    _ENGINE = create_engine(dsn, pool_pre_ping=True)
    return _ENGINE


def read_df(sql: str, params: dict | None = None, show_error: bool = True) -> pd.DataFrame:
    try:
        eng = get_engine()
        with eng.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params or {})
    except Exception as e:
        if show_error:
            st.error("Database query failed (see details).")
            st.code(sql, language="sql")
            st.exception(e)
        return pd.DataFrame()



        